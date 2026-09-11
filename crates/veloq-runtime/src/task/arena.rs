use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    alloc::{Layout, alloc, dealloc},
    boxed::Box,
    marker::PhantomData,
    num::NonZeroUsize,
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::Ordering,
    vec::Vec,
};
use veloq_storage::{StateInt, StateLock, StateOptionPtr, Storage, ThreadSafeStorage};

/// 一个高性能的、块分配器接口。
pub trait Arena {
    type Allocation<'arena>: ArenaAllocation
    where
        Self: 'arena;

    /// # Safety
    /// `layout` must describe the initialized object that will be stored in the returned slot,
    /// and `drop_fn` must be safe to call with the slot's data pointer after that object has been
    /// initialized. The returned token must not be reclaimed before initialization is complete.
    unsafe fn alloc_managed<'arena>(
        &'arena self,
        layout: Layout,
        drop_fn: unsafe fn(*mut u8),
    ) -> Option<Self::Allocation<'arena>>;
}

/// 不可复制的 arena 回收令牌。
///
/// 令牌由带 `DropNode` 的分配产生，并且只能按值消费一次。普通字节分配不产生此令牌，
/// 因而不能被传给对象回收路径。
pub trait ArenaAllocation {
    fn data_ptr(&self) -> NonNull<u8>;

    /// # Safety
    /// The object at [`Self::data_ptr`] must have been initialized with the type described by the
    /// allocation's drop function. Consuming a token without initializing its object is invalid.
    unsafe fn reclaim(self);
}

/// One-shot owner for an arena-backed task node.
///
/// A lease may only be created after the task header has published
/// `RECLAIMABLE`. Its destructor is the single fallback that consumes the
/// managed allocation, so result extraction and error paths cannot leak or
/// reclaim the same node twice.
pub(crate) struct TaskLease<A: ArenaAllocation> {
    allocation: Option<A>,
}

impl<A: ArenaAllocation> TaskLease<A> {
    pub(crate) fn new(allocation: Option<A>) -> Self {
        Self { allocation }
    }
}

impl<A: ArenaAllocation> Drop for TaskLease<A> {
    fn drop(&mut self) {
        if let Some(allocation) = self.allocation.take() {
            unsafe { allocation.reclaim() };
        }
    }
}

/// 通用的块分配器，通过 Storage 策略支持线程安全或本地分配。
///
/// Chunk 的所有权持续到 arena 析构；对象释放后，完整分配块会进入 free-list 供后续
/// 同布局分配复用。
pub struct GenericArena<S: Storage> {
    // 活跃块，支持无锁快速路径分配
    active_chunk: S::OptionPtr<GenericChunk<S>>,
    // 所有块的拥有者，使用侵入式链表管理
    chunks: S::Lock<LinkedList<ChunkAdapter<S>>>,
    // 慢速路径复用已经释放的同布局对象
    free_list: S::Lock<Vec<FreeBlock>>,
}

pub(crate) struct GenericChunk<S: Storage> {
    link: Link, // 用于 Arena 的 chunks 链表
    ptr: NonNull<u8>,
    layout: Layout,
    // 该块已使用的字节数
    used: S::Usize,
    // 该块拥有的析构函数链表，采用双向链表结构，并在锁保护下操作
    drop_list: S::Lock<LinkedList<DropAdapter<S>>>,
}

struct FreeBlock {
    ptr: NonNull<u8>,
    chunk: NonNull<u8>,
    layout: Layout,
}

unsafe impl Send for FreeBlock {}
unsafe impl Sync for FreeBlock {}

pub(crate) struct GenericDropNode<S: Storage> {
    link: Link,
    data_ptr: *mut u8, // 重排字段以优化对齐
    drop_fn: S::Usize,
    // 所属的 Chunk，用于回收
    chunk: *const GenericChunk<S>,
}

intrusive_adapter!(pub(crate) ChunkAdapter<S> = GenericChunk<S> { link: Link } where S: Storage);
intrusive_adapter!(pub(crate) DropAdapter<S> = GenericDropNode<S> { link: Link } where S: Storage);

impl<S: Storage> GenericArena<S> {
    pub fn new() -> Self {
        Self {
            active_chunk: S::OptionPtr::new(None),
            chunks: S::Lock::new(LinkedList::new(ChunkAdapter::<S>::new())),
            free_list: S::Lock::new(Vec::new()),
        }
    }
}

impl<S: Storage> Default for GenericArena<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S: Storage> GenericArena<S> {
    /// 分配一个带 `DropNode` 的未初始化对象槽位。
    /// # Safety
    /// `layout` must be the layout of the object written to the returned data pointer, and
    /// `drop_fn` must be valid for that initialized object.
    pub unsafe fn alloc_managed<'arena>(
        &'arena self,
        layout: Layout,
        drop_fn: unsafe fn(*mut u8),
    ) -> Option<ManagedAllocation<'arena, S>>
    where
        S: 'arena,
    {
        let node_layout = Layout::new::<GenericDropNode<S>>();
        let (total_layout, offset) = node_layout.extend(layout).ok()?;

        let (ptr, chunk_ptr) = self
            .try_alloc_fast(total_layout)
            .or_else(|| self.alloc_slow(total_layout, true))?;
        let node_ptr = ptr as *mut GenericDropNode<S>;
        let data_ptr = unsafe { ptr.add(offset) };

        unsafe {
            ptr::write(
                node_ptr,
                GenericDropNode {
                    link: Link::new(),
                    data_ptr,
                    drop_fn: S::Usize::new(drop_fn as usize),
                    chunk: chunk_ptr,
                },
            );

            let mut drop_list = (*chunk_ptr).drop_list.lock();
            drop_list.push_front(Pin::new_unchecked(&mut *node_ptr));

            Some(ManagedAllocation {
                data_ptr: NonNull::new_unchecked(data_ptr),
                node: NonNull::new_unchecked(node_ptr),
                layout: total_layout,
                arena: self,
                marker: PhantomData,
            })
        }
    }

    /// 分配不会进入对象回收 free-list 的原始字节空间。
    ///
    /// 该接口不创建 `DropNode`，返回的指针不能转换为 [`ArenaAllocation`]，并且其存储会
    /// 一直保留到 arena 析构。零大小布局返回满足其对齐要求的无 provenance 悬空指针，
    /// 不会触碰底层分配器。
    pub fn alloc_bytes(&self, layout: Layout) -> Option<NonNull<u8>> {
        if layout.size() == 0 {
            let align = NonZeroUsize::new(layout.align()).expect("Layout alignment is non-zero");
            return Some(NonNull::without_provenance(align));
        }

        self.try_alloc_fast(layout)
            .or_else(|| self.alloc_slow(layout, false))
            .map(|(ptr, _)| unsafe { NonNull::new_unchecked(ptr) })
    }

    fn try_alloc_fast(&self, layout: Layout) -> Option<(*mut u8, *mut GenericChunk<S>)> {
        let chunk_ptr = self.active_chunk.load(Ordering::Acquire)?;
        let chunk = unsafe { chunk_ptr.as_ref() };
        let data_ptr = chunk.try_alloc(layout);
        if !data_ptr.is_null() {
            return Some((data_ptr, chunk_ptr.as_ptr()));
        }
        None
    }

    #[inline(never)]
    fn alloc_slow(
        &self,
        layout: Layout,
        reuse_managed_block: bool,
    ) -> Option<(*mut u8, *mut GenericChunk<S>)> {
        let mut chunks = self.chunks.lock();

        if reuse_managed_block && let Some(block) = self.take_free_block(layout) {
            return Some((
                block.ptr.as_ptr(),
                block.chunk.as_ptr() as *mut GenericChunk<S>,
            ));
        }

        // Double-check
        if let Some(current_active) = self.active_chunk.load(Ordering::Acquire) {
            let a_ref = unsafe { current_active.as_ref() };
            let p = a_ref.try_alloc(layout);
            if !p.is_null() {
                return Some((p, current_active.as_ptr()));
            }
        }

        // 分配新块
        let required_size = layout.size().checked_add(layout.align())?;
        let chunk_size = 8192.max(required_size);
        let new_chunk_layout = Layout::from_size_align(chunk_size, 64).ok()?;
        let ptr = unsafe { alloc(new_chunk_layout) };
        let ptr = NonNull::new(ptr)?;

        let new_chunk = Box::new(GenericChunk {
            link: Link::new(),
            ptr,
            layout: new_chunk_layout,
            used: S::Usize::new(0),
            drop_list: S::Lock::new(LinkedList::new(DropAdapter::<S>::new())),
        });

        let chunk_ptr: *mut GenericChunk<S> = Box::into_raw(new_chunk);
        let allocated_ptr = unsafe { (*chunk_ptr).try_alloc(layout) };
        if allocated_ptr.is_null() {
            unsafe {
                drop(Box::from_raw(chunk_ptr));
                dealloc(ptr.as_ptr(), new_chunk_layout);
            }
            return None;
        }

        unsafe {
            chunks.push_back(Pin::new_unchecked(&mut *chunk_ptr));
        }

        self.active_chunk
            .swap(Some(NonNull::new(chunk_ptr).unwrap()), Ordering::AcqRel);

        Some((allocated_ptr, chunk_ptr))
    }

    fn take_free_block(&self, layout: Layout) -> Option<FreeBlock> {
        let mut free_list = self.free_list.lock();
        let index = free_list.iter().position(|block| block.layout == layout)?;
        Some(free_list.swap_remove(index))
    }
}

impl<S: Storage> GenericChunk<S> {
    fn try_alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();
        let Some(mask) = align.checked_sub(1) else {
            return ptr::null_mut();
        };

        let base_addr = self.ptr.as_ptr() as usize;
        let mut current_used = self.used.load(Ordering::Acquire);
        loop {
            let current_ptr = match base_addr.checked_add(current_used) {
                Some(addr) => addr,
                None => return ptr::null_mut(),
            };
            let aligned_ptr = match current_ptr.checked_add(mask) {
                Some(addr) => addr & !mask,
                None => return ptr::null_mut(),
            };
            let offset = match aligned_ptr.checked_sub(base_addr) {
                Some(offset) => offset,
                None => return ptr::null_mut(),
            };
            let new_used = match offset.checked_add(size) {
                Some(used) => used,
                None => return ptr::null_mut(),
            };

            if new_used <= self.layout.size() {
                match self.used.compare_exchange_weak(
                    current_used,
                    new_used,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return aligned_ptr as *mut u8,
                    Err(actual) => current_used = actual,
                }
            } else {
                return ptr::null_mut();
            }
        }
    }
}

impl<S: Storage> Drop for GenericArena<S> {
    fn drop(&mut self) {
        self.free_list.lock().clear();
        let mut chunks = self.chunks.lock();
        while let Some(chunk_pin) = chunks.pop_front() {
            unsafe {
                let chunk_ptr = chunk_pin.get_unchecked_mut() as *mut GenericChunk<S>;
                let chunk = Box::from_raw(chunk_ptr);

                // 遍历双向链表并析构所有存活节点
                let mut drop_list = chunk.drop_list.lock();
                while let Some(node_pin) = drop_list.pop_front() {
                    let node = node_pin.get_unchecked_mut();
                    let drop_fn_val = node.drop_fn.fetch_and(0, Ordering::AcqRel);
                    if drop_fn_val != 0 {
                        let drop_fn = *(&drop_fn_val as *const usize as *const unsafe fn(*mut u8));
                        (drop_fn)(node.data_ptr);
                    }
                }
                drop(drop_list);

                dealloc(chunk.ptr.as_ptr(), chunk.layout);
            }
        }
    }
}

impl<S: Storage> Arena for GenericArena<S> {
    type Allocation<'arena>
        = ManagedAllocation<'arena, S>
    where
        Self: 'arena;

    #[inline]
    unsafe fn alloc_managed<'arena>(
        &'arena self,
        layout: Layout,
        drop_fn: unsafe fn(*mut u8),
    ) -> Option<Self::Allocation<'arena>> {
        unsafe { GenericArena::alloc_managed(self, layout, drop_fn) }
    }
}

/// `alloc_managed` 返回的、带有 arena 生命周期的回收令牌。
pub struct ManagedAllocation<'arena, S: Storage> {
    data_ptr: NonNull<u8>,
    node: NonNull<GenericDropNode<S>>,
    layout: Layout,
    arena: &'arena GenericArena<S>,
    marker: PhantomData<&'arena S>,
}

impl<S: Storage> ArenaAllocation for ManagedAllocation<'_, S> {
    fn data_ptr(&self) -> NonNull<u8> {
        self.data_ptr
    }

    unsafe fn reclaim(self) {
        let Self {
            data_ptr,
            node,
            layout,
            arena,
            marker: _,
        } = self;

        let drop_fn_val = unsafe { node.as_ref().drop_fn.fetch_and(0, Ordering::AcqRel) };
        if drop_fn_val == 0 {
            return;
        }

        let drop_fn = unsafe { *(&drop_fn_val as *const usize as *const unsafe fn(*mut u8)) };
        unsafe { drop_fn(data_ptr.as_ptr()) };

        let chunk_ptr = unsafe { node.as_ref().chunk as *mut GenericChunk<S> };
        unsafe {
            let mut drop_list = (*chunk_ptr).drop_list.lock();
            let mut cursor = drop_list.cursor_mut_from_ptr(node);
            debug_assert!(cursor.get_raw().is_some());
            if cursor.get_raw().is_some() {
                cursor.remove();
            }
        }

        let mut free_list = arena.free_list.lock();
        free_list.push(FreeBlock {
            ptr: node.cast(),
            chunk: unsafe { NonNull::new_unchecked(chunk_ptr as *mut u8) },
            layout,
        });
    }
}

unsafe impl<S: ThreadSafeStorage> Send for ManagedAllocation<'_, S> {}
unsafe impl<S: ThreadSafeStorage> Sync for ManagedAllocation<'_, S> {}

// 安全性：GenericArena 的 Send/Sync 性质取决于 Storage 的实现
unsafe impl<S: ThreadSafeStorage> Send for GenericArena<S>
where
    S::OptionPtr<GenericChunk<S>>: Send,
    S::Lock<LinkedList<ChunkAdapter<S>>>: Send,
    S::Lock<Vec<FreeBlock>>: Send,
{
}
unsafe impl<S: ThreadSafeStorage> Sync for GenericArena<S>
where
    S::OptionPtr<GenericChunk<S>>: Sync,
    S::Lock<LinkedList<ChunkAdapter<S>>>: Sync,
    S::Lock<Vec<FreeBlock>>: Sync,
{
}

unsafe impl<S: ThreadSafeStorage> Send for GenericChunk<S> where
    S::Lock<LinkedList<DropAdapter<S>>>: Send
{
}
unsafe impl<S: ThreadSafeStorage> Sync for GenericChunk<S> where
    S::Lock<LinkedList<DropAdapter<S>>>: Sync
{
}

#[cfg(test)]
mod tests {
    use super::{ArenaAllocation, DropAdapter, GenericArena, GenericChunk};
    use veloq_intrusive_linklist::LinkedList;
    use veloq_std::{
        alloc::Layout,
        num::NonZeroUsize,
        ptr::{self, NonNull},
        sync::atomic::{NativeAtomicUsize as AtomicUsize, Ordering},
    };
    use veloq_storage::{AtomicStorage, StateLock, Storage};

    #[repr(C, align(8))]
    struct Large([u8; 9000]);

    #[repr(align(256))]
    struct HighlyAligned {
        _value: u8,
    }

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    unsafe fn count_drop(_ptr: *mut u8) {
        DROP_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    #[test]
    fn reuses_freed_block_after_active_chunk_is_full() {
        DROP_COUNT.store(0, Ordering::Relaxed);
        let arena = GenericArena::<AtomicStorage>::new();
        let layout = Layout::from_size_align(9000, 8).unwrap();

        let first = unsafe { arena.alloc_managed(layout, count_drop) }.unwrap();
        let first_ptr = first.data_ptr();
        unsafe { ptr::write(first_ptr.as_ptr() as *mut Large, Large([0; 9000])) };
        unsafe { first.reclaim() };

        let second = unsafe { arena.alloc_managed(layout, count_drop) }.unwrap();
        assert_eq!(first_ptr, second.data_ptr());
        unsafe { ptr::write(second.data_ptr().as_ptr() as *mut Large, Large([0; 9000])) };
        unsafe { second.reclaim() };

        assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn arena_reclaims_live_managed_allocation_on_drop() {
        DROP_COUNT.store(0, Ordering::Relaxed);
        {
            let arena = GenericArena::<AtomicStorage>::new();
            let allocation =
                unsafe { arena.alloc_managed(Layout::new::<u64>(), count_drop) }.unwrap();
            unsafe { ptr::write(allocation.data_ptr().as_ptr() as *mut u64, 0) };
            assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 0);
        }
        assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn byte_allocation_does_not_enter_managed_free_list() {
        let arena = GenericArena::<AtomicStorage>::new();
        let layout = Layout::from_size_align(9000, 8).unwrap();
        let first = unsafe { arena.alloc_managed(layout, count_drop) }.unwrap();
        let first_ptr = first.data_ptr();
        unsafe { ptr::write(first_ptr.as_ptr() as *mut Large, Large([0; 9000])) };
        unsafe { first.reclaim() };

        let bytes = arena.alloc_bytes(layout).unwrap();
        assert_ne!(bytes, first_ptr);

        let managed = unsafe { arena.alloc_managed(layout, count_drop) }.unwrap();
        assert_eq!(first_ptr, managed.data_ptr());
        unsafe { ptr::write(managed.data_ptr().as_ptr() as *mut Large, Large([0; 9000])) };
        unsafe { managed.reclaim() };
    }

    #[test]
    fn managed_allocation_supports_zero_and_high_alignment() {
        let arena = GenericArena::<AtomicStorage>::new();

        let zero = unsafe { arena.alloc_managed(Layout::new::<()>(), count_drop) }.unwrap();
        unsafe { ptr::write(zero.data_ptr().as_ptr() as *mut (), ()) };
        unsafe { zero.reclaim() };

        let aligned_layout = Layout::new::<HighlyAligned>();
        let aligned = unsafe { arena.alloc_managed(aligned_layout, count_drop) }.unwrap();
        assert_eq!(aligned.data_ptr().as_ptr() as usize % 256, 0);
        unsafe {
            ptr::write(
                aligned.data_ptr().as_ptr() as *mut HighlyAligned,
                HighlyAligned { _value: 0 },
            );
            aligned.reclaim();
        }

        let bytes = arena
            .alloc_bytes(Layout::from_size_align(0, 256).unwrap())
            .unwrap();
        assert_eq!(bytes.as_ptr() as usize % 256, 0);
    }

    #[test]
    fn layout_extension_failure_returns_none() {
        let arena = GenericArena::<AtomicStorage>::new();
        let oversized = Layout::from_size_align(isize::MAX as usize, 1).unwrap();
        assert!(unsafe { arena.alloc_managed(oversized, count_drop) }.is_none());
    }

    #[test]
    fn try_alloc_rejects_address_arithmetic_overflow() {
        let layout = Layout::from_size_align(8, 8).unwrap();
        let base_overflow = GenericChunk::<AtomicStorage> {
            link: veloq_intrusive_linklist::Link::new(),
            ptr: NonNull::without_provenance(NonZeroUsize::new(0x1000).unwrap()),
            layout,
            used: <AtomicStorage as Storage>::Usize::new(usize::MAX),
            drop_list: <AtomicStorage as Storage>::Lock::new(LinkedList::new(DropAdapter::<
                AtomicStorage,
            >::new())),
        };
        assert!(base_overflow.try_alloc(layout).is_null());

        let alignment_overflow = GenericChunk::<AtomicStorage> {
            link: veloq_intrusive_linklist::Link::new(),
            ptr: NonNull::without_provenance(NonZeroUsize::new(usize::MAX).unwrap()),
            layout,
            used: <AtomicStorage as Storage>::Usize::new(0),
            drop_list: <AtomicStorage as Storage>::Lock::new(LinkedList::new(DropAdapter::<
                AtomicStorage,
            >::new())),
        };
        assert!(alignment_overflow.try_alloc(layout).is_null());
    }
}
