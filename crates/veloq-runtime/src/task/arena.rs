use super::TaskJoinGate;
use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    marker::PhantomData,
    pin::Pin,
    ptr::{self, NonNull},
    sync::atomic::Ordering,
};
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_storage::{StateInt, StateLock, StateOptionPtr, Storage, ThreadSafeStorage};

/// 一个高性能的、块分配器接口。
pub trait Arena {
    /// # Safety
    /// The layout must be valid. If `drop_fn` is provided, it must be safe to call on the returned
    /// pointer. Only allocations created with `Some(drop_fn)` may later be passed to
    /// [`Arena::drop_object_raw`]; an allocation created with `None` has no reclaim metadata.
    unsafe fn alloc_raw(
        &self,
        layout: Layout,
        drop_fn: Option<unsafe fn(*mut u8)>,
    ) -> Option<NonNull<u8>>;
    /// # Safety
    /// `data_ptr` must be a pointer previously returned by `alloc_raw` with a non-`None` drop
    /// function, and `layout` must be identical to the allocation layout.
    unsafe fn drop_object_raw(&self, data_ptr: *mut u8, layout: Layout);
}

/// One-shot owner for an arena-backed task node.
///
/// A lease may only be created after the task header has published
/// `RECLAIMABLE`. Its destructor is the single fallback that invokes the node's
/// arena reclaim callback, so result extraction and error paths cannot leak or
/// reclaim the same node twice.
pub(crate) struct TaskLease<'scope_ref, 'arena, T, A: Arena> {
    arena: &'arena A,
    gate: &'scope_ref dyn TaskJoinGate<T>,
    reclaim: Option<unsafe fn(&A, &'scope_ref dyn TaskJoinGate<T>)>,
    marker: PhantomData<T>,
}

impl<'scope_ref, 'arena, T, A: Arena> TaskLease<'scope_ref, 'arena, T, A> {
    /// # Safety
    ///
    /// The caller must have observed `GenericTaskHeader::is_reclaimable()` and
    /// the callback must reclaim exactly the node represented by `gate`.
    pub(crate) unsafe fn new(
        arena: &'arena A,
        gate: &'scope_ref dyn TaskJoinGate<T>,
        reclaim: Option<unsafe fn(&A, &'scope_ref dyn TaskJoinGate<T>)>,
    ) -> Self {
        Self {
            arena,
            gate,
            reclaim,
            marker: PhantomData,
        }
    }
}

impl<T, A: Arena> Drop for TaskLease<'_, '_, T, A> {
    fn drop(&mut self) {
        if let Some(reclaim) = self.reclaim.take() {
            unsafe { reclaim(self.arena, self.gate) };
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
    data: NonNull<u8>,
    chunk: NonNull<u8>,
    size: usize,
    align: usize,
    has_drop_node: bool,
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
    /// 分配内存并记录其析构函数。
    /// # Safety
    /// The caller must ensure that `drop_fn` is valid.
    pub unsafe fn alloc<T>(
        &self,
        layout: Layout,
        drop_fn: Option<unsafe fn(*mut u8)>,
    ) -> Option<NonNull<u8>> {
        // 1. 如果有析构函数，需要额外分配 DropNode 空间
        let (total_layout, offset) = if drop_fn.is_some() {
            let node_layout = Layout::new::<GenericDropNode<S>>();
            match node_layout.extend(layout) {
                Ok(v) => v,
                Err(_) => return None,
            }
        } else {
            (layout, 0)
        };

        // 2. 尝试快速分配
        let mut res = self.try_alloc_fast(total_layout);

        // 3. 快速分配失败，进入慢速路径（分配新块）
        if res.is_none() {
            res = Some(self.alloc_slow(total_layout, drop_fn.is_some()));
        }

        let (ptr, chunk_ptr) = res?;

        // 4. 如果需要销毁，初始化 DropNode 并压入块内链表
        if let Some(drop_fn) = drop_fn {
            let node_ptr = ptr as *mut GenericDropNode<S>;
            let data_ptr = unsafe { ptr.add(offset) };

            unsafe {
                ptr::write(
                    node_ptr,
                    GenericDropNode {
                        link: Link::new(),
                        drop_fn: S::Usize::new(drop_fn as usize),
                        data_ptr,
                        chunk: chunk_ptr,
                    },
                );

                // 在锁保护下插入链表
                let mut drop_list = (*chunk_ptr).drop_list.lock();
                drop_list.push_front(Pin::new_unchecked(&mut *node_ptr));
            }
            NonNull::new(data_ptr)
        } else {
            NonNull::new(ptr)
        }
    }

    /// 手动触发对象的析构并将其完整分配块放入 free-list。
    /// # Safety
    /// The `data_ptr` must be valid and points to an object allocated by this arena.
    pub unsafe fn drop_object<T>(&self, data_ptr: *mut T, layout: Layout) {
        // 1. 计算 DropNode 的位置
        let node_layout = Layout::new::<GenericDropNode<S>>();
        let Ok((total_layout, offset)) = node_layout.extend(layout) else {
            return;
        };
        let node_ptr = unsafe { (data_ptr as *mut u8).sub(offset) as *mut GenericDropNode<S> };

        // 2. 执行析构
        let drop_fn_val = unsafe { (*node_ptr).drop_fn.fetch_and(0, Ordering::AcqRel) };
        let chunk_ptr = unsafe { (*node_ptr).chunk as *mut GenericChunk<S> };

        if drop_fn_val == 0 {
            return;
        }

        unsafe {
            let drop_fn = *(&drop_fn_val as *const usize as *const unsafe fn(*mut u8));
            (drop_fn)(data_ptr as *mut u8);
        }

        // 3. 从双向链表安全移除节点，并把完整分配块放入复用链表
        unsafe {
            let mut drop_list = (*chunk_ptr).drop_list.lock();
            let mut cursor = drop_list.cursor_mut_from_ptr(NonNull::new_unchecked(node_ptr));
            if cursor.get_raw().is_some() {
                cursor.remove();
            }
        }

        let mut free_list = self.free_list.lock();
        free_list.push(FreeBlock {
            data: unsafe { NonNull::new_unchecked(node_ptr as *mut u8) },
            chunk: unsafe { NonNull::new_unchecked(chunk_ptr as *mut u8) },
            size: total_layout.size(),
            align: total_layout.align(),
            has_drop_node: true,
        });
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
    fn alloc_slow(&self, layout: Layout, has_drop_node: bool) -> (*mut u8, *mut GenericChunk<S>) {
        let mut chunks = self.chunks.lock();

        if let Some(block) = self.take_free_block(layout, has_drop_node) {
            return (
                block.data.as_ptr(),
                block.chunk.as_ptr() as *mut GenericChunk<S>,
            );
        }

        // Double-check
        if let Some(current_active) = self.active_chunk.load(Ordering::Acquire) {
            let a_ref = unsafe { current_active.as_ref() };
            let p = a_ref.try_alloc(layout);
            if !p.is_null() {
                return (p, current_active.as_ptr());
            }
        }

        // 分配新块
        let required_size = layout
            .size()
            .checked_add(layout.align())
            .unwrap_or_else(|| handle_alloc_error(layout));
        let chunk_size = 8192.max(required_size);
        let new_chunk_layout = match Layout::from_size_align(chunk_size, 64) {
            Ok(layout) => layout,
            Err(_) => handle_alloc_error(layout),
        };
        let ptr = unsafe { alloc(new_chunk_layout) };
        if ptr.is_null() {
            handle_alloc_error(new_chunk_layout);
        }

        let new_chunk = Box::new(GenericChunk {
            link: Link::new(),
            ptr: NonNull::new(ptr).unwrap_or_else(|| handle_alloc_error(new_chunk_layout)),
            layout: new_chunk_layout,
            used: S::Usize::new(0),
            drop_list: S::Lock::new(LinkedList::new(DropAdapter::<S>::new())),
        });

        let chunk_ptr: *mut GenericChunk<S> = Box::into_raw(new_chunk);
        let allocated_ptr = unsafe { (*chunk_ptr).try_alloc(layout) };

        unsafe {
            chunks.push_back(Pin::new_unchecked(&mut *chunk_ptr));
        }

        self.active_chunk
            .swap(Some(NonNull::new(chunk_ptr).unwrap()), Ordering::AcqRel);

        (allocated_ptr, chunk_ptr)
    }

    fn take_free_block(&self, layout: Layout, has_drop_node: bool) -> Option<FreeBlock> {
        let mut free_list = self.free_list.lock();
        let index = free_list.iter().position(|block| {
            block.size == layout.size()
                && block.align == layout.align()
                && block.has_drop_node == has_drop_node
        })?;
        Some(free_list.swap_remove(index))
    }
}

impl<S: Storage> GenericChunk<S> {
    fn try_alloc(&self, layout: Layout) -> *mut u8 {
        let align = layout.align();
        let size = layout.size();
        let mask = align - 1;

        let base_addr = self.ptr.as_ptr() as usize;
        let mut current_used = self.used.load(Ordering::Acquire);
        loop {
            let current_ptr = match base_addr.checked_add(current_used) {
                Some(addr) => addr,
                None => return ptr::null_mut(),
            };
            let aligned_ptr = (current_ptr + mask) & !mask;
            let offset = aligned_ptr - base_addr;
            let new_used = offset + size;

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
    #[inline]
    unsafe fn alloc_raw(
        &self,
        layout: Layout,
        drop_fn: Option<unsafe fn(*mut u8)>,
    ) -> Option<NonNull<u8>> {
        unsafe { self.alloc::<()>(layout, drop_fn) }
    }

    #[inline]
    unsafe fn drop_object_raw(&self, data_ptr: *mut u8, layout: Layout) {
        unsafe { self.drop_object::<()>(data_ptr as *mut (), layout) }
    }
}

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
    use super::{Arena, GenericArena};
    use std::{
        alloc::Layout,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use veloq_storage::AtomicStorage;

    static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

    unsafe fn count_drop(_ptr: *mut u8) {
        DROP_COUNT.fetch_add(1, Ordering::Relaxed);
    }

    #[test]
    fn reuses_freed_block_after_active_chunk_is_full() {
        DROP_COUNT.store(0, Ordering::Relaxed);
        let arena = GenericArena::<AtomicStorage>::new();
        let layout = Layout::from_size_align(9000, 8).unwrap();

        let first = unsafe { arena.alloc_raw(layout, Some(count_drop)) }.unwrap();
        unsafe { arena.drop_object_raw(first.as_ptr(), layout) };

        let second = unsafe { arena.alloc_raw(layout, Some(count_drop)) }.unwrap();
        assert_eq!(first, second);
        unsafe { arena.drop_object_raw(second.as_ptr(), layout) };

        assert_eq!(DROP_COUNT.load(Ordering::Relaxed), 2);
    }
}
