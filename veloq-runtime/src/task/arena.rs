use crate::utils::storage::{
    StateGuard, StateInt, StateLock, StateOptionPtr, Storage, ThreadSafeStorage,
};
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

/// 一个高性能的、块分配器接口。
pub trait Arena {
    /// # Safety
    /// The layout must be valid. If `drop_fn` is provided, it must be safe to call on the returned pointer.
    unsafe fn alloc_raw(&self, layout: Layout, drop_fn: Option<unsafe fn(*mut u8)>) -> *mut u8;
    /// # Safety
    /// `data_ptr` must be a pointer previously returned by `alloc_raw`.
    unsafe fn drop_object_raw(&self, data_ptr: *mut u8, layout: Layout);
}

/// 通用的块分配器，通过 Storage 策略支持线程安全或本地分配。
pub struct GenericArena<S: Storage> {
    // 活跃块，支持快速路径分配
    active_chunk: S::OptionPtr<GenericChunk<S>>,
    // 所有块的拥有者，使用侵入式链表管理
    chunks: S::Lock<LinkedList<ChunkAdapter<S>>>,
}

pub(crate) struct GenericChunk<S: Storage> {
    link: Link, // 用于 Arena 的 chunks 链表
    ptr: NonNull<u8>,
    layout: Layout,
    // 该块已使用的字节数
    used: S::Usize,
    // 活跃对象计数器：当计数归零时，Chunk 可被回收
    active_count: S::Usize,
    // 该块拥有的析构函数链表，采用双向链表结构，并在锁保护下操作
    drop_list: S::Lock<LinkedList<DropAdapter<S>>>,
}

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
    pub unsafe fn alloc<T>(&self, layout: Layout, drop_fn: Option<unsafe fn(*mut u8)>) -> *mut u8 {
        // 1. 如果有析构函数，需要额外分配 DropNode 空间
        let (total_layout, offset) = if drop_fn.is_some() {
            let node_layout = Layout::new::<GenericDropNode<S>>();
            node_layout.extend(layout).expect("Layout overflow")
        } else {
            (layout, 0)
        };

        // 2. 尝试快速分配
        let mut res = self.try_alloc_fast(total_layout);

        // 3. 快速分配失败，进入慢速路径（分配新块）
        if res.is_none() {
            res = Some(self.alloc_slow(total_layout));
        }

        let (ptr, chunk_ptr) = res.unwrap();

        // 4. 增加活跃对象计数 (已经在 try_alloc_fast/alloc_slow 中处理)

        // 5. 如果需要销毁，初始化 DropNode 并压入块内链表
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
                drop_list.push_front(std::pin::Pin::new_unchecked(&mut *node_ptr));
            }
            data_ptr
        } else {
            ptr
        }
    }

    /// 手动触发对象的析构并尝试回收块。
    /// # Safety
    /// The `data_ptr` must be valid and points to an object allocated by this arena.
    pub unsafe fn drop_object<T>(&self, data_ptr: *mut T, layout: Layout) {
        // 1. 计算 DropNode 的位置
        let node_layout = Layout::new::<GenericDropNode<S>>();
        let offset = node_layout.extend(layout).unwrap().1;
        let node_ptr = unsafe { (data_ptr as *mut u8).sub(offset) as *mut GenericDropNode<S> };

        let _guard = S::pin();

        // 2. 执行析构
        let drop_fn_val = unsafe { (*node_ptr).drop_fn.fetch_and(0, Ordering::AcqRel) };
        let chunk_ptr = unsafe { (*node_ptr).chunk as *mut GenericChunk<S> };

        if drop_fn_val != 0 {
            unsafe {
                let drop_fn = *(&drop_fn_val as *const usize as *const unsafe fn(*mut u8));
                (drop_fn)(data_ptr as *mut u8);
            }
        }

        // 3. 从双向链表安全移除节点
        unsafe {
            let mut drop_list = (*chunk_ptr).drop_list.lock();
            let mut cursor = drop_list.cursor_mut_from_ptr(NonNull::new_unchecked(node_ptr));
            if cursor.get_raw().is_some() {
                cursor.remove();
            }
        }

        // 4. 减少计数并检查回收
        if unsafe {
            (*chunk_ptr)
                .active_count
                .fetch_sub(1usize, Ordering::AcqRel)
                == 1
        } {
            self.reclaim_chunk(chunk_ptr);
        }
    }

    fn reclaim_chunk(&self, chunk_ptr: *mut GenericChunk<S>) {
        let mut chunks = self.chunks.lock();
        unsafe {
            let mut cursor = chunks.cursor_mut_from_ptr(NonNull::new_unchecked(chunk_ptr));
            if cursor.get_raw().is_some() {
                cursor.remove();

                // 将 dealloc 和释放 GenericChunk 的操作放入 epoch 延迟中
                let guard = S::pin();
                let chunk_addr = chunk_ptr as usize;
                guard.defer(move || {
                    let chunk_ptr = chunk_addr as *mut GenericChunk<S>;
                    let chunk = Box::from_raw(chunk_ptr);
                    dealloc(chunk.ptr.as_ptr(), chunk.layout);
                });
            }
        }
    }

    fn try_alloc_fast(&self, layout: Layout) -> Option<(*mut u8, *mut GenericChunk<S>)> {
        let _guard = S::pin();
        if let Some(chunk_ptr) = self.active_chunk.load(Ordering::Acquire) {
            let chunk = unsafe { chunk_ptr.as_ref() };
            // 增加计数以确保在分配期间块不被回收
            if chunk.active_count.fetch_add(1usize, Ordering::AcqRel) > 0 {
                let p = chunk.try_alloc(layout);
                if !p.is_null() {
                    return Some((p, chunk_ptr.as_ptr()));
                }
                // 分配失败，减少计数
                if chunk.active_count.fetch_sub(1usize, Ordering::AcqRel) == 1 {
                    self.reclaim_chunk(chunk_ptr.as_ptr());
                }
            }
        }
        None
    }

    #[inline(never)]
    fn alloc_slow(&self, layout: Layout) -> (*mut u8, *mut GenericChunk<S>) {
        // Double-check
        {
            let _guard = S::pin();
            if let Some(current_active) = self.active_chunk.load(Ordering::Acquire) {
                let a_ref = unsafe { current_active.as_ref() };
                if a_ref.active_count.fetch_add(1usize, Ordering::AcqRel) > 0 {
                    let p = a_ref.try_alloc(layout);
                    if !p.is_null() {
                        return (p, current_active.as_ptr());
                    }
                    // 分配失败，减少计数
                    if a_ref.active_count.fetch_sub(1usize, Ordering::AcqRel) == 1 {
                        self.reclaim_chunk(current_active.as_ptr());
                    }
                }
            }
        }

        // 分配新块
        let chunk_size = 8192.max(layout.size() + layout.align());
        let new_chunk_layout = Layout::from_size_align(chunk_size, 64).unwrap();
        let ptr = unsafe { alloc(new_chunk_layout) };
        if ptr.is_null() {
            std::alloc::handle_alloc_error(new_chunk_layout);
        }

        let new_chunk = Box::new(GenericChunk {
            link: Link::new(),
            ptr: NonNull::new(ptr).unwrap(),
            layout: new_chunk_layout,
            used: S::Usize::new(0),
            active_count: S::Usize::new(1), // 初始计数为 1，代表被 Arena 的 active_chunk 引用
            drop_list: S::Lock::new(LinkedList::new(DropAdapter::<S>::new())),
        });

        let chunk_ptr: *mut GenericChunk<S> = Box::into_raw(new_chunk);
        let allocated_ptr = unsafe { (*chunk_ptr).try_alloc(layout) };
        // 增加对象计数
        unsafe {
            (*chunk_ptr)
                .active_count
                .fetch_add(1usize, Ordering::Relaxed);
        }

        {
            let mut chunks = self.chunks.lock();
            unsafe {
                chunks.push_back(std::pin::Pin::new_unchecked(&mut *chunk_ptr));
            }
        }

        loop {
            let _guard = S::pin();
            let active = self.active_chunk.load(Ordering::Acquire);
            if let Some(a) = active {
                let a_ref = unsafe { a.as_ref() };
                let used = a_ref.used.load(Ordering::Acquire);
                if a_ref.layout.size().saturating_sub(used) >= layout.size() + layout.align() {
                    // 另一个线程已经提供了一个合适的 chunk。
                    // 尝试从当前的 active 分配（需要锁定/引用）
                    if a_ref.active_count.fetch_add(1usize, Ordering::AcqRel) > 0 {
                        let p = a_ref.try_alloc(layout);
                        if !p.is_null() {
                            // 只有当成功从另一个 chunk 分配时，我们才丢弃当前的 new_chunk。
                            // 此时我们一次性扣除它的两个引用计数（active_chunk 占位 + 对象分配占位）。
                            unsafe {
                                if (*chunk_ptr)
                                    .active_count
                                    .fetch_sub(2usize, Ordering::AcqRel)
                                    == 2
                                {
                                    self.reclaim_chunk(chunk_ptr);
                                }
                            }
                            return (p, a.as_ptr());
                        }
                        // 分配失败，减少计数
                        if a_ref.active_count.fetch_sub(1usize, Ordering::AcqRel) == 1 {
                            self.reclaim_chunk(a.as_ptr());
                        }
                    }
                    // 如果分配失败，继续尝试将 new_chunk 设为 active
                }
            }
            if let Ok(old_active) = self.active_chunk.compare_exchange_weak(
                active,
                NonNull::new(chunk_ptr),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                if let Some(old) = old_active {
                    unsafe {
                        if old
                            .as_ref()
                            .active_count
                            .fetch_sub(1usize, Ordering::AcqRel)
                            == 1
                        {
                            self.reclaim_chunk(old.as_ptr());
                        }
                    }
                }
                break;
            }
        }

        (allocated_ptr, chunk_ptr)
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
    unsafe fn alloc_raw(&self, layout: Layout, drop_fn: Option<unsafe fn(*mut u8)>) -> *mut u8 {
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
{
}
unsafe impl<S: ThreadSafeStorage> Sync for GenericArena<S>
where
    S::OptionPtr<GenericChunk<S>>: Sync,
    S::Lock<LinkedList<ChunkAdapter<S>>>: Sync,
{
}

unsafe impl<S: ThreadSafeStorage> Send for GenericChunk<S>
where
    S::Usize: Send,
    S::Lock<LinkedList<DropAdapter<S>>>: Send,
{
}
unsafe impl<S: ThreadSafeStorage> Sync for GenericChunk<S>
where
    S::Usize: Sync,
    S::Lock<LinkedList<DropAdapter<S>>>: Sync,
{
}
