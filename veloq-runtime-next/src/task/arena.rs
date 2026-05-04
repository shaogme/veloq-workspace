use crate::utils::storage::{
    StateInt, StateLock, StateOptionPtr, Storage,
};
use std::alloc::{Layout, alloc, dealloc};
use std::ptr::{self, NonNull};
use std::sync::atomic::Ordering;

/// 一个高性能的、块分配器接口。
pub trait Arena {
    unsafe fn alloc_raw(&self, layout: Layout, drop_fn: Option<unsafe fn(*mut u8)>) -> *mut u8;
    unsafe fn drop_object_raw(&self, data_ptr: *mut u8, layout: Layout);
}

/// 通用的块分配器，通过 Storage 策略支持线程安全或本地分配。
pub struct GenericArena<S: Storage> {
    // 活跃块，支持快速路径分配
    active_chunk: S::OptionPtr<GenericChunk<S>>,
    // 所有块的拥有者
    chunks: S::Lock<Vec<*mut GenericChunk<S>>>,
}

pub(crate) struct GenericChunk<S: Storage> {
    ptr: NonNull<u8>,
    layout: Layout,
    // 该块已使用的字节数
    used: S::Usize,
    // 活跃对象计数器：当计数归零时，Chunk 可被回收
    active_count: S::Usize,
    // 该块拥有的析构函数链表
    drop_head: S::OptionPtr<GenericDropNode<S>>,
}

pub(crate) struct GenericDropNode<S: Storage> {
    next: *mut GenericDropNode<S>,
    drop_fn: unsafe fn(*mut u8),
    data_ptr: *mut u8,
    // 所属的 Chunk，用于回收
    chunk: *const GenericChunk<S>,
}

impl<S: Storage> GenericArena<S> {
    pub fn new() -> Self {
        Self {
            active_chunk: S::OptionPtr::new(None),
            chunks: S::Lock::new(Vec::new()),
        }
    }

    /// 分配内存并记录其析构函数。
    pub unsafe fn alloc<T>(&self, layout: Layout, drop_fn: Option<unsafe fn(*mut u8)>) -> *mut u8 {
        // 1. 如果有析构函数，需要额外分配 DropNode 空间
        let (total_layout, is_drop) = if drop_fn.is_some() {
            let node_layout = Layout::new::<GenericDropNode<S>>();
            let (extended, _) = node_layout.extend(layout).expect("Layout overflow");
            (extended.pad_to_align(), true)
        } else {
            (layout, false)
        };

        // 2. 尝试快速分配
        let mut res = self.try_alloc_fast(total_layout);

        // 3. 快速分配失败，进入慢速路径（分配新块）
        if res.is_none() {
            res = Some(self.alloc_slow(total_layout));
        }

        let (ptr, chunk_ptr) = res.unwrap();

        // 4. 增加活跃对象计数
        unsafe {
            (*chunk_ptr).active_count.fetch_add(1, Ordering::Relaxed);
        }

        // 5. 如果需要销毁，初始化 DropNode 并压入块内链表
        if is_drop {
            let node_ptr = ptr as *mut GenericDropNode<S>;
            // 计算数据指针：在 DropNode 之后，且满足对齐要求
            let data_offset = Layout::new::<GenericDropNode<S>>()
                .extend(layout)
                .unwrap()
                .1;
            let data_ptr = unsafe { ptr.add(data_offset) };

            unsafe {
                (*node_ptr).drop_fn = drop_fn.unwrap();
                (*node_ptr).data_ptr = data_ptr;
                (*node_ptr).chunk = chunk_ptr;

                // 原子压入块内 LIFO 链表
                let mut head = (*chunk_ptr).drop_head.load(Ordering::Acquire);
                loop {
                    (*node_ptr).next = head.map(|p| p.as_ptr()).unwrap_or(ptr::null_mut());
                    match (*chunk_ptr).drop_head.compare_exchange_weak(
                        head,
                        NonNull::new(node_ptr),
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(actual) => head = actual,
                    }
                }
            }
            data_ptr
        } else {
            ptr
        }
    }

    /// 手动触发对象的析构并尝试回收块。
    pub unsafe fn drop_object<T>(&self, data_ptr: *mut T, layout: Layout) {
        // 1. 计算 DropNode 的位置
        let node_layout = Layout::new::<GenericDropNode<S>>();
        let data_offset = node_layout.extend(layout).unwrap().1;
        let node_ptr = unsafe { (data_ptr as *mut u8).sub(data_offset) as *mut GenericDropNode<S> };

        // 2. 执行析构
        let drop_fn = unsafe { (*node_ptr).drop_fn };
        // 将 drop_fn 置为 no-op 避免 Arena 销毁时重复调用
        unsafe {
            ptr::write_volatile(&mut (*node_ptr).drop_fn, |_| {});
            (drop_fn)(data_ptr as *mut u8);
        }

        // 3. 减少计数并检查回收
        let chunk_ptr = unsafe { (*node_ptr).chunk as *mut GenericChunk<S> };
        if unsafe { (*chunk_ptr).active_count.fetch_sub(1, Ordering::AcqRel) == 1 } {
            self.reclaim_chunk(chunk_ptr);
        }
    }

    fn reclaim_chunk(&self, chunk_ptr: *mut GenericChunk<S>) {
        // 如果是当前的活跃块，先尝试将其置为空
        let _ = self.active_chunk.compare_exchange(
            NonNull::new(chunk_ptr),
            None,
            Ordering::AcqRel,
            Ordering::Acquire,
        );

        let mut chunks = self.chunks.lock();
        if let Some(pos) = chunks.iter().position(|&p| p == chunk_ptr) {
            chunks.remove(pos);
            unsafe {
                let chunk = Box::from_raw(chunk_ptr);
                dealloc(chunk.ptr.as_ptr(), chunk.layout);
            }
        }
    }

    #[inline]
    fn try_alloc_fast(&self, layout: Layout) -> Option<(*mut u8, *mut GenericChunk<S>)> {
        if let Some(chunk_ptr) = self.active_chunk.load(Ordering::Acquire) {
            let p = unsafe { chunk_ptr.as_ref().try_alloc(layout) };
            if !p.is_null() {
                return Some((p, chunk_ptr.as_ptr()));
            }
        }
        None
    }

    #[inline(never)]
    fn alloc_slow(&self, layout: Layout) -> (*mut u8, *mut GenericChunk<S>) {
        // Double-check
        if let Some(current_active) = self.active_chunk.load(Ordering::Acquire) {
            let p = unsafe { current_active.as_ref().try_alloc(layout) };
            if !p.is_null() {
                return (p, current_active.as_ptr());
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
            ptr: NonNull::new(ptr).unwrap(),
            layout: new_chunk_layout,
            used: S::Usize::new(0),
            active_count: S::Usize::new(0),
            drop_head: S::OptionPtr::new(None),
        });

        let allocated_ptr = new_chunk.try_alloc(layout);
        let chunk_ptr = Box::into_raw(new_chunk);

        {
            let mut chunks = self.chunks.lock();
            chunks.push(chunk_ptr);
        }

        // 更新活跃块指针
        let mut active = self.active_chunk.load(Ordering::Acquire);
        loop {
            if let Some(a) = active {
                let a_ref = unsafe { a.as_ref() };
                let used = a_ref.used.load(Ordering::Acquire);
                if a_ref.layout.size().saturating_sub(used) >= layout.size() + layout.align() {
                    break;
                }
            }
            match self.active_chunk.compare_exchange_weak(
                active,
                NonNull::new(chunk_ptr),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => active = actual,
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

        let mut current_used = self.used.load(Ordering::Acquire);
        loop {
            let current_ptr = unsafe { self.ptr.as_ptr().add(current_used) } as usize;
            let aligned_ptr = (current_ptr + mask) & !mask;
            let offset = aligned_ptr - self.ptr.as_ptr() as usize;
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
        while let Some(chunk_ptr) = chunks.pop() {
            unsafe {
                let chunk = Box::from_raw(chunk_ptr);
                let mut curr_drop = chunk.drop_head.load(Ordering::Acquire);
                while let Some(drop_node) = curr_drop {
                    let node = drop_node.as_ptr();
                    ((*node).drop_fn)((*node).data_ptr);
                    curr_drop = NonNull::new((*node).next);
                }
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
unsafe impl<S: Storage> Send for GenericArena<S>
where
    S::OptionPtr<GenericChunk<S>>: Send,
    S::Lock<Vec<*mut GenericChunk<S>>>: Send,
{
}
unsafe impl<S: Storage> Sync for GenericArena<S>
where
    S::OptionPtr<GenericChunk<S>>: Sync,
    S::Lock<Vec<*mut GenericChunk<S>>>: Sync,
{
}

unsafe impl<S: Storage> Send for GenericChunk<S>
where
    S::Usize: Send,
    S::OptionPtr<GenericDropNode<S>>: Send,
{
}
unsafe impl<S: Storage> Sync for GenericChunk<S>
where
    S::Usize: Sync,
    S::OptionPtr<GenericDropNode<S>>: Sync,
{
}
