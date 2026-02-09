//! Block: 独立的内存管理单元
//!
//! 每个 Block 包含：
//! - 一个分配器实例（实现 RawAllocator trait）
//! - 一个锁保护并发访问
//! - 元数据：所属线程索引、是否为备用块

use parking_lot::Mutex;
use std::num::NonZeroUsize;
use std::ptr::NonNull;

/// 原始分配结果
#[derive(Debug)]
pub struct RawAllocResult {
    /// 指向分配的内存块的指针
    pub ptr: NonNull<u8>,
    /// 内存块的容量（可能大于请求的大小）
    pub cap: NonZeroUsize,
    /// 分配器特定的上下文信息（用于释放时识别）
    pub context: usize,
}

/// 原始分配器接口
///
/// 这是纯算法层面的内存管理，不包含任何并发控制。
/// 实现者需要提供：
/// - 分配：给定大小，返回内存块
/// - 释放：给定指针和上下文，回收内存
pub trait RawAllocator: Send + 'static {
    /// 分配指定大小的内存块
    ///
    /// # 参数
    /// - `size`: 请求的内存大小（字节）
    ///
    /// # 返回
    /// - `Some(RawAllocResult)`: 分配成功
    /// - `None`: 内存不足或大小不支持
    fn alloc(&mut self, size: usize) -> Option<RawAllocResult>;

    /// 释放内存块
    ///
    /// # 参数
    /// - `ptr`: 内存块的起始地址
    /// - `cap`: 内存块的容量（与分配时返回的 cap 一致）
    /// - `context`: 分配时返回的上下文信息
    ///
    /// # Safety
    /// 调用者必须确保：
    /// - `ptr` 是由本分配器的 `alloc` 返回的
    /// - `cap` 和 `context` 与分配时一致
    /// - 内存块未被释放过（防止 double-free）
    unsafe fn dealloc(&mut self, ptr: NonNull<u8>, cap: usize, context: usize);

    /// 获取此分配器管理的全局内存区域
    ///
    /// 返回 (base_ptr, total_size)
    /// 用于驱动注册（如 io_uring 的固定缓冲区注册）
    fn global_region(&self) -> (NonNull<u8>, usize);
}

/// Block 元数据
#[derive(Debug, Clone, Copy)]
pub struct BlockMeta {
    /// 所属线程的索引
    pub owner_thread_idx: usize,
    /// 是否为备用块（false 表示主块）
    pub is_backup: bool,
}

/// Block 内部状态（被 Mutex 保护）
struct BlockInner {
    /// 内存分配器实例
    allocator: Box<dyn RawAllocator>,
    /// Block 元数据
    meta: BlockMeta,
}

/// Block: 带锁保护的内存管理单元
///
/// 使用 `#[repr(align(64))]` 防止伪共享（False Sharing）
/// Pending deallocation request from remote threads
struct RemoteFree {
    ptr: NonNull<u8>,
    cap: usize,
    context: usize,
}

// SAFETY: Sending a pointer to be freed to another thread is safe as we are transferring ownership
unsafe impl Send for RemoteFree {}

/// Block: 带锁保护的内存管理单元
///
/// 使用 `#[repr(align(64))]` 防止伪共享（False Sharing）
#[repr(align(64))]
pub struct Block {
    /// 被 Mutex 保护的内部状态 (主分配器)
    inner: Mutex<BlockInner>,
    /// 远程释放队列 (Split Lock 优化)
    /// 当 inner 锁竞争时，远程线程将释放请求放入此队列，避免阻塞
    remote_frees: Mutex<Vec<RemoteFree>>,
}

impl Block {
    /// 创建新的 Block
    ///
    /// # 参数
    /// - `allocator`: 分配器实例
    /// - `meta`: Block 元数据
    pub fn new(allocator: Box<dyn RawAllocator>, meta: BlockMeta) -> Self {
        Self {
            inner: Mutex::new(BlockInner { allocator, meta }),
            remote_frees: Mutex::new(Vec::new()),
        }
    }

    /// 尝试从此 Block 分配内存（非阻塞）
    ///
    /// 使用 `try_lock()` 避免阻塞。
    /// 如果锁被占用，立即返回 None。
    pub fn try_alloc(&self, size: usize) -> Option<RawAllocResult> {
        let mut inner = self.inner.try_lock()?;

        // 尝试非阻塞地回收远程释放队列中的内存
        if let Some(mut remote) = self.remote_frees.try_lock() {
            if !remote.is_empty() {
                // 取出所有待释放项
                let pending = std::mem::take(&mut *remote);
                drop(remote); // 尽早释放锁

                for item in pending {
                    unsafe {
                        inner.allocator.dealloc(item.ptr, item.cap, item.context);
                    }
                }
            }
        }

        inner.allocator.alloc(size)
    }

    /// 从此 Block 分配内存（阻塞）
    ///
    /// 获取锁并分配内存。
    /// 在分配前会检查并回收 `remote_frees` 中的待释放内存。
    pub fn alloc(&self, size: usize) -> Option<RawAllocResult> {
        let mut inner = self.inner.lock();

        // 检查并回收远程释放队列
        // 优化：使用局部作用域和 replace 快速释放 remote_frees 锁
        {
            let mut remote = self.remote_frees.lock();
            if !remote.is_empty() {
                let pending = std::mem::take(&mut *remote);
                drop(remote); // 尽早释放锁

                for item in pending {
                    unsafe {
                        inner.allocator.dealloc(item.ptr, item.cap, item.context);
                    }
                }
            }
        }

        inner.allocator.alloc(size)
    }

    /// 释放内存回此 Block
    ///
    /// # Safety
    /// 调用者必须确保：
    /// - `ptr` 是由此 Block 的分配器返回的
    /// - `cap` 和 `context` 与分配时一致
    pub unsafe fn dealloc(&self, ptr: NonNull<u8>, cap: usize, context: usize) {
        // 1. 快速路径：尝试获取主锁直接释放
        if let Some(mut inner) = self.inner.try_lock() {
            unsafe {
                inner.allocator.dealloc(ptr, cap, context);
            }
            return;
        }

        // 2. 慢速路径 (Contention)：推入远程释放队列
        // 避免阻塞等待主锁，减少 Work Stealing 场景下的竞争
        let mut remote = self.remote_frees.lock();
        remote.push(RemoteFree { ptr, cap, context });
    }

    /// 获取 Block 元数据（不需要锁）
    pub fn meta(&self) -> BlockMeta {
        self.inner.lock().meta
    }

    /// 获取全局内存区域信息
    pub fn global_region(&self) -> (NonNull<u8>, usize) {
        self.inner.lock().allocator.global_region()
    }
}

impl std::fmt::Debug for Block {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // 尝试获取锁来读取元数据
        let inner_dbg = if let Some(inner) = self.inner.try_lock() {
            format!(
                "Block {{ owner: {}, backup: {} }}",
                inner.meta.owner_thread_idx, inner.meta.is_backup
            )
        } else {
            "Block { <locked> }".to_string()
        };

        let remote_count = if let Some(remote) = self.remote_frees.try_lock() {
            remote.len()
        } else {
            0 // Just verify lock state
        };

        f.debug_struct("BlockWrapper")
            .field("inner", &inner_dbg)
            .field("remote_pending", &remote_count)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ThreadMemory;

    // Mock allocator for testing
    struct MockAllocator {
        memory: ThreadMemory,
        allocated_count: usize,
    }

    impl MockAllocator {
        fn new(memory: ThreadMemory) -> Self {
            Self {
                memory,
                allocated_count: 0,
            }
        }
    }

    impl RawAllocator for MockAllocator {
        fn alloc(&mut self, size: usize) -> Option<RawAllocResult> {
            if size > self.memory.len() {
                return None;
            }
            self.allocated_count += 1;
            Some(RawAllocResult {
                ptr: unsafe { NonNull::new_unchecked(self.memory.as_ptr() as *mut u8) },
                cap: unsafe { NonZeroUsize::new_unchecked(size) },
                context: 0,
            })
        }

        unsafe fn dealloc(&mut self, _ptr: NonNull<u8>, _cap: usize, _context: usize) {
            self.allocated_count = self.allocated_count.saturating_sub(1);
        }

        fn global_region(&self) -> (NonNull<u8>, usize) {
            self.memory.global_region()
        }
    }

    #[test]
    fn test_block_creation() {
        let size = crate::MIN_THREAD_MEMORY;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

        let allocator = Box::new(MockAllocator::new(memory));
        let meta = BlockMeta {
            owner_thread_idx: 0,
            is_backup: false,
        };
        let block = Block::new(allocator, meta);

        assert_eq!(block.meta().owner_thread_idx, 0);
        assert!(!block.meta().is_backup);
    }

    #[test]
    fn test_block_alloc() {
        let size = crate::MIN_THREAD_MEMORY;
        let memory = crate::ThreadMemory::new_standalone(size).unwrap();

        let allocator = Box::new(MockAllocator::new(memory));
        let meta = BlockMeta {
            owner_thread_idx: 0,
            is_backup: false,
        };
        let block = Block::new(allocator, meta);

        let result = block.alloc(4096);
        assert!(result.is_some());

        let raw = result.unwrap();
        assert_eq!(raw.cap.get(), 4096);

        unsafe {
            block.dealloc(raw.ptr, raw.cap.get(), raw.context);
        }
    }
}
