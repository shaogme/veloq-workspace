//! Global memory allocation and Block pool management
//!
//! This module provides:
//! - GlobalAllocator: Factory for creating memory pools
//! - GlobalBlockPool: N*2 Block pool for all threads
//! - Block allocation strategy with 4-level priority

use crate::block::{Block, BlockMeta, RawAllocator};
use crate::{MIN_THREAD_MEMORY, RawSlab, ThreadMemory, ThreadMemoryMultiplier};
use std::{io, num::NonZeroUsize, ptr::NonNull, sync::Arc};

/// Configuration for GlobalAllocator
#[derive(Debug, Clone)]
pub struct GlobalAllocatorConfig {
    /// Defines the memory multiplier for each thread.
    /// The index corresponds to the thread/worker ID.
    pub multipliers: Vec<ThreadMemoryMultiplier>,
}

/// Global memory allocator factory.
///
/// Currently acts as a pure factory. Can be extended to hold weak references
/// to all RawSlabs for monitoring or management interfaces to support expansion.
pub struct GlobalAllocator;

/// Information about the global memory block for Driver Registration (God View).
#[derive(Debug, Clone, Copy)]
pub struct GlobalMemoryInfo {
    pub ptr: NonNull<u8>,
    pub len: NonZeroUsize,
}

// Guarantee thread safety for the info pointing to shared memory
unsafe impl Send for GlobalMemoryInfo {}
unsafe impl Sync for GlobalMemoryInfo {}

/// Global Block Pool: 管理所有线程的 N*2 个 Block
///
/// 每个线程有 2 个 Block（主块和备块），总共 2N 个 Block。
/// Block 索引规则：
/// - 线程 i 的主块：索引 2*i
/// - 线程 i 的备块：索引 2*i+1
pub struct GlobalBlockPool {
    /// 所有 Block 的列表（扁平化存储）
    blocks: Vec<Block>,
    /// 线程数量
    thread_count: usize,
    /// 全局内存信息（用于驱动注册）
    global_info: GlobalMemoryInfo,
}

impl GlobalBlockPool {
    /// 获取线程 i 的主块索引
    #[inline]
    fn primary_index(thread_idx: usize) -> usize {
        thread_idx * 2
    }

    /// 获取线程 i 的备块索引
    #[inline]
    fn backup_index(thread_idx: usize) -> usize {
        thread_idx * 2 + 1
    }

    /// 按 4 级优先级分配内存
    ///
    /// 优先级顺序：
    /// 1. Own Primary (主块) - 阻塞等待
    /// 2. Own Backup (备块) - 阻塞等待
    /// 3. Others' Backup - 非阻塞尝试
    /// 4. Others' Primary - 非阻塞尝试（最后兜底）
    pub fn alloc(
        &self,
        thread_idx: usize,
        size: usize,
    ) -> Option<(usize, crate::block::RawAllocResult)> {
        // Level 1: Own Primary (阻塞)
        let primary_idx = Self::primary_index(thread_idx);
        if let Some(result) = self.blocks[primary_idx].alloc(size) {
            return Some((primary_idx, result));
        }

        // Level 2: Own Backup (阻塞)
        let backup_idx = Self::backup_index(thread_idx);
        if let Some(result) = self.blocks[backup_idx].alloc(size) {
            return Some((backup_idx, result));
        }

        // Level 3: Others' Backup (非阻塞，try_lock)
        // 从 (thread_idx + 1) 开始遍历，避免所有线程都竞争线程 0
        for i in 1..self.thread_count {
            let other_idx = (thread_idx + i) % self.thread_count;
            let other_backup_idx = Self::backup_index(other_idx);
            if let Some(result) = self.blocks[other_backup_idx].try_alloc(size) {
                return Some((other_backup_idx, result));
            }
        }

        // Level 4: Others' Primary (非阻塞，try_lock，最后兜底)
        for i in 1..self.thread_count {
            let other_idx = (thread_idx + i) % self.thread_count;
            let other_primary_idx = Self::primary_index(other_idx);
            if let Some(result) = self.blocks[other_primary_idx].try_alloc(size) {
                return Some((other_primary_idx, result));
            }
        }

        // 所有 Block 都无法分配
        None
    }

    /// 释放内存回指定的 Block
    ///
    /// # Safety
    /// 调用者必须确保 ptr 是从 block_idx 对应的 Block 分配的
    pub unsafe fn dealloc(&self, block_idx: usize, ptr: NonNull<u8>, cap: usize, context: usize) {
        unsafe {
            self.blocks[block_idx].dealloc(ptr, cap, context);
        }
    }

    /// 获取全局内存信息
    pub fn global_info(&self) -> GlobalMemoryInfo {
        self.global_info
    }

    /// 获取线程数量
    pub fn thread_count(&self) -> usize {
        self.thread_count
    }

    /// 获取 Block 数量（应该是 2 * thread_count）
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
}

impl std::fmt::Debug for GlobalBlockPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GlobalBlockPool")
            .field("thread_count", &self.thread_count)
            .field("block_count", &self.block_count())
            .field("global_info", &self.global_info)
            .finish()
    }
}

/// Block 工厂函数类型
///
/// 给定线程索引和内存，创建对应的分配器实例
pub type BlockFactory = Box<dyn Fn(usize, ThreadMemory) -> Box<dyn RawAllocator> + Send + Sync>;

impl GlobalAllocator {
    /// 创建新的全局 Block Pool
    ///
    /// # 参数
    /// - `config`: 配置，包含每个线程的内存倍数
    /// - `factory`: Block 工厂函数，用于为每个 Block 创建分配器
    ///
    /// # 返回
    /// 返回 `GlobalBlockPool`，包含所有线程的 2N 个 Block
    pub fn new(
        config: GlobalAllocatorConfig,
        factory: BlockFactory,
    ) -> io::Result<GlobalBlockPool> {
        if config.multipliers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Thread multipliers cannot be empty",
            ));
        }

        let thread_count = config.multipliers.len();
        let total_size: usize = config
            .multipliers
            .iter()
            .map(|m| MIN_THREAD_MEMORY.get() * m.0.get() * 2) // 每个线程需要 2 个 Block
            .sum();

        // 1. 分配单个大块物理内存
        let slab = Arc::new(RawSlab::new(unsafe {
            NonZeroUsize::new_unchecked(total_size)
        })?);

        let global_info = GlobalMemoryInfo {
            ptr: slab.ptr,
            len: slab.size,
        };

        let mut blocks = Vec::with_capacity(thread_count * 2);
        let mut current_ptr = slab.ptr.as_ptr();

        // 2. 为每个线程创建 2 个 Block（主块 + 备块）
        for (thread_idx, &multiplier) in config.multipliers.iter().enumerate() {
            let size = unsafe {
                NonZeroUsize::new_unchecked(MIN_THREAD_MEMORY.get() * multiplier.0.get())
            };

            // 主块 (Primary)
            let primary_memory = ThreadMemory {
                _owner: slab.clone(),
                ptr: unsafe { NonNull::new_unchecked(current_ptr) },
                len: size,
            };
            let primary_allocator = factory(thread_idx, primary_memory);
            let primary_meta = BlockMeta {
                owner_thread_idx: thread_idx,
                is_backup: false,
            };
            blocks.push(Block::new(primary_allocator, primary_meta));

            // 移动指针
            unsafe {
                current_ptr = current_ptr.add(size.get());
            }

            // 备块 (Backup)
            let backup_memory = ThreadMemory {
                _owner: slab.clone(),
                ptr: unsafe { NonNull::new_unchecked(current_ptr) },
                len: size,
            };
            let backup_allocator = factory(thread_idx, backup_memory);
            let backup_meta = BlockMeta {
                owner_thread_idx: thread_idx,
                is_backup: true,
            };
            blocks.push(Block::new(backup_allocator, backup_meta));

            // 移动指针
            unsafe {
                current_ptr = current_ptr.add(size.get());
            }
        }

        Ok(GlobalBlockPool {
            blocks,
            thread_count,
            global_info,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::hybrid::HybridAllocator;

    #[test]
    fn test_global_block_pool_creation() {
        let config = GlobalAllocatorConfig {
            multipliers: vec![
                ThreadMemoryMultiplier(crate::nz!(8)), // 16MB per block (HybridAllocator needs 14MB)
                ThreadMemoryMultiplier(crate::nz!(8)),
            ],
        };

        let factory: BlockFactory = Box::new(|_thread_idx, memory| {
            Box::new(HybridAllocator::new(memory).expect("Failed to create allocator"))
        });

        let pool = GlobalAllocator::new(config, factory).unwrap();

        assert_eq!(pool.thread_count(), 2);
        assert_eq!(pool.block_count(), 4); // 2 threads * 2 blocks each
    }

    #[test]
    fn test_block_pool_alloc() {
        let config = GlobalAllocatorConfig {
            multipliers: vec![ThreadMemoryMultiplier(crate::nz!(8))], // 16MB (HybridAllocator needs 14MB)
        };

        let factory: BlockFactory = Box::new(|_thread_idx, memory| {
            Box::new(HybridAllocator::new(memory).expect("Failed to create allocator"))
        });

        let pool = GlobalAllocator::new(config, factory).unwrap();

        // 线程 0 分配内存
        let (block_idx, result) = pool.alloc(0, 4096).expect("Allocation failed");
        assert_eq!(result.cap.get(), 4096);

        // 释放内存
        unsafe {
            pool.dealloc(block_idx, result.ptr, result.cap.get(), result.context);
        }
    }

    #[test]
    fn test_global_allocator_lifecycle() {
        let thread_count = 4;
        let config = GlobalAllocatorConfig {
            multipliers: vec![ThreadMemoryMultiplier(crate::nz!(8)); thread_count],
        };

        let factory: BlockFactory = Box::new(|_thread_idx, memory| {
            Box::new(HybridAllocator::new(memory).expect("Failed to create allocator"))
        });

        match GlobalAllocator::new(config, factory) {
            Ok(pool) => {
                assert_eq!(pool.thread_count(), thread_count);
                assert_eq!(pool.block_count(), thread_count * 2);

                for i in 0..thread_count {
                    // Try alloc from primary
                    let (idx, res) = pool.alloc(i, 4096).expect("Alloc failed");
                    // Ideally we should hit primary block
                    assert!(idx == i * 2 || idx == i * 2 + 1);

                    // Cleanup
                    unsafe {
                        pool.dealloc(idx, res.ptr, res.cap.get(), res.context);
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "GlobalAllocator::new failed (likely due to missing permissions): {}",
                    e
                );
            }
        }
    }

    #[test]
    fn test_block_based_pool_integration() {
        use crate::buffer::{BlockBasedPool, BufPool};

        let config = GlobalAllocatorConfig {
            multipliers: vec![
                ThreadMemoryMultiplier(crate::nz!(8)), // 16MB per block
                ThreadMemoryMultiplier(crate::nz!(8)),
            ],
        };

        let factory: BlockFactory = Box::new(|_thread_idx, memory| {
            use crate::buffer::hybrid::HybridAllocator;
            Box::new(HybridAllocator::new(memory).expect("Failed to create allocator"))
        });

        let global_pool = Box::leak(Box::new(GlobalAllocator::new(config, factory).unwrap()));

        // 为线程 0 创建 BlockBasedPool
        let pool = BlockBasedPool::new(global_pool, 0, None);

        // 测试分配
        let buf = pool.alloc(crate::nz!(4096)).expect("Allocation failed");
        assert_eq!(buf.len(), 4096);

        // 测试自动释放（通过 Drop）
        drop(buf);

        // 再次分配以验证内存已归还
        let buf2 = pool
            .alloc(crate::nz!(4096))
            .expect("Second allocation failed");
        assert_eq!(buf2.len(), 4096);
    }
}
