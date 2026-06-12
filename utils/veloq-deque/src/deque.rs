extern crate alloc;

use alloc::boxed::Box;
use alloc::vec::Vec;
use core::num::NonZeroUsize;
use core::ptr;
use veloq_shim::atomic::{AtomicIsize, Ordering, fence};
use veloq_shim::cell::UnsafeCell;

/// Chase-Lev Deque 的简化实现（固定大小版，配合全局 Injector 使用效率最高）
/// 支持单生产者 (Push/Pop) 和多消费者 (Steal)。
pub struct Deque<T: Copy> {
    top: AtomicIsize,
    bottom: AtomicIsize,
    buffer: Box<[UnsafeCell<Option<T>>]>,
    mask: isize,
}

unsafe impl<T: Copy + Send> Send for Deque<T> {}
unsafe impl<T: Copy + Send> Sync for Deque<T> {}

impl<T: Copy> Deque<T> {
    pub fn new(capacity: NonZeroUsize) -> Self {
        let capacity = capacity.get();
        assert!(capacity.is_power_of_two(), "Capacity must be a power of 2");
        let vec = (0..capacity).map(|_| UnsafeCell::new(None)).collect();
        Self {
            top: AtomicIsize::new(0),
            bottom: AtomicIsize::new(0),
            buffer: vec,
            mask: (capacity - 1) as isize,
        }
    }

    /// 仅由 Owner 调用：压入任务
    pub fn push(&self, item: T) -> Result<(), T> {
        let b = self.bottom.load(Ordering::Relaxed);
        let t = self.top.load(Ordering::Acquire);

        if b.wrapping_sub(t) > self.mask {
            return Err(item);
        }

        unsafe {
            let slot = self
                .buffer
                .get_unchecked((b & self.mask) as usize)
                .get_mut();
            ptr::write(slot, Some(item));
        }

        self.bottom.store(b.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// 仅由 Owner 调用：弹出任务 (LIFO)
    pub fn pop(&self) -> Option<T> {
        let b = self.bottom.load(Ordering::Relaxed).wrapping_sub(1);
        self.bottom.store(b, Ordering::Relaxed);

        fence(Ordering::SeqCst);

        let t = self.top.load(Ordering::Relaxed);

        if b.wrapping_sub(t) >= 0 {
            let item = unsafe {
                let slot = self.buffer.get_unchecked((b & self.mask) as usize).get();
                ptr::read(slot)
            };

            if t == b {
                if self
                    .top
                    .compare_exchange(t, t.wrapping_add(1), Ordering::SeqCst, Ordering::Relaxed)
                    .is_err()
                {
                    self.bottom.store(b.wrapping_add(1), Ordering::Relaxed);
                    return None;
                }
                self.bottom.store(b.wrapping_add(1), Ordering::Relaxed);
            }
            item
        } else {
            self.bottom.store(b.wrapping_add(1), Ordering::Relaxed);
            None
        }
    }

    /// 由 Stealers 调用：窃取任务 (FIFO)
    pub fn steal(&self) -> Steal<T> {
        let t = self.top.load(Ordering::Acquire);

        fence(Ordering::SeqCst);

        let b = self.bottom.load(Ordering::Acquire);

        if b.wrapping_sub(t) > 0 {
            let item = unsafe {
                let slot = self.buffer.get_unchecked((t & self.mask) as usize).get();
                ptr::read(slot)
            };

            if self
                .top
                .compare_exchange(t, t.wrapping_add(1), Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                match item {
                    Some(i) => Steal::Success(i),
                    None => Steal::Retry,
                }
            } else {
                Steal::Retry
            }
        } else {
            Steal::Empty
        }
    }

    /// 由 Stealers 调用：批量窃取任务
    pub fn steal_batch(&self) -> Steal<BatchStealResult<T>> {
        let t = self.top.load(Ordering::Acquire);

        fence(Ordering::SeqCst);

        let b = self.bottom.load(Ordering::Acquire);

        let n = b.wrapping_sub(t);
        if n > 0 {
            let num_to_steal = (n + 1) / 2;

            // 先拷贝到本地临时变量
            let mut temp = Vec::with_capacity(num_to_steal as usize);
            for i in 0..num_to_steal {
                let slot = unsafe {
                    self.buffer
                        .get_unchecked((t.wrapping_add(i) & self.mask) as usize)
                        .get()
                };
                let item = unsafe { ptr::read(slot) };
                temp.push(item);
            }

            if self
                .top
                .compare_exchange(
                    t,
                    t.wrapping_add(num_to_steal),
                    Ordering::SeqCst,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                let first_item = temp[0].expect("Deque was not empty");
                let mut overflow = Vec::with_capacity((num_to_steal - 1) as usize);
                for item_opt in temp.into_iter().skip(1) {
                    if let Some(item) = item_opt {
                        overflow.push(item);
                    }
                }
                return Steal::Success(BatchStealResult {
                    item: first_item,
                    overflow,
                });
            }
            return Steal::Retry;
        }
        Steal::Empty
    }

    pub fn is_empty(&self) -> bool {
        let b = self.bottom.load(Ordering::Relaxed);
        let t = self.top.load(Ordering::Relaxed);
        b.wrapping_sub(t) <= 0
    }
}

pub struct BatchStealResult<T> {
    pub item: T,
    pub overflow: Vec<T>,
}

pub enum Steal<T> {
    Success(T),
    Empty,
    Retry,
}
