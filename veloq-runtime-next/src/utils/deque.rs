use std::cell::UnsafeCell;
use std::num::NonZeroUsize;
use std::ptr;
use std::sync::atomic::{AtomicIsize, Ordering};

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

        if b - t > self.mask {
            // 队列已满，返回任务交由 Injector 处理
            return Err(item);
        }

        unsafe {
            let slot = self.buffer.get_unchecked((b & self.mask) as usize).get();
            ptr::write(slot, Some(item));
        }

        self.bottom.store(b + 1, Ordering::Release);
        Ok(())
    }

    /// 仅由 Owner 调用：弹出任务 (LIFO)
    pub fn pop(&self) -> Option<T> {
        let b = self.bottom.load(Ordering::Relaxed) - 1;
        self.bottom.store(b, Ordering::Relaxed);

        // 必须有内存屏障，确保 bottom 的修改对 Stealer 可见
        std::sync::atomic::fence(Ordering::SeqCst);

        let t = self.top.load(Ordering::Relaxed);

        if t <= b {
            // 队列非空
            let item = unsafe {
                let slot = self.buffer.get_unchecked((b & self.mask) as usize).get();
                ptr::read(slot)
            };

            if t == b {
                // 竞争最后一个任务
                if self
                    .top
                    .compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed)
                    .is_err()
                {
                    // 窃取者赢了
                    self.bottom.store(b + 1, Ordering::Relaxed);
                    return None;
                }
                self.bottom.store(b + 1, Ordering::Relaxed);
            }
            item
        } else {
            // 队列已空
            self.bottom.store(b + 1, Ordering::Relaxed);
            None
        }
    }

    /// 由 Stealers 调用：窃取任务 (FIFO)
    pub fn steal(&self) -> Steal<T> {
        let t = self.top.load(Ordering::Acquire);

        // 确保在读取 bottom 之前看到 top
        std::sync::atomic::fence(Ordering::SeqCst);

        let b = self.bottom.load(Ordering::Acquire);

        if t < b {
            // 队列非空
            let item = unsafe {
                let slot = self.buffer.get_unchecked((t & self.mask) as usize).get();
                ptr::read(slot)
            };

            if self
                .top
                .compare_exchange(t, t + 1, Ordering::SeqCst, Ordering::Relaxed)
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
    pub fn steal_batch(&self, dest: &Deque<T>) -> Steal<T> {
        let t = self.top.load(Ordering::Acquire);

        // 确保在读取 bottom 之前看到 top
        std::sync::atomic::fence(Ordering::SeqCst);

        let b = self.bottom.load(Ordering::Acquire);

        if t < b {
            let n = b - t;
            // 窃取一半任务，至少 1 个
            let num_to_steal = (n + 1) / 2;

            if self
                .top
                .compare_exchange(t, t + num_to_steal, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
            {
                let first_slot =
                    unsafe { self.buffer.get_unchecked((t & self.mask) as usize).get() };
                let first_item = unsafe { ptr::read(first_slot) }.expect("Deque was not empty");

                for i in 1..num_to_steal {
                    let slot = unsafe {
                        self.buffer
                            .get_unchecked(((t + i) & self.mask) as usize)
                            .get()
                    };
                    if let Some(item) = unsafe { ptr::read(slot) } {
                        // 压入窃取者的队列。因为窃取者是 dest 的 owner，所以安全。
                        let _ = dest.push(item);
                    }
                }
                return Steal::Success(first_item);
            }
            return Steal::Retry;
        }
        Steal::Empty
    }

    pub fn is_empty(&self) -> bool {
        let b = self.bottom.load(Ordering::Relaxed);
        let t = self.top.load(Ordering::Relaxed);
        t >= b
    }
}

pub enum Steal<T> {
    Success(T),
    Empty,
    Retry,
}
