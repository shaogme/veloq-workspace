use crate::{
    fmt,
    sync::{UnpoisonedCondvar, UnpoisonedMutex},
};

/// 用于同步多个线程到达同一执行点的屏障。
///
/// 屏障会阻塞调用 [`Barrier::wait`] 的线程，直到指定数量的线程都到达，
/// 然后同时唤醒这一轮的所有线程。屏障在每一轮完成后可以重复使用。
pub struct Barrier {
    lock: UnpoisonedMutex<BarrierState>,
    cvar: UnpoisonedCondvar,
    num_threads: usize,
}

struct BarrierState {
    count: usize,
    generation_id: usize,
}

/// [`Barrier::wait`] 返回的结果。
pub struct BarrierWaitResult(bool);

impl fmt::Debug for Barrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Barrier").finish_non_exhaustive()
    }
}

impl Barrier {
    /// 创建一个需要指定数量线程到达的屏障。
    #[cfg(not(feature = "loom"))]
    pub const fn new(n: usize) -> Self {
        Self {
            lock: UnpoisonedMutex::new(BarrierState {
                count: 0,
                generation_id: 0,
            }),
            cvar: UnpoisonedCondvar::new(),
            num_threads: n,
        }
    }

    /// 创建一个需要指定数量线程到达的屏障。
    #[cfg(feature = "loom")]
    pub fn new(n: usize) -> Self {
        Self {
            lock: UnpoisonedMutex::new(BarrierState {
                count: 0,
                generation_id: 0,
            }),
            cvar: UnpoisonedCondvar::new(),
            num_threads: n,
        }
    }

    /// 阻塞当前线程，直到本轮所有线程都到达屏障。
    ///
    /// 每轮恰有一个任意线程会获得 leader 结果，其余线程获得普通结果。
    pub fn wait(&self) -> BarrierWaitResult {
        let mut state = self.lock.lock();
        let local_generation = state.generation_id;
        state.count += 1;

        if state.count < self.num_threads {
            while state.generation_id == local_generation {
                state = self.cvar.wait(state);
            }
            BarrierWaitResult(false)
        } else {
            state.count = 0;
            state.generation_id = state.generation_id.wrapping_add(1);
            self.cvar.notify_all();
            BarrierWaitResult(true)
        }
    }
}

impl fmt::Debug for BarrierWaitResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BarrierWaitResult")
            .field("is_leader", &self.is_leader())
            .finish()
    }
}

impl BarrierWaitResult {
    /// 返回当前线程是否是本轮屏障的 leader。
    #[must_use]
    pub fn is_leader(&self) -> bool {
        self.0
    }
}
