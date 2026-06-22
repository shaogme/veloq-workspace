use std::{
    mem::ManuallyDrop,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

use crate::{
    error::{Result, RuntimeError},
    runtime::{
        context::IdleWaitStrategy,
        primitives::{Parker, ParkerInner, Signal, Unparker},
    },
};
use diagweave::Transform;

pub trait ExternalWake: Send + Sync {
    fn wake(&self);
}

const DRIVER_WAIT_WORKER: u8 = 1 << 0;
const DRIVER_WAIT_BLOCK_ON: u8 = 1 << 1;

pub struct WakeCoordinator {
    epoch: AtomicU64,
    driver_waits: AtomicU8,
    parker: Arc<ParkerInner>,
    block_on: Signal,
    external_wake: OnceLock<Arc<dyn ExternalWake>>,
}

impl WakeCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            driver_waits: AtomicU8::new(0),
            parker: Arc::new(ParkerInner {
                state: AtomicU32::new(0),
            }),
            block_on: Signal::new(false),
            external_wake: OnceLock::new(),
        }
    }

    #[inline]
    pub(crate) fn bind_external_wake(&self, wake: Arc<dyn ExternalWake>) -> Result<()> {
        if self.external_wake.set(wake).is_err() {
            return RuntimeError::InvariantViolation {
                site: "WakeCoordinator::bind_external_wake",
                detail: "external wake already bound",
            }
            .trans();
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn current_epoch(&self) -> u64 {
        self.epoch.load(Ordering::Acquire)
    }

    #[inline]
    pub(crate) fn notify_runtime_progress(&self) {
        // Runtime progress is always published locally first so block_on/runtime
        // waiters cannot miss a wake just because the current wait path happens
        // to be a driver wait.
        let _ = self.epoch.fetch_add(1, Ordering::AcqRel);
        self.local_unpark();

        if self.driver_waits.load(Ordering::Acquire) != 0
            && let Some(wake) = self.external_wake.get()
        {
            wake.wake();
        }
    }

    pub(crate) fn wait_worker_runtime(
        &self,
        observed_epoch: u64,
        strategy: IdleWaitStrategy,
        completion_wait: bool,
    ) {
        if self.epoch.load(Ordering::Acquire) != observed_epoch {
            return;
        }
        self.wait_runtime(strategy, completion_wait);
    }

    pub(crate) fn wait_worker_driver<F>(
        &self,
        observed_epoch: u64,
        strategy: IdleWaitStrategy,
        external_wait: F,
    ) -> Result<()>
    where
        F: FnOnce(IdleWaitStrategy) -> Result<()>,
    {
        let Some(_guard) = self.begin_driver_wait(DRIVER_WAIT_WORKER, observed_epoch) else {
            return Ok(());
        };
        external_wait(strategy)
    }

    pub(crate) fn wait_block_on_runtime(&self, observed_epoch: u64, strategy: IdleWaitStrategy) {
        if self.epoch.load(Ordering::Acquire) != observed_epoch {
            return;
        }
        self.wait_block_signal(strategy);
    }

    pub(crate) fn wait_block_on_driver<F>(
        &self,
        observed_epoch: u64,
        strategy: IdleWaitStrategy,
        external_wait: F,
    ) -> Result<()>
    where
        F: FnOnce(IdleWaitStrategy) -> Result<()>,
    {
        let Some(_guard) = self.begin_driver_wait(DRIVER_WAIT_BLOCK_ON, observed_epoch) else {
            return Ok(());
        };
        external_wait(strategy)
    }

    fn begin_driver_wait(&self, bit: u8, observed_epoch: u64) -> Option<DriverWaitGuard<'_>> {
        loop {
            if self.epoch.load(Ordering::Acquire) != observed_epoch {
                return None;
            }

            let waits = self.driver_waits.load(Ordering::Acquire);
            match self.driver_waits.compare_exchange(
                waits,
                waits | bit,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        if self.epoch.load(Ordering::Acquire) != observed_epoch {
            self.driver_waits.fetch_and(!bit, Ordering::AcqRel);
            return None;
        }

        Some(DriverWaitGuard { wake: self, bit })
    }

    fn wait_runtime(&self, strategy: IdleWaitStrategy, completion_wait: bool) {
        let parker = Parker::from_inner(self.parker.clone());
        match strategy {
            IdleWaitStrategy::Timeout(duration) => {
                let _ = parker.park_timeout(duration);
            }
            IdleWaitStrategy::Block => {
                if completion_wait {
                    let _ = parker.park_timeout(std::time::Duration::from_millis(1));
                } else {
                    parker.park();
                }
            }
        }
    }

    fn wait_block_signal(&self, strategy: IdleWaitStrategy) {
        match strategy {
            IdleWaitStrategy::Timeout(duration) => {
                let _ = self.block_on.wait_timeout(duration);
            }
            IdleWaitStrategy::Block => {
                self.block_on.wait();
            }
        }
    }

    fn local_unpark(&self) {
        Unparker::from_inner(self.parker.clone()).unpark();
        self.block_on.notify();
    }
}

struct DriverWaitGuard<'a> {
    wake: &'a WakeCoordinator,
    bit: u8,
}

impl Drop for DriverWaitGuard<'_> {
    fn drop(&mut self) {
        self.wake
            .driver_waits
            .fetch_and(!self.bit, Ordering::AcqRel);
    }
}

pub(crate) fn create_runtime_waker(wake: Arc<WakeCoordinator>) -> Waker {
    let raw = Arc::into_raw(wake) as *const ();
    unsafe { Waker::from_raw(RawWaker::new(raw, &WAKE_VTABLE)) }
}

static WAKE_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const WakeCoordinator);
        RawWaker::new(p, &WAKE_VTABLE)
    },
    |p| unsafe {
        Arc::from_raw(p as *const WakeCoordinator).notify_runtime_progress();
    },
    |p| unsafe {
        let wake = ManuallyDrop::new(Arc::from_raw(p as *const WakeCoordinator));
        wake.notify_runtime_progress();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const WakeCoordinator));
    },
);

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    struct CountingWake {
        hits: AtomicUsize,
    }

    impl CountingWake {
        fn new() -> Self {
            Self {
                hits: AtomicUsize::new(0),
            }
        }
    }

    impl ExternalWake for CountingWake {
        fn wake(&self) {
            self.hits.fetch_add(1, Ordering::AcqRel);
        }
    }

    #[test]
    fn wait_is_skipped_when_epoch_changes_before_arm() {
        let wake = WakeCoordinator::new();
        let epoch = wake.current_epoch();
        wake.notify_runtime_progress();

        let mut called = false;
        let res = wake.wait_worker_driver(epoch, IdleWaitStrategy::block(), |_| {
            called = true;
            Ok(())
        });

        assert!(res.is_ok());
        assert!(!called);
    }

    #[test]
    fn runtime_progress_notifies_external_wake_while_driver_wait_is_armed() {
        let wake = WakeCoordinator::new();
        let external = Arc::new(CountingWake::new());
        wake.bind_external_wake(external.clone()).unwrap();
        let epoch = wake.current_epoch();

        let res = wake.wait_worker_driver(epoch, IdleWaitStrategy::block(), |_| {
            wake.notify_runtime_progress();
            Ok(())
        });

        assert!(res.is_ok());
        assert_eq!(external.hits.load(Ordering::Acquire), 1);
        assert!(wake.current_epoch() > epoch);
    }
}
