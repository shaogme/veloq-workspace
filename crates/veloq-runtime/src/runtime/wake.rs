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
        context::{IdleWaitStrategy, WaitBackend},
        primitives::{Parker, ParkerInner, Signal, Unparker},
    },
};
use diagweave::Transform;

pub trait ExternalWake: Send + Sync {
    fn wake(&self);
}

const WAIT_MODE_ACTIVE: u8 = 0;
const WAIT_MODE_WORKER_RUNTIME: u8 = 1;
const WAIT_MODE_WORKER_DRIVER: u8 = 2;
const WAIT_MODE_BLOCK_ON_RUNTIME: u8 = 3;
const WAIT_MODE_BLOCK_ON_DRIVER: u8 = 4;

pub struct WakeCoordinator {
    epoch: AtomicU64,
    wait_mode: AtomicU8,
    parker: Arc<ParkerInner>,
    block_on: Signal,
    external_wake: OnceLock<Arc<dyn ExternalWake>>,
}

impl WakeCoordinator {
    pub(crate) fn new() -> Self {
        Self {
            epoch: AtomicU64::new(0),
            wait_mode: AtomicU8::new(WAIT_MODE_ACTIVE),
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
    pub(crate) fn notify(&self) {
        // Publish a new wake epoch before routing the underlying wake so waiters
        // can detect notifications that happen during their own poll path.
        let _ = self.epoch.fetch_add(1, Ordering::AcqRel);

        match self.wait_mode.load(Ordering::Acquire) {
            WAIT_MODE_WORKER_DRIVER | WAIT_MODE_BLOCK_ON_DRIVER => {
                if let Some(wake) = self.external_wake.get() {
                    wake.wake();
                } else {
                    self.local_unpark();
                }
            }
            WAIT_MODE_BLOCK_ON_RUNTIME => {
                self.block_on.notify();
            }
            WAIT_MODE_ACTIVE | WAIT_MODE_WORKER_RUNTIME | _ => {
                self.local_unpark();
            }
        }
    }

    pub(crate) fn wait_worker<F>(
        &self,
        observed_epoch: u64,
        backend: WaitBackend,
        strategy: IdleWaitStrategy,
        completion_wait: bool,
        external_wait: F,
    ) -> Result<()>
    where
        F: FnOnce(IdleWaitStrategy) -> Result<()>,
    {
        let mode = match backend {
            WaitBackend::RuntimePark => WAIT_MODE_WORKER_RUNTIME,
            WaitBackend::Driver => WAIT_MODE_WORKER_DRIVER,
        };
        let Some(_guard) = self.begin_wait(mode, observed_epoch) else {
            return Ok(());
        };

        match backend {
            WaitBackend::RuntimePark => {
                self.wait_runtime(strategy, completion_wait);
                Ok(())
            }
            WaitBackend::Driver => external_wait(strategy),
        }
    }

    pub(crate) fn wait_block_on<F>(
        &self,
        observed_epoch: u64,
        backend: WaitBackend,
        strategy: IdleWaitStrategy,
        external_wait: F,
    ) -> Result<()>
    where
        F: FnOnce(IdleWaitStrategy) -> Result<()>,
    {
        let mode = match backend {
            WaitBackend::RuntimePark => WAIT_MODE_BLOCK_ON_RUNTIME,
            WaitBackend::Driver => WAIT_MODE_BLOCK_ON_DRIVER,
        };
        let Some(_guard) = self.begin_wait(mode, observed_epoch) else {
            return Ok(());
        };

        match backend {
            WaitBackend::RuntimePark => {
                self.wait_block_signal(strategy);
                Ok(())
            }
            WaitBackend::Driver => external_wait(strategy),
        }
    }

    fn begin_wait(&self, mode: u8, observed_epoch: u64) -> Option<WaitGuard<'_>> {
        loop {
            if self.epoch.load(Ordering::Acquire) != observed_epoch {
                return None;
            }

            match self.wait_mode.compare_exchange(
                WAIT_MODE_ACTIVE,
                mode,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(WAIT_MODE_ACTIVE) => continue,
                Err(_) => {
                    return None;
                }
            }
        }

        if self.epoch.load(Ordering::Acquire) != observed_epoch {
            self.wait_mode.store(WAIT_MODE_ACTIVE, Ordering::Release);
            return None;
        }

        Some(WaitGuard { wake: self, mode })
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

struct WaitGuard<'a> {
    wake: &'a WakeCoordinator,
    mode: u8,
}

impl Drop for WaitGuard<'_> {
    fn drop(&mut self) {
        let _ = self.wake.wait_mode.compare_exchange(
            self.mode,
            WAIT_MODE_ACTIVE,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
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
        Arc::from_raw(p as *const WakeCoordinator).notify();
    },
    |p| unsafe {
        let wake = ManuallyDrop::new(Arc::from_raw(p as *const WakeCoordinator));
        wake.notify();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const WakeCoordinator));
    },
);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime::context::IdleWaitStrategy;

    #[test]
    fn wait_is_skipped_when_epoch_changes_before_arm() {
        let wake = WakeCoordinator::new();
        let epoch = wake.current_epoch();
        wake.notify();

        let mut called = false;
        let res = wake.wait_worker(
            epoch,
            WaitBackend::Driver,
            IdleWaitStrategy::block(),
            false,
            |_| {
                called = true;
                Ok(())
            },
        );

        assert!(res.is_ok());
        assert!(!called);
    }
}
