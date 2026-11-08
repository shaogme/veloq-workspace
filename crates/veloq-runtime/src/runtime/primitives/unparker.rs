use super::{Signal, WaitError, WakeFailureState};
use crate::error::RuntimeWakeError;
use veloq_std::{
    mem::ManuallyDrop,
    result::Result as StdResult,
    sync::{NativeArc as Arc, NativeWeak as Weak, OnceLock},
    task::{RawWaker, RawWakerVTable, Waker},
    time::Duration,
};

pub trait RuntimeWaker: Send + Sync {
    fn wake(&self) -> StdResult<(), RuntimeWakeError>;
}

pub(crate) struct UnparkerInner {
    signal: Signal,
    waker: OnceLock<Arc<dyn RuntimeWaker>>,
    failure: Arc<WakeFailureState>,
}

static UNPARK_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |p| unsafe {
        Arc::increment_strong_count(p as *const UnparkerInner);
        RawWaker::new(p, &UNPARK_VTABLE)
    },
    |p| unsafe {
        let inner = Arc::from_raw(p as *const UnparkerInner);
        inner.wake_from_raw();
    },
    |p| unsafe {
        let inner = ManuallyDrop::new(Arc::from_raw(p as *const UnparkerInner));
        inner.wake_from_raw();
    },
    |p| unsafe {
        drop(Arc::from_raw(p as *const UnparkerInner));
    },
);

impl UnparkerInner {
    fn wake(&self) -> StdResult<(), RuntimeWakeError> {
        self.signal.notify();
        if self.failure.is_failed()
            && let Some(error) = self.failure.first_error()
        {
            return Err(error);
        }
        if let Some(waker) = self.waker.get()
            && let Err(error) = waker.wake()
        {
            self.failure.record(error.clone());
            return Err(error);
        }
        Ok(())
    }

    fn wake_from_raw(&self) {
        if self.wake().is_err() {
            // The raw Waker ABI has no return channel for the stored wake failure.
        }
    }

    fn shutdown_wake(&self) {
        self.signal.notify();
        if let Some(waker) = self.waker.get() {
            let _ = waker.wake();
        }
    }
}

#[derive(Clone)]
pub struct Unparker {
    inner: Arc<UnparkerInner>,
}

impl Default for Unparker {
    fn default() -> Self {
        Self::new()
    }
}

impl Unparker {
    pub fn into_waker(self) -> Waker {
        let raw = Arc::into_raw(self.inner) as *const ();
        unsafe { Waker::from_raw(RawWaker::new(raw, &UNPARK_VTABLE)) }
    }

    pub fn new() -> Self {
        Self::with_failure_state(Arc::new(WakeFailureState::new(Weak::new())))
    }

    pub(crate) fn with_failure_state(failure: Arc<WakeFailureState>) -> Self {
        Self {
            inner: Arc::new(UnparkerInner {
                signal: Signal::new(false),
                waker: OnceLock::new(),
                failure,
            }),
        }
    }

    pub fn bind(&self, waker: Arc<dyn RuntimeWaker>) -> StdResult<(), RuntimeWakeError> {
        self.inner.waker.set(waker).map_err(|_| {
            RuntimeWakeError::new(
                "runtime",
                usize::MAX,
                "bind",
                "remote waker is already bound",
            )
        })
    }

    pub fn unpark(&self) -> StdResult<(), RuntimeWakeError> {
        self.inner.wake()
    }

    pub(crate) fn shutdown_wake(&self) {
        self.inner.shutdown_wake();
    }

    pub(crate) fn park(&self) -> StdResult<(), WaitError> {
        self.inner.signal.wait()
    }

    pub(crate) fn park_timeout(&self, timeout: Duration) -> StdResult<bool, WaitError> {
        self.inner.signal.wait_timeout(timeout)
    }

    #[cfg(test)]
    pub(crate) fn signal_is_notified(&self) -> bool {
        self.inner.signal.is_notified()
    }

    #[cfg(test)]
    pub(crate) fn failure_count(&self) -> u64 {
        self.inner.failure.error_count()
    }

    #[cfg(test)]
    pub(crate) fn first_failure(&self) -> Option<RuntimeWakeError> {
        self.inner.failure.first_error()
    }

    #[cfg(test)]
    pub(crate) fn has_failure(&self) -> bool {
        self.inner.failure.is_failed()
    }
}
