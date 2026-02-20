use std::cell::RefCell;
use std::cell::UnsafeCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::task::{Context, Poll, Waker};
use tracing::debug;

/// A handle to a spawned task.
pub struct LocalJoinHandle<T> {
    state: Rc<RefCell<LocalJoinState<T>>>,
}

enum LocalJoinState<T> {
    Pending(Option<Waker>),
    Ready(T),
    Aborted,
    Consumed,
}

impl<T> LocalJoinHandle<T> {
    pub(crate) fn new() -> (Self, LocalJoinProducer<T>) {
        let state = Rc::new(RefCell::new(LocalJoinState::Pending(None)));
        (
            Self {
                state: state.clone(),
            },
            LocalJoinProducer { state },
        )
    }
}

impl<T> Future for LocalJoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();
        match &*state {
            LocalJoinState::Pending(_) => {
                *state = LocalJoinState::Pending(Some(cx.waker().clone()));
                Poll::Pending
            }
            LocalJoinState::Ready(_) => {
                if let LocalJoinState::Ready(val) =
                    std::mem::replace(&mut *state, LocalJoinState::Consumed)
                {
                    Poll::Ready(val)
                } else {
                    unreachable!()
                }
            }
            LocalJoinState::Aborted => panic!("LocalJoinHandle: task failed"),
            LocalJoinState::Consumed => panic!("LocalJoinHandle: polled after completion"),
        }
    }
}

pub(crate) struct LocalJoinProducer<T> {
    state: Rc<RefCell<LocalJoinState<T>>>,
}

impl<T> LocalJoinProducer<T> {
    pub(crate) fn set(self, value: T) {
        let mut state = self.state.borrow_mut();
        if let LocalJoinState::Pending(Some(w)) =
            std::mem::replace(&mut *state, LocalJoinState::Ready(value))
        {
            w.wake();
        }
    }
}

impl<T> Drop for LocalJoinProducer<T> {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        if let LocalJoinState::Pending(waker) = &*state {
            debug!("LocalJoinProducer dropped -> task aborted");
            let waker = waker.clone();
            *state = LocalJoinState::Aborted;
            if let Some(w) = waker {
                w.wake();
            }
        }
    }
}

/// A handle to a spawned task (Send).
pub struct JoinHandle<T> {
    state: Arc<JoinState<T>>,
}

// States for JoinState
const IDLE: u8 = 0;
const WAITING: u8 = 1;
const READY: u8 = 2;
const ABORTED: u8 = 3;

struct JoinState<T> {
    state: AtomicU8,
    value: UnsafeCell<Option<T>>,
    waker: UnsafeCell<Option<Waker>>,
}

unsafe impl<T: Send> Sync for JoinState<T> {}
unsafe impl<T: Send> Send for JoinState<T> {}

impl<T> JoinHandle<T> {
    pub(crate) fn new() -> (Self, JoinProducer<T>) {
        let state = Arc::new(JoinState {
            state: AtomicU8::new(IDLE),
            value: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
        });
        (
            Self {
                state: state.clone(),
            },
            JoinProducer { state },
        )
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = &self.state;

        loop {
            let current = state.state.load(Ordering::Acquire);

            if current == READY {
                // SAFETY: State is READY, producer has finished writing value.
                // We are the only consumer.
                let val = unsafe { (*state.value.get()).take() };
                if let Some(v) = val {
                    return Poll::Ready(v);
                } else {
                    panic!("JoinHandle: polled after completion");
                }
            } else if current == ABORTED {
                panic!("JoinHandle: task failed");
            }

            // If we are currently WAITING, try to reset to IDLE to update the waker.
            // This is necessary if poll is called multiple times with different wakers.
            if current == WAITING
                && state
                    .state
                    .compare_exchange(WAITING, IDLE, Ordering::Acquire, Ordering::Relaxed)
                    .is_err()
            {
                // State changed (likely to READY or ABORTED), retry loop
                continue;
            }

            // Now state is effectively IDLE (either we saw IDLE or we successfully transitioned WAITING -> IDLE).
            // It is safe to update the waker because Producer only reads waker if it sees WAITING.
            // Since we are single-consumer, no other polling thread is writing.

            unsafe {
                *state.waker.get() = Some(cx.waker().clone());
            }

            // Publish our waiting state.
            // We use compare_exchange because Producer might have transitioned IDLE -> READY/ABORTED while we were working.
            if state
                .state
                .compare_exchange(IDLE, WAITING, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                return Poll::Pending;
            }

            // If CAS failed, state must have changed to READY/ABORTED. Loop will handle it.
        }
    }
}

pub(crate) struct JoinProducer<T> {
    state: Arc<JoinState<T>>,
}

impl<T> JoinProducer<T> {
    pub(crate) fn set(self, value: T) {
        let state = &self.state;

        // Write value first
        unsafe {
            *state.value.get() = Some(value);
        }

        // Publish READY state
        let old_state = state.state.swap(READY, Ordering::AcqRel);

        if old_state == WAITING {
            // Consumer was waiting, wake them up.
            // SAFETY: We observed WAITING, so Consumer has finished writing waker.
            // We transitioned to READY, so Consumer will not touch waker again until they see READY.
            let waker = unsafe { (*state.waker.get()).take() };
            if let Some(w) = waker {
                w.wake();
            }
        }

        // Prevent Drop from running to avoid double-state-change logic (though set consumes self so Drop not called on it)
        // However, we must ensure we don't accidentally fall into Drop if we are using ManuallyDrop or something later.
        // Currently self is consumed, so Drop is NOT called. Correct.
    }
}

impl<T> Drop for JoinProducer<T> {
    fn drop(&mut self) {
        let state = &self.state;

        // Only switch to ABORTED if we are NOT already READY.
        // We need a CAS loop to ensure we don't overwrite READY.
        let mut current = state.state.load(Ordering::Acquire);
        loop {
            if current == READY || current == ABORTED {
                return;
            }

            // Try to transition to ABORTED
            match state.state.compare_exchange(
                current,
                ABORTED,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    debug!("JoinProducer dropped -> task aborted");
                    // Success detected.
                    if current == WAITING {
                        let waker = unsafe { (*state.waker.get()).take() };
                        if let Some(w) = waker {
                            w.wake();
                        }
                    }
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }
}
