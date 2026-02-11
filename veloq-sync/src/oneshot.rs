use crate::shim::Arc;
use crate::shim::atomic::AtomicUsize;
use crate::shim::cell::UnsafeCell;

use veloq_atomic_waker::AtomicWaker;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll};

/// Creates a new one-shot channel for sending a single value.
///
/// The function returns separate `Sender` and `Receiver` handles. The `Sender`
/// handle is used by the producer to send the value. The `Receiver` handle is
/// used by the consumer to receive the value.
///
/// Each handle can only be used once.
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(State::new().as_usize()),
        value: UnsafeCell::new(None),
        tx_task: AtomicWaker::new(),
        rx_task: AtomicWaker::new(),
    });

    let tx = Sender {
        inner: Some(inner.clone()),
    };
    let rx = Receiver { inner: Some(inner) };

    (tx, rx)
}

#[derive(Debug)]
pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

pub mod error {
    use std::fmt;

    /// Error returned by the `Future` implementation for `Receiver`.
    #[derive(Debug, Eq, PartialEq, Clone)]
    pub struct RecvError(pub ());

    /// Error returned by the `try_recv` function on `Receiver`.
    #[derive(Debug, Eq, PartialEq, Clone)]
    pub enum TryRecvError {
        /// The send half of the channel has not yet sent a value.
        Empty,
        /// The send half of the channel was dropped without sending a value.
        Closed,
    }

    impl fmt::Display for RecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "channel closed")
        }
    }

    impl std::error::Error for RecvError {}

    impl fmt::Display for TryRecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TryRecvError::Empty => write!(fmt, "channel empty"),
                TryRecvError::Closed => write!(fmt, "channel closed"),
            }
        }
    }

    impl std::error::Error for TryRecvError {}
}

use self::error::*;

struct Inner<T> {
    /// Manages the state of the inner cell.
    state: AtomicUsize,

    /// The value. This is set by `Sender` and read by `Receiver`.
    /// The state of the cell is tracked by `state`.
    value: UnsafeCell<Option<T>>,

    /// The task to notify when the receiver drops without consuming the value.
    tx_task: AtomicWaker,

    /// The task to notify when the value is sent.
    rx_task: AtomicWaker,
}

#[derive(Clone, Copy)]
struct State(usize);

// ===== impl Sender =====

impl<T> Sender<T> {
    /// Sends a value.
    ///
    /// This method consumes the sender, ensuring that it is only called once.
    ///
    /// If the receiver has already hung up, this method returns the error `Err(T)`.
    pub fn send(mut self, t: T) -> Result<(), T> {
        let inner = self
            .inner
            .take()
            .expect("Sender::inner cannot be None unless consumed");

        // Write the value to the unsafe cell.
        // SAFETY: We have not yet set the `VALUE_SENT` bit (via `complete`),
        // so we are the only one accessing the cell.
        unsafe { inner.value.with_mut(|ptr| *ptr = Some(t)) };

        // Attempt to transition the state to complete.
        if !inner.complete() {
            // If `complete()` returns false, the channel was closed by the receiver.
            // We must retrieve the value to return it to the caller.
            //
            // SAFETY: `complete()` failing implies the `CLOSED` bit is set.
            // When `CLOSED` is set, the receiver will strictly NOT access the value
            // (see `Receiver::try_recv` logic regarding priority).
            // Since we failed to set `VALUE_SENT`, the receiver considers the channel empty/closed.
            // Therefore, we have exclusive access to take the value back.
            unsafe {
                return Err(inner.consume_value().unwrap());
            }
        }

        Ok(())
    }

    /// Waits for the channel to be closed.
    pub async fn closed(&mut self) {
        use std::future::poll_fn;
        poll_fn(|cx| self.poll_closed(cx)).await;
    }

    /// Returns `true` if the receiver has closed the channel.
    pub fn is_closed(&self) -> bool {
        let inner = self.inner.as_ref().unwrap();
        let state = State::load(&inner.state, Ordering::Acquire);
        state.is_closed()
    }

    /// Polls to check if the receiver has closed the channel.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let inner = self.inner.as_ref().unwrap();

        // Fast path check
        if State::load(&inner.state, Ordering::Acquire).is_closed() {
            return Ready(());
        }

        inner.tx_task.register(cx.waker());

        // Double check after registration to avoid races
        if State::load(&inner.state, Ordering::Acquire).is_closed() {
            return Ready(());
        }

        Pending
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        // If `self.inner` is Some, it means `send` was not called.
        // We trigger completion (effectively sending `None`) to notify the receiver.
        if let Some(inner) = self.inner.as_ref() {
            inner.complete();
        }
    }
}

// ===== impl Receiver =====

impl<T> Receiver<T> {
    /// Prevents the channel from ever delivering a message.
    pub fn close(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.close();
        }
    }

    /// Returns true if the channel has terminated (inner is gone).
    pub fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }

    /// Checks if the channel is empty.
    ///
    /// Returns true if the value has not been sent yet or if the value has already been consumed.
    pub fn is_empty(&self) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return true;
        };

        let state = State::load(&inner.state, Ordering::Acquire);
        if state.is_complete() {
            // SAFETY: `is_complete` implies `VALUE_SENT` is set.
            // This synchronizes with the sender's writes.
            // Only the receiver can access now.
            unsafe { !inner.has_value() }
        } else {
            true
        }
    }

    /// Attempts to receive a value.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let inner = match self.inner.as_ref() {
            Some(inner) => inner,
            None => return Err(TryRecvError::Closed),
        };

        let state = State::load(&inner.state, Ordering::Acquire);

        if state.is_complete() {
            // SAFETY: `VALUE_SENT` is set, exclusive access granted to Receiver.
            match unsafe { inner.consume_value() } {
                Some(value) => {
                    // We successfully consumed the value, so we can drop the reference to inner.
                    self.inner = None;
                    Ok(value)
                }
                // Sender dropped without sending a value.
                None => {
                    self.inner = None;
                    Err(TryRecvError::Closed)
                }
            }
        } else if state.is_closed() {
            self.inner = None;
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            // Mark as closed to notify Sender.
            let state = inner.close();

            // If the sender had already completed sending, we are responsible for cleaning up the value.
            if state.is_complete() {
                // SAFETY: `VALUE_SENT` set, we own the data.
                unsafe { drop(inner.consume_value()) };
            }
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // If inner is None, we've already consumed the result or been polled to completion.
        let inner = self
            .inner
            .as_ref()
            .expect("Receiver polled after completion");

        // Fast path: check if ready without registering waker.
        let state = State::load(&inner.state, Ordering::Acquire);
        if state.is_complete() {
            // SAFETY: standard consume logic
            return match unsafe { inner.consume_value() } {
                Some(v) => {
                    self.inner = None;
                    Ready(Ok(v))
                }
                None => {
                    self.inner = None;
                    Ready(Err(RecvError(())))
                }
            };
        }

        if state.is_closed() {
            self.inner = None;
            return Ready(Err(RecvError(())));
        }

        // Register waker
        inner.rx_task.register(cx.waker());

        // Double check state
        let state = State::load(&inner.state, Ordering::Acquire);
        if state.is_complete() {
            match unsafe { inner.consume_value() } {
                Some(v) => {
                    self.inner = None;
                    Ready(Ok(v))
                }
                None => {
                    self.inner = None;
                    Ready(Err(RecvError(())))
                }
            }
        } else if state.is_closed() {
            self.inner = None;
            Ready(Err(RecvError(())))
        } else {
            Pending
        }
    }
}

// ===== impl Inner =====

impl<T> Inner<T> {
    /// Try to set the state to complete. Returns `true` if successful, `false` if closed.
    fn complete(&self) -> bool {
        let prev = State::set_complete(&self.state);

        if prev.is_closed() {
            return false;
        }

        // Notify the receiver task.
        self.rx_task.wake();
        true
    }

    /// Set the state to closed and notify the sender logic.
    fn close(&self) -> State {
        let prev = State::set_closed(&self.state);
        // Notify the sender task (waiting in `closed()`).
        self.tx_task.wake();
        prev
    }

    /// Consumes the value.
    ///
    /// # Safety
    /// Must only be called if `VALUE_SENT` is set, or if we have guaranteed exclusive access
    /// (e.g., inside `Sender::send` failure path).
    unsafe fn consume_value(&self) -> Option<T> {
        unsafe { self.value.with_mut(|ptr| (*ptr).take()) }
    }

    /// Returns true if there is a value.
    ///
    /// # Safety
    /// Must only be called if `VALUE_SENT` is set.
    unsafe fn has_value(&self) -> bool {
        unsafe { self.value.with(|ptr| (*ptr).is_some()) }
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // SAFETY: `Inner` is dropping, meaning the refcount is 0.
        // We have exclusive access to the `UnsafeCell`.
        // We must ensure the contained value is dropped to avoid memory leaks.
        unsafe {
            self.value.with_mut(|ptr| {
                let _ = (*ptr).take();
            });
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Inner<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Inner")
            .field("state", &State::load(&self.state, Ordering::Relaxed))
            .finish()
    }
}

// ===== State Management =====

const VALUE_SENT: usize = 0b00010;
const CLOSED: usize = 0b00100;

impl State {
    fn new() -> State {
        State(0)
    }

    fn is_complete(self) -> bool {
        self.0 & VALUE_SENT == VALUE_SENT
    }

    fn is_closed(self) -> bool {
        self.0 & CLOSED == CLOSED
    }

    fn set_complete(cell: &AtomicUsize) -> State {
        let mut state = cell.load(Ordering::Relaxed);
        loop {
            if State(state).is_closed() {
                break;
            }

            match cell.compare_exchange_weak(
                state,
                state | VALUE_SENT,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => state = actual,
            }
        }
        State(state)
    }

    fn set_closed(cell: &AtomicUsize) -> State {
        // Using AcqRel ensures that:
        // 1. Release: The closed state is published to other threads.
        // 2. Acquire: We see any state changes that happened before this (though less critical for close).
        let val = cell.fetch_or(CLOSED, Ordering::AcqRel);
        State(val)
    }

    fn as_usize(self) -> usize {
        self.0
    }

    fn load(cell: &AtomicUsize, order: Ordering) -> State {
        State(cell.load(order))
    }
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("State")
            .field("is_complete", &self.is_complete())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}
