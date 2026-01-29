use crate::shim::Arc;
use crate::shim::atomic::AtomicUsize;
use crate::shim::cell::UnsafeCell;

use veloq_atomic_waker::AtomicWaker;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering::{self, Acquire};
use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll, ready};

#[derive(Debug)]
pub struct Sender<T> {
    inner: Option<Arc<Inner<T>>>,
}

#[derive(Debug)]
pub struct Receiver<T> {
    inner: Option<Arc<Inner<T>>>,
}

pub mod error {
    //! `Oneshot` error types.

    use std::fmt;

    /// Error returned by the `Future` implementation for `Receiver`.
    ///
    /// This error is returned by the receiver when the sender is dropped without sending.
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

    // ===== impl RecvError =====

    impl fmt::Display for RecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(fmt, "channel closed")
        }
    }

    impl std::error::Error for RecvError {}

    // ===== impl TryRecvError =====

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

    /// The value. This is set by `Sender` and read by `Receiver`. The state of
    /// the cell is tracked by `state`.
    value: UnsafeCell<Option<T>>,

    /// The task to notify when the receiver drops without consuming the value.
    tx_task: AtomicWaker,

    /// The task to notify when the value is sent.
    rx_task: AtomicWaker,
}

#[derive(Clone, Copy)]
struct State(usize);

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

impl<T> Sender<T> {
    pub fn send(mut self, t: T) -> Result<(), T> {
        let inner = self.inner.take().unwrap();

        unsafe { inner.value.with_mut(|ptr| *ptr = Some(t)) };

        if !inner.complete() {
            unsafe {
                // SAFETY: The receiver will not access the `UnsafeCell` unless
                // the channel has been marked as "complete". Calling
                // `complete()` will return true if this bit is set, and false
                // if it is not set. Thus, if `complete()` returned false, it is
                // safe for us to access the value, because we know that the
                // receiver will not.
                return Err(inner.consume_value().unwrap());
            }
        }

        Ok(())
    }

    pub async fn closed(&mut self) {
        use std::future::poll_fn;

        let closed = poll_fn(|cx| self.poll_closed(cx));

        closed.await;
    }

    pub fn is_closed(&self) -> bool {
        let inner = self.inner.as_ref().unwrap();

        let state = State::load(&inner.state, Acquire);
        state.is_closed()
    }

    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let inner = self.inner.as_ref().unwrap();

        let state = State::load(&inner.state, Acquire);

        if state.is_closed() {
            return Ready(());
        }

        inner.tx_task.register(cx.waker());

        let state = State::load(&inner.state, Acquire);
        if state.is_closed() {
            return Ready(());
        }

        Pending
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.complete();
        }
    }
}

impl<T> Receiver<T> {
    pub fn close(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            inner.close();
        }
    }

    pub fn is_terminated(&self) -> bool {
        self.inner.is_none()
    }

    pub fn is_empty(&self) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            // The channel has already terminated.
            return true;
        };

        let state = State::load(&inner.state, Acquire);
        if state.is_complete() {
            // SAFETY: If `state.is_complete()` returns true, then the
            // `VALUE_SENT` bit has been set and the sender side of the
            // channel will no longer attempt to access the inner
            // `UnsafeCell`. Therefore, it is now safe for us to access the
            // cell.
            //
            // The channel is empty if it does not have a value.
            unsafe { !inner.has_value() }
        } else {
            // The receiver closed the channel or no value has been sent yet.
            true
        }
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let result = if let Some(inner) = self.inner.as_ref() {
            let state = State::load(&inner.state, Acquire);

            if state.is_complete() {
                // SAFETY: If `state.is_complete()` returns true, then the
                // `VALUE_SENT` bit has been set and the sender side of the
                // channel will no longer attempt to access the inner
                // `UnsafeCell`. Therefore, it is now safe for us to access the
                // cell.
                match unsafe { inner.consume_value() } {
                    Some(value) => Ok(value),
                    None => Err(TryRecvError::Closed),
                }
            } else if state.is_closed() {
                Err(TryRecvError::Closed)
            } else {
                // Not ready, this does not clear `inner`
                return Err(TryRecvError::Empty);
            }
        } else {
            Err(TryRecvError::Closed)
        };

        self.inner = None;
        result
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.as_ref() {
            let state = inner.close();

            if state.is_complete() {
                // SAFETY: we have ensured that the `VALUE_SENT` bit has been set,
                // so only the receiver can access the value.
                drop(unsafe { inner.consume_value() });
            }
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let ret = if let Some(inner) = self.as_ref().get_ref().inner.as_ref() {
            let res = ready!(inner.poll_recv(cx)).map_err(Into::into);

            res
        } else {
            panic!("called after complete");
        };

        self.inner = None;
        Ready(ret)
    }
}

impl<T> Inner<T> {
    fn complete(&self) -> bool {
        let prev = State::set_complete(&self.state);

        if prev.is_closed() {
            return false;
        }

        // Try to consume the waker to avoid cloning it.
        // AtomicWaker::take() handles the synchronization.
        if let Some(waker) = self.rx_task.take() {
            waker.wake();
        }

        true
    }

    fn poll_recv(&self, cx: &mut Context<'_>) -> Poll<Result<T, RecvError>> {
        // Load the state
        let mut state = State::load(&self.state, Acquire);

        if state.is_complete() {
            match unsafe { self.consume_value() } {
                Some(value) => Ready(Ok(value)),
                None => Ready(Err(RecvError(()))),
            }
        } else if state.is_closed() {
            Ready(Err(RecvError(())))
        } else {
            self.rx_task.register(cx.waker());

            // Check the state again to avoid race conditions.
            // If the state became complete while we were registering,
            // the wake might have been missed or we might have overwritten
            // the waker that was about to be woken.
            // However, AtomicWaker handles the lost wake case: if wake() was called
            // concurrently with register(), AtomicWaker ensures the task is woken.
            // We just need to check if the value is ready now.
            state = State::load(&self.state, Acquire);

            if state.is_complete() {
                match unsafe { self.consume_value() } {
                    Some(value) => Ready(Ok(value)),
                    None => Ready(Err(RecvError(()))),
                }
            } else if state.is_closed() {
                Ready(Err(RecvError(())))
            } else {
                Pending
            }
        }
    }

    /// Called by `Receiver` to indicate that the value will never be received.
    fn close(&self) -> State {
        let prev = State::set_closed(&self.state);

        // We can just wake by ref or take here. AtomicWaker::wake() is safe.
        self.tx_task.wake();

        prev
    }

    /// Consumes the value. This function does not check `state`.
    ///
    /// # Safety
    ///
    /// Calling this method concurrently on multiple threads will result in a
    /// data race. The `VALUE_SENT` state bit is used to ensure that only the
    /// sender *or* the receiver will call this method at a given point in time.
    /// If `VALUE_SENT` is not set, then only the sender may call this method;
    /// if it is set, then only the receiver may call this method.
    unsafe fn consume_value(&self) -> Option<T> {
        unsafe { self.value.with_mut(|ptr| (*ptr).take()) }
    }

    /// Returns true if there is a value. This function does not check `state`.
    ///
    /// # Safety
    ///
    /// Calling this method concurrently on multiple threads will result in a
    /// data race. The `VALUE_SENT` state bit is used to ensure that only the
    /// sender *or* the receiver will call this method at a given point in time.
    /// If `VALUE_SENT` is not set, then only the sender may call this method;
    /// if it is set, then only the receiver may call this method.
    unsafe fn has_value(&self) -> bool {
        unsafe { self.value.with(|ptr| (*ptr).is_some()) }
    }
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        // SAFETY: we have `&mut self`, and therefore we have
        // exclusive access to the value.
        unsafe {
            // Note: the assertion holds because if the value has been sent by sender,
            // we must ensure that the value must have been consumed by the receiver before
            // dropping the `Inner`.
            debug_assert!(self.consume_value().is_none());
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Inner<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        use std::sync::atomic::Ordering::Relaxed;

        fmt.debug_struct("Inner")
            .field("state", &State::load(&self.state, Relaxed))
            .finish()
    }
}

/// Indicates that a value has been stored in the channel's inner `UnsafeCell`.
///
/// # Safety
///
/// This bit controls which side of the channel is permitted to access the
/// `UnsafeCell`. If it is set, the `UnsafeCell` may ONLY be accessed by the
/// receiver. If this bit is NOT set, the `UnsafeCell` may ONLY be accessed by
/// the sender.
const VALUE_SENT: usize = 0b00010;
const CLOSED: usize = 0b00100;

impl State {
    fn new() -> State {
        State(0)
    }

    fn is_complete(self) -> bool {
        self.0 & VALUE_SENT == VALUE_SENT
    }

    fn set_complete(cell: &AtomicUsize) -> State {
        // This method is a compare-and-swap loop rather than a fetch-or like
        // other `set_$WHATEVER` methods on `State`. This is because we must
        // check if the state has been closed before setting the `VALUE_SENT`
        // bit.
        //
        // We don't want to set both the `VALUE_SENT` bit if the `CLOSED`
        // bit is already set, because `VALUE_SENT` will tell the receiver that
        // it's okay to access the inner `UnsafeCell`. Immediately after calling
        // `set_complete`, if the channel was closed, the sender will _also_
        // access the `UnsafeCell` to take the value back out, so if a
        // `poll_recv` or `try_recv` call is occurring concurrently, both
        // threads may try to access the `UnsafeCell` if we were to set the
        // `VALUE_SENT` bit on a closed channel.
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
                Ok(_) => {
                    break;
                }
                Err(actual) => state = actual,
            }
        }
        State(state)
    }

    fn is_closed(self) -> bool {
        self.0 & CLOSED == CLOSED
    }

    fn set_closed(cell: &AtomicUsize) -> State {
        // Acquire because we want all later writes (attempting to poll) to be
        // ordered after this.
        let val = cell.fetch_or(CLOSED, Acquire);
        State(val)
    }

    fn as_usize(self) -> usize {
        self.0
    }

    fn load(cell: &AtomicUsize, order: Ordering) -> State {
        let val = cell.load(order);
        State(val)
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
