use crate::shim::Arc;
use crate::shim::atomic::AtomicUsize;
use crate::shim::cell::UnsafeCell;

use veloq_atomic_waker::AtomicWaker;

use std::fmt;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll};

/// Creates a new one-shot channel state.
pub fn channel<T>() -> State<T> {
    State::new()
}

pub struct State<T> {
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
struct StateVal(usize);

impl<T> Default for State<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> State<T> {
    /// Creates a new oneshot channel state.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        State {
            state: AtomicUsize::new(StateVal::new().as_usize()),
            value: UnsafeCell::new(None),
            tx_task: AtomicWaker::new(),
            rx_task: AtomicWaker::new(),
        }
    }

    /// Creates a new oneshot channel state.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        State {
            state: AtomicUsize::new(StateVal::new().as_usize()),
            value: UnsafeCell::new(None),
            tx_task: AtomicWaker::new(),
            rx_task: AtomicWaker::new(),
        }
    }

    /// Splits the state into a sender and a receiver.
    pub fn split(&self) -> (Sender<'_, T>, Receiver<'_, T>) {
        (Sender { state: self }, Receiver { state: Some(self) })
    }

    /// Try to set the state to complete. Returns `true` if successful, `false` if closed.
    fn complete(&self) -> bool {
        let prev = StateVal::set_complete(&self.state);

        if prev.is_closed() {
            return false;
        }

        // Notify the receiver task.
        self.rx_task.wake();
        true
    }

    /// Set the state to closed and notify the sender logic.
    fn close(&self) -> StateVal {
        let prev = StateVal::set_closed(&self.state);
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

unsafe impl<T: Send> Send for State<T> {}
unsafe impl<T: Send> Sync for State<T> {}

impl<T> Drop for State<T> {
    fn drop(&mut self) {
        // SAFETY: `State` is dropping, meaning the refcount is 0 or it is owned.
        // We have exclusive access to the `UnsafeCell`.
        // We must ensure the contained value is dropped to avoid memory leaks.
        unsafe {
            self.value.with_mut(|ptr| {
                let _ = (*ptr).take();
            });
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for State<T> {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("State")
            .field("state", &StateVal::load(&self.state, Ordering::Relaxed))
            .finish()
    }
}

pub struct Sender<'a, T> {
    state: &'a State<T>,
}

pub struct Receiver<'a, T> {
    state: Option<&'a State<T>>,
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

// ===== impl Sender =====

impl<'a, T> Sender<'a, T> {
    /// Sends a value.
    ///
    /// This method consumes the sender, ensuring that it is only called once.
    ///
    /// If the receiver has already hung up, this method returns the error `Err(T)`.
    pub fn send(self, t: T) -> Result<(), T> {
        // Write the value to the unsafe cell.
        unsafe { self.state.value.with_mut(|ptr| *ptr = Some(t)) };

        // Attempt to transition the state to complete.
        if !self.state.complete() {
            unsafe {
                return Err(self.state.consume_value().unwrap());
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
        let state = StateVal::load(&self.state.state, Ordering::Acquire);
        state.is_closed()
    }

    /// Polls to check if the receiver has closed the channel.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        // Fast path check
        if StateVal::load(&self.state.state, Ordering::Acquire).is_closed() {
            return Ready(());
        }

        self.state.tx_task.register(cx.waker());

        // Double check after registration to avoid races
        if StateVal::load(&self.state.state, Ordering::Acquire).is_closed() {
            return Ready(());
        }

        Pending
    }
}

impl<'a, T> Drop for Sender<'a, T> {
    fn drop(&mut self) {
        let state = StateVal::load(&self.state.state, Ordering::Acquire);
        if !state.is_complete() {
            self.state.complete();
        }
    }
}

// ===== impl Receiver =====

impl<'a, T> Receiver<'a, T> {
    /// Prevents the channel from ever delivering a message.
    pub fn close(&mut self) {
        if let Some(state) = self.state {
            state.close();
        }
    }

    /// Returns true if the channel has terminated (inner is gone).
    pub fn is_terminated(&self) -> bool {
        self.state.is_none()
    }

    /// Checks if the channel is empty.
    ///
    /// Returns true if the value has not been sent yet or if the value has already been consumed.
    pub fn is_empty(&self) -> bool {
        let Some(state) = self.state else {
            return true;
        };

        let state = StateVal::load(&state.state, Ordering::Acquire);
        if state.is_complete() {
            // SAFETY: `is_complete` implies `VALUE_SENT` is set.
            // This synchronizes with the sender's writes.
            // Only the receiver can access now.
            unsafe { !self.state.unwrap().has_value() }
        } else {
            true
        }
    }

    /// Attempts to receive a value.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let state = match self.state {
            Some(state) => state,
            None => return Err(TryRecvError::Closed),
        };

        let state_val = StateVal::load(&state.state, Ordering::Acquire);

        if state_val.is_complete() {
            // SAFETY: `VALUE_SENT` is set, exclusive access granted to Receiver.
            match unsafe { state.consume_value() } {
                Some(value) => {
                    self.state = None;
                    Ok(value)
                }
                // Sender dropped without sending a value.
                None => {
                    self.state = None;
                    Err(TryRecvError::Closed)
                }
            }
        } else if state_val.is_closed() {
            self.state = None;
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

impl<'a, T> Drop for Receiver<'a, T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            // Mark as closed to notify Sender.
            let state_val = state.close();

            // If the sender had already completed sending, we are responsible for cleaning up the value.
            if state_val.is_complete() {
                // SAFETY: `VALUE_SENT` set, we own the data.
                unsafe { drop(state.consume_value()) };
            }
        }
    }
}

impl<'a, T> Future for Receiver<'a, T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        // If inner is None, we've already consumed the result or been polled to completion.
        let state = self
            .state
            .as_ref()
            .copied()
            .expect("Receiver polled after completion");

        // Fast path: check if ready without registering waker.
        let state_val = StateVal::load(&state.state, Ordering::Acquire);
        if state_val.is_complete() {
            // SAFETY: standard consume logic
            return match unsafe { state.consume_value() } {
                Some(v) => {
                    self.state = None;
                    Ready(Ok(v))
                }
                None => {
                    self.state = None;
                    Ready(Err(RecvError(())))
                }
            };
        }

        if state_val.is_closed() {
            self.state = None;
            return Ready(Err(RecvError(())));
        }

        // Register waker
        state.rx_task.register(cx.waker());

        // Double check state
        let state_val = StateVal::load(&state.state, Ordering::Acquire);
        if state_val.is_complete() {
            match unsafe { state.consume_value() } {
                Some(v) => {
                    self.state = None;
                    Ready(Ok(v))
                }
                None => {
                    self.state = None;
                    Ready(Err(RecvError(())))
                }
            }
        } else if state_val.is_closed() {
            self.state = None;
            Ready(Err(RecvError(())))
        } else {
            Pending
        }
    }
}

impl<'a, T> fmt::Debug for Sender<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<'a, T> fmt::Debug for Receiver<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

pub struct OwnedSender<T> {
    state: ManuallyDrop<Arc<State<T>>>,
}

pub struct OwnedReceiver<T> {
    state: Option<Arc<State<T>>>,
}

pub fn owned_channel<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Arc::new(State::new());
    (
        OwnedSender {
            state: ManuallyDrop::new(state.clone()),
        },
        OwnedReceiver { state: Some(state) },
    )
}

impl<T> OwnedSender<T> {
    /// Sends a value.
    pub fn send(self, t: T) -> Result<(), T> {
        let this = ManuallyDrop::new(self);
        let state = unsafe { std::ptr::read(&*this.state) };
        let sender = Sender { state: &state };
        sender.send(t)
    }

    /// Waits for the channel to be closed.
    pub async fn closed(&mut self) {
        let mut sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.closed().await;
    }

    /// Returns `true` if the receiver has closed the channel.
    pub fn is_closed(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.is_closed()
    }

    /// Polls to check if the receiver has closed the channel.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let mut sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.poll_closed(cx)
    }
}

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        drop(Sender { state: &self.state });
        unsafe {
            ManuallyDrop::drop(&mut self.state);
        }
    }
}

impl<T> fmt::Debug for OwnedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSender").finish()
    }
}

impl<T> OwnedReceiver<T> {
    /// Prevents the channel from ever delivering a message.
    pub fn close(&mut self) {
        if let Some(state) = &self.state {
            let mut receiver = ManuallyDrop::new(Receiver { state: Some(state) });
            receiver.close();
        }
    }

    /// Returns true if the channel has terminated.
    pub fn is_terminated(&self) -> bool {
        self.state.is_none()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        match &self.state {
            Some(state) => {
                let receiver = ManuallyDrop::new(Receiver { state: Some(state) });
                receiver.is_empty()
            }
            None => true,
        }
    }

    /// Attempts to receive a value.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let state = match &self.state {
            Some(state) => state,
            None => return Err(TryRecvError::Closed),
        };
        let mut receiver = ManuallyDrop::new(Receiver { state: Some(state) });
        let res = receiver.try_recv();
        if res.is_ok() || matches!(res, Err(TryRecvError::Closed)) {
            self.state = None;
        }
        res
    }
}

impl<T> Future for OwnedReceiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self
            .state
            .as_ref()
            .expect("Receiver polled after completion");
        let mut receiver = ManuallyDrop::new(Receiver { state: Some(state) });
        let res = Pin::new(&mut *receiver).poll(cx);
        if res.is_ready() {
            self.state = None;
        }
        res
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            drop(Receiver {
                state: Some(&state),
            });
        }
    }
}

impl<T> fmt::Debug for OwnedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedReceiver").finish()
    }
}

// ===== StateVal Management =====

const VALUE_SENT: usize = 0b00010;
const CLOSED: usize = 0b00100;

impl StateVal {
    const fn new() -> StateVal {
        StateVal(0)
    }

    fn is_complete(self) -> bool {
        self.0 & VALUE_SENT == VALUE_SENT
    }

    fn is_closed(self) -> bool {
        self.0 & CLOSED == CLOSED
    }

    fn set_complete(cell: &AtomicUsize) -> StateVal {
        let mut state = cell.load(Ordering::Relaxed);
        loop {
            if StateVal(state).is_closed() {
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
        StateVal(state)
    }

    fn set_closed(cell: &AtomicUsize) -> StateVal {
        let val = cell.fetch_or(CLOSED, Ordering::AcqRel);
        StateVal(val)
    }

    const fn as_usize(self) -> usize {
        self.0
    }

    fn load(cell: &AtomicUsize, order: Ordering) -> StateVal {
        StateVal(cell.load(order))
    }
}

impl fmt::Debug for StateVal {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("StateVal")
            .field("is_complete", &self.is_complete())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}
