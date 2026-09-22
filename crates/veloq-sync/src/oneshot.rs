use veloq_waker::MwsrWaker;

use crate::notify::{Notified, Notify};
use veloq_std::{
    cell::UnsafeCell,
    fmt,
    future::Future,
    mem::ManuallyDrop,
    ops::AsyncFnOnce,
    pin::{Pin, pin},
    sync::atomic::Ordering,
    sync::{Arc, atomic::AtomicUsize},
    task::{
        Context, Poll,
        Poll::{Pending, Ready},
    },
};

/// Creates a new borrowed one-shot channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_channel<T, F, R>(f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = State::new();
    let (tx, rx) = state.split();
    f(tx, rx).await
}

pub struct State<T> {
    /// Manages the state of the inner cell.
    state: AtomicUsize,

    /// The value. This is set by `Sender` and read by `Receiver`.
    /// The state of the cell is tracked by `state`.
    value: UnsafeCell<Option<T>>,

    /// The notification primitive when the receiver drops without consuming the value.
    tx_notify: Notify,

    /// The task to notify when the value is sent.
    rx_task: MwsrWaker,
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
            tx_notify: Notify::new(),
            rx_task: MwsrWaker::new(),
        }
    }

    /// Creates a new oneshot channel state.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        State {
            state: AtomicUsize::new(StateVal::new().as_usize()),
            value: UnsafeCell::new(None),
            tx_notify: Notify::new(),
            rx_task: MwsrWaker::new(),
        }
    }

    /// Splits the state into a sender and a receiver.
    pub fn split(&self) -> (BorrowedSender<'_, T>, BorrowedReceiver<'_, T>) {
        (
            BorrowedSender {
                state: self,
                closed_notified: None,
            },
            BorrowedReceiver { state: Some(self) },
        )
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
        self.tx_notify.notify_waiters();
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

pub struct BorrowedSender<'a, T> {
    state: &'a State<T>,
    closed_notified: Option<Notified<'a>>,
}

pub struct BorrowedReceiver<'a, T> {
    state: Option<&'a State<T>>,
}

pub mod error {
    use veloq_std::fmt;

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

    impl veloq_std::error::Error for RecvError {}

    impl fmt::Display for TryRecvError {
        fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                TryRecvError::Empty => write!(fmt, "channel empty"),
                TryRecvError::Closed => write!(fmt, "channel closed"),
            }
        }
    }

    impl veloq_std::error::Error for TryRecvError {}
}

use self::error::*;

// ===== impl BorrowedSender =====

impl<'a, T> BorrowedSender<'a, T> {
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
        if self.is_closed() {
            return;
        }

        let mut notified = pin!(self.state.tx_notify.notified());
        notified.as_mut().enable();

        if self.is_closed() {
            return;
        }

        notified.await;
    }

    /// Returns `true` if the receiver has closed the channel.
    pub fn is_closed(&self) -> bool {
        let state = StateVal::load(&self.state.state, Ordering::Acquire);
        state.is_closed()
    }

    /// Polls to check if the receiver has closed the channel.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            self.closed_notified = None;
            return Ready(());
        }

        if self.closed_notified.is_none() {
            self.closed_notified = Some(self.state.tx_notify.notified());
            let notified = self.closed_notified.as_mut().unwrap();
            let notified_pin = unsafe { Pin::new_unchecked(notified) };
            notified_pin.enable();
        }

        if self.is_closed() {
            self.closed_notified = None;
            return Ready(());
        }

        let notified = self.closed_notified.as_mut().unwrap();
        let notified_pin = unsafe { Pin::new_unchecked(notified) };
        match Future::poll(notified_pin, cx) {
            Ready(()) => {
                self.closed_notified = None;
                Ready(())
            }
            Pending => {
                if self.is_closed() {
                    self.closed_notified = None;
                    Ready(())
                } else {
                    Pending
                }
            }
        }
    }
}

impl<'a, T> Drop for BorrowedSender<'a, T> {
    fn drop(&mut self) {
        let state = StateVal::load(&self.state.state, Ordering::Acquire);
        if !state.is_complete() {
            self.state.complete();
        }
    }
}

// ===== impl BorrowedReceiver =====

impl<'a, T> BorrowedReceiver<'a, T> {
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

impl<'a, T> Drop for BorrowedReceiver<'a, T> {
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

impl<'a, T> Future for BorrowedReceiver<'a, T> {
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
        unsafe {
            state.rx_task.register(cx.waker());
        }

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

impl<'a, T> fmt::Debug for BorrowedSender<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedSender").finish()
    }
}

impl<'a, T> fmt::Debug for BorrowedReceiver<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedReceiver").finish()
    }
}

pub struct Sender<T> {
    state: ManuallyDrop<Arc<State<T>>>,
    closed_notified: Option<Notified<'static>>,
}

pub struct Receiver<T> {
    state: Option<Arc<State<T>>>,
}

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::new());
    (
        Sender {
            state: ManuallyDrop::new(state.clone()),
            closed_notified: None,
        },
        Receiver { state: Some(state) },
    )
}

impl<T> Sender<T> {
    /// Sends a value.
    pub fn send(mut self, t: T) -> Result<(), T> {
        self.closed_notified = None;
        let this = ManuallyDrop::new(self);
        let state = unsafe { veloq_std::ptr::read(&*this.state) };
        let sender = BorrowedSender {
            state: &state,
            closed_notified: None,
        };
        sender.send(t)
    }

    /// Waits for the channel to be closed.
    pub async fn closed(&mut self) {
        if self.is_closed() {
            return;
        }

        let mut notified = pin!(self.state.tx_notify.notified());
        notified.as_mut().enable();

        if self.is_closed() {
            return;
        }

        notified.await;
    }

    /// Returns `true` if the receiver has closed the channel.
    pub fn is_closed(&self) -> bool {
        let sender = ManuallyDrop::new(BorrowedSender {
            state: &self.state,
            closed_notified: None,
        });
        sender.is_closed()
    }

    /// Polls to check if the receiver has closed the channel.
    pub fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.is_closed() {
            self.closed_notified = None;
            return Ready(());
        }

        if self.closed_notified.is_none() {
            let notify_ptr = &self.state.tx_notify as *const Notify;
            let notify_ref: &'static Notify = unsafe { &*notify_ptr };
            self.closed_notified = Some(notify_ref.notified());
            let notified = self.closed_notified.as_mut().unwrap();
            let notified_pin = unsafe { Pin::new_unchecked(notified) };
            notified_pin.enable();
        }

        if self.is_closed() {
            self.closed_notified = None;
            return Ready(());
        }

        let notified = self.closed_notified.as_mut().unwrap();
        let notified_pin = unsafe { Pin::new_unchecked(notified) };
        match Future::poll(notified_pin, cx) {
            Ready(()) => {
                self.closed_notified = None;
                Ready(())
            }
            Pending => {
                if self.is_closed() {
                    self.closed_notified = None;
                    Ready(())
                } else {
                    Pending
                }
            }
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.closed_notified = None;
        drop(BorrowedSender {
            state: &self.state,
            closed_notified: None,
        });
        unsafe {
            ManuallyDrop::drop(&mut self.state);
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<T> Receiver<T> {
    /// Prevents the channel from ever delivering a message.
    pub fn close(&mut self) {
        if let Some(state) = &self.state {
            let mut receiver = ManuallyDrop::new(BorrowedReceiver { state: Some(state) });
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
                let receiver = ManuallyDrop::new(BorrowedReceiver { state: Some(state) });
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
        let mut receiver = ManuallyDrop::new(BorrowedReceiver { state: Some(state) });
        let res = receiver.try_recv();
        if res.is_ok() || matches!(res, Err(TryRecvError::Closed)) {
            self.state = None;
        }
        res
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self
            .state
            .as_ref()
            .expect("Receiver polled after completion");
        let mut receiver = ManuallyDrop::new(BorrowedReceiver { state: Some(state) });
        let res = Pin::new(&mut *receiver).poll(cx);
        if res.is_ready() {
            self.state = None;
        }
        res
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        if let Some(state) = self.state.take() {
            drop(BorrowedReceiver {
                state: Some(&state),
            });
        }
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
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
