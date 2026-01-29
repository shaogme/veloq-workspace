use crate::shim::Arc;
use crate::shim::atomic::{AtomicUsize, fence};
use crate::shim::cell::UnsafeCell;

use std::fmt;
use std::future::Future;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::sync::atomic::Ordering::{self, AcqRel, Acquire};
use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll, Waker, ready};

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
    ///
    /// ## Safety
    ///
    /// The `TX_TASK_SET` bit in the `state` field is set if this field is
    /// initialized. If that bit is unset, this field may be uninitialized.
    tx_task: Task,

    /// The task to notify when the value is sent.
    ///
    /// ## Safety
    ///
    /// The `RX_TASK_SET` bit in the `state` field is set if this field is
    /// initialized. If that bit is unset, this field may be uninitialized.
    rx_task: Task,
}

struct Task(UnsafeCell<MaybeUninit<Waker>>);

impl Task {
    /// # Safety
    ///
    /// The caller must do the necessary synchronization to ensure that
    /// the [`Self::0`] contains the valid [`Waker`] during the call.
    unsafe fn will_wake(&self, cx: &mut Context<'_>) -> bool {
        unsafe { self.with_task(|w| w.will_wake(cx.waker())) }
    }

    /// # Safety
    ///
    /// The caller must do the necessary synchronization to ensure that
    /// the [`Self::0`] contains the valid [`Waker`] during the call.
    unsafe fn with_task<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&Waker) -> R,
    {
        unsafe {
            self.0.with(|ptr| {
                let waker: *const Waker = (*ptr).as_ptr();
                f(&*waker)
            })
        }
    }

    /// # Safety
    ///
    /// The caller must do the necessary synchronization to ensure that
    /// the [`Self::0`] contains the valid [`Waker`] during the call.
    unsafe fn drop_task(&self) {
        unsafe {
            self.0.with_mut(|ptr| {
                let ptr: *mut Waker = (*ptr).as_mut_ptr();
                ptr.drop_in_place();
            });
        }
    }

    /// # Safety
    ///
    /// The caller must do the necessary synchronization to ensure that
    /// the [`Self::0`] contains the valid [`Waker`] during the call.
    unsafe fn set_task(&self, cx: &mut Context<'_>) {
        unsafe {
            self.0.with_mut(|ptr| {
                let ptr: *mut Waker = (*ptr).as_mut_ptr();
                ptr.write(cx.waker().clone());
            });
        }
    }

    /// # Safety
    ///
    /// The caller must ensure that the `UnsafeCell` contains a valid `Waker`
    /// and that no other thread is accessing it concurrently.
    /// The caller effectively takes ownership of the `Waker`, so the
    /// `UnsafeCell` should be considered uninitialized after this call.
    unsafe fn take_task(&self) -> Waker {
        unsafe {
            self.0.with_mut(|ptr| {
                let ptr: *mut Waker = (*ptr).as_mut_ptr();
                ptr.read()
            })
        }
    }
}

#[derive(Clone, Copy)]
struct State(usize);

pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let inner = Arc::new(Inner {
        state: AtomicUsize::new(State::new().as_usize()),
        value: UnsafeCell::new(None),
        tx_task: Task(UnsafeCell::new(MaybeUninit::uninit())),
        rx_task: Task(UnsafeCell::new(MaybeUninit::uninit())),
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

        let mut state = State::load(&inner.state, Acquire);

        if state.is_closed() {
            return Ready(());
        }

        if state.is_tx_task_set() {
            let will_notify = unsafe { inner.tx_task.will_wake(cx) };

            if !will_notify {
                state = State::unset_tx_task(&inner.state);

                if state.is_closed() {
                    // Set the flag again so that the waker is released in drop
                    State::set_tx_task(&inner.state);
                    return Ready(());
                } else {
                    unsafe { inner.tx_task.drop_task() };
                }
            }
        }

        if !state.is_tx_task_set() {
            // Attempt to set the task
            unsafe {
                inner.tx_task.set_task(cx);
            }

            // Update the state
            state = State::set_tx_task(&inner.state);

            if state.is_closed() {
                return Ready(());
            }
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

        if prev.is_rx_task_set() {
            State::acquire_rx_lock(&self.state);

            let state = State::load(&self.state, Ordering::Relaxed);

            if state.is_rx_task_set() {
                State::unset_rx_task(&self.state);
                let waker = unsafe { self.rx_task.take_task() };
                State::release_rx_lock(&self.state);
                waker.wake();
            } else {
                State::release_rx_lock(&self.state);
            }
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
            if state.is_rx_task_set() {
                State::acquire_rx_lock(&self.state);

                let current_state = State::load(&self.state, Ordering::Relaxed);

                if current_state.is_rx_task_set() {
                    let will_notify = unsafe { self.rx_task.will_wake(cx) };

                    // Check if the task is still the same
                    if !will_notify {
                        // Unset the task
                        state = State::unset_rx_task(&self.state);
                        State::release_rx_lock(&self.state);

                        if state.is_complete() {
                            // The sender has set the `VALUE_SENT` bit.
                            // Since we held the lock and unset the `RX_TASK_SET` bit,
                            // the sender did not consume the waker.
                            // We are responsible for dropping it.
                            unsafe { self.rx_task.drop_task() };

                            // SAFETY: If `state.is_complete()` returns true, then the
                            // `VALUE_SENT` bit has been set and the sender side of the
                            // channel will no longer attempt to access the inner
                            // `UnsafeCell`. Therefore, it is now safe for us to access the
                            // cell.
                            return match unsafe { self.consume_value() } {
                                Some(value) => Ready(Ok(value)),
                                None => Ready(Err(RecvError(()))),
                            };
                        } else {
                            unsafe { self.rx_task.drop_task() };
                        }
                    } else {
                        State::release_rx_lock(&self.state);
                    }
                } else {
                    State::release_rx_lock(&self.state);

                    // The task was unset, likely by `complete`.
                    state = current_state;
                    if state.is_complete() {
                        return match unsafe { self.consume_value() } {
                            Some(value) => Ready(Ok(value)),
                            None => Ready(Err(RecvError(()))),
                        };
                    }
                }
            }

            if !state.is_rx_task_set() {
                // Attempt to set the task
                unsafe {
                    self.rx_task.set_task(cx);
                }

                // Update the state
                state = State::set_rx_task(&self.state);

                if state.is_complete() {
                    match unsafe { self.consume_value() } {
                        Some(value) => Ready(Ok(value)),
                        None => Ready(Err(RecvError(()))),
                    }
                } else {
                    Pending
                }
            } else {
                Pending
            }
        }
    }

    /// Called by `Receiver` to indicate that the value will never be received.
    fn close(&self) -> State {
        let prev = State::set_closed(&self.state);

        if prev.is_tx_task_set() && !prev.is_complete() {
            unsafe {
                self.tx_task.with_task(Waker::wake_by_ref);
            }
        }

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

fn mut_load(this: &mut AtomicUsize) -> usize {
    this.with_mut(|v| *v)
}

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let state = State(mut_load(&mut self.state));

        if state.is_rx_task_set() {
            unsafe {
                self.rx_task.drop_task();
            }
        }

        if state.is_tx_task_set() {
            unsafe {
                self.tx_task.drop_task();
            }
        }

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

/// Indicates that a waker for the receiving task has been set.
///
/// # Safety
///
/// If this bit is not set, the `rx_task` field may be uninitialized.
const RX_TASK_SET: usize = 0b00001;
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

/// Indicates that a waker for the sending task has been set.
///
/// # Safety
///
/// If this bit is not set, the `tx_task` field may be uninitialized.
const TX_TASK_SET: usize = 0b01000;

const RX_TASK_LOCKED: usize = 0b10000;

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
                    if State(state).is_rx_task_set() {
                        fence(Ordering::Acquire);
                    }
                    break;
                }
                Err(actual) => state = actual,
            }
        }
        State(state)
    }

    fn is_rx_task_set(self) -> bool {
        self.0 & RX_TASK_SET == RX_TASK_SET
    }

    fn set_rx_task(cell: &AtomicUsize) -> State {
        let val = cell.fetch_or(RX_TASK_SET, AcqRel);
        State(val | RX_TASK_SET)
    }

    fn unset_rx_task(cell: &AtomicUsize) -> State {
        let val = cell.fetch_and(!RX_TASK_SET, AcqRel);
        State(val & !RX_TASK_SET)
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

    fn set_tx_task(cell: &AtomicUsize) -> State {
        let val = cell.fetch_or(TX_TASK_SET, AcqRel);
        State(val | TX_TASK_SET)
    }

    fn unset_tx_task(cell: &AtomicUsize) -> State {
        let val = cell.fetch_and(!TX_TASK_SET, AcqRel);
        State(val & !TX_TASK_SET)
    }

    fn is_tx_task_set(self) -> bool {
        self.0 & TX_TASK_SET == TX_TASK_SET
    }

    fn as_usize(self) -> usize {
        self.0
    }

    fn load(cell: &AtomicUsize, order: Ordering) -> State {
        let val = cell.load(order);
        State(val)
    }

    fn acquire_rx_lock(cell: &AtomicUsize) {
        #[cfg(feature = "loom")]
        {
            loop {
                let state = cell.fetch_or(RX_TASK_LOCKED, Acquire);
                if state & RX_TASK_LOCKED == 0 {
                    return;
                }
                crate::shim::yield_now();
            }
        }

        #[cfg(not(feature = "loom"))]
        {
            // Simple spinlock implementation
            let mut backoff = 1;
            loop {
                let state = cell.fetch_or(RX_TASK_LOCKED, Acquire);
                if state & RX_TASK_LOCKED == 0 {
                    return;
                }
                while cell.load(Ordering::Relaxed) & RX_TASK_LOCKED != 0 {
                    for _ in 0..backoff {
                        std::hint::spin_loop();
                    }
                    if backoff < 8 {
                        backoff <<= 1;
                    }
                }
            }
        }
    }

    fn release_rx_lock(cell: &AtomicUsize) {
        cell.fetch_and(!RX_TASK_LOCKED, Ordering::Release);
    }
}

impl fmt::Debug for State {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("State")
            .field("is_complete", &self.is_complete())
            .field("is_closed", &self.is_closed())
            .field("is_rx_task_set", &self.is_rx_task_set())
            .field("is_tx_task_set", &self.is_tx_task_set())
            .finish()
    }
}
