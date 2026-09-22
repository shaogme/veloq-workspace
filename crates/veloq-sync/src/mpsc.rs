use crate::{
    SendError, TryRecvError,
    notify::Notify,
    shim::queue::{ArrayQueue, Queue, SegQueue},
};
use futures_core::stream::Stream;
use veloq_std::{
    future::Future,
    mem::ManuallyDrop,
    pin::{Pin, pin},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use veloq_waker::MwsrWaker;

/// A multi-producer, single-consumer channel state.
pub struct State<T, S, Q> {
    pub(crate) queue: Q,
    pub(crate) state: ChannelState,
    pub(crate) strategy: S,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

impl<T> State<T, UnboundedStrategy, SegQueue<T>> {
    pub fn unbounded() -> Self {
        State {
            queue: SegQueue::new(),
            state: ChannelState::new(),
            strategy: UnboundedStrategy,
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<T> State<T, BoundedStrategy, ArrayQueue<T>> {
    pub fn bounded(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");
        State {
            queue: ArrayQueue::new(capacity),
            state: ChannelState::new(),
            strategy: BoundedStrategy::new(capacity),
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<T, S: ChannelStrategy, Q> State<T, S, Q> {
    pub fn split(
        &self,
    ) -> (
        GenericBorrowedSender<'_, T, S, Q>,
        GenericBorrowedReceiver<'_, T, S, Q>,
    ) {
        self.state.sender_count.store(1, Ordering::SeqCst);
        self.state.receiver_active.store(true, Ordering::SeqCst);
        (
            GenericBorrowedSender {
                state: self,
                _marker: veloq_std::marker::PhantomData,
            },
            GenericBorrowedReceiver {
                state: self,
                _marker: veloq_std::marker::PhantomData,
            },
        )
    }
}

/// Creates a new unbounded channel state.
pub fn borrowed_unbounded<T>() -> State<T, UnboundedStrategy, SegQueue<T>> {
    State::unbounded()
}

/// Creates a new bounded channel state.
pub fn borrowed_bounded<T>(capacity: usize) -> State<T, BoundedStrategy, ArrayQueue<T>> {
    State::bounded(capacity)
}

// Type Aliases to maintain API compatibility
pub type BorrowedSender<'a, T> = GenericBorrowedSender<'a, T, UnboundedStrategy, SegQueue<T>>;
pub type BorrowedReceiver<'a, T> = GenericBorrowedReceiver<'a, T, UnboundedStrategy, SegQueue<T>>;
pub type BorrowedBoundedSender<'a, T> =
    GenericBorrowedSender<'a, T, BoundedStrategy, ArrayQueue<T>>;
pub type BorrowedBoundedReceiver<'a, T> =
    GenericBorrowedReceiver<'a, T, BoundedStrategy, ArrayQueue<T>>;

pub type BoundedBorrowedSender<'a, T> = BorrowedBoundedSender<'a, T>;
pub type BoundedBorrowedReceiver<'a, T> = BorrowedBoundedReceiver<'a, T>;

pub type Sender<T> = GenericSender<T, UnboundedStrategy, SegQueue<T>>;
pub type Receiver<T> = GenericReceiver<T, UnboundedStrategy, SegQueue<T>>;
pub type BoundedSender<T> = GenericSender<T, BoundedStrategy, ArrayQueue<T>>;
pub type BoundedReceiver<T> = GenericReceiver<T, BoundedStrategy, ArrayQueue<T>>;

// --- Core State Logic ---

pub(crate) struct ChannelState {
    rx_waker: MwsrWaker,
    /// Number of active senders. Used to determine when to wake the receiver
    /// upon the last sender disconnecting.
    sender_count: AtomicUsize,
    /// Indicates if the receiver is still active.
    receiver_active: AtomicBool,
}

impl ChannelState {
    fn new() -> Self {
        Self {
            rx_waker: MwsrWaker::new(),
            sender_count: AtomicUsize::new(1),
            receiver_active: AtomicBool::new(true),
        }
    }

    fn inc_sender(&self) {
        self.sender_count.fetch_add(1, Ordering::Relaxed);
    }

    fn dec_sender(&self) -> bool {
        self.sender_count.fetch_sub(1, Ordering::AcqRel) == 1
    }

    fn is_rx_active(&self) -> bool {
        self.receiver_active.load(Ordering::Relaxed)
    }

    fn set_rx_inactive(&self) {
        self.receiver_active.store(false, Ordering::Release);
    }

    fn wake_rx(&self) {
        self.rx_waker.wake();
    }
}

// --- Strategies ---

pub trait ChannelStrategy: Send + Sync {
    fn on_rx_drop(&self);
    fn on_msg_recv(&self);
}

pub struct UnboundedStrategy;

impl ChannelStrategy for UnboundedStrategy {
    fn on_rx_drop(&self) {}
    fn on_msg_recv(&self) {}
}

pub struct BoundedStrategy {
    notify: Notify,
}

impl BoundedStrategy {
    fn new(_capacity: usize) -> Self {
        Self {
            notify: Notify::new(),
        }
    }
}

impl ChannelStrategy for BoundedStrategy {
    fn on_rx_drop(&self) {
        self.notify.notify_waiters();
    }

    fn on_msg_recv(&self) {
        self.notify.notify_one();
    }
}

// --- Generic Structures ---

pub struct GenericBorrowedSender<'a, T, S: ChannelStrategy, Q> {
    state: &'a State<T, S, Q>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub type BorrowedGenericSender<'a, T, S, Q> = GenericBorrowedSender<'a, T, S, Q>;

pub struct GenericBorrowedReceiver<'a, T, S: ChannelStrategy, Q> {
    state: &'a State<T, S, Q>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub type BorrowedGenericReceiver<'a, T, S, Q> = GenericBorrowedReceiver<'a, T, S, Q>;

pub struct GenericSender<T, S: ChannelStrategy, Q> {
    state: Arc<State<T, S, Q>>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub struct GenericReceiver<T, S: ChannelStrategy, Q> {
    state: Arc<State<T, S, Q>>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

// --- Implementations ---

impl<'a, T, S: ChannelStrategy, Q> Clone for GenericBorrowedSender<'a, T, S, Q> {
    fn clone(&self) -> Self {
        self.state.state.inc_sender();
        Self {
            state: self.state,
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<'a, T, S: ChannelStrategy, Q> Drop for GenericBorrowedSender<'a, T, S, Q> {
    fn drop(&mut self) {
        if self.state.state.dec_sender() {
            self.state.state.wake_rx();
        }
    }
}

impl<'a, T, S: ChannelStrategy, Q> Drop for GenericBorrowedReceiver<'a, T, S, Q> {
    fn drop(&mut self) {
        self.state.state.set_rx_inactive();
        self.state.strategy.on_rx_drop();
    }
}

// Unbounded Specifics
impl<'a, T> GenericBorrowedSender<'a, T, UnboundedStrategy, SegQueue<T>> {
    /// Sends a value to the channel.
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        if !self.state.state.is_rx_active() {
            return Err(SendError(val));
        }

        self.state.queue.push(val);
        self.state.state.wake_rx();
        Ok(())
    }
}

// Bounded Specifics
impl<'a, T> GenericBorrowedSender<'a, T, BoundedStrategy, ArrayQueue<T>> {
    /// Sends a value to the channel.
    pub async fn send(&self, mut val: T) -> Result<(), SendError<T>> {
        loop {
            if !self.state.state.is_rx_active() {
                return Err(SendError(val));
            }

            match self.state.queue.push(val) {
                Ok(_) => {
                    self.state.state.wake_rx();
                    return Ok(());
                }
                Err(returned_val) => {
                    val = returned_val;
                }
            }

            let mut notified = pin!(self.state.strategy.notify.notified());
            notified.as_mut().enable();

            if !self.state.state.is_rx_active() {
                return Err(SendError(val));
            }
            if !self.state.queue.is_full() {
                continue;
            }

            notified.await;
        }
    }
}

// Receiver Methods (Unified)
impl<'a, T, S: ChannelStrategy, Q: Queue<T>> GenericBorrowedReceiver<'a, T, S, Q> {
    /// Async receive method.
    pub async fn recv(&mut self) -> Option<T> {
        RecvFuture { receiver: self }.await
    }

    /// Try to receive a value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.state.queue.pop() {
            self.state.strategy.on_msg_recv();
            Ok(msg)
        } else if self.state.state.sender_count.load(Ordering::Acquire) == 0 {
            // Re-check queue after seeing sender_count == 0 to avoid race with send+drop
            if let Some(msg) = self.state.queue.pop() {
                self.state.strategy.on_msg_recv();
                Ok(msg)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

struct RecvFuture<'a, 'b, T, S: ChannelStrategy, Q: Queue<T>> {
    receiver: &'b mut GenericBorrowedReceiver<'a, T, S, Q>,
}

impl<'a, 'b, T, S: ChannelStrategy, Q: Queue<T>> Future for RecvFuture<'a, 'b, T, S, Q> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        Pin::new(&mut *this.receiver).poll_next(cx)
    }
}

impl<'a, T, S: ChannelStrategy, Q: Queue<T>> Stream for GenericBorrowedReceiver<'a, T, S, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        unsafe {
            this.state.state.rx_waker.register(cx.waker());
        }

        if let Some(val) = this.state.queue.pop() {
            this.state.strategy.on_msg_recv();
            return Poll::Ready(Some(val));
        }

        if this.state.state.sender_count.load(Ordering::Acquire) == 0 {
            // Re-check queue after seeing sender_count == 0 to avoid race with send+drop
            if let Some(val) = this.state.queue.pop() {
                this.state.strategy.on_msg_recv();
                return Poll::Ready(Some(val));
            }
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

// --- Channel Implementations ---

pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(State::unbounded());
    (
        GenericSender {
            state: state.clone(),
            _marker: veloq_std::marker::PhantomData,
        },
        GenericReceiver {
            state,
            _marker: veloq_std::marker::PhantomData,
        },
    )
}

pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    let state = Arc::new(State::bounded(capacity));
    (
        GenericSender {
            state: state.clone(),
            _marker: veloq_std::marker::PhantomData,
        },
        GenericReceiver {
            state,
            _marker: veloq_std::marker::PhantomData,
        },
    )
}

impl<T, S: ChannelStrategy, Q> Clone for GenericSender<T, S, Q> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(GenericBorrowedSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        let _cloned = ManuallyDrop::new(sender.clone());
        GenericSender {
            state: self.state.clone(),
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericSender<T, S, Q> {
    fn drop(&mut self) {
        drop(GenericBorrowedSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericReceiver<T, S, Q> {
    fn drop(&mut self) {
        drop(GenericBorrowedReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
    }
}

impl<T> GenericSender<T, UnboundedStrategy, SegQueue<T>> {
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericBorrowedSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        sender.send(val)
    }
}

impl<T> GenericSender<T, BoundedStrategy, ArrayQueue<T>> {
    pub async fn send(&self, val: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericBorrowedSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        sender.send(val).await
    }
}

impl<T, S: ChannelStrategy, Q: Queue<T>> GenericReceiver<T, S, Q> {
    pub async fn recv(&mut self) -> Option<T> {
        let mut receiver = ManuallyDrop::new(GenericBorrowedReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut receiver = ManuallyDrop::new(GenericBorrowedReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        receiver.try_recv()
    }
}

impl<T, S: ChannelStrategy, Q: Queue<T>> Stream for GenericReceiver<T, S, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut receiver = ManuallyDrop::new(GenericBorrowedReceiver {
            state: &this.state,
            _marker: veloq_std::marker::PhantomData,
        });
        Pin::new(&mut *receiver).poll_next(cx)
    }
}
