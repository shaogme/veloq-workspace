use crate::{
    SendError, TryRecvError,
    shim::queue::{ArrayQueue, Queue, SegQueue},
};
use futures_core::stream::Stream;
use veloq_std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
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
    pub fn split(&self) -> (GenericSender<'_, T, S, Q>, GenericReceiver<'_, T, S, Q>) {
        self.state.sender_count.store(1, Ordering::SeqCst);
        self.state.receiver_active.store(true, Ordering::SeqCst);
        (
            GenericSender {
                state: self,
                _marker: veloq_std::marker::PhantomData,
            },
            GenericReceiver {
                state: self,
                _marker: veloq_std::marker::PhantomData,
            },
        )
    }
}

/// Creates a new unbounded channel state.
pub fn unbounded<T>() -> State<T, UnboundedStrategy, SegQueue<T>> {
    State::unbounded()
}

/// Creates a new bounded channel state.
pub fn bounded<T>(capacity: usize) -> State<T, BoundedStrategy, ArrayQueue<T>> {
    State::bounded(capacity)
}

// Type Aliases to maintain API compatibility
pub type Sender<'a, T> = GenericSender<'a, T, UnboundedStrategy, SegQueue<T>>;
pub type Receiver<'a, T> = GenericReceiver<'a, T, UnboundedStrategy, SegQueue<T>>;
pub type BoundedSender<'a, T> = GenericSender<'a, T, BoundedStrategy, ArrayQueue<T>>;
pub type BoundedReceiver<'a, T> = GenericReceiver<'a, T, BoundedStrategy, ArrayQueue<T>>;

pub type OwnedSender<T> = GenericOwnedSender<T, UnboundedStrategy, SegQueue<T>>;
pub type OwnedReceiver<T> = GenericOwnedReceiver<T, UnboundedStrategy, SegQueue<T>>;
pub type BoundedOwnedSender<T> = GenericOwnedSender<T, BoundedStrategy, ArrayQueue<T>>;
pub type BoundedOwnedReceiver<T> = GenericOwnedReceiver<T, BoundedStrategy, ArrayQueue<T>>;

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
    send_waiters: SegQueue<Arc<WakerState>>,
}

struct WakerState {
    waker: MwsrWaker,
    waiting: AtomicBool,
}

impl BoundedStrategy {
    fn new(_capacity: usize) -> Self {
        Self {
            send_waiters: SegQueue::new(),
        }
    }

    fn wake_one_sender(&self) {
        while let Some(ws) = self.send_waiters.pop() {
            if ws.waiting.swap(false, Ordering::AcqRel) {
                ws.waker.wake();
                break;
            }
        }
    }
}

impl ChannelStrategy for BoundedStrategy {
    fn on_rx_drop(&self) {
        while let Some(ws) = self.send_waiters.pop() {
            ws.waker.wake();
        }
    }

    fn on_msg_recv(&self) {
        self.wake_one_sender();
    }
}

// --- Generic Structures ---

pub struct GenericSender<'a, T, S: ChannelStrategy, Q> {
    state: &'a State<T, S, Q>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub struct GenericReceiver<'a, T, S: ChannelStrategy, Q> {
    state: &'a State<T, S, Q>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub struct GenericOwnedSender<T, S: ChannelStrategy, Q> {
    state: Arc<State<T, S, Q>>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

pub struct GenericOwnedReceiver<T, S: ChannelStrategy, Q> {
    state: Arc<State<T, S, Q>>,
    _marker: veloq_std::marker::PhantomData<fn() -> T>,
}

// --- Implementations ---

impl<'a, T, S: ChannelStrategy, Q> Clone for GenericSender<'a, T, S, Q> {
    fn clone(&self) -> Self {
        self.state.state.inc_sender();
        Self {
            state: self.state,
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<'a, T, S: ChannelStrategy, Q> Drop for GenericSender<'a, T, S, Q> {
    fn drop(&mut self) {
        if self.state.state.dec_sender() {
            self.state.state.wake_rx();
        }
    }
}

impl<'a, T, S: ChannelStrategy, Q> Drop for GenericReceiver<'a, T, S, Q> {
    fn drop(&mut self) {
        self.state.state.set_rx_inactive();
        self.state.strategy.on_rx_drop();
    }
}

// Unbounded Specifics
impl<'a, T> GenericSender<'a, T, UnboundedStrategy, SegQueue<T>> {
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
impl<'a, T> GenericSender<'a, T, BoundedStrategy, ArrayQueue<T>> {
    /// Sends a value to the channel.
    pub async fn send(&self, val: T) -> Result<(), SendError<T>> {
        BoundedSendFuture {
            sender: self,
            val: Some(val),
            waker_state: None,
        }
        .await
    }
}

struct BoundedSendFuture<'a, 'b, T> {
    sender: &'b GenericSender<'a, T, BoundedStrategy, ArrayQueue<T>>,
    val: Option<T>,
    waker_state: Option<Arc<WakerState>>,
}

impl<'a, 'b, T> Drop for BoundedSendFuture<'a, 'b, T> {
    fn drop(&mut self) {
        if let Some(ws) = &self.waker_state {
            ws.waiting.store(false, Ordering::Release);
        }
    }
}

impl<'a, 'b, T> Future for BoundedSendFuture<'a, 'b, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let state = this.sender.state;
        let strategy = &state.strategy;

        // Check Rx active
        if !state.state.is_rx_active() {
            return Poll::Ready(Err(SendError(this.val.take().unwrap())));
        }

        // Try acquire capacity
        let val = this.val.take().unwrap();
        match state.queue.push(val) {
            Ok(_) => {
                state.state.wake_rx();
                return Poll::Ready(Ok(()));
            }
            Err(returned_val) => {
                this.val = Some(returned_val);
            }
        }

        // Register waiter
        if this.waker_state.is_none() {
            this.waker_state = Some(Arc::new(WakerState {
                waker: MwsrWaker::new(),
                waiting: AtomicBool::new(false),
            }));
        }
        let ws = this.waker_state.as_ref().unwrap();
        unsafe {
            ws.waker.register(cx.waker());
        }

        if !ws.waiting.swap(true, Ordering::AcqRel) {
            strategy.send_waiters.push(ws.clone());
        }

        // Re-check
        if !state.state.is_rx_active() {
            return Poll::Ready(Err(SendError(this.val.take().unwrap())));
        }
        if !state.queue.is_full() {
            ws.waker.wake();
        }

        Poll::Pending
    }
}

// Receiver Methods (Unified)
impl<'a, T, S: ChannelStrategy, Q: Queue<T>> GenericReceiver<'a, T, S, Q> {
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
    receiver: &'b mut GenericReceiver<'a, T, S, Q>,
}

impl<'a, 'b, T, S: ChannelStrategy, Q: Queue<T>> Future for RecvFuture<'a, 'b, T, S, Q> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        Pin::new(&mut *this.receiver).poll_next(cx)
    }
}

impl<'a, T, S: ChannelStrategy, Q: Queue<T>> Stream for GenericReceiver<'a, T, S, Q> {
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

// --- Owned Implementations ---

pub fn owned_unbounded<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Arc::new(State::unbounded());
    (
        GenericOwnedSender {
            state: state.clone(),
            _marker: veloq_std::marker::PhantomData,
        },
        GenericOwnedReceiver {
            state,
            _marker: veloq_std::marker::PhantomData,
        },
    )
}

pub fn owned_bounded<T>(capacity: usize) -> (BoundedOwnedSender<T>, BoundedOwnedReceiver<T>) {
    let state = Arc::new(State::bounded(capacity));
    (
        GenericOwnedSender {
            state: state.clone(),
            _marker: veloq_std::marker::PhantomData,
        },
        GenericOwnedReceiver {
            state,
            _marker: veloq_std::marker::PhantomData,
        },
    )
}

impl<T, S: ChannelStrategy, Q> Clone for GenericOwnedSender<T, S, Q> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(GenericSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        let _cloned = ManuallyDrop::new(sender.clone());
        GenericOwnedSender {
            state: self.state.clone(),
            _marker: veloq_std::marker::PhantomData,
        }
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericOwnedSender<T, S, Q> {
    fn drop(&mut self) {
        drop(GenericSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericOwnedReceiver<T, S, Q> {
    fn drop(&mut self) {
        drop(GenericReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
    }
}

impl<T> GenericOwnedSender<T, UnboundedStrategy, SegQueue<T>> {
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        sender.send(val)
    }
}

impl<T> GenericOwnedSender<T, BoundedStrategy, ArrayQueue<T>> {
    pub async fn send(&self, val: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericSender {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        sender.send(val).await
    }
}

impl<T, S: ChannelStrategy, Q: Queue<T>> GenericOwnedReceiver<T, S, Q> {
    pub async fn recv(&mut self) -> Option<T> {
        let mut receiver = ManuallyDrop::new(GenericReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        receiver.recv().await
    }

    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        let mut receiver = ManuallyDrop::new(GenericReceiver {
            state: &self.state,
            _marker: veloq_std::marker::PhantomData,
        });
        receiver.try_recv()
    }
}

impl<T, S: ChannelStrategy, Q: Queue<T>> Stream for GenericOwnedReceiver<T, S, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let mut receiver = ManuallyDrop::new(GenericReceiver {
            state: &this.state,
            _marker: veloq_std::marker::PhantomData,
        });
        Pin::new(&mut *receiver).poll_next(cx)
    }
}
