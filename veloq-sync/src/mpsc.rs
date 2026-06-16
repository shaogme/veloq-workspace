use crate::{
    SendError, TryRecvError,
    shim::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        queue::{ArrayQueue, SegQueue},
    },
};
use futures_core::stream::Stream;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};
use veloq_atomic_waker::AtomicWaker;

#[doc(hidden)]
pub trait Queue<T> {
    fn pop(&self) -> Option<T>;
}

impl<T> Queue<T> for SegQueue<T> {
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

impl<T> Queue<T> for ArrayQueue<T> {
    fn pop(&self) -> Option<T> {
        self.pop()
    }
}

/// A multi-producer, single-consumer channel for sending values across threads
/// to a local executor task.
///
/// This implementation uses `crossbeam_queue::SegQueue` for efficient lock-free queuing
/// and `atomic_waker` for async notification.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Arc::new(Shared {
        queue: SegQueue::new(),
        state: ChannelState::new(),
        strategy: UnboundedStrategy,
        _marker: std::marker::PhantomData,
    });

    (
        GenericSender {
            shared: shared.clone(),
            _marker: std::marker::PhantomData,
        },
        GenericReceiver {
            shared,
            _marker: std::marker::PhantomData,
        },
    )
}

/// A bounded multi-producer, single-consumer channel.
///
/// This implementation uses `crossbeam_queue::ArrayQueue` and explicitly tracks capacity.
/// Senders wait asynchronously if the channel is full.
pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "capacity must be > 0");
    let shared = Arc::new(Shared {
        queue: ArrayQueue::new(capacity),
        state: ChannelState::new(),
        strategy: BoundedStrategy::new(capacity),
        _marker: std::marker::PhantomData,
    });

    (
        GenericSender {
            shared: shared.clone(),
            _marker: std::marker::PhantomData,
        },
        GenericReceiver {
            shared,
            _marker: std::marker::PhantomData,
        },
    )
}

// Type Aliases to maintain API compatibility
pub type Sender<T> = GenericSender<T, UnboundedStrategy, SegQueue<T>>;
pub type Receiver<T> = GenericReceiver<T, UnboundedStrategy, SegQueue<T>>;
pub type BoundedSender<T> = GenericSender<T, BoundedStrategy, ArrayQueue<T>>;
pub type BoundedReceiver<T> = GenericReceiver<T, BoundedStrategy, ArrayQueue<T>>;

// --- Core State Logic ---

struct ChannelState {
    rx_waker: AtomicWaker,
    /// Number of active senders. Used to determine when to wake the receiver
    /// upon the last sender disconnecting.
    sender_count: AtomicUsize,
    /// Indicates if the receiver is still active.
    receiver_active: AtomicBool,
}

impl ChannelState {
    fn new() -> Self {
        Self {
            rx_waker: AtomicWaker::new(),
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

pub trait ChannelStrategy: Send + Sync + 'static {
    fn on_rx_drop(&self);
    fn on_msg_recv(&self);
}

pub struct UnboundedStrategy;

impl ChannelStrategy for UnboundedStrategy {
    fn on_rx_drop(&self) {}
    fn on_msg_recv(&self) {}
}

pub struct BoundedStrategy {
    capacity: usize,
    size: AtomicUsize,
    send_waiters: SegQueue<Arc<WakerState>>,
}

struct WakerState {
    waker: AtomicWaker,
    waiting: AtomicBool,
}

impl BoundedStrategy {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            size: AtomicUsize::new(0),
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
        self.size.fetch_sub(1, Ordering::Release);
        self.wake_one_sender();
    }
}

// --- Shared Structure ---

struct Shared<T, S, Q> {
    queue: Q,
    state: ChannelState,
    strategy: S,
    _marker: std::marker::PhantomData<fn() -> T>,
}

// --- Generic Structures ---

pub struct GenericSender<T, S: ChannelStrategy, Q> {
    shared: Arc<Shared<T, S, Q>>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

pub struct GenericReceiver<T, S: ChannelStrategy, Q> {
    shared: Arc<Shared<T, S, Q>>,
    _marker: std::marker::PhantomData<fn() -> T>,
}

// --- Implementations ---

impl<T, S: ChannelStrategy, Q> Clone for GenericSender<T, S, Q> {
    fn clone(&self) -> Self {
        self.shared.state.inc_sender();
        Self {
            shared: self.shared.clone(),
            _marker: std::marker::PhantomData,
        }
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericSender<T, S, Q> {
    fn drop(&mut self) {
        if self.shared.state.dec_sender() {
            self.shared.state.wake_rx();
        }
    }
}

impl<T, S: ChannelStrategy, Q> Drop for GenericReceiver<T, S, Q> {
    fn drop(&mut self) {
        self.shared.state.set_rx_inactive();
        self.shared.strategy.on_rx_drop();
    }
}

// Unbounded Specifics
impl<T> GenericSender<T, UnboundedStrategy, SegQueue<T>> {
    /// Sends a value to the channel.
    ///
    /// Returns an error if the receiver has been dropped.
    pub fn send(&self, val: T) -> Result<(), SendError<T>> {
        if !self.shared.state.is_rx_active() {
            return Err(SendError(val));
        }

        self.shared.queue.push(val);
        self.shared.state.wake_rx();
        Ok(())
    }
}

// Bounded Specifics
impl<T> GenericSender<T, BoundedStrategy, ArrayQueue<T>> {
    /// Sends a value to the channel.
    ///
    /// Waits if the channel is full. Returns an error if the receiver is dropped.
    pub async fn send(&self, val: T) -> Result<(), SendError<T>> {
        BoundedSendFuture {
            sender: self,
            val: Some(val),
            waker_state: None,
        }
        .await
    }
}

struct BoundedSendFuture<'a, T> {
    sender: &'a GenericSender<T, BoundedStrategy, ArrayQueue<T>>,
    val: Option<T>,
    waker_state: Option<Arc<WakerState>>,
}

impl<'a, T> Drop for BoundedSendFuture<'a, T> {
    fn drop(&mut self) {
        if let Some(ws) = &self.waker_state {
            ws.waiting.store(false, Ordering::Release);
        }
    }
}

impl<'a, T> Future for BoundedSendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let shared = &this.sender.shared;
        let strategy = &shared.strategy;

        // Check Rx active
        if !shared.state.is_rx_active() {
            return Poll::Ready(Err(SendError(this.val.take().unwrap())));
        }

        // Try acquire capacity
        let mut size = strategy.size.load(Ordering::Relaxed);
        loop {
            if size >= strategy.capacity {
                break;
            }
            match strategy.size.compare_exchange_weak(
                size,
                size + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let _ = shared.queue.push(this.val.take().unwrap());
                    shared.state.wake_rx();
                    return Poll::Ready(Ok(()));
                }
                Err(s) => size = s,
            }
        }

        // Register waiter
        if this.waker_state.is_none() {
            this.waker_state = Some(Arc::new(WakerState {
                waker: AtomicWaker::new(),
                waiting: AtomicBool::new(false),
            }));
        }
        let ws = this.waker_state.as_ref().unwrap();
        ws.waker.register(cx.waker());

        if !ws.waiting.swap(true, Ordering::AcqRel) {
            strategy.send_waiters.push(ws.clone());
        }

        // Re-check
        if !shared.state.is_rx_active() {
            return Poll::Ready(Err(SendError(this.val.take().unwrap())));
        }
        if strategy.size.load(Ordering::Acquire) < strategy.capacity {
            ws.waker.wake();
        }

        Poll::Pending
    }
}

// Receiver Methods (Unified)
impl<T, S: ChannelStrategy, Q: Queue<T>> GenericReceiver<T, S, Q> {
    /// Async receive method.
    ///
    /// Returns `None` if the channel is closed (all senders dropped).
    pub async fn recv(&mut self) -> Option<T> {
        RecvFuture { receiver: self }.await
    }

    /// Try to receive a value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.shared.queue.pop() {
            self.shared.strategy.on_msg_recv();
            Ok(msg)
        } else if self.shared.state.sender_count.load(Ordering::Acquire) == 0 {
            // Re-check queue after seeing sender_count == 0 to avoid race with send+drop
            if let Some(msg) = self.shared.queue.pop() {
                self.shared.strategy.on_msg_recv();
                Ok(msg)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }
}

struct RecvFuture<'a, T, S: ChannelStrategy, Q: Queue<T>> {
    receiver: &'a mut GenericReceiver<T, S, Q>,
}

impl<'a, T, S: ChannelStrategy, Q: Queue<T>> Future for RecvFuture<'a, T, S, Q> {
    type Output = Option<T>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.receiver).poll_next(cx)
    }
}

impl<T, S: ChannelStrategy, Q: Queue<T>> Stream for GenericReceiver<T, S, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.shared.state.rx_waker.register(cx.waker());

        if let Some(val) = self.shared.queue.pop() {
            self.shared.strategy.on_msg_recv();
            return Poll::Ready(Some(val));
        }

        if self.shared.state.sender_count.load(Ordering::Acquire) == 0 {
            // Re-check queue after seeing sender_count == 0 to avoid race with send+drop
            if let Some(val) = self.shared.queue.pop() {
                self.shared.strategy.on_msg_recv();
                return Poll::Ready(Some(val));
            }
            return Poll::Ready(None);
        }

        Poll::Pending
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_simple_send_recv() {
        let (tx, mut rx) = unbounded();
        tx.send(1).unwrap();
        tx.send(2).unwrap();

        assert_eq!(rx.try_recv(), Ok(1));
        assert_eq!(rx.try_recv(), Ok(2));
        assert_eq!(rx.try_recv(), Err(TryRecvError::Empty));
    }

    #[test]
    fn test_threaded_send() {
        let (tx, mut rx) = unbounded();
        let tx = Arc::new(tx);

        let mut handles = vec![];
        for i in 0..10 {
            let tx = tx.clone();
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    tx.send(i * 100 + j).unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Wait for all messages
        let mut count = 0;
        // Drain the channel
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 1000);
    }

    #[test]
    fn test_bounded_async() {
        let (tx, mut rx) = bounded(1);

        let mut send_fut = Box::pin(tx.send(1));
        let waker = dummy_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        // First send succeeds
        assert!(send_fut.as_mut().poll(&mut cx).is_ready());

        let mut send_fut2 = Box::pin(tx.send(2));
        // Second send blocks
        assert!(send_fut2.as_mut().poll(&mut cx).is_pending());

        // Recv frees space
        assert_eq!(rx.try_recv(), Ok(1));

        // Second send succeeds now (after re-poll)
        assert!(send_fut2.as_mut().poll(&mut cx).is_ready());

        assert_eq!(rx.try_recv(), Ok(2));
    }

    fn dummy_waker() -> std::task::Waker {
        use std::task::{RawWaker, RawWakerVTable};
        unsafe fn clone(_: *const ()) -> RawWaker {
            dummy_raw_waker()
        }
        unsafe fn wake(_: *const ()) {}
        unsafe fn wake_by_ref(_: *const ()) {}
        unsafe fn drop(_: *const ()) {}
        static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
        fn dummy_raw_waker() -> RawWaker {
            RawWaker::new(std::ptr::null(), &VTABLE)
        }
        unsafe { std::task::Waker::from_raw(dummy_raw_waker()) }
    }
}
