use crate::{
    SendError, TryRecvError, TrySendError,
    notify::Notify,
    shim::queue::{ArrayQueue, Queue, SegQueue},
    waker::{ConcurrentWaiterAdapter, ConcurrentWaiterNode},
};
use futures_core::stream::Stream;
use veloq_intrusive_linklist::ConcurrentLinkedList;
use veloq_std::{
    future::Future,
    mem::ManuallyDrop,
    ops::AsyncFnOnce,
    pin::{Pin, pin},
    ptr::NonNull,
    sync::{
        Arc, SpinLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

pub mod flavor {
    use super::*;

    pub trait ChannelFlavor: Send + Sync {
        fn new() -> Self;
        fn release(&self);
        fn notify_all_senders(&self);
        fn wait_send(&self, is_full: impl Fn() -> bool + Send) -> impl Future<Output = ()> + Send;
    }

    pub struct Unbounded;

    impl ChannelFlavor for Unbounded {
        fn new() -> Self {
            Unbounded
        }
        fn release(&self) {}
        fn notify_all_senders(&self) {}
        async fn wait_send(&self, _is_full: impl Fn() -> bool + Send) {}
    }

    pub struct Bounded {
        notify: Notify,
    }

    unsafe impl Send for Bounded {}
    unsafe impl Sync for Bounded {}

    impl ChannelFlavor for Bounded {
        fn new() -> Self {
            Self {
                notify: Notify::new(),
            }
        }

        fn release(&self) {
            self.notify.notify_one();
        }

        fn notify_all_senders(&self) {
            self.notify.notify_waiters();
        }

        async fn wait_send(&self, is_full: impl Fn() -> bool + Send) {
            let mut notified = pin!(self.notify.notified());
            notified.as_mut().enable();
            if !is_full() {
                return;
            }
            notified.await;
        }
    }
}

use flavor::{Bounded, ChannelFlavor, Unbounded};

// --- API ---

// Type Aliases to maintain API compatibility
pub type BorrowedSender<'a, T> = GenericBorrowedSender<'a, T, Unbounded, SegQueue<T>>;
pub type BorrowedReceiver<'a, T> = GenericBorrowedReceiver<'a, T, Unbounded, SegQueue<T>>;
pub type BorrowedBoundedSender<'a, T> = GenericBorrowedSender<'a, T, Bounded, ArrayQueue<T>>;
pub type BorrowedBoundedReceiver<'a, T> = GenericBorrowedReceiver<'a, T, Bounded, ArrayQueue<T>>;

pub type BoundedBorrowedSender<'a, T> = BorrowedBoundedSender<'a, T>;
pub type BoundedBorrowedReceiver<'a, T> = BorrowedBoundedReceiver<'a, T>;

pub type BorrowedGenericSender<'a, T, F, Q> = GenericBorrowedSender<'a, T, F, Q>;
pub type BorrowedGenericReceiver<'a, T, F, Q> = GenericBorrowedReceiver<'a, T, F, Q>;

pub type Sender<T> = GenericSender<T, Unbounded, SegQueue<T>>;
pub type Receiver<T> = GenericReceiver<T, Unbounded, SegQueue<T>>;
pub type BoundedSender<T> = GenericSender<T, Bounded, ArrayQueue<T>>;
pub type BoundedReceiver<T> = GenericReceiver<T, Bounded, ArrayQueue<T>>;

/// Creates a new borrowed unbounded MPMC channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_unbounded<T: Send, F, R>(f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = Inner::new(0);
    let (tx, rx) = state.split();
    f(tx, rx).await
}

/// Creates a new borrowed bounded MPMC channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_bounded<T: Send, F, R>(capacity: usize, f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedBoundedSender<'a, T>, BorrowedBoundedReceiver<'a, T>) -> R,
{
    assert!(capacity > 0);
    let state = Inner::new(capacity);
    let (tx, rx) = state.split();
    f(tx, rx).await
}

// --- Inner ---

struct Inner<T, F: ChannelFlavor, Q: Queue<T>> {
    pub(crate) queue: Q,

    // 接收协调锁同时保护队列检查、等待节点注册和节点摘除。
    pub(crate) recv_coord: SpinLock<ConcurrentLinkedList<ConcurrentWaiterAdapter>>,

    pub(crate) is_closed: AtomicBool,
    pub(crate) sender_count: AtomicUsize,
    pub(crate) receiver_count: AtomicUsize,

    pub(crate) flavor: F,
    _marker: veloq_std::marker::PhantomData<T>,
}

unsafe impl<T: Send, F: ChannelFlavor, Q: Queue<T>> Send for Inner<T, F, Q> {}
unsafe impl<T: Send, F: ChannelFlavor, Q: Queue<T>> Sync for Inner<T, F, Q> {}

impl<T, F: ChannelFlavor, Q: Queue<T>> Inner<T, F, Q> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Q::new(capacity),
            recv_coord: SpinLock::new(ConcurrentLinkedList::new(ConcurrentWaiterAdapter::NEW)),
            is_closed: AtomicBool::new(false),
            sender_count: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(1),
            flavor: F::new(),
            _marker: veloq_std::marker::PhantomData,
        }
    }

    fn split(
        &self,
    ) -> (
        GenericBorrowedSender<'_, T, F, Q>,
        GenericBorrowedReceiver<'_, T, F, Q>,
    ) {
        self.sender_count.store(1, Ordering::SeqCst);
        self.receiver_count.store(1, Ordering::SeqCst);
        self.is_closed.store(false, Ordering::SeqCst);
        (
            GenericBorrowedSender { state: self },
            GenericBorrowedReceiver { state: self },
        )
    }

    fn close(&self) {
        if self.close_receivers() {
            self.wake_all_receivers();
        }
    }

    fn close_recv(&self) {
        if self.close_receivers() {
            self.wake_all_receivers();
            // Wake all senders after closing under the receive coordination lock.
            self.flavor.notify_all_senders();
        }
    }

    fn notify_recv_one(&self) {
        let waker = {
            let mut lock = self.recv_coord.lock();
            lock.with_mut(|waiters| {
                let node = unsafe { waiters.pop_front_ptr() }?;
                unsafe { node.as_ref().waker.take() }
            })
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn close_receivers(&self) -> bool {
        let mut lock = self.recv_coord.lock();
        lock.with_mut(|_| !self.is_closed.swap(true, Ordering::SeqCst))
    }

    fn wake_all_receivers(&self) {
        loop {
            let waker = {
                let mut lock = self.recv_coord.lock();
                lock.with_mut(|waiters| {
                    let node = unsafe { waiters.pop_front_ptr() }?;
                    unsafe { node.as_ref().waker.take() }
                })
            };
            let Some(waker) = waker else {
                let lock = self.recv_coord.lock();
                let empty = lock.with(|waiters| waiters.is_empty());
                drop(lock);
                if empty {
                    return;
                }
                continue;
            };
            waker.wake();
        }
    }
}

// --- Borrowed Structs ---

pub struct GenericBorrowedSender<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a Inner<T, F, Q>,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericBorrowedSender<'a, T, F, Q> {
    fn clone(&self) -> Self {
        self.state.sender_count.fetch_add(1, Ordering::Relaxed);
        Self { state: self.state }
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericBorrowedSender<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.state.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.close();
        }
    }
}

pub struct GenericBorrowedReceiver<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a Inner<T, F, Q>,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericBorrowedReceiver<'a, T, F, Q> {
    fn clone(&self) -> Self {
        self.state.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self { state: self.state }
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericBorrowedReceiver<'a, T, F, Q> {
    fn drop(&mut self) {
        self.state.receiver_count.fetch_sub(1, Ordering::Relaxed);
        if self.state.receiver_count.load(Ordering::Acquire) == 0 {
            self.state.close_recv();
        }
    }
}

// --- Implementations ---

impl<'a, T: Send, F: ChannelFlavor, Q: Queue<T>> GenericBorrowedSender<'a, T, F, Q> {
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if self.state.is_closed.load(Ordering::Relaxed) {
            return Err(TrySendError::Closed(msg));
        }

        match self.state.queue.push(msg) {
            Ok(_) => {
                self.state.notify_recv_one();
                Ok(())
            }
            Err(msg) => Err(TrySendError::Full(msg)),
        }
    }

    pub async fn send(&self, mut msg: T) -> Result<(), SendError<T>> {
        loop {
            if self.state.is_closed.load(Ordering::Relaxed) {
                return Err(SendError(msg));
            }

            match self.try_send(msg) {
                Ok(_) => return Ok(()),
                Err(TrySendError::Closed(m)) => return Err(SendError(m)),
                Err(TrySendError::Full(m)) => {
                    msg = m;
                }
            }

            self.state
                .flavor
                .wait_send(|| self.state.queue.is_full())
                .await;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.is_closed.load(Ordering::Relaxed)
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> GenericBorrowedReceiver<'a, T, F, Q> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        try_recv(self.state)
    }

    pub async fn recv(&self) -> Result<T, TryRecvError> {
        if let Ok(msg) = self.try_recv() {
            return Ok(msg);
        }

        RecvFuture {
            receiver: self,
            node: ConcurrentWaiterNode::new(),
        }
        .await
    }

    pub fn stream(&self) -> BorrowedReceiverStream<'_, 'a, T, F, Q> {
        BorrowedReceiverStream {
            receiver: self,
            node: ConcurrentWaiterNode::new(),
        }
    }
}

// --- Futures ---

struct RecvFuture<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> {
    receiver: &'b GenericBorrowedReceiver<'a, T, F, Q>,
    node: ConcurrentWaiterNode,
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Future for RecvFuture<'a, 'b, T, F, Q> {
    type Output = Result<T, TryRecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_recv(
            this.receiver.state,
            unsafe { Pin::new_unchecked(&mut this.node) },
            cx,
        )
    }
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Drop for RecvFuture<'a, 'b, T, F, Q> {
    fn drop(&mut self) {
        remove_recv_waiter(self.receiver.state, &self.node);
    }
}

fn try_recv<T, F: ChannelFlavor, Q: Queue<T>>(state: &Inner<T, F, Q>) -> Result<T, TryRecvError> {
    let result = {
        let mut lock = state.recv_coord.lock();
        lock.with_mut(|_| {
            if let Some(msg) = state.queue.pop() {
                Ok(msg)
            } else if state.is_closed.load(Ordering::Acquire) {
                Err(TryRecvError::Disconnected)
            } else {
                Err(TryRecvError::Empty)
            }
        })
    };
    if result.is_ok() {
        state.flavor.release();
    }
    result
}

fn poll_recv<T, F: ChannelFlavor, Q: Queue<T>>(
    state: &Inner<T, F, Q>,
    node: Pin<&mut ConcurrentWaiterNode>,
    cx: &mut Context<'_>,
) -> Poll<Result<T, TryRecvError>> {
    let mut pending_waker = None;
    unsafe {
        node.as_ref().get_ref().waker.register(cx.waker());
    }
    let result = {
        let mut lock = state.recv_coord.lock();
        lock.with_mut(|waiters| {
            let node_ref = node.as_ref().get_ref();
            if let Some(msg) = state.queue.pop() {
                unlink_waiter_locked(waiters, node_ref);
                pending_waker = node_ref.waker.take();
                return Poll::Ready(Ok(msg));
            }

            if state.is_closed.load(Ordering::Acquire) {
                unlink_waiter_locked(waiters, node_ref);
                pending_waker = node_ref.waker.take();
                return Poll::Ready(Err(TryRecvError::Disconnected));
            }

            if !node_ref.link.is_linked() {
                unsafe {
                    waiters.push_back_ptr(NonNull::from(node_ref));
                }
            }
            Poll::Pending
        })
    };
    drop(pending_waker);
    if matches!(&result, Poll::Ready(Ok(_))) {
        state.flavor.release();
    }
    result
}

fn unlink_waiter_locked(
    waiters: &mut ConcurrentLinkedList<ConcurrentWaiterAdapter>,
    node: &ConcurrentWaiterNode,
) {
    if node.link.is_linked() {
        unsafe {
            let ptr = NonNull::from(node);
            let mut cursor = waiters.cursor_mut_from_ptr(ptr);
            cursor.remove();
        }
    }
}

fn remove_recv_waiter<T, F: ChannelFlavor, Q: Queue<T>>(
    state: &Inner<T, F, Q>,
    node: &ConcurrentWaiterNode,
) {
    let mut pending_waker = None;
    {
        let mut lock = state.recv_coord.lock();
        lock.with_mut(|waiters| {
            if node.link.is_linked() {
                unlink_waiter_locked(waiters, node);
                pending_waker = node.waker.take();
            }
        });
    }
    drop(pending_waker);
}

pub struct BorrowedReceiverStream<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> {
    receiver: &'b GenericBorrowedReceiver<'a, T, F, Q>,
    node: ConcurrentWaiterNode,
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Stream for BorrowedReceiverStream<'a, 'b, T, F, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_recv(
            this.receiver.state,
            unsafe { Pin::new_unchecked(&mut this.node) },
            cx,
        )
        .map(|result| result.ok())
    }
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Drop for BorrowedReceiverStream<'a, 'b, T, F, Q> {
    fn drop(&mut self) {
        remove_recv_waiter(self.receiver.state, &self.node);
    }
}

// --- Owned Structs ---

pub struct GenericSender<T, F: ChannelFlavor, Q: Queue<T>> {
    state: Arc<Inner<T, F, Q>>,
}

pub struct GenericReceiver<T, F: ChannelFlavor, Q: Queue<T>> {
    state: Arc<Inner<T, F, Q>>,
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericSender<T, F, Q> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(GenericBorrowedSender { state: &self.state });
        let _cloned = ManuallyDrop::new(sender.clone());
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericSender<T, F, Q> {
    fn drop(&mut self) {
        drop(GenericBorrowedSender { state: &self.state });
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericReceiver<T, F, Q> {
    fn clone(&self) -> Self {
        let receiver = ManuallyDrop::new(GenericBorrowedReceiver { state: &self.state });
        let _cloned = ManuallyDrop::new(receiver.clone());
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericReceiver<T, F, Q> {
    fn drop(&mut self) {
        drop(GenericBorrowedReceiver { state: &self.state });
    }
}

impl<T: Send, F: ChannelFlavor, Q: Queue<T>> GenericSender<T, F, Q> {
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        let sender = ManuallyDrop::new(GenericBorrowedSender { state: &self.state });
        sender.try_send(msg)
    }

    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericBorrowedSender { state: &self.state });
        sender.send(msg).await
    }

    pub fn is_closed(&self) -> bool {
        let sender = ManuallyDrop::new(GenericBorrowedSender { state: &self.state });
        sender.is_closed()
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> GenericReceiver<T, F, Q> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(GenericBorrowedReceiver { state: &self.state });
        receiver.try_recv()
    }

    pub async fn recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(GenericBorrowedReceiver { state: &self.state });
        receiver.recv().await
    }

    pub fn stream(&self) -> ReceiverStream<'_, T, F, Q> {
        ReceiverStream {
            state: &self.state,
            node: ConcurrentWaiterNode::new(),
        }
    }
}

pub struct ReceiverStream<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a Inner<T, F, Q>,
    node: ConcurrentWaiterNode,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Stream for ReceiverStream<'a, T, F, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        poll_recv(
            this.state,
            unsafe { Pin::new_unchecked(&mut this.node) },
            cx,
        )
        .map(|result| result.ok())
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for ReceiverStream<'a, T, F, Q> {
    fn drop(&mut self) {
        remove_recv_waiter(self.state, &self.node);
    }
}

pub fn unbounded<T: Send>() -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Inner::new(0));
    (
        GenericSender {
            state: state.clone(),
        },
        GenericReceiver { state },
    )
}

pub fn bounded<T: Send>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0);
    let state = Arc::new(Inner::new(capacity));
    (
        GenericSender {
            state: state.clone(),
        },
        GenericReceiver { state },
    )
}
