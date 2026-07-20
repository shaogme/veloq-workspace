use crate::{
    SendError, TryRecvError, TrySendError,
    shim::queue::{ArrayQueue, Queue, SegQueue},
    waker::{ConcurrentWaiterAdapter, ConcurrentWaiterNode},
};
use futures_core::stream::Stream;
use veloq_intrusive_linklist::ConcurrentLinkedList;
use veloq_std::{
    future::Future,
    mem::ManuallyDrop,
    pin::Pin,
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
        fn register_send_wait(
            &self,
            node: Pin<&mut ConcurrentWaiterNode>,
            cx: &Context<'_>,
            is_full: impl Fn() -> bool,
        ) -> bool;
        fn remove_send_wait(&self, node: Pin<&mut ConcurrentWaiterNode>);
        fn notify_all_senders(&self);
    }

    pub struct Unbounded;

    impl ChannelFlavor for Unbounded {
        fn new() -> Self {
            Unbounded
        }
        fn release(&self) {}
        fn register_send_wait(
            &self,
            _node: Pin<&mut ConcurrentWaiterNode>,
            _cx: &Context<'_>,
            _is_full: impl Fn() -> bool,
        ) -> bool {
            false
        }
        fn remove_send_wait(&self, _node: Pin<&mut ConcurrentWaiterNode>) {}
        fn notify_all_senders(&self) {}
    }

    pub struct Bounded {
        waiters: SpinLock<ConcurrentLinkedList<ConcurrentWaiterAdapter>>,
        waiter_count: AtomicUsize,
    }

    // Safety: The LinkedList holds NonNull pointers which are !Send/!Sync.
    // However, the nodes they point to are pinned Futures or Tasks which remain valid
    // while linked. Access is synchronized via Mutex.
    unsafe impl Send for Bounded {}
    unsafe impl Sync for Bounded {}

    impl ChannelFlavor for Bounded {
        fn new() -> Self {
            Self {
                waiters: SpinLock::new(ConcurrentLinkedList::new(ConcurrentWaiterAdapter::NEW)),
                waiter_count: AtomicUsize::new(0),
            }
        }

        fn release(&self) {
            // 优化：只有当有等待者时才尝试锁
            if self.waiter_count.load(Ordering::Relaxed) > 0 {
                let mut lock = self.waiters.lock();
                lock.with_mut(|l| {
                    if let Some(node) = l.pop_front() {
                        self.waiter_count.fetch_sub(1, Ordering::Relaxed);
                        node.as_ref().waker.wake();
                    }
                });
            }
        }

        fn register_send_wait(
            &self,
            node: Pin<&mut ConcurrentWaiterNode>,
            cx: &Context<'_>,
            is_full: impl Fn() -> bool,
        ) -> bool {
            unsafe {
                node.as_ref().waker.register(cx.waker());
            }
            let mut lock = self.waiters.lock();
            // Double check
            if !is_full() {
                return false; // Retry acquire
            }
            let is_linked = lock.with(|_| node.as_ref().link.is_linked());
            unsafe {
                if !is_linked {
                    lock.with_mut(|l| l.push_back(node));
                    self.waiter_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            true
        }

        fn remove_send_wait(&self, node: Pin<&mut ConcurrentWaiterNode>) {
            // Must acquire lock to check linkage safely to avoid race with notify
            let mut lock = self.waiters.lock();
            let is_linked = lock.with(|_| node.link.is_linked());
            if is_linked {
                unsafe {
                    let ptr = NonNull::from(&*node);
                    lock.with_mut(|l| {
                        let mut cursor = l.cursor_mut_from_ptr(ptr);
                        cursor.remove();
                    });
                    self.waiter_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }

        fn notify_all_senders(&self) {
            let mut lock = self.waiters.lock();
            lock.with_mut(|l| {
                while let Some(node) = l.pop_front() {
                    node.as_ref().waker.wake();
                }
            });
            self.waiter_count.store(0, Ordering::Relaxed);
        }
    }
}

use flavor::{Bounded, ChannelFlavor, Unbounded};

// --- API ---

pub type Sender<'a, T> = GenericSender<'a, T, Unbounded, SegQueue<T>>;
pub type Receiver<'a, T> = GenericReceiver<'a, T, Unbounded, SegQueue<T>>;

pub type BoundedSender<'a, T> = GenericSender<'a, T, Bounded, ArrayQueue<T>>;
pub type BoundedReceiver<'a, T> = GenericReceiver<'a, T, Bounded, ArrayQueue<T>>;

pub type OwnedSender<T> = GenericOwnedSender<T, Unbounded, SegQueue<T>>;
pub type OwnedReceiver<T> = GenericOwnedReceiver<T, Unbounded, SegQueue<T>>;
pub type BoundedOwnedSender<T> = GenericOwnedSender<T, Bounded, ArrayQueue<T>>;
pub type BoundedOwnedReceiver<T> = GenericOwnedReceiver<T, Bounded, ArrayQueue<T>>;

pub fn unbounded<T: Send>() -> State<T, Unbounded, SegQueue<T>> {
    State::new(0)
}

pub fn bounded<T: Send>(capacity: usize) -> State<T, Bounded, ArrayQueue<T>> {
    assert!(capacity > 0);
    State::new(capacity)
}

// --- State ---

pub struct State<T, F: ChannelFlavor, Q: Queue<T>> {
    pub(crate) queue: Q,

    // 接收等待队列 (通用)
    pub(crate) recv_waiters: SpinLock<ConcurrentLinkedList<ConcurrentWaiterAdapter>>,
    pub(crate) recv_waiter_count: AtomicUsize,

    pub(crate) is_closed: AtomicBool,
    pub(crate) sender_count: AtomicUsize,
    pub(crate) receiver_count: AtomicUsize,

    pub(crate) flavor: F,
    _marker: veloq_std::marker::PhantomData<T>,
}

unsafe impl<T: Send, F: ChannelFlavor, Q: Queue<T>> Send for State<T, F, Q> {}
unsafe impl<T: Send, F: ChannelFlavor, Q: Queue<T>> Sync for State<T, F, Q> {}

impl<T, F: ChannelFlavor, Q: Queue<T>> State<T, F, Q> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: Q::new(capacity),
            recv_waiters: SpinLock::new(ConcurrentLinkedList::new(ConcurrentWaiterAdapter::NEW)),
            recv_waiter_count: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            sender_count: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(1),
            flavor: F::new(),
            _marker: veloq_std::marker::PhantomData,
        }
    }

    pub fn split(&self) -> (GenericSender<'_, T, F, Q>, GenericReceiver<'_, T, F, Q>) {
        self.sender_count.store(1, Ordering::SeqCst);
        self.receiver_count.store(1, Ordering::SeqCst);
        self.is_closed.store(false, Ordering::SeqCst);
        (
            GenericSender { state: self },
            GenericReceiver { state: self },
        )
    }

    fn close(&self) {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            // Wake all receivers
            let mut lock = self.recv_waiters.lock();
            lock.with_mut(|l| {
                while let Some(node) = l.pop_front() {
                    node.as_ref().waker.wake();
                }
            });
        }
    }

    fn close_recv(&self) {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            // Wake all senders
            self.flavor.notify_all_senders();
        }
    }

    fn notify_recv_one(&self) {
        if self.recv_waiter_count.load(Ordering::Relaxed) > 0 {
            let mut lock = self.recv_waiters.lock();
            lock.with_mut(|l| {
                if let Some(node) = l.pop_front() {
                    self.recv_waiter_count.fetch_sub(1, Ordering::Relaxed);
                    node.as_ref().waker.wake();
                }
            });
        }
    }
}

// --- Borrowed Structs ---

pub struct GenericSender<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a State<T, F, Q>,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericSender<'a, T, F, Q> {
    fn clone(&self) -> Self {
        self.state.sender_count.fetch_add(1, Ordering::Relaxed);
        Self { state: self.state }
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericSender<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.state.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.state.close();
        }
    }
}

pub struct GenericReceiver<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a State<T, F, Q>,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericReceiver<'a, T, F, Q> {
    fn clone(&self) -> Self {
        self.state.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self { state: self.state }
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericReceiver<'a, T, F, Q> {
    fn drop(&mut self) {
        self.state.receiver_count.fetch_sub(1, Ordering::Relaxed);
        if self.state.receiver_count.load(Ordering::Acquire) == 0 {
            self.state.close_recv();
        }
    }
}

// --- Implementations ---

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> GenericSender<'a, T, F, Q> {
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

    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        match self.try_send(msg) {
            Ok(_) => Ok(()),
            Err(TrySendError::Closed(m)) => Err(SendError(m)),
            Err(TrySendError::Full(m)) => {
                SendFuture {
                    sender: self,
                    msg: Some(m),
                    node: ConcurrentWaiterNode::new(),
                    queued: false,
                }
                .await
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.state.is_closed.load(Ordering::Relaxed)
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> GenericReceiver<'a, T, F, Q> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.state.queue.pop() {
            self.state.flavor.release();
            Ok(msg)
        } else if self.state.is_closed.load(Ordering::Relaxed) {
            if let Some(msg) = self.state.queue.pop() {
                self.state.flavor.release();
                Ok(msg)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub async fn recv(&self) -> Result<T, TryRecvError> {
        if let Ok(msg) = self.try_recv() {
            return Ok(msg);
        }

        RecvFuture {
            receiver: self,
            node: ConcurrentWaiterNode::new(),
            queued: false,
        }
        .await
    }

    pub fn stream(&self) -> ReceiverStream<'_, 'a, T, F, Q> {
        ReceiverStream {
            receiver: self,
            node: ConcurrentWaiterNode::new(),
            queued: false,
        }
    }
}

// --- Futures ---

struct SendFuture<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> {
    sender: &'b GenericSender<'a, T, F, Q>,
    msg: Option<T>,
    node: ConcurrentWaiterNode,
    queued: bool,
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Future for SendFuture<'a, 'b, T, F, Q> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let state = this.sender.state;

        loop {
            match state.queue.push(this.msg.take().unwrap()) {
                Ok(_) => {
                    if this.queued {
                        let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                        state.flavor.remove_send_wait(node_pin);
                        this.queued = false;
                    }
                    state.notify_recv_one();
                    return Poll::Ready(Ok(()));
                }
                Err(m) => {
                    this.msg = Some(m);
                }
            }

            if state.is_closed.load(Ordering::Relaxed) {
                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    state.flavor.remove_send_wait(node_pin);
                }
                return Poll::Ready(Err(SendError(this.msg.take().unwrap())));
            }

            if !this.queued || !this.node.link.is_linked() {
                let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                if state
                    .flavor
                    .register_send_wait(node_pin, cx, || state.queue.is_full())
                {
                    this.queued = true;
                    return Poll::Pending;
                } else {
                    continue;
                }
            } else {
                unsafe {
                    this.node.waker.register(cx.waker());
                }
                return Poll::Pending;
            }
        }
    }
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Drop for SendFuture<'a, 'b, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            let node_pin = unsafe { Pin::new_unchecked(&mut self.node) };
            self.sender.state.flavor.remove_send_wait(node_pin);
        }
    }
}

struct RecvFuture<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> {
    receiver: &'b GenericReceiver<'a, T, F, Q>,
    node: ConcurrentWaiterNode,
    queued: bool,
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Future for RecvFuture<'a, 'b, T, F, Q> {
    type Output = Result<T, TryRecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        let state = this.receiver.state;

        loop {
            if let Some(msg) = state.queue.pop() {
                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(state, node_pin);
                    this.queued = false;
                }
                state.flavor.release();
                return Poll::Ready(Ok(msg));
            }

            if state.is_closed.load(Ordering::Relaxed) {
                if let Some(msg) = state.queue.pop() {
                    if this.queued {
                        let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                        remove_recv_waiter(state, node_pin);
                        this.queued = false;
                    }
                    state.flavor.release();
                    return Poll::Ready(Ok(msg));
                }

                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(state, node_pin);
                }
                return Poll::Ready(Err(TryRecvError::Disconnected));
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            if !this.queued || !this.node.link.is_linked() {
                let mut lock = state.recv_waiters.lock();
                if !lock.with(|_| this.node.link.is_linked()) {
                    unsafe {
                        let node_pin = Pin::new_unchecked(&mut this.node);
                        lock.with_mut(|l| l.push_back(node_pin));
                    }
                    state.recv_waiter_count.fetch_add(1, Ordering::Relaxed);
                }
                this.queued = true;
            } else {
                return Poll::Pending;
            }
        }
    }
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Drop for RecvFuture<'a, 'b, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            let node_pin = unsafe { Pin::new_unchecked(&mut self.node) };
            remove_recv_waiter(self.receiver.state, node_pin);
        }
    }
}

fn remove_recv_waiter<T, F: ChannelFlavor, Q: Queue<T>>(
    state: &State<T, F, Q>,
    node: Pin<&mut ConcurrentWaiterNode>,
) {
    let mut lock = state.recv_waiters.lock();
    let is_linked = lock.with(|_| node.link.is_linked());
    if is_linked {
        unsafe {
            let ptr = NonNull::from(&*node);
            lock.with_mut(|l| {
                let mut cursor = l.cursor_mut_from_ptr(ptr);
                cursor.remove();
            });
            state.recv_waiter_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct ReceiverStream<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> {
    receiver: &'b GenericReceiver<'a, T, F, Q>,
    node: ConcurrentWaiterNode,
    queued: bool,
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Stream for ReceiverStream<'a, 'b, T, F, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        let state = this.receiver.state;

        loop {
            if let Some(msg) = state.queue.pop() {
                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(state, node_pin);
                    this.queued = false;
                }
                state.flavor.release();
                return Poll::Ready(Some(msg));
            }

            if state.is_closed.load(Ordering::Relaxed) {
                if let Some(msg) = state.queue.pop() {
                    if this.queued {
                        let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                        remove_recv_waiter(state, node_pin);
                        this.queued = false;
                    }
                    state.flavor.release();
                    return Poll::Ready(Some(msg));
                }

                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(state, node_pin);
                }
                return Poll::Ready(None);
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            let is_linked = this.node.link.is_linked();
            if !this.queued || !is_linked {
                let mut lock = state.recv_waiters.lock();
                let is_linked_under_lock = lock.with(|_| this.node.link.is_linked());
                if !is_linked_under_lock {
                    unsafe {
                        let node_pin = Pin::new_unchecked(&mut this.node);
                        lock.with_mut(|l| l.push_back(node_pin));
                    }
                    state.recv_waiter_count.fetch_add(1, Ordering::Relaxed);
                }
                this.queued = true;
            } else {
                return Poll::Pending;
            }
        }
    }
}

impl<'a, 'b, T, F: ChannelFlavor, Q: Queue<T>> Drop for ReceiverStream<'a, 'b, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            let node_pin = unsafe { Pin::new_unchecked(&mut self.node) };
            remove_recv_waiter(self.receiver.state, node_pin);
        }
    }
}

// --- Owned Structs ---

pub struct GenericOwnedSender<T, F: ChannelFlavor, Q: Queue<T>> {
    state: Arc<State<T, F, Q>>,
}

pub struct GenericOwnedReceiver<T, F: ChannelFlavor, Q: Queue<T>> {
    state: Arc<State<T, F, Q>>,
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericOwnedSender<T, F, Q> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(GenericSender { state: &self.state });
        let _cloned = ManuallyDrop::new(sender.clone());
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericOwnedSender<T, F, Q> {
    fn drop(&mut self) {
        drop(GenericSender { state: &self.state });
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Clone for GenericOwnedReceiver<T, F, Q> {
    fn clone(&self) -> Self {
        let receiver = ManuallyDrop::new(GenericReceiver { state: &self.state });
        let _cloned = ManuallyDrop::new(receiver.clone());
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> Drop for GenericOwnedReceiver<T, F, Q> {
    fn drop(&mut self) {
        drop(GenericReceiver { state: &self.state });
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> GenericOwnedSender<T, F, Q> {
    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        let sender = ManuallyDrop::new(GenericSender { state: &self.state });
        sender.try_send(msg)
    }

    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(GenericSender { state: &self.state });
        sender.send(msg).await
    }

    pub fn is_closed(&self) -> bool {
        let sender = ManuallyDrop::new(GenericSender { state: &self.state });
        sender.is_closed()
    }
}

impl<T, F: ChannelFlavor, Q: Queue<T>> GenericOwnedReceiver<T, F, Q> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(GenericReceiver { state: &self.state });
        receiver.try_recv()
    }

    pub async fn recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(GenericReceiver { state: &self.state });
        receiver.recv().await
    }

    pub fn stream(&self) -> OwnedReceiverStream<'_, T, F, Q> {
        OwnedReceiverStream {
            state: &self.state,
            node: ConcurrentWaiterNode::new(),
            queued: false,
        }
    }
}

pub struct OwnedReceiverStream<'a, T, F: ChannelFlavor, Q: Queue<T>> {
    state: &'a State<T, F, Q>,
    node: ConcurrentWaiterNode,
    queued: bool,
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Stream for OwnedReceiverStream<'a, T, F, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(msg) = this.state.queue.pop() {
                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(this.state, node_pin);
                    this.queued = false;
                }
                this.state.flavor.release();
                return Poll::Ready(Some(msg));
            }

            if this.state.is_closed.load(Ordering::Relaxed) {
                if let Some(msg) = this.state.queue.pop() {
                    if this.queued {
                        let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                        remove_recv_waiter(this.state, node_pin);
                        this.queued = false;
                    }
                    this.state.flavor.release();
                    return Poll::Ready(Some(msg));
                }

                if this.queued {
                    let node_pin = unsafe { Pin::new_unchecked(&mut this.node) };
                    remove_recv_waiter(this.state, node_pin);
                }
                return Poll::Ready(None);
            }

            unsafe {
                this.node.waker.register(cx.waker());
            }

            let is_linked = this.node.link.is_linked();
            if !this.queued || !is_linked {
                let mut lock = this.state.recv_waiters.lock();
                let is_linked_under_lock = lock.with(|_| this.node.link.is_linked());
                if !is_linked_under_lock {
                    unsafe {
                        let node_pin = Pin::new_unchecked(&mut this.node);
                        lock.with_mut(|l| l.push_back(node_pin));
                    }
                    this.state.recv_waiter_count.fetch_add(1, Ordering::Relaxed);
                }
                this.queued = true;
            } else {
                return Poll::Pending;
            }
        }
    }
}

impl<'a, T, F: ChannelFlavor, Q: Queue<T>> Drop for OwnedReceiverStream<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            let node_pin = unsafe { Pin::new_unchecked(&mut self.node) };
            remove_recv_waiter(self.state, node_pin);
        }
    }
}

pub fn owned_unbounded<T: Send>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Arc::new(unbounded());
    (
        GenericOwnedSender {
            state: state.clone(),
        },
        GenericOwnedReceiver { state },
    )
}

pub fn owned_bounded<T: Send>(capacity: usize) -> (BoundedOwnedSender<T>, BoundedOwnedReceiver<T>) {
    let state = Arc::new(bounded(capacity));
    (
        GenericOwnedSender {
            state: state.clone(),
        },
        GenericOwnedReceiver { state },
    )
}
