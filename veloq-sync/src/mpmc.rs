use crate::{
    SendError, TryRecvError, TrySendError,
    shim::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        lock::SpinLock,
        queue::{ArrayQueue, SegQueue},
    },
    waker::{WaiterAdapter, WaiterNode},
};
use futures_core::stream::Stream;
use std::{
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};
use veloq_intrusive_linklist::LinkedList;

mod flavor {
    use super::*;

    pub trait RawQueue<T>: Send + Sync {
        fn new(cap: usize) -> Self;
        fn push(&self, val: T) -> Result<(), T>;
        fn pop(&self) -> Option<T>;
        fn is_full(&self) -> bool;
    }

    impl<T: Send> RawQueue<T> for SegQueue<T> {
        fn new(_cap: usize) -> Self {
            SegQueue::new()
        }
        fn push(&self, val: T) -> Result<(), T> {
            self.push(val);
            Ok(())
        }
        fn pop(&self) -> Option<T> {
            self.pop()
        }
        fn is_full(&self) -> bool {
            false
        }
    }

    impl<T: Send> RawQueue<T> for ArrayQueue<T> {
        fn new(cap: usize) -> Self {
            ArrayQueue::new(cap)
        }
        fn push(&self, val: T) -> Result<(), T> {
            self.push(val)
        }
        fn pop(&self) -> Option<T> {
            self.pop()
        }
        fn is_full(&self) -> bool {
            self.is_full()
        }
    }

    pub trait ChannelFlavor: Send + Sync + 'static {
        fn new() -> Self;
        fn release(&self);
        fn register_send_wait(
            &self,
            node: Pin<&mut WaiterNode>,
            cx: &Context<'_>,
            is_full: impl Fn() -> bool,
        ) -> bool;
        fn remove_send_wait(&self, node: Pin<&mut WaiterNode>);
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
            _node: Pin<&mut WaiterNode>,
            _cx: &Context<'_>,
            _is_full: impl Fn() -> bool,
        ) -> bool {
            false
        }
        fn remove_send_wait(&self, _node: Pin<&mut WaiterNode>) {}
        fn notify_all_senders(&self) {}
    }

    pub struct Bounded {
        waiters: SpinLock<LinkedList<WaiterAdapter>>,
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
                waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
                waiter_count: AtomicUsize::new(0),
            }
        }

        fn release(&self) {
            // 优化：只有当有等待者时才尝试锁
            if self.waiter_count.load(Ordering::Relaxed) > 0 {
                let mut lock = self.waiters.lock();
                if let Some(node) = lock.pop_front() {
                    self.waiter_count.fetch_sub(1, Ordering::Relaxed);
                    node.as_ref().waker.wake();
                }
            }
        }

        fn register_send_wait(
            &self,
            node: Pin<&mut WaiterNode>,
            cx: &Context<'_>,
            is_full: impl Fn() -> bool,
        ) -> bool {
            node.as_ref().waker.register(cx.waker());
            let mut lock = self.waiters.lock();
            // Double check
            if !is_full() {
                return false; // Retry acquire
            }
            unsafe {
                // 如果已经在链表中，就不重复添加？
                // 通常 register 每次 poll 调用。
                if !node.as_ref().link.is_linked() {
                    lock.push_back(node);
                    self.waiter_count.fetch_add(1, Ordering::Relaxed);
                }
            }
            true
        }

        fn remove_send_wait(&self, node: Pin<&mut WaiterNode>) {
            // Must acquire lock to check linkage safely to avoid race with notify
            let mut lock = self.waiters.lock();
            if node.link.is_linked() {
                unsafe {
                    // cursor_mut_from_ptr 需要 unsafe
                    let ptr = NonNull::from(&*node);
                    let mut cursor = lock.cursor_mut_from_ptr(ptr);
                    cursor.remove();
                    self.waiter_count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }

        fn notify_all_senders(&self) {
            let mut lock = self.waiters.lock();
            while let Some(node) = lock.pop_front() {
                node.as_ref().waker.wake();
            }
            self.waiter_count.store(0, Ordering::Relaxed);
        }
    }
}

use flavor::{Bounded, ChannelFlavor, Unbounded};

// --- API ---

pub type Sender<T> = GenericSender<T, Unbounded, SegQueue<T>>;
pub type Receiver<T> = GenericReceiver<T, Unbounded, SegQueue<T>>;

pub type BoundedSender<T> = GenericSender<T, Bounded, ArrayQueue<T>>;
pub type BoundedReceiver<T> = GenericReceiver<T, Bounded, ArrayQueue<T>>;

pub fn unbounded<T: Send>() -> (Sender<T>, Receiver<T>) {
    GenericSender::new(0) // 0 ignored for unbounded
}

pub fn bounded<T: Send>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0);
    GenericSender::new(capacity)
}

// --- Generic Structs ---

pub struct GenericSender<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    shared: Arc<Shared<T, F, Q>>,
}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Clone for GenericSender<T, F, Q> {
    fn clone(&self) -> Self {
        self.shared.sender_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Drop for GenericSender<T, F, Q> {
    fn drop(&mut self) {
        if self.shared.sender_count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shared.close();
        }
    }
}

pub struct GenericReceiver<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    shared: Arc<Shared<T, F, Q>>,
}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Clone for GenericReceiver<T, F, Q> {
    fn clone(&self) -> Self {
        self.shared.receiver_count.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Drop for GenericReceiver<T, F, Q> {
    fn drop(&mut self) {
        self.shared.receiver_count.fetch_sub(1, Ordering::Relaxed);
        // 只有当 receiver_count 降为 0 时，才意味着 CLOSED。
        if self.shared.receiver_count.load(Ordering::Acquire) == 0 {
            // 理论上 Shared::close 会被设置，如果所有 Receiver 都没了，
            // 应该通知 Sender，虽然 send 会报错。
            // 现有的 Shared 只处理了 sender 没了关 channel。
            self.shared.close_recv();
        }
    }
}

// --- Shared ---

struct Shared<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    queue: Q,

    // 接收等待队列 (通用)
    recv_waiters: SpinLock<LinkedList<WaiterAdapter>>,
    recv_waiter_count: AtomicUsize,

    is_closed: AtomicBool,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,

    flavor: F,
    _marker: std::marker::PhantomData<T>,
}

unsafe impl<T: Send, F: ChannelFlavor, Q: flavor::RawQueue<T>> Send for Shared<T, F, Q> {}
unsafe impl<T: Send, F: ChannelFlavor, Q: flavor::RawQueue<T>> Sync for Shared<T, F, Q> {}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Shared<T, F, Q> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: Q::new(capacity),
            recv_waiters: SpinLock::new(LinkedList::new(WaiterAdapter::NEW)),
            recv_waiter_count: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            sender_count: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(1),
            flavor: F::new(),
            _marker: std::marker::PhantomData,
        }
    }

    fn close(&self) {
        if !self.is_closed.swap(true, Ordering::SeqCst) {
            // Wake all receivers
            let mut lock = self.recv_waiters.lock();
            while let Some(node) = lock.pop_front() {
                node.as_ref().waker.wake();
            }
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
            // Recheck
            if let Some(node) = lock.pop_front() {
                self.recv_waiter_count.fetch_sub(1, Ordering::Relaxed);
                node.as_ref().waker.wake();
            }
        }
    }
}

// --- Implementation ---

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> GenericSender<T, F, Q> {
    fn new(capacity: usize) -> (Self, GenericReceiver<T, F, Q>) {
        let shared = Arc::new(Shared::new(capacity));
        (
            Self {
                shared: shared.clone(),
            },
            GenericReceiver { shared },
        )
    }

    pub fn try_send(&self, msg: T) -> Result<(), TrySendError<T>> {
        if self.shared.is_closed.load(Ordering::Relaxed) {
            return Err(TrySendError::Closed(msg));
        }

        match self.shared.queue.push(msg) {
            Ok(_) => {
                self.shared.notify_recv_one();
                Ok(())
            }
            Err(msg) => Err(TrySendError::Full(msg)),
        }
    }

    pub async fn send(&self, msg: T) -> Result<(), SendError<T>> {
        // Optimistic path
        match self.try_send(msg) {
            Ok(_) => Ok(()),
            Err(TrySendError::Closed(m)) => Err(SendError(m)),
            Err(TrySendError::Full(m)) => {
                // Slow path
                SendFuture {
                    sender: self,
                    msg: Some(m),
                    node: Box::pin(WaiterNode::new()),
                    queued: false,
                }
                .await
            }
        }
    }

    pub fn is_closed(&self) -> bool {
        self.shared.is_closed.load(Ordering::Relaxed)
    }
}

impl<T, F: ChannelFlavor, Q: flavor::RawQueue<T>> GenericReceiver<T, F, Q> {
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        if let Some(msg) = self.shared.queue.pop() {
            self.shared.flavor.release(); // For bounded, this wakes sender
            Ok(msg)
        } else if self.shared.is_closed.load(Ordering::Relaxed) {
            // Re-check queue to ensure we didn't miss a message sent just before close
            if let Some(msg) = self.shared.queue.pop() {
                self.shared.flavor.release();
                Ok(msg)
            } else {
                Err(TryRecvError::Disconnected)
            }
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub async fn recv(&self) -> Result<T, TryRecvError> {
        // Optimistic
        if let Ok(msg) = self.try_recv() {
            return Ok(msg);
        }

        RecvFuture {
            receiver: self,
            node: Box::pin(WaiterNode::new()),
            queued: false,
        }
        .await
    }

    pub fn stream(&self) -> ReceiverStream<'_, T, F, Q> {
        ReceiverStream {
            shared: &self.shared,
            node: Box::pin(WaiterNode::new()),
            queued: false,
        }
    }
}

// --- Futures ---

struct SendFuture<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    sender: &'a GenericSender<T, F, Q>,
    msg: Option<T>,
    node: Pin<Box<WaiterNode>>,
    queued: bool,
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Future for SendFuture<'a, T, F, Q> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            // 1. Try acquire again
            match this.sender.shared.queue.push(this.msg.take().unwrap()) {
                Ok(_) => {
                    if this.queued {
                        this.sender
                            .shared
                            .flavor
                            .remove_send_wait(this.node.as_mut());
                        this.queued = false;
                    }
                    this.sender.shared.notify_recv_one();
                    return Poll::Ready(Ok(()));
                }
                Err(m) => {
                    this.msg = Some(m); // restore msg
                }
            }

            // 2. Check closed
            if this.sender.shared.is_closed.load(Ordering::Relaxed) {
                if this.queued {
                    this.sender
                        .shared
                        .flavor
                        .remove_send_wait(this.node.as_mut());
                }
                return Poll::Ready(Err(SendError(this.msg.take().unwrap())));
            }

            // 3. Register wait
            if !this.queued {
                if this
                    .sender
                    .shared
                    .flavor
                    .register_send_wait(this.node.as_mut(), cx, || {
                        this.sender.shared.queue.is_full()
                    })
                {
                    this.queued = true;
                    // registered successfully means we should wait now
                    // Note: register_send_wait typically does a double check inside.
                    return Poll::Pending;
                } else {
                    // retry loop immediately
                    continue;
                }
            } else {
                // Update waker if needed? atomic_waker handles this via register.
                this.node.waker.register(cx.waker());
                // Since we are already queued, and try_acquire failed, we return Pending.
                return Poll::Pending;
            }
        }
    }
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Drop for SendFuture<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            self.sender
                .shared
                .flavor
                .remove_send_wait(self.node.as_mut());
        }
    }
}

struct RecvFuture<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    receiver: &'a GenericReceiver<T, F, Q>,
    node: Pin<Box<WaiterNode>>,
    queued: bool,
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Future for RecvFuture<'a, T, F, Q> {
    type Output = Result<T, TryRecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            // 1. Try Recv
            if let Some(msg) = this.receiver.shared.queue.pop() {
                if this.queued {
                    remove_recv_waiter(&this.receiver.shared, this.node.as_mut());
                    this.queued = false;
                }
                this.receiver.shared.flavor.release();
                return Poll::Ready(Ok(msg));
            }

            // 2. Check Closed
            if this.receiver.shared.is_closed.load(Ordering::Relaxed) {
                // Re-check to ensure no race where item was pushed before close
                if let Some(msg) = this.receiver.shared.queue.pop() {
                    if this.queued {
                        remove_recv_waiter(&this.receiver.shared, this.node.as_mut());
                        this.queued = false;
                    }
                    this.receiver.shared.flavor.release();
                    return Poll::Ready(Ok(msg));
                }

                if this.queued {
                    remove_recv_waiter(&this.receiver.shared, this.node.as_mut());
                }
                return Poll::Ready(Err(TryRecvError::Disconnected));
            }

            this.node.waker.register(cx.waker());

            // 3. Register
            if !this.queued {
                let mut lock = this.receiver.shared.recv_waiters.lock();
                unsafe {
                    lock.push_back(this.node.as_mut());
                }
                this.receiver
                    .shared
                    .recv_waiter_count
                    .fetch_add(1, Ordering::Relaxed);
                this.queued = true;
            } else {
                return Poll::Pending;
            }
        }
    }
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Drop for RecvFuture<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            remove_recv_waiter(&self.receiver.shared, self.node.as_mut());
        }
    }
}

fn remove_recv_waiter<T, F: ChannelFlavor, Q: flavor::RawQueue<T>>(
    shared: &Shared<T, F, Q>,
    node: Pin<&mut WaiterNode>,
) {
    let mut lock = shared.recv_waiters.lock();
    if node.link.is_linked() {
        unsafe {
            let ptr = NonNull::from(&*node);
            let mut cursor = lock.cursor_mut_from_ptr(ptr);
            cursor.remove();
            shared.recv_waiter_count.fetch_sub(1, Ordering::Relaxed);
        }
    }
}

pub struct ReceiverStream<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> {
    shared: &'a Shared<T, F, Q>,
    node: Pin<Box<WaiterNode>>,
    queued: bool,
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Stream for ReceiverStream<'a, T, F, Q> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };

        loop {
            if let Some(msg) = this.shared.queue.pop() {
                if this.queued {
                    remove_recv_waiter(this.shared, this.node.as_mut());
                    this.queued = false;
                }
                this.shared.flavor.release();
                return Poll::Ready(Some(msg));
            }

            if this.shared.is_closed.load(Ordering::Relaxed) {
                // Re-check to ensure no race where item was pushed before close
                if let Some(msg) = this.shared.queue.pop() {
                    if this.queued {
                        remove_recv_waiter(this.shared, this.node.as_mut());
                        this.queued = false;
                    }
                    this.shared.flavor.release();
                    return Poll::Ready(Some(msg));
                }

                if this.queued {
                    remove_recv_waiter(this.shared, this.node.as_mut());
                }
                return Poll::Ready(None);
            }

            this.node.waker.register(cx.waker());

            if !this.queued {
                let mut lock = this.shared.recv_waiters.lock();
                unsafe {
                    lock.push_back(this.node.as_mut());
                }
                this.shared
                    .recv_waiter_count
                    .fetch_add(1, Ordering::Relaxed);
                this.queued = true;
            } else {
                return Poll::Pending;
            }
        }
    }
}

impl<'a, T, F: ChannelFlavor, Q: flavor::RawQueue<T>> Drop for ReceiverStream<'a, T, F, Q> {
    fn drop(&mut self) {
        if self.queued {
            remove_recv_waiter(self.shared, self.node.as_mut());
        }
    }
}
