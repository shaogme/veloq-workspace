use futures_core::stream::Stream;
use veloq_std::{
    cell::RefCell,
    collections::VecDeque,
    future::Future,
    mem::ManuallyDrop,
    ops::AsyncFnOnce,
    pin::{Pin, pin},
    rc::Rc,
    task::{Context, Poll},
};

pub use crate::common::{ChannelCapacity, SendError, TryRecvError};
use crate::notify::{Notified, Notify};

#[derive(Debug)]
pub struct State<T> {
    inner: RefCell<StateInner<T>>,
    send_notify: Notify,
    recv_notify: Notify,
}

impl<T> State<T> {
    /// Creates a new MPSC channel state.
    pub fn new(capacity: ChannelCapacity) -> Self {
        let channel_buffer = match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(x) => VecDeque::with_capacity(x),
        };

        State {
            inner: RefCell::new(StateInner {
                capacity,
                channel: channel_buffer,
                tx_count: 1,
                is_closed: false,
            }),
            send_notify: Notify::new(),
            recv_notify: Notify::new(),
        }
    }

    /// Creates a new unbounded MPSC channel state.
    pub fn unbounded() -> Self {
        Self::new(ChannelCapacity::Unbounded)
    }

    /// Creates a new bounded MPSC channel state.
    pub fn bounded(size: usize) -> Self {
        Self::new(ChannelCapacity::Bounded(size))
    }

    /// Splits the state into a sender and a receiver.
    pub fn split<'a>(&'a self) -> (BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) {
        // Reset tx_count to 1 on split
        self.inner.borrow_mut().tx_count = 1;
        (
            BorrowedSender { state: self },
            BorrowedReceiver { state: self },
        )
    }
}

/// Creates a new borrowed bounded MPSC channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_bounded<T, F, R>(size: usize, f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = State::bounded(size);
    let (tx, rx) = state.split();
    f(tx, rx).await
}

/// Creates a new borrowed unbounded MPSC channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_unbounded<T, F, R>(f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = State::unbounded();
    let (tx, rx) = state.split();
    f(tx, rx).await
}

/// 本地通道的发送端（借用）
#[derive(Debug)]
pub struct BorrowedSender<'a, T> {
    state: &'a State<T>,
}

/// 本地通道的接收端（借用）
#[derive(Debug)]
pub struct BorrowedReceiver<'a, T> {
    state: &'a State<T>,
}

#[derive(Debug)]
struct StateInner<T> {
    capacity: ChannelCapacity,
    channel: VecDeque<T>,
    tx_count: usize,
    is_closed: bool,
}

impl<T> StateInner<T> {
    fn is_full(&self) -> bool {
        match self.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(x) => self.channel.len() >= x,
        }
    }
}

impl<'a, T> Clone for BorrowedSender<'a, T> {
    fn clone(&self) -> Self {
        self.state.inner.borrow_mut().tx_count += 1;
        Self { state: self.state }
    }
}

impl<'a, T> BorrowedSender<'a, T> {
    /// 尝试发送数据，如果通道已满或接收端关闭则返回错误
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let mut inner = self.state.inner.borrow_mut();
        if inner.is_closed {
            return Err(SendError::Closed(item));
        }
        if inner.is_full() {
            return Err(SendError::Full(item));
        }
        inner.channel.push_back(item);
        drop(inner);
        self.state.recv_notify.notify_one();
        Ok(())
    }

    /// 异步发送数据，如果通道已满则等待
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        loop {
            {
                let mut inner = self.state.inner.borrow_mut();
                if inner.is_closed {
                    return Err(SendError::Closed(item));
                }
                if !inner.is_full() {
                    inner.channel.push_back(item);
                    drop(inner);
                    self.state.recv_notify.notify_one();
                    return Ok(());
                }
            }

            let mut notified = pin!(self.state.send_notify.notified());
            notified.as_mut().enable();

            {
                let inner = self.state.inner.borrow();
                if inner.is_closed {
                    return Err(SendError::Closed(item));
                }
                if !inner.is_full() {
                    continue;
                }
            }

            notified.await;
        }
    }

    /// 检查通道是否已满
    pub fn is_full(&self) -> bool {
        self.state.inner.borrow().is_full()
    }

    /// 获取当前通道中的消息数量
    pub fn len(&self) -> usize {
        self.state.inner.borrow().channel.len()
    }

    /// 检查通道是否为空
    pub fn is_empty(&self) -> bool {
        self.state.inner.borrow().channel.is_empty()
    }
}

impl<'a, T> Drop for BorrowedSender<'a, T> {
    fn drop(&mut self) {
        let mut inner = self.state.inner.borrow_mut();
        inner.tx_count -= 1;

        if inner.tx_count == 0 {
            drop(inner);
            self.state.send_notify.notify_waiters();
            self.state.recv_notify.notify_waiters();
        }
    }
}

impl<'a, T> Drop for BorrowedReceiver<'a, T> {
    fn drop(&mut self) {
        let mut inner = self.state.inner.borrow_mut();
        inner.is_closed = true;
        drop(inner);
        self.state.recv_notify.notify_waiters();
        self.state.send_notify.notify_waiters();
    }
}

pub struct BorrowedChannelStream<'a, T> {
    state: &'a State<T>,
    notified: Option<Notified<'a>>,
}

impl<'a, T> BorrowedChannelStream<'a, T> {
    fn new(state: &'a State<T>) -> Self {
        BorrowedChannelStream {
            state,
            notified: None,
        }
    }
}

impl<T> Stream for BorrowedChannelStream<'_, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            {
                let mut inner = this.state.inner.borrow_mut();
                if let Some(item) = inner.channel.pop_front() {
                    drop(inner);
                    this.state.send_notify.notify_one();
                    this.notified = None;
                    return Poll::Ready(Some(item));
                }
                if inner.tx_count == 0 || inner.is_closed {
                    this.notified = None;
                    return Poll::Ready(None);
                }
            }

            if this.notified.is_none() {
                this.notified = Some(this.state.recv_notify.notified());
            }

            let notified = this.notified.as_mut().unwrap();
            let notified_pin = unsafe { Pin::new_unchecked(notified) };
            match Future::poll(notified_pin, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    this.notified = None;
                    continue;
                }
            }
        }
    }
}

impl<'a, T> BorrowedReceiver<'a, T> {
    /// 尝试非阻塞接收
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut inner = self.state.inner.borrow_mut();
        if let Some(item) = inner.channel.pop_front() {
            drop(inner);
            self.state.send_notify.notify_one();
            Ok(item)
        } else if inner.tx_count == 0 || inner.is_closed {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// 接收下一条消息
    pub async fn recv(&self) -> Option<T> {
        loop {
            {
                let mut inner = self.state.inner.borrow_mut();
                if let Some(item) = inner.channel.pop_front() {
                    drop(inner);
                    self.state.send_notify.notify_one();
                    return Some(item);
                }
                if inner.tx_count == 0 || inner.is_closed {
                    return None;
                }
            }

            let mut notified = pin!(self.state.recv_notify.notified());
            notified.as_mut().enable();

            {
                let inner = self.state.inner.borrow();
                if !inner.channel.is_empty() || inner.tx_count == 0 || inner.is_closed {
                    continue;
                }
            }

            notified.await;
        }
    }

    /// 转换为 Stream
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        BorrowedChannelStream::new(self.state)
    }
}

/// MPSC channel sender.
pub struct Sender<T> {
    state: Rc<State<T>>,
}

/// MPSC channel receiver.
pub struct Receiver<T> {
    state: Rc<State<T>>,
}

/// Creates a new MPSC channel.
pub fn channel<T>(capacity: ChannelCapacity) -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(State::new(capacity));
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state },
    )
}

/// Creates a new bounded MPSC channel.
pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    channel(ChannelCapacity::Bounded(size))
}

/// Creates a new unbounded MPSC channel.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    channel(ChannelCapacity::Unbounded)
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        let _cloned = ManuallyDrop::new(sender.clone());
        Sender {
            state: self.state.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to send a message without blocking.
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        sender.try_send(item)
    }

    /// Asynchronously sends a message.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        sender.send(item).await
    }

    /// Checks if the channel is full.
    pub fn is_full(&self) -> bool {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        sender.is_full()
    }

    /// Returns the number of messages in the channel.
    pub fn len(&self) -> usize {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        sender.len()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let sender = ManuallyDrop::new(BorrowedSender { state: &self.state });
        sender.is_empty()
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        drop(BorrowedSender { state: &self.state });
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a message without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(BorrowedReceiver { state: &self.state });
        receiver.try_recv()
    }

    /// Asynchronously receives a message.
    pub async fn recv(&self) -> Option<T> {
        let receiver = ManuallyDrop::new(BorrowedReceiver { state: &self.state });
        receiver.recv().await
    }

    /// Converts the receiver into a stream.
    pub fn stream(&self) -> ChannelStream<T> {
        ChannelStream::new(self.state.clone())
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        drop(BorrowedReceiver { state: &self.state });
    }
}

/// A stream of messages from an MPSC channel.
pub struct ChannelStream<T> {
    state: Rc<State<T>>,
    notified: Option<Notified<'static>>,
}

impl<T> ChannelStream<T> {
    fn new(state: Rc<State<T>>) -> Self {
        ChannelStream {
            state,
            notified: None,
        }
    }
}

impl<T> Stream for ChannelStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            {
                let mut inner = this.state.inner.borrow_mut();
                if let Some(item) = inner.channel.pop_front() {
                    drop(inner);
                    this.state.send_notify.notify_one();
                    this.notified = None;
                    return Poll::Ready(Some(item));
                }
                if inner.tx_count == 0 || inner.is_closed {
                    this.notified = None;
                    return Poll::Ready(None);
                }
            }

            if this.notified.is_none() {
                let notify_ptr = &this.state.recv_notify as *const Notify;
                let notify_ref: &'static Notify = unsafe { &*notify_ptr };
                this.notified = Some(notify_ref.notified());
            }

            let notified = this.notified.as_mut().unwrap();
            let notified_pin = unsafe { Pin::new_unchecked(notified) };
            match Future::poll(notified_pin, cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    this.notified = None;
                    continue;
                }
            }
        }
    }
}

impl<T> Drop for ChannelStream<T> {
    fn drop(&mut self) {
        self.notified = None;
    }
}
