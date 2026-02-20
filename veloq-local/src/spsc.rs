use futures_core::Future;
use futures_core::stream::Stream;
use std::{
    cell::RefCell,
    collections::VecDeque,
    fmt,
    pin::Pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// 发送操作可能遇到的错误
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SendError<T> {
    /// 接收端已关闭
    Closed(T),
    /// 通道已满
    Full(T),
}

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SendError::Closed(_) => write!(f, "SendError::Closed(..)"),
            SendError::Full(_) => write!(f, "SendError::Full(..)"),
        }
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T> std::error::Error for SendError<T> {}

/// 非阻塞接收操作可能遇到的错误
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct TryRecvError;

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Channel is empty")
    }
}

impl std::error::Error for TryRecvError {}

/// 通道容量配置
#[derive(Debug, Clone, Copy)]
pub enum ChannelCapacity {
    Unbounded,
    Bounded(usize),
}

#[derive(Debug)]
struct State<T> {
    capacity: ChannelCapacity,
    buffer: VecDeque<T>,
    is_closed: bool,
    producer_waker: Option<Waker>,
    consumer_waker: Option<Waker>,
}

impl<T> State<T> {
    fn is_full(&self) -> bool {
        match self.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(cap) => self.buffer.len() >= cap,
        }
    }
}

/// SPSC 通道发送端
///
/// 不实现 `Clone`，确保单一生产者。
#[derive(Debug)]
pub struct Sender<T> {
    state: Rc<RefCell<State<T>>>,
}

/// SPSC 通道接收端
///
/// 不实现 `Clone`，确保单一消费者。
#[derive(Debug)]
pub struct Receiver<T> {
    state: Rc<RefCell<State<T>>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        state.is_closed = true;
        // 唤醒消费者，让其知道发送端已关闭
        if let Some(waker) = state.consumer_waker.take() {
            waker.wake();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        state.is_closed = true;
        // 唤醒生产者，让其知道接收端已关闭
        if let Some(waker) = state.producer_waker.take() {
            waker.wake();
        }
    }
}

impl<T> Sender<T> {
    /// 尝试发送数据
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let mut state = self.state.borrow_mut();

        if state.is_closed {
            return Err(SendError::Closed(item));
        }

        if state.is_full() {
            return Err(SendError::Full(item));
        }

        state.buffer.push_back(item);

        // 唤醒消费者
        if let Some(waker) = state.consumer_waker.take() {
            waker.wake();
        }

        Ok(())
    }

    /// 异步发送数据
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        SendFuture {
            state: &self.state,
            item: Some(item),
        }
        .await
    }

    /// 检查通道是否已满
    pub fn is_full(&self) -> bool {
        self.state.borrow().is_full()
    }

    /// 获取当前通道中的消息数量
    pub fn len(&self) -> usize {
        self.state.borrow().buffer.len()
    }

    /// 检查通道是否为空
    pub fn is_empty(&self) -> bool {
        self.state.borrow().buffer.is_empty()
    }
}

struct SendFuture<'a, T> {
    state: &'a Rc<RefCell<State<T>>>,
    item: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();

        if state.is_closed {
            let item = self
                .item
                .take()
                .expect("Polled SendFuture after completion");
            return Poll::Ready(Err(SendError::Closed(item)));
        }

        if !state.is_full() {
            let item = self
                .item
                .take()
                .expect("Polled SendFuture after completion");
            state.buffer.push_back(item);

            if let Some(waker) = state.consumer_waker.take() {
                waker.wake();
            }
            return Poll::Ready(Ok(()));
        }

        // 通道已满，注册 Waker
        state.producer_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Receiver<T> {
    /// 尝试接收数据
    pub fn try_recv(&self) -> Result<Option<T>, TryRecvError> {
        let mut state = self.state.borrow_mut();

        if let Some(item) = state.buffer.pop_front() {
            // 成功取走数据，唤醒生产者
            if let Some(waker) = state.producer_waker.take() {
                waker.wake();
            }
            Ok(Some(item))
        } else if state.is_closed {
            Ok(None)
        } else {
            Err(TryRecvError) // Pending
        }
    }

    /// 异步接收数据
    pub async fn recv(&self) -> Option<T> {
        RecvFuture { state: &self.state }.await
    }

    /// 转换为 Stream
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream { state: &self.state }
    }
}

struct RecvFuture<'a, T> {
    state: &'a Rc<RefCell<State<T>>>,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();

        if let Some(item) = state.buffer.pop_front() {
            if let Some(waker) = state.producer_waker.take() {
                waker.wake();
            }
            return Poll::Ready(Some(item));
        }

        if state.is_closed {
            return Poll::Ready(None);
        }

        state.consumer_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

struct ChannelStream<'a, T> {
    state: &'a Rc<RefCell<State<T>>>,
}

impl<'a, T> Stream for ChannelStream<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut state = self.state.borrow_mut();

        if let Some(item) = state.buffer.pop_front() {
            if let Some(waker) = state.producer_waker.take() {
                waker.wake();
            }
            return Poll::Ready(Some(item));
        }

        if state.is_closed {
            return Poll::Ready(None);
        }

        state.consumer_waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

/// 创建一个新的无界 SPSC 通道
pub fn new_unbounded<T>() -> (Sender<T>, Receiver<T>) {
    new(ChannelCapacity::Unbounded)
}

/// 创建一个新的有界 SPSC 通道
pub fn new_bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    new(ChannelCapacity::Bounded(size))
}

fn new<T>(capacity: ChannelCapacity) -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(RefCell::new(State {
        capacity,
        buffer: match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(size) => VecDeque::with_capacity(size),
        },
        is_closed: false,
        producer_waker: None,
        consumer_waker: None,
    }));

    (
        Sender {
            state: state.clone(),
        },
        Receiver { state },
    )
}
