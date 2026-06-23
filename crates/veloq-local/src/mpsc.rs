use futures_core::Future;
use futures_core::stream::Stream;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

use crate::common::update_waker;
pub use crate::common::{ChannelCapacity, SendError, TryRecvError};

use std::{
    cell::RefCell,
    collections::VecDeque,
    marker::PhantomPinned,
    mem::ManuallyDrop,
    pin::Pin,
    ptr::NonNull,
    rc::Rc,
    task::{Context, Poll, Waker},
};

#[derive(Debug)]
pub struct State<T> {
    state: RefCell<StateInner<T>>,
}

impl<T> State<T> {
    /// Creates a new MPSC channel state.
    pub fn new(capacity: ChannelCapacity) -> Self {
        let channel_buffer = match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(x) => VecDeque::with_capacity(x),
        };

        State {
            state: RefCell::new(StateInner {
                capacity,
                channel: channel_buffer,
                tx_count: 1,
                is_closed: false,
                send_waiters: LinkedList::new(WaiterAdapter::NEW),
                recv_waiters: LinkedList::new(WaiterAdapter::NEW),
            }),
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
    pub fn split<'a>(&'a self) -> (Sender<'a, T>, Receiver<'a, T>) {
        // Reset tx_count to 1 on split
        self.state.borrow_mut().tx_count = 1;
        (Sender { state: self }, Receiver { state: self })
    }
}

/// Creates a new bounded MPSC channel state.
pub fn bounded<T>(size: usize) -> State<T> {
    State::bounded(size)
}

/// Creates a new unbounded MPSC channel state.
pub fn unbounded<T>() -> State<T> {
    State::unbounded()
}

/// 本地通道的发送端
#[derive(Debug)]
pub struct Sender<'a, T> {
    state: &'a State<T>,
}

/// 本地通道的接收端
#[derive(Debug)]
pub struct Receiver<'a, T> {
    state: &'a State<T>,
}

trait WaiterAction {
    fn get_list<T>(state: &mut StateInner<T>) -> &mut LinkedList<WaiterAdapter>;
}

struct SenderAction;

impl WaiterAction for SenderAction {
    fn get_list<T>(state: &mut StateInner<T>) -> &mut LinkedList<WaiterAdapter> {
        &mut state.send_waiters
    }
}

struct ReceiverAction;

impl WaiterAction for ReceiverAction {
    fn get_list<T>(state: &mut StateInner<T>) -> &mut LinkedList<WaiterAdapter> {
        &mut state.recv_waiters
    }
}

#[derive(Debug)]
struct Waiter<'a, T, A, F>
where
    A: WaiterAction,
{
    node: WaiterNode,
    state: &'a State<T>,
    poll_fn: F,
    _action: std::marker::PhantomData<A>,
}

#[derive(Debug)]
enum PollResult<T> {
    Pending,
    Ready(T),
}

impl<'a, T, A, F, R> Waiter<'a, T, A, F>
where
    F: FnMut() -> PollResult<R>,
    A: WaiterAction,
{
    fn new(poll_fn: F, state: &'a State<T>) -> Self {
        Waiter {
            poll_fn,
            state,
            node: WaiterNode {
                waker: RefCell::new(None),
                link: Link::new(),
                _p: PhantomPinned,
            },
            _action: std::marker::PhantomData,
        }
    }
}

impl<T, A, F, R> Future for Waiter<'_, T, A, F>
where
    F: FnMut() -> PollResult<R>,
    A: WaiterAction,
{
    type Output = R;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let future_mut = unsafe { self.get_unchecked_mut() };
        let pinned_node = unsafe { Pin::new_unchecked(&mut future_mut.node) };

        let result = (future_mut.poll_fn)();
        match result {
            PollResult::Pending => {
                let mut waker = pinned_node.waker.borrow_mut();
                update_waker(&mut waker, cx.waker());
                drop(waker);

                if !pinned_node.link.is_linked() {
                    register_into_waiting_queue::<T, A>(
                        pinned_node,
                        &mut future_mut.state.state.borrow_mut(),
                    );
                }
                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue::<T, A>(
                    pinned_node,
                    &mut future_mut.state.state.borrow_mut(),
                );
                Poll::Ready(result)
            }
        }
    }
}

fn register_into_waiting_queue<T, A: WaiterAction>(
    node: Pin<&mut WaiterNode>,
    state: &mut StateInner<T>,
) {
    if node.link.is_linked() {
        return;
    }

    unsafe { A::get_list(state).push_back(node) };
}

fn remove_from_the_waiting_queue<T, A: WaiterAction>(
    node: Pin<&mut WaiterNode>,
    state: &mut StateInner<T>,
) {
    if !node.link.is_linked() {
        return;
    }

    let ptr = unsafe { NonNull::new_unchecked(node.get_unchecked_mut()) };
    let mut cursor = unsafe { A::get_list(state).cursor_mut_from_ptr(ptr) };

    cursor.remove();
}

impl<T, A, F> Drop for Waiter<'_, T, A, F>
where
    A: WaiterAction,
{
    fn drop(&mut self) {
        if self.node.link.is_linked() {
            let pinned_node = unsafe { Pin::new_unchecked(&mut self.node) };

            let mut state = self.state.state.borrow_mut();
            remove_from_the_waiting_queue::<T, A>(pinned_node, &mut state);
        }
    }
}

#[derive(Debug)]
struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    _p: PhantomPinned,
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    pub const NEW: Self = WaiterAdapter;
}

#[derive(Debug)]
struct StateInner<T> {
    capacity: ChannelCapacity,
    channel: VecDeque<T>,
    tx_count: usize,
    is_closed: bool,
    recv_waiters: LinkedList<WaiterAdapter>,
    send_waiters: LinkedList<WaiterAdapter>,
}

impl<T> StateInner<T> {
    fn push(&mut self, item: T) -> Result<Option<Waker>, SendError<T>> {
        if self.is_closed {
            Err(SendError::Closed(item))
        } else if self.is_full() {
            Err(SendError::Full(item))
        } else {
            self.channel.push_back(item);

            Ok(self.recv_waiters.pop_front().map(|n| {
                n.as_ref()
                    .waker
                    .borrow_mut()
                    .take()
                    .expect("Future was added to the waiting queue without a waker")
            }))
        }
    }

    fn is_full(&self) -> bool {
        match self.capacity {
            ChannelCapacity::Unbounded => false,
            ChannelCapacity::Bounded(x) => self.channel.len() >= x,
        }
    }

    fn wait_for_room(&mut self) -> PollResult<()> {
        if self.is_closed || !self.is_full() {
            PollResult::Ready(())
        } else {
            PollResult::Pending
        }
    }

    fn recv_one(&mut self) -> PollResult<Option<(T, Option<Waker>)>> {
        match self.channel.pop_front() {
            Some(item) => PollResult::Ready(Some((
                item,
                self.send_waiters
                    .pop_front()
                    .and_then(|node| node.as_ref().waker.borrow_mut().take()),
            ))),
            None => {
                if self.tx_count > 0 {
                    PollResult::Pending
                } else {
                    PollResult::Ready(None)
                }
            }
        }
    }
}

impl<T> Drop for State<T> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            if let Ok(state) = self.state.try_borrow() {
                assert!(state.recv_waiters.is_empty(), "Receiver waiters mismatch");
                assert!(state.send_waiters.is_empty(), "Sender waiters mismatch");
            }
        }
    }
}

impl<'a, T> Clone for Sender<'a, T> {
    fn clone(&self) -> Self {
        self.state.state.borrow_mut().tx_count += 1;
        Self { state: self.state }
    }
}

impl<'a, T> Sender<'a, T> {
    /// 尝试发送数据，如果通道已满或接收端关闭则返回错误
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        if let Some(w) = self.state.state.borrow_mut().push(item)? {
            w.wake();
        }
        Ok(())
    }

    /// 异步发送数据，如果通道已满则等待
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        // 先等待空间，但不持有 borrow
        Waiter::<T, SenderAction, _>::new(|| self.wait_for_room(), self.state).await;
        // 等待结束后尝试发送
        self.try_send(item)
    }

    /// 检查通道是否已满
    pub fn is_full(&self) -> bool {
        self.state.state.borrow().is_full()
    }

    /// 获取当前通道中的消息数量
    pub fn len(&self) -> usize {
        self.state.state.borrow().channel.len()
    }

    /// 检查通道是否为空
    pub fn is_empty(&self) -> bool {
        self.state.state.borrow().channel.is_empty()
    }

    fn wait_for_room(&self) -> PollResult<()> {
        self.state.state.borrow_mut().wait_for_room()
    }
}

fn wake_up_all(waiters: &mut LinkedList<WaiterAdapter>) {
    let mut cursor = waiters.front_mut();
    while !cursor.is_null() {
        {
            let node = cursor.get().expect("Waiter queue check");
            node.waker
                .borrow_mut()
                .take()
                .expect("Future queued without waker")
                .wake();
        }
        cursor.remove();
    }
}

impl<'a, T> Drop for Sender<'a, T> {
    fn drop(&mut self) {
        let mut state = self.state.state.borrow_mut();
        state.tx_count -= 1;

        if state.tx_count == 0 {
            wake_up_all(&mut state.send_waiters);
            wake_up_all(&mut state.recv_waiters);
        }
    }
}

impl<'a, T> Drop for Receiver<'a, T> {
    fn drop(&mut self) {
        let mut state = self.state.state.borrow_mut();
        state.is_closed = true;
        wake_up_all(&mut state.recv_waiters);
        wake_up_all(&mut state.send_waiters);
    }
}

struct ChannelStream<'a, T> {
    state: &'a State<T>,
    node: WaiterNode,
}

impl<'a, T> ChannelStream<'a, T> {
    fn new(state: &'a State<T>) -> Self {
        ChannelStream {
            state,
            node: WaiterNode {
                waker: RefCell::new(None),
                link: Link::new(),
                _p: PhantomPinned,
            },
        }
    }
}

impl<T> Stream for ChannelStream<'_, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.state.state.borrow_mut().recv_one();
        let this = unsafe { self.get_unchecked_mut() };
        let node = unsafe { Pin::new_unchecked(&mut this.node) };

        match result {
            PollResult::Pending => {
                let mut waker = node.waker.borrow_mut();
                update_waker(&mut waker, cx.waker());
                drop(waker);

                if !node.link.is_linked() {
                    register_into_waiting_queue::<T, ReceiverAction>(
                        node,
                        &mut this.state.state.borrow_mut(),
                    );
                }

                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue::<T, ReceiverAction>(
                    node,
                    &mut this.state.state.borrow_mut(),
                );

                Poll::Ready(result.map(|(ret, mw)| {
                    if let Some(waker) = mw {
                        waker.wake();
                    }
                    ret
                }))
            }
        }
    }
}

impl<T> Drop for ChannelStream<'_, T> {
    fn drop(&mut self) {
        let mut state = self.state.state.borrow_mut();
        let node = unsafe { Pin::new_unchecked(&mut self.node) };
        remove_from_the_waiting_queue::<T, ReceiverAction>(node, &mut state);
    }
}

impl<'a, T> Receiver<'a, T> {
    /// 尝试非阻塞接收
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let result = self.state.state.borrow_mut().recv_one();
        match result {
            PollResult::Pending => Err(TryRecvError::Empty),
            PollResult::Ready(opt) => match opt {
                Some((ret, mw)) => {
                    if let Some(w) = mw {
                        w.wake();
                    }
                    Ok(ret)
                }
                None => Err(TryRecvError::Closed),
            },
        }
    }

    /// 接收下一条消息
    pub async fn recv(&self) -> Option<T> {
        Waiter::<T, ReceiverAction, _>::new(|| self.recv_one(), self.state).await
    }

    /// 转换为 Stream
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream::new(self.state)
    }

    fn recv_one(&self) -> PollResult<Option<T>> {
        let result = self.state.state.borrow_mut().recv_one();
        match result {
            PollResult::Pending => PollResult::Pending,
            PollResult::Ready(opt) => PollResult::Ready(opt.map(|(ret, mw)| {
                if let Some(w) = mw {
                    w.wake();
                }
                ret
            })),
        }
    }
}

/// Owned MPSC channel sender.
pub struct OwnedSender<T> {
    state: Rc<State<T>>,
}

/// Owned MPSC channel receiver.
pub struct OwnedReceiver<T> {
    state: Rc<State<T>>,
}

/// Creates a new owned MPSC channel.
pub fn owned_channel<T>(capacity: ChannelCapacity) -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Rc::new(State::new(capacity));
    (
        OwnedSender {
            state: state.clone(),
        },
        OwnedReceiver { state },
    )
}

/// Creates a new bounded owned MPSC channel.
pub fn owned_bounded<T>(size: usize) -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_channel(ChannelCapacity::Bounded(size))
}

/// Creates a new unbounded owned MPSC channel.
pub fn owned_unbounded<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_channel(ChannelCapacity::Unbounded)
}

impl<T> Clone for OwnedSender<T> {
    fn clone(&self) -> Self {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        let _cloned = ManuallyDrop::new(sender.clone());
        OwnedSender {
            state: self.state.clone(),
        }
    }
}

impl<T> OwnedSender<T> {
    /// Attempts to send a message without blocking.
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.try_send(item)
    }

    /// Asynchronously sends a message.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.send(item).await
    }

    /// Checks if the channel is full.
    pub fn is_full(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.is_full()
    }

    /// Returns the number of messages in the channel.
    pub fn len(&self) -> usize {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.len()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.is_empty()
    }
}

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        drop(Sender { state: &self.state });
    }
}

impl<T> OwnedReceiver<T> {
    /// Attempts to receive a message without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(Receiver { state: &self.state });
        receiver.try_recv()
    }

    /// Asynchronously receives a message.
    pub async fn recv(&self) -> Option<T> {
        let receiver = ManuallyDrop::new(Receiver { state: &self.state });
        receiver.recv().await
    }

    /// Converts the receiver into a stream.
    pub fn stream(&self) -> OwnedChannelStream<T> {
        OwnedChannelStream::new(self.state.clone())
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        drop(Receiver { state: &self.state });
    }
}

/// A stream of messages from an owned MPSC channel.
pub struct OwnedChannelStream<T> {
    state: Rc<State<T>>,
    node: WaiterNode,
}

impl<T> OwnedChannelStream<T> {
    fn new(state: Rc<State<T>>) -> Self {
        OwnedChannelStream {
            state,
            node: WaiterNode {
                waker: RefCell::new(None),
                link: Link::new(),
                _p: PhantomPinned,
            },
        }
    }
}

impl<T> Stream for OwnedChannelStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = self.state.state.borrow_mut().recv_one();
        let this = unsafe { self.get_unchecked_mut() };
        let node = unsafe { Pin::new_unchecked(&mut this.node) };

        match result {
            PollResult::Pending => {
                let mut waker = node.waker.borrow_mut();
                update_waker(&mut waker, cx.waker());
                drop(waker);

                if !node.link.is_linked() {
                    register_into_waiting_queue::<T, ReceiverAction>(
                        node,
                        &mut this.state.state.borrow_mut(),
                    );
                }

                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue::<T, ReceiverAction>(
                    node,
                    &mut this.state.state.borrow_mut(),
                );

                Poll::Ready(result.map(|(ret, mw)| {
                    if let Some(waker) = mw {
                        waker.wake();
                    }
                    ret
                }))
            }
        }
    }
}

impl<T> Drop for OwnedChannelStream<T> {
    fn drop(&mut self) {
        let mut state = self.state.state.borrow_mut();
        let node = unsafe { Pin::new_unchecked(&mut self.node) };
        remove_from_the_waiting_queue::<T, ReceiverAction>(node, &mut state);
    }
}
