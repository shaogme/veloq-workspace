use futures_core::Future;
use futures_core::stream::Stream;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

use crate::common::update_waker;
pub use crate::common::{ChannelCapacity, SendError, TryRecvError};

use std::{
    cell::RefCell,
    collections::VecDeque,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    rc::Rc,
    task::{Context, Poll, Waker},
};

/// 本地通道的发送端
///
/// 由于基于 `Rc` 和 `RefCell`，只能在单线程（Local）使用。
#[derive(Debug)]
pub struct Sender<T> {
    channel: LocalChannel<T>,
}

/// 本地通道的接收端
///
/// 提供 `recv` 方法用于接收消息，也可以通过 `stream()` 转换为 `Stream`。
#[derive(Debug)]
pub struct Receiver<T> {
    channel: LocalChannel<T>,
}

trait WaiterAction {
    fn get_list<T>(state: &mut State<T>) -> &mut LinkedList<WaiterAdapter>;
}

struct SenderAction;

impl WaiterAction for SenderAction {
    fn get_list<T>(state: &mut State<T>) -> &mut LinkedList<WaiterAdapter> {
        &mut state.send_waiters
    }
}

struct ReceiverAction;

impl WaiterAction for ReceiverAction {
    fn get_list<T>(state: &mut State<T>) -> &mut LinkedList<WaiterAdapter> {
        &mut state.recv_waiters
    }
}

#[derive(Debug)]
struct Waiter<'a, T, A, F>
where
    A: WaiterAction,
{
    node: WaiterNode,
    channel: &'a LocalChannel<T>,
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
    fn new(poll_fn: F, channel: &'a LocalChannel<T>) -> Self {
        Waiter {
            poll_fn,
            channel,
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
                        &mut future_mut.channel.state.borrow_mut(),
                    );
                }
                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue::<T, A>(
                    pinned_node,
                    &mut future_mut.channel.state.borrow_mut(),
                );
                Poll::Ready(result)
            }
        }
    }
}

fn register_into_waiting_queue<T, A: WaiterAction>(
    node: Pin<&mut WaiterNode>,
    state: &mut State<T>,
) {
    if node.link.is_linked() {
        return;
    }

    unsafe { A::get_list(state).push_back(node) };
}

fn remove_from_the_waiting_queue<T, A: WaiterAction>(
    node: Pin<&mut WaiterNode>,
    state: &mut State<T>,
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

            let mut state = self.channel.state.borrow_mut();
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
struct State<T> {
    capacity: ChannelCapacity,
    channel: VecDeque<T>,
    tx_count: usize,
    is_closed: bool,
    recv_waiters: LinkedList<WaiterAdapter>,
    send_waiters: LinkedList<WaiterAdapter>,
}

impl<T> State<T> {
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

#[derive(Debug)]
struct LocalChannel<T> {
    state: Rc<RefCell<State<T>>>,
}

impl<T> Clone for LocalChannel<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for LocalChannel<T> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let should_check = Rc::strong_count(&self.state) == 1;
            if should_check && let Ok(state) = self.state.try_borrow() {
                assert!(state.recv_waiters.is_empty(), "Receiver waiters mismatch");
                assert!(state.send_waiters.is_empty(), "Sender waiters mismatch");
            }
        }
    }
}

impl<T> LocalChannel<T> {
    #[allow(clippy::new_ret_no_self)]
    fn new(capacity: ChannelCapacity) -> (Sender<T>, Receiver<T>) {
        let channel_buffer = match capacity {
            ChannelCapacity::Unbounded => VecDeque::new(),
            ChannelCapacity::Bounded(x) => VecDeque::with_capacity(x),
        };

        let channel = LocalChannel {
            state: Rc::new(RefCell::new(State {
                capacity,
                channel: channel_buffer,
                tx_count: 1,
                is_closed: false,
                send_waiters: LinkedList::new(WaiterAdapter::NEW),
                recv_waiters: LinkedList::new(WaiterAdapter::NEW),
            })),
        };

        (
            Sender {
                channel: channel.clone(),
            },
            Receiver { channel },
        )
    }
}

/// 创建一个新的无界通道
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    LocalChannel::new(ChannelCapacity::Unbounded)
}

/// 创建一个新的有界通道
pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    LocalChannel::new(ChannelCapacity::Bounded(size))
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.channel.state.borrow_mut().tx_count += 1;
        Self {
            channel: self.channel.clone(),
        }
    }
}

impl<T> Sender<T> {
    /// 尝试发送数据，如果通道已满或接收端关闭则返回错误
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        if let Some(w) = self.channel.state.borrow_mut().push(item)? {
            w.wake();
        }
        Ok(())
    }

    /// 异步发送数据，如果通道已满则等待
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        // 先等待空间，但不持有 borrow
        Waiter::<T, SenderAction, _>::new(|| self.wait_for_room(), &self.channel).await;
        // 等待结束后尝试发送
        self.try_send(item)
    }

    /// 检查通道是否已满
    pub fn is_full(&self) -> bool {
        self.channel.state.borrow().is_full()
    }

    /// 获取当前通道中的消息数量
    pub fn len(&self) -> usize {
        self.channel.state.borrow().channel.len()
    }

    /// 检查通道是否为空
    pub fn is_empty(&self) -> bool {
        self.channel.state.borrow().channel.is_empty()
    }

    fn wait_for_room(&self) -> PollResult<()> {
        self.channel.state.borrow_mut().wait_for_room()
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

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        state.tx_count -= 1;

        if state.tx_count == 0 {
            wake_up_all(&mut state.send_waiters);
            wake_up_all(&mut state.recv_waiters);
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.channel.state.borrow_mut();
        // Receiver 只有一个，所以可以直接关闭
        state.is_closed = true;
        wake_up_all(&mut state.recv_waiters);
        wake_up_all(&mut state.send_waiters);
    }
}

struct ChannelStream<'a, T> {
    channel: &'a LocalChannel<T>,
    node: WaiterNode,
}

impl<'a, T> ChannelStream<'a, T> {
    fn new(channel: &'a LocalChannel<T>) -> Self {
        ChannelStream {
            channel,
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
        let result = self.channel.state.borrow_mut().recv_one();
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
                        &mut this.channel.state.borrow_mut(),
                    );
                }

                Poll::Pending
            }
            PollResult::Ready(result) => {
                remove_from_the_waiting_queue::<T, ReceiverAction>(
                    node,
                    &mut this.channel.state.borrow_mut(),
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
        // 必须确保移除 waiting node，否则会导致悬挂指针（unsafe linked list）。
        // 使用 borrow_mut() 而非 try_borrow_mut() 以确保在异常情况下也能清理。
        // 如果 panic 发生，RefCell 已经 poison 也没关系，主要是防止后续 UB。
        let mut state = self.channel.state.borrow_mut();
        // Safety: ChannelStream contains PhantomPinned via WaiterNode, so it is !Unpin.
        // Once pinned (which must happen before node is linked), it cannot be moved.
        // Drop is called with &mut self, so we can access fields.
        // If node is linked, it must be valid to pin it here as it hasn't moved since being linked.
        let node = unsafe { Pin::new_unchecked(&mut self.node) };
        remove_from_the_waiting_queue::<T, ReceiverAction>(node, &mut state);
    }
}

impl<T> Receiver<T> {
    /// 尝试非阻塞接收
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let result = self.channel.state.borrow_mut().recv_one();
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
        Waiter::<T, ReceiverAction, _>::new(|| self.recv_one(), &self.channel).await
    }

    /// 转换为 Stream
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream::new(&self.channel)
    }

    fn recv_one(&self) -> PollResult<Option<T>> {
        let result = self.channel.state.borrow_mut().recv_one();
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
