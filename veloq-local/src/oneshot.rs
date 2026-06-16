use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

pub use crate::common::TryRecvError;
use crate::common::update_waker;

/// 接收端已关闭错误
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oneshot channel closed")
    }
}

impl std::error::Error for RecvError {}

pub struct State<T> {
    value: UnsafeCell<Option<T>>,
    waker: UnsafeCell<Option<Waker>>,
    is_tx_closed: Cell<bool>,
    is_rx_closed: Cell<bool>,
}

/// Oneshot 通道发送端
pub struct Sender<'a, T> {
    state: &'a State<T>,
}

/// Oneshot 通道接收端
pub struct Receiver<'a, T> {
    state: &'a State<T>,
}

impl<T> Default for State<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> State<T> {
    /// 创建一个新的 oneshot 通道状态
    pub const fn new() -> Self {
        State {
            value: UnsafeCell::new(None),
            waker: UnsafeCell::new(None),
            is_tx_closed: Cell::new(false),
            is_rx_closed: Cell::new(false),
        }
    }

    /// 分离为发送端和接收端
    pub fn split(&self) -> (Sender<'_, T>, Receiver<'_, T>) {
        (Sender { state: self }, Receiver { state: self })
    }
}

/// 创建一个新的 oneshot 通道状态
pub const fn channel<T>() -> State<T> {
    State::new()
}

impl<'a, T> Sender<'a, T> {
    /// 发送消息
    ///
    /// 成功时返回 `Ok(())`，如果接收端已关闭则返回 `Err(t)`。
    pub fn send(self, t: T) -> Result<(), T> {
        let waker;
        {
            if self.state.is_rx_closed.get() {
                return Err(t);
            }
            let value = unsafe { &mut *self.state.value.get() };
            *value = Some(t);
            let waker_slot = unsafe { &mut *self.state.waker.get() };
            waker = waker_slot.take();
        }

        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }

    /// 检查接收端是否已关闭
    pub fn is_closed(&self) -> bool {
        self.state.is_rx_closed.get()
    }
}

impl<'a, T> Drop for Sender<'a, T> {
    fn drop(&mut self) {
        let waker;
        {
            self.state.is_tx_closed.set(true);
            let value = unsafe { &*self.state.value.get() };
            // 如果发送端 drop 了且没发送值，唤醒接收端以让其感知错误
            if value.is_none() {
                let waker_slot = unsafe { &mut *self.state.waker.get() };
                waker = waker_slot.take();
            } else {
                waker = None;
            }
        }

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<'a, T> Future for Receiver<'a, T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = self.state;
        let value = unsafe { &mut *state.value.get() };

        if let Some(val) = value.take() {
            return Poll::Ready(Ok(val));
        }

        if state.is_tx_closed.get() {
            return Poll::Ready(Err(RecvError));
        }

        let waker_slot = unsafe { &mut *state.waker.get() };
        update_waker(waker_slot, cx.waker());
        Poll::Pending
    }
}

impl<'a, T> Receiver<'a, T> {
    /// 尝试非阻塞接收
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let state = self.state;
        let value = unsafe { &mut *state.value.get() };

        if let Some(val) = value.take() {
            return Ok(val);
        }

        if state.is_tx_closed.get() {
            return Err(TryRecvError::Closed);
        }

        Err(TryRecvError::Empty)
    }

    /// 关闭接收端
    pub fn close(&mut self) {
        self.state.is_rx_closed.set(true);
    }
}

impl<'a, T> Drop for Receiver<'a, T> {
    fn drop(&mut self) {
        self.state.is_rx_closed.set(true);
    }
}

impl<'a, T> fmt::Debug for Sender<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<'a, T> fmt::Debug for Receiver<'a, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}

/// Owned oneshot channel sender.
pub struct OwnedSender<T> {
    state: ManuallyDrop<Rc<State<T>>>,
}

/// Owned oneshot channel receiver.
pub struct OwnedReceiver<T> {
    state: Rc<State<T>>,
}

/// Creates a new owned oneshot channel.
pub fn owned_channel<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Rc::new(State::new());
    (
        OwnedSender {
            state: ManuallyDrop::new(state.clone()),
        },
        OwnedReceiver { state },
    )
}

impl<T> OwnedSender<T> {
    /// Sends a value on the channel.
    pub fn send(self, t: T) -> Result<(), T> {
        let this = ManuallyDrop::new(self);
        let state = unsafe { std::ptr::read(&*this.state) };
        let sender = Sender { state: &state };
        sender.send(t)
    }

    /// Checks if the receiver has been dropped.
    pub fn is_closed(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { state: &self.state });
        sender.is_closed()
    }
}

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        drop(Sender { state: &self.state });
        unsafe {
            ManuallyDrop::drop(&mut self.state);
        }
    }
}

impl<T> fmt::Debug for OwnedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSender").finish()
    }
}

impl<T> OwnedReceiver<T> {
    /// Attempts to receive a value without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(Receiver { state: &self.state });
        receiver.try_recv()
    }

    /// Closes the receiver.
    pub fn close(&mut self) {
        let mut receiver = ManuallyDrop::new(Receiver { state: &self.state });
        receiver.close();
    }
}

impl<T> Future for OwnedReceiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut receiver = ManuallyDrop::new(Receiver { state: &self.state });
        Pin::new(&mut *receiver).poll(cx)
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        drop(Receiver { state: &self.state });
    }
}

impl<T> fmt::Debug for OwnedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedReceiver").finish()
    }
}
