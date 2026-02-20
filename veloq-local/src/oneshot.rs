use std::cell::RefCell;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

/// 接收端已关闭错误
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Oneshot channel closed")
    }
}

impl std::error::Error for RecvError {}

struct State<T> {
    value: Option<T>,
    waker: Option<Waker>,
    is_tx_closed: bool,
    is_rx_closed: bool,
}

/// Oneshot 通道发送端
pub struct Sender<T> {
    state: Rc<RefCell<State<T>>>,
}

/// Oneshot 通道接收端
pub struct Receiver<T> {
    state: Rc<RefCell<State<T>>>,
}

/// 创建一个新的 oneshot 通道
pub fn channel<T>() -> (Sender<T>, Receiver<T>) {
    let state = Rc::new(RefCell::new(State {
        value: None,
        waker: None,
        is_tx_closed: false,
        is_rx_closed: false,
    }));
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state },
    )
}

impl<T> Sender<T> {
    /// 发送消息
    ///
    /// 成功时返回 `Ok(())`，如果接收端已关闭则返回 `Err(t)`。
    pub fn send(self, t: T) -> Result<(), T> {
        let mut state = self.state.borrow_mut();
        if state.is_rx_closed {
            return Err(t);
        }
        state.value = Some(t);
        if let Some(waker) = state.waker.take() {
            waker.wake();
        }
        Ok(())
    }

    /// 检查接收端是否已关闭
    pub fn is_closed(&self) -> bool {
        self.state.borrow().is_rx_closed
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        state.is_tx_closed = true;
        // 如果发送端 drop 了且没发送值，唤醒接收端以让其感知错误
        if state.value.is_none()
            && let Some(waker) = state.waker.take()
        {
            waker.wake();
        }
    }
}

impl<T> Future for Receiver<T> {
    type Output = Result<T, RecvError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.state.borrow_mut();

        if let Some(val) = state.value.take() {
            return Poll::Ready(Ok(val));
        }

        if state.is_tx_closed {
            return Poll::Ready(Err(RecvError));
        }

        state.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

impl<T> Receiver<T> {
    /// 尝试非阻塞接收
    pub fn try_recv(&mut self) -> Result<Option<T>, RecvError> {
        let mut state = self.state.borrow_mut();

        if let Some(val) = state.value.take() {
            return Ok(Some(val));
        }

        if state.is_tx_closed {
            return Err(RecvError);
        }

        Ok(None)
    }

    /// 关闭接收端
    pub fn close(&mut self) {
        let mut state = self.state.borrow_mut();
        state.is_rx_closed = true;
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let mut state = self.state.borrow_mut();
        state.is_rx_closed = true;
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender").finish()
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish()
    }
}
