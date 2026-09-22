use core::pin::pin;

use veloq_std::{cell::RefCell, collections::VecDeque, error::Error, fmt, ptr, rc::Rc};

use crate::notify::Notify;

/// Error produced when sending a value fails because all receivers have been dropped.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SendError<T>(pub T);

impl<T> fmt::Debug for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SendError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "sending on a closed channel")
    }
}

impl<T> Error for SendError<T> {}

/// Error returned from [`Receiver::recv`] or [`OwnedReceiver::recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecvError {
    /// The channel is closed and all queued messages have been received.
    Closed,
    /// The receiver lagged behind by the given number of messages.
    Lagged(u64),
}

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RecvError::Closed => write!(f, "channel closed"),
            RecvError::Lagged(missed) => write!(f, "receiver lagged by {missed} messages"),
        }
    }
}

impl Error for RecvError {}

/// Error returned from [`Receiver::try_recv`] or [`OwnedReceiver::try_recv`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryRecvError {
    /// The channel currently has no new messages.
    Empty,
    /// The channel is closed and all queued messages have been received.
    Closed,
    /// The receiver lagged behind by the given number of messages.
    Lagged(u64),
}

impl fmt::Display for TryRecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TryRecvError::Empty => write!(f, "channel empty"),
            TryRecvError::Closed => write!(f, "channel closed"),
            TryRecvError::Lagged(missed) => write!(f, "receiver lagged by {missed} messages"),
        }
    }
}

impl Error for TryRecvError {}

#[derive(Debug)]
struct StateInner<T> {
    capacity: usize,
    queue: VecDeque<T>,
    tail: u64,
    sender_count: usize,
    receiver_count: usize,
}

impl<T> StateInner<T> {
    #[inline]
    fn oldest_seq(&self) -> u64 {
        self.tail.saturating_sub(self.queue.len() as u64)
    }
}

/// A single-threaded, multi-producer, multi-consumer broadcast channel state.
#[derive(Debug)]
pub struct State<T> {
    inner: RefCell<StateInner<T>>,
    rx_notify: Notify,
}

impl<T> State<T> {
    /// Creates a new broadcast channel state with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        Self {
            inner: RefCell::new(StateInner {
                capacity,
                queue: VecDeque::with_capacity(capacity),
                tail: 0,
                sender_count: 0,
                receiver_count: 0,
            }),
            rx_notify: Notify::new(),
        }
    }

    /// Splits the state into a borrowed sender and receiver pair.
    pub fn split(&self) -> (Sender<'_, T>, Receiver<'_, T>) {
        let mut inner = self.inner.borrow_mut();
        inner.sender_count = 1;
        inner.receiver_count = 1;
        let next_seq = inner.tail;
        drop(inner);
        (
            Sender { state: self },
            Receiver {
                state: self,
                next_seq,
            },
        )
    }

    /// Returns the capacity of the channel.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.inner.borrow().capacity
    }

    /// Returns the number of active senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.inner.borrow().sender_count
    }

    /// Returns the number of active receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.inner.borrow().receiver_count
    }

    /// Returns the number of buffered messages.
    #[inline]
    pub fn len(&self) -> usize {
        self.inner.borrow().queue.len()
    }

    /// Returns `true` if no messages are currently buffered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if all receivers have been dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.receiver_count() == 0
    }

    /// Sends a value over the channel to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        let mut inner = self.inner.borrow_mut();
        if inner.receiver_count == 0 {
            return Err(SendError(value));
        }

        if inner.queue.len() == inner.capacity {
            inner.queue.pop_front();
        }
        inner.queue.push_back(value);
        inner.tail += 1;
        let rx_count = inner.receiver_count;
        drop(inner);

        self.rx_notify.notify_waiters();
        Ok(rx_count)
    }

    /// Attempts to receive a message for a given receiver cursor.
    pub fn try_recv(&self, next_seq: &mut u64) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let inner = self.inner.borrow();
        let oldest_seq = inner.oldest_seq();

        if *next_seq < oldest_seq {
            let missed = oldest_seq - *next_seq;
            *next_seq = oldest_seq;
            return Err(TryRecvError::Lagged(missed));
        }

        if *next_seq < inner.tail {
            let idx = (*next_seq - oldest_seq) as usize;
            let val = inner.queue[idx].clone();
            *next_seq += 1;
            return Ok(val);
        }

        if inner.sender_count == 0 {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Asynchronously receives a message for a given receiver cursor.
    pub async fn recv(&self, next_seq: &mut u64) -> Result<T, RecvError>
    where
        T: Clone,
    {
        loop {
            match self.try_recv(next_seq) {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Lagged(n)) => return Err(RecvError::Lagged(n)),
                Err(TryRecvError::Closed) => return Err(RecvError::Closed),
                Err(TryRecvError::Empty) => {}
            }

            let mut notified = pin!(self.rx_notify.notified());
            notified.as_mut().enable();

            match self.try_recv(next_seq) {
                Ok(val) => return Ok(val),
                Err(TryRecvError::Lagged(n)) => return Err(RecvError::Lagged(n)),
                Err(TryRecvError::Closed) => return Err(RecvError::Closed),
                Err(TryRecvError::Empty) => {}
            }

            notified.await;
        }
    }
}

/// The sender half of a borrowed local broadcast channel.
pub struct Sender<'a, T> {
    state: &'a State<T>,
}

impl<'a, T> Sender<'a, T> {
    /// Sends a value to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> Receiver<'a, T> {
        let mut inner = self.state.inner.borrow_mut();
        inner.receiver_count += 1;
        let next_seq = inner.tail;
        Receiver {
            state: self.state,
            next_seq,
        }
    }

    /// Returns the number of active receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.state.receiver_count()
    }

    /// Returns the number of active senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.state.sender_count()
    }

    /// Returns the capacity of the channel.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.state.capacity()
    }

    /// Returns the number of buffered messages.
    #[inline]
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` if no messages are currently buffered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Returns `true` if all receivers have been dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns `true` if both senders belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        ptr::eq(self.state, other.state)
    }
}

impl<T> Clone for Sender<'_, T> {
    fn clone(&self) -> Self {
        self.state.inner.borrow_mut().sender_count += 1;
        Self { state: self.state }
    }
}

impl<T> Drop for Sender<'_, T> {
    fn drop(&mut self) {
        let mut inner = self.state.inner.borrow_mut();
        inner.sender_count -= 1;
        if inner.sender_count == 0 {
            drop(inner);
            self.state.rx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for Sender<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("capacity", &self.capacity())
            .field("receiver_count", &self.receiver_count())
            .field("sender_count", &self.sender_count())
            .finish()
    }
}

/// The receiver half of a borrowed local broadcast channel.
pub struct Receiver<'a, T> {
    state: &'a State<T>,
    next_seq: u64,
}

impl<'a, T> Receiver<'a, T> {
    /// Asynchronously receives the next value for this receiver.
    pub async fn recv(&mut self) -> Result<T, RecvError>
    where
        T: Clone,
    {
        self.state.recv(&mut self.next_seq).await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        self.state.try_recv(&mut self.next_seq)
    }

    /// Creates a new receiver subscribed to this channel starting from the latest message.
    pub fn resubscribe(&self) -> Receiver<'a, T> {
        let mut inner = self.state.inner.borrow_mut();
        inner.receiver_count += 1;
        let next_seq = inner.tail;
        Receiver {
            state: self.state,
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let inner = self.state.inner.borrow();
        let oldest_seq = inner.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        inner.tail.saturating_sub(eff_seq) as usize
    }

    /// Returns `true` if there are no messages available to this receiver.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if all senders have been dropped and all messages consumed.
    pub fn is_closed(&self) -> bool {
        self.state.sender_count() == 0 && self.is_empty()
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        ptr::eq(self.state, other.state)
    }
}

impl<T> Clone for Receiver<'_, T> {
    fn clone(&self) -> Self {
        self.state.inner.borrow_mut().receiver_count += 1;
        Self {
            state: self.state,
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for Receiver<'_, T> {
    fn drop(&mut self) {
        self.state.inner.borrow_mut().receiver_count -= 1;
    }
}

impl<T> fmt::Debug for Receiver<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("next_seq", &self.next_seq)
            .field("len", &self.len())
            .finish()
    }
}

/// An owned sender for a local broadcast channel.
pub struct OwnedSender<T> {
    state: Rc<State<T>>,
}

impl<T> OwnedSender<T> {
    /// Sends a value to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new owned receiver subscribed to this channel.
    pub fn subscribe(&self) -> OwnedReceiver<T> {
        let mut inner = self.state.inner.borrow_mut();
        inner.receiver_count += 1;
        let next_seq = inner.tail;
        OwnedReceiver {
            state: self.state.clone(),
            next_seq,
        }
    }

    /// Returns the number of active receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.state.receiver_count()
    }

    /// Returns the number of active senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.state.sender_count()
    }

    /// Returns the capacity of the channel.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.state.capacity()
    }

    /// Returns the number of buffered messages.
    #[inline]
    pub fn len(&self) -> usize {
        self.state.len()
    }

    /// Returns `true` if no messages are currently buffered.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.state.is_empty()
    }

    /// Returns `true` if all receivers have been dropped.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns `true` if both senders belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for OwnedSender<T> {
    fn clone(&self) -> Self {
        self.state.inner.borrow_mut().sender_count += 1;
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        let mut inner = self.state.inner.borrow_mut();
        inner.sender_count -= 1;
        if inner.sender_count == 0 {
            drop(inner);
            self.state.rx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for OwnedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSender")
            .field("capacity", &self.capacity())
            .field("receiver_count", &self.receiver_count())
            .field("sender_count", &self.sender_count())
            .finish()
    }
}

/// An owned receiver for a local broadcast channel.
pub struct OwnedReceiver<T> {
    state: Rc<State<T>>,
    next_seq: u64,
}

impl<T> OwnedReceiver<T> {
    /// Asynchronously receives the next value for this receiver.
    pub async fn recv(&mut self) -> Result<T, RecvError>
    where
        T: Clone,
    {
        self.state.recv(&mut self.next_seq).await
    }

    /// Attempts to receive the next value without waiting.
    pub fn try_recv(&mut self) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        self.state.try_recv(&mut self.next_seq)
    }

    /// Creates a new owned receiver subscribed to this channel starting from the latest message.
    pub fn resubscribe(&self) -> OwnedReceiver<T> {
        let mut inner = self.state.inner.borrow_mut();
        inner.receiver_count += 1;
        let next_seq = inner.tail;
        OwnedReceiver {
            state: self.state.clone(),
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let inner = self.state.inner.borrow();
        let oldest_seq = inner.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        inner.tail.saturating_sub(eff_seq) as usize
    }

    /// Returns `true` if there are no messages available to this receiver.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if all senders have been dropped and all messages consumed.
    pub fn is_closed(&self) -> bool {
        self.state.sender_count() == 0 && self.is_empty()
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for OwnedReceiver<T> {
    fn clone(&self) -> Self {
        self.state.inner.borrow_mut().receiver_count += 1;
        Self {
            state: self.state.clone(),
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        self.state.inner.borrow_mut().receiver_count -= 1;
    }
}

impl<T> fmt::Debug for OwnedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedReceiver")
            .field("next_seq", &self.next_seq)
            .field("len", &self.len())
            .finish()
    }
}

/// Creates a new broadcast channel returning an owned sender and receiver pair.
pub fn channel<T>(capacity: usize) -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Rc::new(State::new(capacity));
    {
        let mut inner = state.inner.borrow_mut();
        inner.sender_count = 1;
        inner.receiver_count = 1;
    }
    let next_seq = state.inner.borrow().tail;
    (
        OwnedSender {
            state: state.clone(),
        },
        OwnedReceiver { state, next_seq },
    )
}

/// Creates a new owned broadcast channel returning an owned sender and receiver pair.
pub fn owned_channel<T>(capacity: usize) -> (OwnedSender<T>, OwnedReceiver<T>) {
    channel(capacity)
}
