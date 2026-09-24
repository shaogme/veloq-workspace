use core::pin::pin;

use veloq_std::{
    collections::VecDeque,
    error::Error,
    fmt,
    ops::AsyncFnOnce,
    ptr,
    sync::{Arc, UnpoisonedMutex},
};

pub use crate::SendError;
use crate::notify::Notify;

/// Error returned from [`Receiver::recv`] or [`BorrowedReceiver::recv`].
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

/// Error returned from [`Receiver::try_recv`] or [`BorrowedReceiver::try_recv`].
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

struct ChannelState<T> {
    capacity: usize,
    queue: VecDeque<T>,
    tail: u64,
    sender_count: usize,
    receiver_count: usize,
}

impl<T> ChannelState<T> {
    fn new(capacity: usize, sender_count: usize, receiver_count: usize) -> Self {
        Self {
            capacity,
            queue: VecDeque::with_capacity(capacity),
            tail: 0,
            sender_count,
            receiver_count,
        }
    }

    #[inline]
    fn oldest_seq(&self) -> u64 {
        self.tail.saturating_sub(self.queue.len() as u64)
    }
}

/// A multi-producer, multi-consumer broadcast channel state.
struct Inner<T> {
    state: UnpoisonedMutex<ChannelState<T>>,
    rx_notify: Notify,
}

unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

impl<T> Inner<T> {
    /// Creates a new broadcast channel state with the given capacity.
    fn new(capacity: usize, sender_count: usize, receiver_count: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        Self {
            state: UnpoisonedMutex::new(ChannelState::new(capacity, sender_count, receiver_count)),
            rx_notify: Notify::new(),
        }
    }

    /// Returns the capacity of the channel.
    #[inline]
    fn capacity(&self) -> usize {
        self.state.lock().capacity
    }

    /// Returns the number of active senders.
    #[inline]
    fn sender_count(&self) -> usize {
        self.state.lock().sender_count
    }

    /// Returns the number of active receivers.
    #[inline]
    fn receiver_count(&self) -> usize {
        self.state.lock().receiver_count
    }

    /// Returns the number of buffered messages.
    #[inline]
    fn len(&self) -> usize {
        self.state.lock().queue.len()
    }

    /// Returns `true` if no messages are currently buffered.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns `true` if all receivers have been dropped.
    #[inline]
    fn is_closed(&self) -> bool {
        self.receiver_count() == 0
    }

    /// Commits a value and returns the receiver count at the commit point.
    fn commit_send(&self, value: T) -> Result<usize, SendError<T>> {
        let (rx_count, evicted) = {
            let mut state = self.state.lock();
            if state.receiver_count == 0 {
                return Err(SendError(value));
            }

            let evicted = if state.queue.len() == state.capacity {
                state.queue.pop_front()
            } else {
                None
            };
            state.queue.push_back(value);
            state.tail += 1;
            (state.receiver_count, evicted)
        };

        drop(evicted);
        self.rx_notify.notify_waiters();
        Ok(rx_count)
    }

    /// Sends a value over the channel to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.commit_send(value)
    }

    /// Attempts to receive a message for a given receiver cursor.
    pub fn try_recv(&self, next_seq: &mut u64) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let state = self.state.lock();
        let oldest_seq = state.oldest_seq();

        if *next_seq < oldest_seq {
            let missed = oldest_seq - *next_seq;
            *next_seq = oldest_seq;
            return Err(TryRecvError::Lagged(missed));
        }

        if *next_seq < state.tail {
            let idx = (*next_seq - oldest_seq) as usize;
            let val = state.queue[idx].clone();
            *next_seq += 1;
            return Ok(val);
        }

        if state.sender_count == 0 {
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

/// The sender half of a borrowed broadcast channel.
pub struct BorrowedSender<'a, T> {
    state: &'a Inner<T>,
}

unsafe impl<T: Send> Send for BorrowedSender<'_, T> {}
unsafe impl<T: Send> Sync for BorrowedSender<'_, T> {}

impl<'a, T> BorrowedSender<'a, T> {
    /// Sends a value to all active receivers.
    ///
    /// On success, the returned count is the receiver snapshot at the send
    /// linearization point.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> BorrowedReceiver<'a, T> {
        let mut state = self.state.state.lock();
        state.receiver_count += 1;
        let next_seq = state.tail;
        BorrowedReceiver {
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

impl<T> Clone for BorrowedSender<'_, T> {
    fn clone(&self) -> Self {
        self.state.state.lock().sender_count += 1;
        Self { state: self.state }
    }
}

impl<T> Drop for BorrowedSender<'_, T> {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.state.state.lock();
            state.sender_count -= 1;
            state.sender_count == 0
        };
        if notify {
            self.state.rx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for BorrowedSender<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedSender")
            .field("capacity", &self.capacity())
            .field("receiver_count", &self.receiver_count())
            .field("sender_count", &self.sender_count())
            .finish()
    }
}

/// The receiver half of a borrowed broadcast channel.
pub struct BorrowedReceiver<'a, T> {
    state: &'a Inner<T>,
    next_seq: u64,
}

unsafe impl<T: Send> Send for BorrowedReceiver<'_, T> {}
unsafe impl<T: Send> Sync for BorrowedReceiver<'_, T> {}

impl<'a, T> BorrowedReceiver<'a, T> {
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
    pub fn resubscribe(&self) -> BorrowedReceiver<'a, T> {
        let mut state = self.state.state.lock();
        state.receiver_count += 1;
        let next_seq = state.tail;
        BorrowedReceiver {
            state: self.state,
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let state = self.state.state.lock();
        let oldest_seq = state.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        state.tail.saturating_sub(eff_seq) as usize
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

impl<T> Clone for BorrowedReceiver<'_, T> {
    fn clone(&self) -> Self {
        self.state.state.lock().receiver_count += 1;
        Self {
            state: self.state,
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for BorrowedReceiver<'_, T> {
    fn drop(&mut self) {
        self.state.state.lock().receiver_count -= 1;
    }
}

impl<T> fmt::Debug for BorrowedReceiver<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedReceiver")
            .field("next_seq", &self.next_seq)
            .field("len", &self.len())
            .finish()
    }
}

/// A sender for a broadcast channel.
pub struct Sender<T> {
    state: Arc<Inner<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// Sends a value to all active receivers.
    ///
    /// On success, the returned count is the receiver snapshot at the send
    /// linearization point.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> Receiver<T> {
        let mut state = self.state.state.lock();
        state.receiver_count += 1;
        let next_seq = state.tail;
        Receiver {
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
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for Sender<T> {
    fn clone(&self) -> Self {
        self.state.state.lock().sender_count += 1;
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let notify = {
            let mut state = self.state.state.lock();
            state.sender_count -= 1;
            state.sender_count == 0
        };
        if notify {
            self.state.rx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("capacity", &self.capacity())
            .field("receiver_count", &self.receiver_count())
            .field("sender_count", &self.sender_count())
            .finish()
    }
}

/// A receiver for a broadcast channel.
pub struct Receiver<T> {
    state: Arc<Inner<T>>,
    next_seq: u64,
}

unsafe impl<T: Send> Send for Receiver<T> {}
unsafe impl<T: Send> Sync for Receiver<T> {}

impl<T> Receiver<T> {
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
    pub fn resubscribe(&self) -> Receiver<T> {
        let mut state = self.state.state.lock();
        state.receiver_count += 1;
        let next_seq = state.tail;
        Receiver {
            state: self.state.clone(),
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let state = self.state.state.lock();
        let oldest_seq = state.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        state.tail.saturating_sub(eff_seq) as usize
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
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.state.state.lock().receiver_count += 1;
        Self {
            state: self.state.clone(),
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.state.state.lock().receiver_count -= 1;
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("next_seq", &self.next_seq)
            .field("len", &self.len())
            .finish()
    }
}

/// Creates a new broadcast channel returning a sender and receiver pair.
pub fn channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Inner::new(capacity, 1, 1));
    let next_seq = state.state.lock().tail;
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state, next_seq },
    )
}

/// Creates a new borrowed broadcast channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_channel<T, F, R>(capacity: usize, f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = Inner::new(capacity, 1, 1);
    let next_seq = state.state.lock().tail;
    let tx = BorrowedSender { state: &state };
    let rx = BorrowedReceiver {
        state: &state,
        next_seq,
    };
    f(tx, rx).await
}
