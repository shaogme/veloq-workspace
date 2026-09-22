use core::pin::pin;

use veloq_std::{
    collections::VecDeque,
    error::Error,
    fmt, ptr,
    sync::{
        Arc, UnpoisonedMutex,
        atomic::{AtomicUsize, Ordering},
    },
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

struct ChannelBuffer<T> {
    queue: VecDeque<T>,
    tail: u64,
}

impl<T> ChannelBuffer<T> {
    fn new(capacity: usize) -> Self {
        Self {
            queue: VecDeque::with_capacity(capacity),
            tail: 0,
        }
    }

    #[inline]
    fn oldest_seq(&self) -> u64 {
        self.tail.saturating_sub(self.queue.len() as u64)
    }
}

/// A multi-producer, multi-consumer broadcast channel state.
pub struct State<T> {
    capacity: usize,
    buffer: UnpoisonedMutex<ChannelBuffer<T>>,
    sender_count: AtomicUsize,
    receiver_count: AtomicUsize,
    rx_notify: Notify,
}

unsafe impl<T: Send> Send for State<T> {}
unsafe impl<T: Send> Sync for State<T> {}

impl<T> State<T> {
    /// Creates a new broadcast channel state with the given capacity.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be greater than 0");
        Self {
            capacity,
            buffer: UnpoisonedMutex::new(ChannelBuffer::new(capacity)),
            sender_count: AtomicUsize::new(0),
            receiver_count: AtomicUsize::new(0),
            rx_notify: Notify::new(),
        }
    }

    /// Splits the state into a borrowed sender and receiver pair.
    pub fn split(&self) -> (BorrowedSender<'_, T>, BorrowedReceiver<'_, T>) {
        self.sender_count.store(1, Ordering::Release);
        self.receiver_count.store(1, Ordering::Release);
        let next_seq = self.buffer.lock().tail;
        (
            BorrowedSender { state: self },
            BorrowedReceiver {
                state: self,
                next_seq,
            },
        )
    }

    /// Returns the capacity of the channel.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the number of active senders.
    #[inline]
    pub fn sender_count(&self) -> usize {
        self.sender_count.load(Ordering::Acquire)
    }

    /// Returns the number of active receivers.
    #[inline]
    pub fn receiver_count(&self) -> usize {
        self.receiver_count.load(Ordering::Acquire)
    }

    /// Returns the number of buffered messages.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.lock().queue.len()
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
        let rx_count = self.receiver_count.load(Ordering::Acquire);
        if rx_count == 0 {
            return Err(SendError(value));
        }

        {
            let mut buf = self.buffer.lock();
            if self.receiver_count.load(Ordering::Acquire) == 0 {
                return Err(SendError(value));
            }

            if buf.queue.len() == self.capacity {
                buf.queue.pop_front();
            }
            buf.queue.push_back(value);
            buf.tail += 1;
        }

        self.rx_notify.notify_waiters();
        Ok(rx_count)
    }

    /// Attempts to receive a message for a given receiver cursor.
    pub fn try_recv(&self, next_seq: &mut u64) -> Result<T, TryRecvError>
    where
        T: Clone,
    {
        let buf = self.buffer.lock();
        let oldest_seq = buf.oldest_seq();

        if *next_seq < oldest_seq {
            let missed = oldest_seq - *next_seq;
            *next_seq = oldest_seq;
            return Err(TryRecvError::Lagged(missed));
        }

        if *next_seq < buf.tail {
            let idx = (*next_seq - oldest_seq) as usize;
            let val = buf.queue[idx].clone();
            *next_seq += 1;
            return Ok(val);
        }

        if self.sender_count.load(Ordering::Acquire) == 0 {
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
    state: &'a State<T>,
}

unsafe impl<T: Send> Send for BorrowedSender<'_, T> {}
unsafe impl<T: Send> Sync for BorrowedSender<'_, T> {}

impl<'a, T> BorrowedSender<'a, T> {
    /// Sends a value to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> BorrowedReceiver<'a, T> {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let next_seq = self.state.buffer.lock().tail;
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
        self.state.sender_count.fetch_add(1, Ordering::AcqRel);
        Self { state: self.state }
    }
}

impl<T> Drop for BorrowedSender<'_, T> {
    fn drop(&mut self) {
        let prev = self.state.sender_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
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
    state: &'a State<T>,
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
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let next_seq = self.state.buffer.lock().tail;
        BorrowedReceiver {
            state: self.state,
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let buf = self.state.buffer.lock();
        let oldest_seq = buf.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        buf.tail.saturating_sub(eff_seq) as usize
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
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            state: self.state,
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for BorrowedReceiver<'_, T> {
    fn drop(&mut self) {
        self.state.receiver_count.fetch_sub(1, Ordering::AcqRel);
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
    state: Arc<State<T>>,
}

unsafe impl<T: Send> Send for Sender<T> {}
unsafe impl<T: Send> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// Sends a value to all active receivers.
    pub fn send(&self, value: T) -> Result<usize, SendError<T>> {
        self.state.send(value)
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> Receiver<T> {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let next_seq = self.state.buffer.lock().tail;
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
        self.state.sender_count.fetch_add(1, Ordering::AcqRel);
        Self {
            state: self.state.clone(),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let prev = self.state.sender_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
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
    state: Arc<State<T>>,
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
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let next_seq = self.state.buffer.lock().tail;
        Receiver {
            state: self.state.clone(),
            next_seq,
        }
    }

    /// Returns the number of messages currently available to this receiver.
    pub fn len(&self) -> usize {
        let buf = self.state.buffer.lock();
        let oldest_seq = buf.oldest_seq();
        let eff_seq = self.next_seq.max(oldest_seq);
        buf.tail.saturating_sub(eff_seq) as usize
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
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            state: self.state.clone(),
            next_seq: self.next_seq,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.state.receiver_count.fetch_sub(1, Ordering::AcqRel);
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
    let state = Arc::new(State::new(capacity));
    state.sender_count.store(1, Ordering::Release);
    state.receiver_count.store(1, Ordering::Release);
    let next_seq = state.buffer.lock().tail;
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state, next_seq },
    )
}

/// Creates a new borrowed broadcast channel state.
pub fn borrowed_channel<T>(capacity: usize) -> State<T> {
    State::new(capacity)
}
