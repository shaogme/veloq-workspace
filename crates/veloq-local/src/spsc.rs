use core::cell::UnsafeCell;
use futures_core::Future;
use futures_core::stream::Stream;

use veloq_std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    fmt, mem,
    mem::ManuallyDrop,
    pin::Pin,
    ptr::{self, NonNull},
    rc::Rc,
    task::{Context, Poll},
};

pub use crate::common::{ChannelCapacity, SendError, TryRecvError};
use crate::notify::{Notified, Notify};

struct ChannelState<T> {
    buffer: NonNull<T>,
    capacity: usize,
    mask: usize,
    head: usize,
    tail: usize,
    is_closed: bool,
    is_bounded: bool,
}

impl<T> ChannelState<T> {
    fn new(capacity: ChannelCapacity) -> Self {
        let (cap, is_bounded) = match capacity {
            ChannelCapacity::Unbounded => (8, false), // Start small
            ChannelCapacity::Bounded(size) => {
                let size = size.max(1);
                // Round up to power of 2
                let cap = size.next_power_of_two();
                (cap, true)
            }
        };

        let ptr = if mem::size_of::<T>() > 0 {
            let layout = Layout::array::<T>(cap).unwrap();
            let ptr = unsafe { alloc(layout) } as *mut T;
            if ptr.is_null() {
                handle_alloc_error(layout);
            }
            unsafe { NonNull::new_unchecked(ptr) }
        } else {
            NonNull::dangling()
        };

        ChannelState {
            buffer: ptr,
            capacity: cap,
            mask: cap - 1,
            head: 0,
            tail: 0,
            is_closed: false,
            is_bounded,
        }
    }

    fn len(&self) -> usize {
        self.tail.wrapping_sub(self.head)
    }

    fn is_empty(&self) -> bool {
        self.head == self.tail
    }

    fn is_full(&self) -> bool {
        self.len() == self.capacity
    }

    fn push(&mut self, item: T) -> Result<(), T> {
        if self.is_full() {
            if self.is_bounded {
                return Err(item);
            } else {
                self.grow();
            }
        }

        unsafe {
            let offset = self.tail & self.mask;
            ptr::write(self.buffer.as_ptr().add(offset), item);
        }
        self.tail = self.tail.wrapping_add(1);
        Ok(())
    }

    fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let item = unsafe {
            let offset = self.head & self.mask;
            ptr::read(self.buffer.as_ptr().add(offset))
        };
        self.head = self.head.wrapping_add(1);
        Some(item)
    }

    fn grow(&mut self) {
        let old_cap = self.capacity;
        let new_cap = old_cap * 2;

        if mem::size_of::<T>() > 0 {
            let layout = Layout::array::<T>(new_cap).unwrap();
            let new_ptr = unsafe { alloc(layout) } as *mut T;
            if new_ptr.is_null() {
                handle_alloc_error(layout);
            }

            // Copy elements
            let count = self.len();
            unsafe {
                let head_idx = self.head & self.mask;
                let tail_idx = self.tail & self.mask;

                if head_idx < tail_idx {
                    // Contiguous
                    ptr::copy_nonoverlapping(self.buffer.as_ptr().add(head_idx), new_ptr, count);
                } else {
                    // Wrapped or full
                    let first_part = old_cap - head_idx;
                    ptr::copy_nonoverlapping(
                        self.buffer.as_ptr().add(head_idx),
                        new_ptr,
                        first_part,
                    );
                    let second_part = count - first_part;
                    ptr::copy_nonoverlapping(
                        self.buffer.as_ptr(),
                        new_ptr.add(first_part),
                        second_part,
                    );
                }

                // Deallocate old buffer
                let old_layout = Layout::array::<T>(old_cap).unwrap();
                dealloc(self.buffer.as_ptr() as *mut u8, old_layout);
            }
            self.buffer = unsafe { NonNull::new_unchecked(new_ptr) };
        }

        // Common updates
        self.capacity = new_cap;
        self.mask = new_cap - 1;

        // When growing, we realign the buffer (if it was wrapped) to start at 0.
        self.head = 0;
        self.tail = self.len(); // old length
    }
}

impl<T> Drop for ChannelState<T> {
    fn drop(&mut self) {
        // Drop remaining elements
        if mem::needs_drop::<T>() {
            while self.pop().is_some() {}
        }

        // Deallocate buffer
        if mem::size_of::<T>() > 0 {
            let layout = Layout::array::<T>(self.capacity).unwrap();
            unsafe {
                dealloc(self.buffer.as_ptr() as *mut u8, layout);
            }
        }
    }
}

struct Inner<T> {
    inner: UnsafeCell<ChannelState<T>>,
    not_empty: Notify,
    not_full: Notify,
}

impl<T> fmt::Debug for Inner<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Inner").finish_non_exhaustive()
    }
}

impl<T> Inner<T> {
    /// Creates a new SPSC channel state.
    fn new(capacity: ChannelCapacity) -> Self {
        Self {
            inner: UnsafeCell::new(ChannelState::new(capacity)),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    /// Creates a new bounded SPSC channel state.
    fn bounded(size: usize) -> Self {
        Self::new(ChannelCapacity::Bounded(size))
    }

    /// Creates a new unbounded SPSC channel state.
    fn unbounded() -> Self {
        Self::new(ChannelCapacity::Unbounded)
    }

    /// Splits the state into a sender and a receiver.
    fn split<'a>(&'a self) -> (Sender<'a, T>, Receiver<'a, T>) {
        (Sender { inner: self }, Receiver { inner: self })
    }
}

/// Creates a new bounded SPSC channel.
pub fn bounded<T>(size: usize) -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_bounded(size)
}

/// Creates a new unbounded SPSC channel.
pub fn unbounded<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_unbounded()
}

/// Creates a new borrowed bounded SPSC channel and runs the provided closure.
pub async fn with_borrowed_bounded<T, F, R>(size: usize, f: F) -> R
where
    F: for<'a> veloq_std::ops::AsyncFnOnce(Sender<'a, T>, Receiver<'a, T>) -> R,
{
    let state = Inner::bounded(size);
    let (tx, rx) = state.split();
    f(tx, rx).await
}

/// Creates a new borrowed unbounded SPSC channel and runs the provided closure.
pub async fn with_borrowed_unbounded<T, F, R>(f: F) -> R
where
    F: for<'a> veloq_std::ops::AsyncFnOnce(Sender<'a, T>, Receiver<'a, T>) -> R,
{
    let state = Inner::unbounded();
    let (tx, rx) = state.split();
    f(tx, rx).await
}

/// SPSC Channel Sender
#[derive(Debug)]
pub struct Sender<'a, T> {
    inner: &'a Inner<T>,
}

/// SPSC Channel Receiver
#[derive(Debug)]
pub struct Receiver<'a, T> {
    inner: &'a Inner<T>,
}

impl<'a, T> Drop for Sender<'a, T> {
    fn drop(&mut self) {
        unsafe {
            let inner = &mut *self.inner.inner.get();
            inner.is_closed = true;
        }
        self.inner.not_empty.notify_waiters();
    }
}

impl<'a, T> Drop for Receiver<'a, T> {
    fn drop(&mut self) {
        unsafe {
            let inner = &mut *self.inner.inner.get();
            inner.is_closed = true;
        }
        self.inner.not_full.notify_waiters();
    }
}

impl<'a, T> Sender<'a, T> {
    /// Attempts to send a message.
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let inner = unsafe { &mut *self.inner.inner.get() };

        if inner.is_closed {
            return Err(SendError::Closed(item));
        }

        if let Err(item) = inner.push(item) {
            return Err(SendError::Full(item));
        }

        self.inner.not_empty.notify_one();
        Ok(())
    }

    /// Asynchronously sends a message.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        SendFuture {
            sender: self,
            item: Some(item),
            notified: None,
        }
        .await
    }

    /// Checks if the channel is full.
    pub fn is_full(&self) -> bool {
        let inner = unsafe { &*self.inner.inner.get() };
        inner.is_full()
    }

    /// Returns the number of messages currently in the channel.
    pub fn len(&self) -> usize {
        let inner = unsafe { &*self.inner.inner.get() };
        inner.len()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.inner.get() };
        inner.is_empty()
    }
}

pub struct SendFuture<'a, 'b, T> {
    sender: &'b Sender<'a, T>,
    item: Option<T>,
    notified: Option<Notified<'a>>,
}

impl<T> Unpin for SendFuture<'_, '_, T> {}

impl<'a, 'b, T> Future for SendFuture<'a, 'b, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            let inner = unsafe { &mut *this.sender.inner.inner.get() };

            if inner.is_closed {
                this.notified = None;
                let item = this
                    .item
                    .take()
                    .expect("Polled SendFuture after completion");
                return Poll::Ready(Err(SendError::Closed(item)));
            }

            let item = this
                .item
                .take()
                .expect("Polled SendFuture after completion");
            match inner.push(item) {
                Ok(()) => {
                    this.sender.inner.not_empty.notify_one();
                    this.notified = None;
                    return Poll::Ready(Ok(()));
                }
                Err(item) => {
                    this.item = Some(item);
                }
            }

            if this.notified.is_none() {
                this.notified = Some(this.sender.inner.not_full.notified());
                let notified = this.notified.as_mut().unwrap();
                let notified_pin = unsafe { Pin::new_unchecked(notified) };
                notified_pin.enable();
            }

            let inner = unsafe { &*this.sender.inner.inner.get() };
            if inner.is_closed || !inner.is_full() {
                this.notified = None;
                continue;
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

impl<'a, T> Receiver<'a, T> {
    /// Attempts to receive a message.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let inner = unsafe { &mut *self.inner.inner.get() };

        if let Some(item) = inner.pop() {
            self.inner.not_full.notify_one();
            Ok(item)
        } else if inner.is_closed {
            Err(TryRecvError::Closed)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    /// Asynchronously receives a message.
    pub async fn recv(&self) -> Option<T> {
        RecvFuture {
            receiver: self,
            notified: None,
        }
        .await
    }

    /// Converts the receiver into a `Stream`.
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream {
            receiver: self,
            notified: None,
        }
    }
}

pub struct RecvFuture<'a, 'b, T> {
    receiver: &'b Receiver<'a, T>,
    notified: Option<Notified<'a>>,
}

impl<'a, 'b, T> Future for RecvFuture<'a, 'b, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            let inner = unsafe { &mut *this.receiver.inner.inner.get() };

            if let Some(item) = inner.pop() {
                this.receiver.inner.not_full.notify_one();
                this.notified = None;
                return Poll::Ready(Some(item));
            }
            if inner.is_closed {
                this.notified = None;
                return Poll::Ready(None);
            }

            if this.notified.is_none() {
                this.notified = Some(this.receiver.inner.not_empty.notified());
                let notified = this.notified.as_mut().unwrap();
                let notified_pin = unsafe { Pin::new_unchecked(notified) };
                notified_pin.enable();
            }

            let inner = unsafe { &*this.receiver.inner.inner.get() };
            if inner.is_closed || !inner.is_empty() {
                this.notified = None;
                continue;
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

pub struct ChannelStream<'a, 'b, T> {
    receiver: &'b Receiver<'a, T>,
    notified: Option<Notified<'a>>,
}

impl<'a, 'b, T> Stream for ChannelStream<'a, 'b, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            let inner = unsafe { &mut *this.receiver.inner.inner.get() };

            if let Some(item) = inner.pop() {
                this.receiver.inner.not_full.notify_one();
                this.notified = None;
                return Poll::Ready(Some(item));
            }
            if inner.is_closed {
                this.notified = None;
                return Poll::Ready(None);
            }

            if this.notified.is_none() {
                this.notified = Some(this.receiver.inner.not_empty.notified());
                let notified = this.notified.as_mut().unwrap();
                let notified_pin = unsafe { Pin::new_unchecked(notified) };
                notified_pin.enable();
            }

            let inner = unsafe { &*this.receiver.inner.inner.get() };
            if inner.is_closed || !inner.is_empty() {
                this.notified = None;
                continue;
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

/// Owned SPSC channel sender.
pub struct OwnedSender<T> {
    inner: Rc<Inner<T>>,
}

/// Owned SPSC channel receiver.
pub struct OwnedReceiver<T> {
    inner: Rc<Inner<T>>,
}

/// Creates a new owned SPSC channel.
pub fn owned_channel<T>(capacity: ChannelCapacity) -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Rc::new(Inner::new(capacity));
    (
        OwnedSender {
            inner: state.clone(),
        },
        OwnedReceiver { inner: state },
    )
}

/// Creates a new bounded owned SPSC channel.
pub fn owned_bounded<T>(size: usize) -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_channel(ChannelCapacity::Bounded(size))
}

/// Creates a new unbounded owned SPSC channel.
pub fn owned_unbounded<T>() -> (OwnedSender<T>, OwnedReceiver<T>) {
    owned_channel(ChannelCapacity::Unbounded)
}

impl<T> OwnedSender<T> {
    /// Attempts to send a message without blocking.
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(Sender { inner: &self.inner });
        sender.try_send(item)
    }

    /// Asynchronously sends a message.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        let sender = ManuallyDrop::new(Sender { inner: &self.inner });
        sender.send(item).await
    }

    /// Checks if the channel is full.
    pub fn is_full(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { inner: &self.inner });
        sender.is_full()
    }

    /// Returns the number of messages in the channel.
    pub fn len(&self) -> usize {
        let sender = ManuallyDrop::new(Sender { inner: &self.inner });
        sender.len()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let sender = ManuallyDrop::new(Sender { inner: &self.inner });
        sender.is_empty()
    }
}

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        drop(Sender { inner: &self.inner });
    }
}

impl<T> OwnedReceiver<T> {
    /// Attempts to receive a message without blocking.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let receiver = ManuallyDrop::new(Receiver { inner: &self.inner });
        receiver.try_recv()
    }

    /// Asynchronously receives a message.
    pub async fn recv(&self) -> Option<T> {
        let receiver = ManuallyDrop::new(Receiver { inner: &self.inner });
        receiver.recv().await
    }

    /// Converts the receiver into a stream.
    pub fn stream(&self) -> OwnedChannelStream<T> {
        OwnedChannelStream {
            receiver: OwnedReceiver {
                inner: self.inner.clone(),
            },
            notified: None,
        }
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        drop(Receiver { inner: &self.inner });
    }
}

/// A stream of messages from an owned SPSC channel.
pub struct OwnedChannelStream<T> {
    receiver: OwnedReceiver<T>,
    notified: Option<Notified<'static>>,
}

impl<T> Stream for OwnedChannelStream<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = unsafe { self.get_unchecked_mut() };
        loop {
            let inner = unsafe { &mut *this.receiver.inner.inner.get() };

            if let Some(item) = inner.pop() {
                this.receiver.inner.not_full.notify_one();
                this.notified = None;
                return Poll::Ready(Some(item));
            }
            if inner.is_closed {
                this.notified = None;
                return Poll::Ready(None);
            }

            if this.notified.is_none() {
                let notify_ptr = &this.receiver.inner.not_empty as *const Notify;
                let notify_ref: &'static Notify = unsafe { &*notify_ptr };
                this.notified = Some(notify_ref.notified());
                let notified = this.notified.as_mut().unwrap();
                let notified_pin = unsafe { Pin::new_unchecked(notified) };
                notified_pin.enable();
            }

            let inner = unsafe { &*this.receiver.inner.inner.get() };
            if inner.is_closed || !inner.is_empty() {
                this.notified = None;
                continue;
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

impl<T> Drop for OwnedChannelStream<T> {
    fn drop(&mut self) {
        self.notified = None;
    }
}
