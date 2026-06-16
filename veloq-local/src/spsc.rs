use futures_core::Future;
use futures_core::stream::Stream;
use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    cell::UnsafeCell,
    mem,
    pin::Pin,
    ptr::{self, NonNull},
    rc::Rc,
    task::{Context, Poll, Waker},
};

use crate::common::update_waker;
pub use crate::common::{ChannelCapacity, SendError, TryRecvError};

struct Inner<T> {
    buffer: NonNull<T>,
    capacity: usize,
    mask: usize,
    head: usize,
    tail: usize,
    is_closed: bool,
    producer_waker: Option<Waker>,
    consumer_waker: Option<Waker>,
    is_bounded: bool,
}

impl<T> Inner<T> {
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

        Inner {
            buffer: ptr,
            capacity: cap,
            mask: cap - 1,
            head: 0,
            tail: 0,
            is_closed: false,
            producer_waker: None,
            consumer_waker: None,
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
        // This applies to both normal types (where we physically moved memory)
        // and ZSTs (where we conceptually reset the window).
        self.head = 0;
        self.tail = self.len(); // old length
    }
}

impl<T> Drop for Inner<T> {
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

/// SPSC Channel Sender
#[derive(Debug)]
pub struct Sender<T> {
    inner: Rc<UnsafeCell<Inner<T>>>,
}

/// SPSC Channel Receiver
#[derive(Debug)]
pub struct Receiver<T> {
    inner: Rc<UnsafeCell<Inner<T>>>,
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let waker = {
            let inner = unsafe { &mut *self.inner.get() };
            inner.is_closed = true;
            inner.consumer_waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let waker = {
            let inner = unsafe { &mut *self.inner.get() };
            inner.is_closed = true;
            inner.producer_waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

impl<T> Sender<T> {
    /// Attempts to send a message.
    pub fn try_send(&self, item: T) -> Result<(), SendError<T>> {
        let waker = {
            let inner = unsafe { &mut *self.inner.get() };

            if inner.is_closed {
                return Err(SendError::Closed(item));
            }

            if let Err(item) = inner.push(item) {
                return Err(SendError::Full(item));
            }

            inner.consumer_waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }

        Ok(())
    }

    /// Asynchronously sends a message.
    pub async fn send(&self, item: T) -> Result<(), SendError<T>> {
        SendFuture {
            sender: self,
            item: Some(item),
        }
        .await
    }

    /// Checks if the channel is full.
    pub fn is_full(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.is_full()
    }

    /// Returns the number of messages currently in the channel.
    pub fn len(&self) -> usize {
        let inner = unsafe { &*self.inner.get() };
        inner.len()
    }

    /// Checks if the channel is empty.
    pub fn is_empty(&self) -> bool {
        let inner = unsafe { &*self.inner.get() };
        inner.is_empty()
    }
}

struct SendFuture<'a, T> {
    sender: &'a Sender<T>,
    item: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), SendError<T>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let waker_to_wake = {
            let inner = unsafe { &mut *self.sender.inner.get() };

            if inner.is_closed {
                let item = self
                    .item
                    .take()
                    .expect("Polled SendFuture after completion");
                return Poll::Ready(Err(SendError::Closed(item)));
            }

            let item = self
                .item
                .take()
                .expect("Polled SendFuture after completion");
            match inner.push(item) {
                Ok(()) => inner.consumer_waker.take(),
                Err(item) => {
                    self.item = Some(item);

                    update_waker(&mut inner.producer_waker, cx.waker());
                    return Poll::Pending;
                }
            }
        };

        // If we reached here, push succeeded
        if let Some(waker) = waker_to_wake {
            waker.wake();
        }
        Poll::Ready(Ok(()))
    }
}

impl<T> Receiver<T> {
    /// Attempts to receive a message.
    pub fn try_recv(&self) -> Result<T, TryRecvError> {
        let (item, waker) = {
            let inner = unsafe { &mut *self.inner.get() };

            if let Some(item) = inner.pop() {
                (Some(item), inner.producer_waker.take())
            } else if inner.is_closed {
                (None, None)
            } else {
                return Err(TryRecvError::Empty);
            }
        };

        if let Some(waker) = waker {
            waker.wake();
        }

        if let Some(item) = item {
            Ok(item)
        } else {
            Err(TryRecvError::Closed)
        }
    }

    /// Asynchronously receives a message.
    pub async fn recv(&self) -> Option<T> {
        RecvFuture { receiver: self }.await
    }

    /// Converts the receiver into a `Stream`.
    pub fn stream(&self) -> impl Stream<Item = T> + '_ {
        ChannelStream { receiver: self }
    }
}

struct RecvFuture<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<T> Future for RecvFuture<'_, T> {
    type Output = Option<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let (item, waker) = {
            let inner = unsafe { &mut *self.receiver.inner.get() };

            if let Some(item) = inner.pop() {
                (Some(item), inner.producer_waker.take())
            } else if inner.is_closed {
                return Poll::Ready(None);
            } else {
                update_waker(&mut inner.consumer_waker, cx.waker());
                return Poll::Pending;
            }
        };

        if let Some(w) = waker {
            w.wake();
        }
        Poll::Ready(item)
    }
}

struct ChannelStream<'a, T> {
    receiver: &'a Receiver<T>,
}

impl<'a, T> Stream for ChannelStream<'a, T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let (item, waker) = {
            let inner = unsafe { &mut *self.receiver.inner.get() };

            if let Some(item) = inner.pop() {
                (Some(item), inner.producer_waker.take())
            } else if inner.is_closed {
                return Poll::Ready(None);
            } else {
                update_waker(&mut inner.consumer_waker, cx.waker());
                return Poll::Pending;
            }
        };

        if let Some(w) = waker {
            w.wake();
        }
        Poll::Ready(item)
    }
}

/// Creates a new unbounded SPSC channel.
pub fn unbounded<T>() -> (Sender<T>, Receiver<T>) {
    new(ChannelCapacity::Unbounded)
}

/// Creates a new bounded SPSC channel.
pub fn bounded<T>(size: usize) -> (Sender<T>, Receiver<T>) {
    new(ChannelCapacity::Bounded(size))
}

fn new<T>(capacity: ChannelCapacity) -> (Sender<T>, Receiver<T>) {
    let inner = Inner::new(capacity);
    let state = Rc::new(UnsafeCell::new(inner));

    (
        Sender {
            inner: state.clone(),
        },
        Receiver { inner: state },
    )
}
