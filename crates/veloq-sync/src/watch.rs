use core::ops::Deref;

use veloq_std::{
    fmt,
    ops::AsyncFnOnce,
    pin::pin,
    sync::{Arc, UnpoisonedRwLock, UnpoisonedRwLockReadGuard},
};

use crate::{RecvError, SendError, notify::Notify};

/// The value and metadata that make up one watch channel state.
struct WatchState<T> {
    value: T,
    version: usize,
    receiver_count: usize,
    sender_closed: bool,
}

impl<T> WatchState<T> {
    fn new(value: T, receiver_count: usize) -> Self {
        Self {
            value,
            version: 1,
            receiver_count,
            sender_closed: false,
        }
    }
}

/// A single-producer, multi-consumer watch channel state.
struct Inner<T> {
    state: UnpoisonedRwLock<WatchState<T>>,
    rx_notify: Notify,
    tx_notify: Notify,
}

unsafe impl<T: Send + Sync> Send for Inner<T> {}
unsafe impl<T: Send + Sync> Sync for Inner<T> {}

impl<T> Inner<T> {
    /// Creates a new channel state initialized with the given value and receiver.
    fn new(init: T) -> Self {
        Self {
            state: UnpoisonedRwLock::new(WatchState::new(init, 1)),
            rx_notify: Notify::new(),
            tx_notify: Notify::new(),
        }
    }

    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        {
            let mut state = self.state.write();
            if state.receiver_count == 0 {
                return Err(SendError(value));
            }
            state.value = value;
            state.version = state.version.wrapping_add(1);
        }
        self.rx_notify.notify_waiters();
        Ok(())
    }

    /// Modifies the value in place using the given closure and notifies receivers.
    ///
    /// The closure runs while the channel state is write-locked. It must not
    /// re-enter this channel through sending, subscribing, receiver counting,
    /// borrowing, or any endpoint operation.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        let (res, notify) = {
            let mut state = self.state.write();
            let res = modify(&mut state.value);
            let notify = state.receiver_count > 0;
            if notify {
                state.version = state.version.wrapping_add(1);
            }
            (res, notify)
        };
        if notify {
            self.rx_notify.notify_waiters();
        }
        res
    }

    /// Modifies the value in place if the closure returns `true`, and notifies receivers.
    ///
    /// The closure runs while the channel state is write-locked. It must not
    /// re-enter this channel through sending, subscribing, receiver counting,
    /// borrowing, or any endpoint operation.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        let (modified, notify) = {
            let mut state = self.state.write();
            let modified = modify(&mut state.value);
            let notify = modified && state.receiver_count > 0;
            if notify {
                state.version = state.version.wrapping_add(1);
            }
            (modified, notify)
        };
        if notify {
            self.rx_notify.notify_waiters();
        }
        modified
    }

    /// Borrows the current value from the channel.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            inner: self.state.read(),
        }
    }

    /// Borrows the current value and updates the seen version.
    pub fn borrow_and_update(&self, version: &mut usize) -> Ref<'_, T> {
        let state = self.state.read();
        *version = state.version;
        Ref { inner: state }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.state.read().receiver_count
    }

    /// Returns `true` if all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        self.receiver_count() == 0
    }

    /// Waits asynchronously until all receivers have been dropped.
    pub async fn closed(&self) {
        loop {
            if self.is_closed() {
                return;
            }
            let mut notified = pin!(self.tx_notify.notified());
            notified.as_mut().enable();
            if self.is_closed() {
                return;
            }
            notified.await;
        }
    }

    /// Waits until a new value has been sent or the sender is dropped.
    pub async fn changed(&self, version: &mut usize) -> Result<(), RecvError> {
        loop {
            {
                let state = self.state.read();
                if state.version != *version {
                    *version = state.version;
                    return Ok(());
                }
                if state.sender_closed {
                    return Err(RecvError);
                }
            }

            let mut notified = pin!(self.rx_notify.notified());
            notified.as_mut().enable();

            {
                let state = self.state.read();
                if state.version != *version {
                    *version = state.version;
                    return Ok(());
                }
                if state.sender_closed {
                    return Err(RecvError);
                }
            }

            notified.await;
        }
    }

    /// Checks if the channel has a new value since the last seen version.
    pub fn has_changed(&self, version: usize) -> Result<bool, RecvError> {
        let state = self.state.read();
        if state.version != version {
            Ok(true)
        } else if state.sender_closed {
            Err(RecvError)
        } else {
            Ok(false)
        }
    }

    fn current_version(&self) -> usize {
        self.state.read().version
    }

    fn subscribe(&self) -> usize {
        let mut state = self.state.write();
        state.receiver_count = state.receiver_count.wrapping_add(1);
        state.version
    }

    fn clone_receiver(&self, version: usize) -> usize {
        let mut state = self.state.write();
        state.receiver_count = state.receiver_count.wrapping_add(1);
        version
    }

    fn drop_receiver(&self) -> bool {
        let notify = {
            let mut state = self.state.write();
            let previous = state.receiver_count;
            state.receiver_count = previous.saturating_sub(1);
            previous == 1
        };
        if notify {
            self.tx_notify.notify_waiters();
        }
        notify
    }

    fn close_sender(&self) {
        {
            let mut state = self.state.write();
            state.sender_closed = true;
        }
        self.rx_notify.notify_waiters();
    }

    fn borrowed_parts(&self) -> (BorrowedSender<'_, T>, BorrowedReceiver<'_, T>) {
        let version = self.current_version();
        (
            BorrowedSender { state: self },
            BorrowedReceiver {
                state: self,
                version,
            },
        )
    }
}

/// A borrowed reference to a value in a watch channel.
///
/// This read guard protects the complete channel state. Holding it can delay
/// sends, subscriptions, receiver destruction, and sender shutdown.
pub struct Ref<'a, T> {
    inner: UnpoisonedRwLockReadGuard<'a, WatchState<T>>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner.value
    }
}

impl<T: fmt::Debug> fmt::Debug for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&**self, f)
    }
}

impl<T: fmt::Display> fmt::Display for Ref<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&**self, f)
    }
}

/// The sender half of a borrowed watch channel.
pub struct BorrowedSender<'a, T> {
    state: &'a Inner<T>,
}

unsafe impl<T: Send + Sync> Send for BorrowedSender<'_, T> {}
unsafe impl<T: Send + Sync> Sync for BorrowedSender<'_, T> {}

impl<'a, T> BorrowedSender<'a, T> {
    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.state.send(value)
    }

    /// Modifies the value in place using the given closure and notifies receivers.
    ///
    /// The closure runs while the channel state is write-locked and must not
    /// re-enter this channel.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        self.state.send_modify(modify)
    }

    /// Modifies the value in place if the closure returns `true`, and notifies receivers.
    ///
    /// The closure runs while the channel state is write-locked and must not
    /// re-enter this channel.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        self.state.send_if_modified(modify)
    }

    /// Borrows the current value from the channel.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> BorrowedReceiver<'a, T> {
        let version = self.state.subscribe();
        BorrowedReceiver {
            state: self.state,
            version,
        }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.state.receiver_count()
    }

    /// Returns `true` if all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Waits asynchronously until all receivers have been dropped.
    pub async fn closed(&mut self) {
        self.state.closed().await;
    }
}

impl<T> Drop for BorrowedSender<'_, T> {
    fn drop(&mut self) {
        self.state.close_sender();
    }
}

impl<T> fmt::Debug for BorrowedSender<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedSender")
            .field("receiver_count", &self.receiver_count())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// The receiver half of a borrowed watch channel.
pub struct BorrowedReceiver<'a, T> {
    state: &'a Inner<T>,
    version: usize,
}

unsafe impl<T: Send + Sync> Send for BorrowedReceiver<'_, T> {}
unsafe impl<T: Send + Sync> Sync for BorrowedReceiver<'_, T> {}

impl<'a, T> BorrowedReceiver<'a, T> {
    /// Borrows the current value without updating the seen version.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Borrows the current value and updates the seen version.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.state.borrow_and_update(&mut self.version)
    }

    /// Waits until a new value has been sent or the sender is dropped.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        self.state.changed(&mut self.version).await
    }

    /// Checks if the channel has a new value since the last seen version.
    pub fn has_changed(&self) -> Result<bool, RecvError> {
        self.state.has_changed(self.version)
    }

    /// Marks the current value as changed.
    pub fn mark_changed(&mut self) {
        self.version = 0;
    }

    /// Marks the current value as unchanged.
    pub fn mark_unchanged(&mut self) {
        self.version = self.state.current_version();
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        veloq_std::ptr::eq(self.state, other.state)
    }
}

impl<T> Clone for BorrowedReceiver<'_, T> {
    fn clone(&self) -> Self {
        let version = self.state.clone_receiver(self.version);
        Self {
            state: self.state,
            version,
        }
    }
}

impl<T> Drop for BorrowedReceiver<'_, T> {
    fn drop(&mut self) {
        self.state.drop_receiver();
    }
}

impl<T> fmt::Debug for BorrowedReceiver<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BorrowedReceiver")
            .field("version", &self.version)
            .finish()
    }
}

/// A sender for a watch channel.
pub struct Sender<T> {
    state: Arc<Inner<T>>,
}

unsafe impl<T: Send + Sync> Send for Sender<T> {}
unsafe impl<T: Send + Sync> Sync for Sender<T> {}

impl<T> Sender<T> {
    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.state.send(value)
    }

    /// Modifies the value in place and notifies receivers.
    ///
    /// The closure runs while the channel state is write-locked and must not
    /// re-enter this channel.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        self.state.send_modify(modify)
    }

    /// Modifies the value in place if the condition is met.
    ///
    /// The closure runs while the channel state is write-locked and must not
    /// re-enter this channel.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        self.state.send_if_modified(modify)
    }

    /// Borrows the current value.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Creates a new subscribed receiver.
    pub fn subscribe(&self) -> Receiver<T> {
        let version = self.state.subscribe();
        Receiver {
            state: self.state.clone(),
            version,
        }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.state.receiver_count()
    }

    /// Returns `true` if all receivers have been dropped.
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Waits asynchronously until all receivers have been dropped.
    pub async fn closed(&mut self) {
        self.state.closed().await;
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        self.state.close_sender();
    }
}

impl<T> fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("receiver_count", &self.receiver_count())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// A receiver for a watch channel.
pub struct Receiver<T> {
    state: Arc<Inner<T>>,
    version: usize,
}

unsafe impl<T: Send + Sync> Send for Receiver<T> {}
unsafe impl<T: Send + Sync> Sync for Receiver<T> {}

impl<T> Receiver<T> {
    /// Borrows the current value without updating the seen version.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Borrows the current value and updates the seen version.
    pub fn borrow_and_update(&mut self) -> Ref<'_, T> {
        self.state.borrow_and_update(&mut self.version)
    }

    /// Waits until a new value has been sent or the sender is dropped.
    pub async fn changed(&mut self) -> Result<(), RecvError> {
        self.state.changed(&mut self.version).await
    }

    /// Checks if the channel has a new value since the last seen version.
    pub fn has_changed(&self) -> Result<bool, RecvError> {
        self.state.has_changed(self.version)
    }

    /// Marks the current value as changed.
    pub fn mark_changed(&mut self) {
        self.version = 0;
    }

    /// Marks the current value as unchanged.
    pub fn mark_unchanged(&mut self) {
        self.version = self.state.current_version();
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        let version = self.state.clone_receiver(self.version);
        Self {
            state: self.state.clone(),
            version,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        self.state.drop_receiver();
    }
}

impl<T> fmt::Debug for Receiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("version", &self.version)
            .finish()
    }
}

/// Creates a new watch channel returning a sender and receiver pair.
pub fn channel<T>(init: T) -> (Sender<T>, Receiver<T>) {
    let state = Arc::new(Inner::new(init));
    let version = state.current_version();
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state, version },
    )
}

/// Creates a new borrowed watch channel and runs the provided asynchronous closure with it.
pub async fn with_borrowed_channel<T, F, R>(init: T, f: F) -> R
where
    F: for<'a> AsyncFnOnce(BorrowedSender<'a, T>, BorrowedReceiver<'a, T>) -> R,
{
    let state = Inner::new(init);
    let (tx, rx) = state.borrowed_parts();
    f(tx, rx).await
}
