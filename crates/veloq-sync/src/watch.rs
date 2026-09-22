use core::ops::Deref;

use veloq_std::{
    fmt,
    pin::pin,
    sync::{
        Arc, UnpoisonedRwLock, UnpoisonedRwLockReadGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

use crate::{RecvError, SendError, notify::Notify};

/// A single-producer, multi-consumer watch channel state.
pub struct State<T> {
    value: UnpoisonedRwLock<T>,
    version: AtomicUsize,
    receiver_count: AtomicUsize,
    is_closed: AtomicBool,
    rx_notify: Notify,
    tx_notify: Notify,
}

unsafe impl<T: Send + Sync> Send for State<T> {}
unsafe impl<T: Send + Sync> Sync for State<T> {}

impl<T> State<T> {
    /// Creates a new channel state initialized with the given value.
    pub fn new(init: T) -> Self {
        Self {
            value: UnpoisonedRwLock::new(init),
            version: AtomicUsize::new(1),
            receiver_count: AtomicUsize::new(0),
            is_closed: AtomicBool::new(false),
            rx_notify: Notify::new(),
            tx_notify: Notify::new(),
        }
    }

    /// Splits the state into a borrowed sender and a borrowed receiver.
    pub fn split(&self) -> (Sender<'_, T>, Receiver<'_, T>) {
        self.receiver_count.store(1, Ordering::Release);
        self.is_closed.store(false, Ordering::Release);
        let version = self.version.load(Ordering::Acquire);
        (
            Sender { state: self },
            Receiver {
                state: self,
                version,
            },
        )
    }

    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        if self.receiver_count() == 0 {
            return Err(SendError(value));
        }

        {
            let mut guard = self.value.write();
            *guard = value;
        }
        self.version.fetch_add(1, Ordering::Release);
        self.rx_notify.notify_waiters();
        Ok(())
    }

    /// Modifies the value in place using the given closure and notifies receivers.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        let res = {
            let mut guard = self.value.write();
            modify(&mut *guard)
        };
        if self.receiver_count() > 0 {
            self.version.fetch_add(1, Ordering::Release);
            self.rx_notify.notify_waiters();
        }
        res
    }

    /// Modifies the value in place if the closure returns `true`, and notifies receivers.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        let modified = {
            let mut guard = self.value.write();
            modify(&mut *guard)
        };
        if modified && self.receiver_count() > 0 {
            self.version.fetch_add(1, Ordering::Release);
            self.rx_notify.notify_waiters();
        }
        modified
    }

    /// Borrows the current value from the channel.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            inner: self.value.read(),
        }
    }

    /// Borrows the current value and updates the seen version.
    pub fn borrow_and_update(&self, version: &mut usize) -> Ref<'_, T> {
        let guard = self.value.read();
        *version = self.version.load(Ordering::Acquire);
        Ref { inner: guard }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.receiver_count.load(Ordering::Acquire)
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
            let current_version = self.version.load(Ordering::Acquire);
            if current_version != *version {
                *version = current_version;
                return Ok(());
            }
            if self.is_closed.load(Ordering::Acquire) {
                return Err(RecvError);
            }

            let mut notified = pin!(self.rx_notify.notified());
            notified.as_mut().enable();

            let current_version = self.version.load(Ordering::Acquire);
            if current_version != *version {
                *version = current_version;
                return Ok(());
            }
            if self.is_closed.load(Ordering::Acquire) {
                return Err(RecvError);
            }

            notified.await;
        }
    }

    /// Checks if the channel has a new value since the last seen version.
    pub fn has_changed(&self, version: usize) -> Result<bool, RecvError> {
        let current_version = self.version.load(Ordering::Acquire);
        if current_version != version {
            Ok(true)
        } else if self.is_closed.load(Ordering::Acquire) {
            Err(RecvError)
        } else {
            Ok(false)
        }
    }
}

/// A borrowed reference to the value in a watch channel.
pub struct Ref<'a, T> {
    inner: UnpoisonedRwLockReadGuard<'a, T>,
}

impl<T> Deref for Ref<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
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
pub struct Sender<'a, T> {
    state: &'a State<T>,
}

unsafe impl<T: Send + Sync> Send for Sender<'_, T> {}
unsafe impl<T: Send + Sync> Sync for Sender<'_, T> {}

impl<'a, T> Sender<'a, T> {
    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.state.send(value)
    }

    /// Modifies the value in place using the given closure and notifies receivers.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        self.state.send_modify(modify)
    }

    /// Modifies the value in place if the closure returns `true`, and notifies receivers.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        self.state.send_if_modified(modify)
    }

    /// Borrows the current value from the channel.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Creates a new receiver subscribed to this channel.
    pub fn subscribe(&self) -> Receiver<'a, T> {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let version = self.state.version.load(Ordering::Acquire);
        Receiver {
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

impl<T> Drop for Sender<'_, T> {
    fn drop(&mut self) {
        self.state.is_closed.store(true, Ordering::Release);
        self.state.rx_notify.notify_waiters();
    }
}

impl<T> fmt::Debug for Sender<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Sender")
            .field("receiver_count", &self.receiver_count())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// The receiver half of a borrowed watch channel.
pub struct Receiver<'a, T> {
    state: &'a State<T>,
    version: usize,
}

unsafe impl<T: Send + Sync> Send for Receiver<'_, T> {}
unsafe impl<T: Send + Sync> Sync for Receiver<'_, T> {}

impl<'a, T> Receiver<'a, T> {
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
        self.version = self.state.version.load(Ordering::Acquire);
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        veloq_std::ptr::eq(self.state, other.state)
    }
}

impl<'a, T> Clone for Receiver<'a, T> {
    fn clone(&self) -> Self {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            state: self.state,
            version: self.version,
        }
    }
}

impl<T> Drop for Receiver<'_, T> {
    fn drop(&mut self) {
        let prev = self.state.receiver_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.state.tx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for Receiver<'_, T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver")
            .field("version", &self.version)
            .finish()
    }
}

/// An owned sender for a watch channel.
pub struct OwnedSender<T> {
    state: Arc<State<T>>,
}

unsafe impl<T: Send + Sync> Send for OwnedSender<T> {}
unsafe impl<T: Send + Sync> Sync for OwnedSender<T> {}

impl<T> OwnedSender<T> {
    /// Sends a new value over the channel.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.state.send(value)
    }

    /// Modifies the value in place and notifies receivers.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        self.state.send_modify(modify)
    }

    /// Modifies the value in place if the condition is met.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        self.state.send_if_modified(modify)
    }

    /// Borrows the current value.
    pub fn borrow(&self) -> Ref<'_, T> {
        self.state.borrow()
    }

    /// Creates a new subscribed receiver.
    pub fn subscribe(&self) -> OwnedReceiver<T> {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        let version = self.state.version.load(Ordering::Acquire);
        OwnedReceiver {
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

impl<T> Drop for OwnedSender<T> {
    fn drop(&mut self) {
        self.state.is_closed.store(true, Ordering::Release);
        self.state.rx_notify.notify_waiters();
    }
}

impl<T> fmt::Debug for OwnedSender<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedSender")
            .field("receiver_count", &self.receiver_count())
            .field("is_closed", &self.is_closed())
            .finish()
    }
}

/// An owned receiver for a watch channel.
pub struct OwnedReceiver<T> {
    state: Arc<State<T>>,
    version: usize,
}

unsafe impl<T: Send + Sync> Send for OwnedReceiver<T> {}
unsafe impl<T: Send + Sync> Sync for OwnedReceiver<T> {}

impl<T> OwnedReceiver<T> {
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
        self.version = self.state.version.load(Ordering::Acquire);
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for OwnedReceiver<T> {
    fn clone(&self) -> Self {
        self.state.receiver_count.fetch_add(1, Ordering::AcqRel);
        Self {
            state: self.state.clone(),
            version: self.version,
        }
    }
}

impl<T> Drop for OwnedReceiver<T> {
    fn drop(&mut self) {
        let prev = self.state.receiver_count.fetch_sub(1, Ordering::AcqRel);
        if prev == 1 {
            self.state.tx_notify.notify_waiters();
        }
    }
}

impl<T> fmt::Debug for OwnedReceiver<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedReceiver")
            .field("version", &self.version)
            .finish()
    }
}

/// Creates a new watch channel returning an owned sender and receiver pair.
pub fn channel<T>(init: T) -> (OwnedSender<T>, OwnedReceiver<T>) {
    let state = Arc::new(State::new(init));
    state.receiver_count.store(1, Ordering::Release);
    let version = state.version.load(Ordering::Acquire);
    (
        OwnedSender {
            state: state.clone(),
        },
        OwnedReceiver { state, version },
    )
}

/// Creates a new owned watch channel returning an owned sender and receiver pair.
pub fn owned_channel<T>(init: T) -> (OwnedSender<T>, OwnedReceiver<T>) {
    channel(init)
}
