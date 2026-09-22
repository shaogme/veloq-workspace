use core::{cell::Ref as CoreRef, ops::Deref};

use veloq_std::{
    cell::{Cell, RefCell},
    error::Error,
    fmt,
    pin::pin,
    rc::Rc,
};

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

/// Error produced when receiving a value fails because the sender has been dropped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecvError;

impl fmt::Display for RecvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "receiving on a closed channel")
    }
}

impl Error for RecvError {}

/// A single-producer, multi-consumer local watch channel state.
pub struct State<T> {
    value: RefCell<T>,
    version: Cell<usize>,
    receiver_count: Cell<usize>,
    is_closed: Cell<bool>,
    rx_notify: Notify,
    tx_notify: Notify,
}

impl<T> State<T> {
    /// Creates a new channel state initialized with the given value.
    pub fn new(init: T) -> Self {
        Self {
            value: RefCell::new(init),
            version: Cell::new(1),
            receiver_count: Cell::new(0),
            is_closed: Cell::new(false),
            rx_notify: Notify::new(),
            tx_notify: Notify::new(),
        }
    }

    /// Splits the state into a borrowed sender and a borrowed receiver.
    pub fn split(&self) -> (BorrowedSender<'_, T>, BorrowedReceiver<'_, T>) {
        self.receiver_count.set(1);
        self.is_closed.set(false);
        let version = self.version.get();
        (
            BorrowedSender { state: self },
            BorrowedReceiver {
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

        *self.value.borrow_mut() = value;
        self.version.set(self.version.get().wrapping_add(1));
        self.rx_notify.notify_waiters();
        Ok(())
    }

    /// Modifies the value in place using the given closure and notifies receivers.
    pub fn send_modify<R>(&self, modify: impl FnOnce(&mut T) -> R) -> R {
        let res = modify(&mut *self.value.borrow_mut());
        if self.receiver_count() > 0 {
            self.version.set(self.version.get().wrapping_add(1));
            self.rx_notify.notify_waiters();
        }
        res
    }

    /// Modifies the value in place if the closure returns `true`, and notifies receivers.
    pub fn send_if_modified(&self, modify: impl FnOnce(&mut T) -> bool) -> bool {
        let modified = modify(&mut *self.value.borrow_mut());
        if modified && self.receiver_count() > 0 {
            self.version.set(self.version.get().wrapping_add(1));
            self.rx_notify.notify_waiters();
        }
        modified
    }

    /// Borrows the current value from the channel.
    pub fn borrow(&self) -> Ref<'_, T> {
        Ref {
            inner: self.value.borrow(),
        }
    }

    /// Borrows the current value and updates the seen version.
    pub fn borrow_and_update(&self, version: &mut usize) -> Ref<'_, T> {
        let guard = self.value.borrow();
        *version = self.version.get();
        Ref { inner: guard }
    }

    /// Returns the number of active receivers.
    pub fn receiver_count(&self) -> usize {
        self.receiver_count.get()
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
            let current_version = self.version.get();
            if current_version != *version {
                *version = current_version;
                return Ok(());
            }
            if self.is_closed.get() {
                return Err(RecvError);
            }

            let mut notified = pin!(self.rx_notify.notified());
            notified.as_mut().enable();

            let current_version = self.version.get();
            if current_version != *version {
                *version = current_version;
                return Ok(());
            }
            if self.is_closed.get() {
                return Err(RecvError);
            }

            notified.await;
        }
    }

    /// Checks if the channel has a new value since the last seen version.
    pub fn has_changed(&self, version: usize) -> Result<bool, RecvError> {
        let current_version = self.version.get();
        if current_version != version {
            Ok(true)
        } else if self.is_closed.get() {
            Err(RecvError)
        } else {
            Ok(false)
        }
    }
}

/// A borrowed reference to the value in a watch channel.
pub struct Ref<'a, T> {
    inner: CoreRef<'a, T>,
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
pub struct BorrowedSender<'a, T> {
    state: &'a State<T>,
}

impl<'a, T> BorrowedSender<'a, T> {
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
    pub fn subscribe(&self) -> BorrowedReceiver<'a, T> {
        self.state
            .receiver_count
            .set(self.state.receiver_count.get().wrapping_add(1));
        let version = self.state.version.get();
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
        self.state.is_closed.set(true);
        self.state.rx_notify.notify_waiters();
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
    state: &'a State<T>,
    version: usize,
}

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
        self.version = self.state.version.get();
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        veloq_std::ptr::eq(self.state, other.state)
    }
}

impl<'a, T> Clone for BorrowedReceiver<'a, T> {
    fn clone(&self) -> Self {
        self.state
            .receiver_count
            .set(self.state.receiver_count.get().wrapping_add(1));
        Self {
            state: self.state,
            version: self.version,
        }
    }
}

impl<T> Drop for BorrowedReceiver<'_, T> {
    fn drop(&mut self) {
        let prev = self.state.receiver_count.get();
        self.state.receiver_count.set(prev.saturating_sub(1));
        if prev == 1 {
            self.state.tx_notify.notify_waiters();
        }
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
    state: Rc<State<T>>,
}

impl<T> Sender<T> {
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
    pub fn subscribe(&self) -> Receiver<T> {
        self.state
            .receiver_count
            .set(self.state.receiver_count.get().wrapping_add(1));
        let version = self.state.version.get();
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
        self.state.is_closed.set(true);
        self.state.rx_notify.notify_waiters();
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
    state: Rc<State<T>>,
    version: usize,
}

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
        self.version = self.state.version.get();
    }

    /// Returns `true` if both receivers belong to the same channel.
    pub fn same_channel(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.state, &other.state)
    }
}

impl<T> Clone for Receiver<T> {
    fn clone(&self) -> Self {
        self.state
            .receiver_count
            .set(self.state.receiver_count.get().wrapping_add(1));
        Self {
            state: self.state.clone(),
            version: self.version,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let prev = self.state.receiver_count.get();
        self.state.receiver_count.set(prev.saturating_sub(1));
        if prev == 1 {
            self.state.tx_notify.notify_waiters();
        }
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
    let state = Rc::new(State::new(init));
    state.receiver_count.set(1);
    let version = state.version.get();
    (
        Sender {
            state: state.clone(),
        },
        Receiver { state, version },
    )
}

/// Creates a new borrowed watch channel state.
pub fn borrowed_channel<T>(init: T) -> State<T> {
    State::new(init)
}
