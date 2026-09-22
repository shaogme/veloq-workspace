use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use veloq_std::{
    cell::{Cell, RefCell, UnsafeCell},
    fmt,
    future::Future,
    marker::PhantomPinned,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll, Waker},
};

use crate::common::update_waker;

struct WaiterNode {
    waker: RefCell<Option<Waker>>,
    link: Link,
    _p: PhantomPinned,
}

impl WaiterNode {
    fn new() -> Self {
        Self {
            waker: RefCell::new(None),
            link: Link::new(),
            _p: PhantomPinned,
        }
    }
}

intrusive_adapter!(WaiterAdapter = WaiterNode { link: Link });

impl WaiterAdapter {
    const NEW: Self = Self;
}

/// Error returned when setting a value on an already initialized `SetOnce`.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct SetOnceError<T>(pub T);

impl<T> SetOnceError<T> {
    /// Consumes the error, returning the value that failed to be set.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T> fmt::Debug for SetOnceError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SetOnceError").finish_non_exhaustive()
    }
}

impl<T> fmt::Display for SetOnceError<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SetOnce already initialized")
    }
}

impl<T> veloq_std::error::Error for SetOnceError<T> {}

/// A single-threaded asynchronous cell that can be written to at most once
/// and awaited asynchronously by multiple readers.
pub struct SetOnce<T> {
    initialized: Cell<bool>,
    waiters: RefCell<LinkedList<WaiterAdapter>>,
    value: UnsafeCell<Option<T>>,
}

impl<T> SetOnce<T> {
    /// Creates a new uninitialized `SetOnce`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            initialized: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            value: UnsafeCell::new(None),
        }
    }

    /// Creates a new uninitialized `SetOnce`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            initialized: Cell::new(false),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            value: UnsafeCell::new(None),
        }
    }

    /// Creates a new `SetOnce` with an optional initial value.
    pub fn new_with(value: Option<T>) -> Self {
        match value {
            Some(v) => Self::with_value(v),
            None => Self::new(),
        }
    }

    /// Creates a new `SetOnce` initialized with the given value.
    #[cfg(not(feature = "loom"))]
    pub const fn with_value(value: T) -> Self {
        Self {
            initialized: Cell::new(true),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            value: UnsafeCell::new(Some(value)),
        }
    }

    /// Creates a new `SetOnce` initialized with the given value.
    #[cfg(feature = "loom")]
    pub fn with_value(value: T) -> Self {
        Self {
            initialized: Cell::new(true),
            waiters: RefCell::new(LinkedList::new(WaiterAdapter::NEW)),
            value: UnsafeCell::new(Some(value)),
        }
    }

    /// Returns `true` if the `SetOnce` contains a value.
    #[inline]
    pub fn initialized(&self) -> bool {
        self.initialized.get()
    }

    /// Alias for [`initialized`].
    #[inline]
    pub fn is_initialized(&self) -> bool {
        self.initialized()
    }

    /// Sets the value of the `SetOnce`.
    ///
    /// Returns `Ok(())` if the value was successfully set.
    /// Returns `Err(SetOnceError(value))` if the `SetOnce` was already initialized.
    pub fn set(&self, value: T) -> Result<(), SetOnceError<T>> {
        if self.initialized.get() {
            return Err(SetOnceError(value));
        }

        unsafe {
            *self.value.with_mut(|p| p as *mut Option<T>) = Some(value);
        }
        self.initialized.set(true);

        let mut waiters = self.waiters.borrow_mut();
        while let Some(node) = waiters.pop_front() {
            if let Some(waker) = node.waker.borrow_mut().take() {
                waker.wake();
            }
        }

        Ok(())
    }

    /// Gets a reference to the contained value, or `None` if not yet initialized.
    #[inline]
    pub fn get(&self) -> Option<&T> {
        if self.initialized.get() {
            unsafe { (*self.value.with(|p| p as *const Option<T>)).as_ref() }
        } else {
            None
        }
    }

    /// Gets a mutable reference to the contained value, or `None` if not yet initialized.
    #[inline]
    pub fn get_mut(&mut self) -> Option<&mut T> {
        if self.initialized.get() {
            unsafe { (*self.value.with_mut(|p| p as *mut Option<T>)).as_mut() }
        } else {
            None
        }
    }

    /// Consumes the `SetOnce`, returning the contained value, or `None` if not yet initialized.
    pub fn into_inner(self) -> Option<T> {
        self.value.into_inner()
    }

    /// Takes the contained value, leaving the `SetOnce` in an uninitialized state.
    pub fn take(&mut self) -> Option<T> {
        if self.initialized.get() {
            self.initialized.set(false);
            unsafe { (*self.value.with_mut(|p| p as *mut Option<T>)).take() }
        } else {
            None
        }
    }

    /// Returns a future that resolves to a reference to the contained value once set.
    pub fn wait(&self) -> Wait<'_, T> {
        Wait {
            set_once: self,
            node: WaiterNode::new(),
            queued: false,
            _pin: PhantomPinned,
        }
    }
}

/// A future that waits for a `SetOnce` to be initialized.
pub struct Wait<'a, T> {
    set_once: &'a SetOnce<T>,
    node: WaiterNode,
    queued: bool,
    _pin: PhantomPinned,
}

pub type SetOnceWait<'a, T> = Wait<'a, T>;

impl<'a, T> Future for Wait<'a, T> {
    type Output = &'a T;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.set_once.initialized.get() {
            let val = unsafe {
                (*this.set_once.value.with(|p| p as *const Option<T>))
                    .as_ref()
                    .unwrap_unchecked()
            };
            return Poll::Ready(val);
        }

        if this.queued {
            update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
            return Poll::Pending;
        }

        update_waker(&mut this.node.waker.borrow_mut(), cx.waker());
        unsafe {
            let node_pin = Pin::new_unchecked(&mut this.node);
            this.set_once.waiters.borrow_mut().push_back(node_pin);
        }
        this.queued = true;
        Poll::Pending
    }
}

impl<T> Drop for Wait<'_, T> {
    fn drop(&mut self) {
        if self.queued && self.node.link.is_linked() {
            unsafe {
                let ptr = NonNull::from(&self.node);
                let mut waiters = self.set_once.waiters.borrow_mut();
                let mut cursor = waiters.cursor_mut_from_ptr(ptr);
                cursor.remove();
            }
            self.queued = false;
        }
    }
}

impl<T> Default for SetOnce<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: fmt::Debug> fmt::Debug for SetOnce<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("SetOnce");
        match self.get() {
            Some(val) => d.field("value", val),
            None => d.field("value", &format_args!("<uninitialized>")),
        };
        d.finish()
    }
}

impl<T: Clone> Clone for SetOnce<T> {
    fn clone(&self) -> Self {
        let cell = Self::new();
        if let Some(val) = self.get() {
            let _ = cell.set(val.clone());
        }
        cell
    }
}

impl<T: PartialEq> PartialEq for SetOnce<T> {
    fn eq(&self, other: &Self) -> bool {
        self.get() == other.get()
    }
}

impl<T: Eq> Eq for SetOnce<T> {}

impl<T> From<T> for SetOnce<T> {
    fn from(value: T) -> Self {
        Self::with_value(value)
    }
}
