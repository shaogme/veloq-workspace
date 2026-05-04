use std::ops::Deref;
use std::rc::{Rc, Weak as RcWeak};
use std::sync::{Arc, Weak as ArcWeak};

pub trait Ownership {
    type Shared<T: ?Sized>: Clone + Deref<Target = T>;
    type Weak<T: ?Sized>: Clone;

    fn new<T>(value: T) -> Self::Shared<T>;
    fn downgrade<T: ?Sized>(shared: &Self::Shared<T>) -> Self::Weak<T>;
    fn upgrade<T: ?Sized>(weak: &Self::Weak<T>) -> Option<Self::Shared<T>>;
    fn strong_count<T: ?Sized>(weak: &Self::Weak<T>) -> usize;
    fn as_ptr<T: ?Sized>(shared: &Self::Shared<T>) -> *const T;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ArcOwnership;

impl Ownership for ArcOwnership {
    type Shared<T: ?Sized> = Arc<T>;
    type Weak<T: ?Sized> = ArcWeak<T>;

    #[inline]
    fn new<T>(value: T) -> Self::Shared<T> {
        Arc::new(value)
    }

    #[inline]
    fn downgrade<T: ?Sized>(shared: &Self::Shared<T>) -> Self::Weak<T> {
        Arc::downgrade(shared)
    }

    #[inline]
    fn upgrade<T: ?Sized>(weak: &Self::Weak<T>) -> Option<Self::Shared<T>> {
        weak.upgrade()
    }

    #[inline]
    fn strong_count<T: ?Sized>(weak: &Self::Weak<T>) -> usize {
        ArcWeak::strong_count(weak)
    }

    #[inline]
    fn as_ptr<T: ?Sized>(shared: &Self::Shared<T>) -> *const T {
        Arc::as_ptr(shared)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RcOwnership;

impl Ownership for RcOwnership {
    type Shared<T: ?Sized> = Rc<T>;
    type Weak<T: ?Sized> = RcWeak<T>;

    #[inline]
    fn new<T>(value: T) -> Self::Shared<T> {
        Rc::new(value)
    }

    #[inline]
    fn downgrade<T: ?Sized>(shared: &Self::Shared<T>) -> Self::Weak<T> {
        Rc::downgrade(shared)
    }

    #[inline]
    fn upgrade<T: ?Sized>(weak: &Self::Weak<T>) -> Option<Self::Shared<T>> {
        weak.upgrade()
    }

    #[inline]
    fn strong_count<T: ?Sized>(weak: &Self::Weak<T>) -> usize {
        RcWeak::strong_count(weak)
    }

    #[inline]
    fn as_ptr<T: ?Sized>(shared: &Self::Shared<T>) -> *const T {
        Rc::as_ptr(shared)
    }
}
