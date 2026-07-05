use veloq_std::{
    boxed::Box,
    cell::{Cell, RefCell, RefMut},
    marker::PhantomData,
    mem::take,
    ptr::{NonNull, null_mut},
    rc::Rc,
    sync::{Arc, atomic::Ordering},
    task::Waker,
    vec::Vec,
};

use crate::{
    LocalOnlyStorage, StateLock, StateOptionArc, StateOptionBox, StateWakerQueue, Storage,
    StrategyType, sealed,
};

pub struct LocalStorage(PhantomData<Rc<()>>);
impl sealed::Sealed for LocalStorage {}
impl LocalOnlyStorage for LocalStorage {}

impl Storage for LocalStorage {
    fn strategy_type() -> StrategyType {
        StrategyType::Local
    }
    type Usize = Usize;
    type OptionPtr<T> = OptionPtr<T>;
    type NonNullPtr<T> = NonNullPtr<T>;
    type Lock<T> = LocalLock<T>;
    type WakerQueue = LocalWakerQueue;
    type OptionBox<T: Send> = OptionBox<T>;
    type OptionFatBox<T: ?Sized + Send> = OptionBox<T>;
    type OptionArc<T: Send + Sync> = OptionArc<T>;
    type OptionFatArc<T: ?Sized + Send + Sync> = OptionArc<T>;
}

pub struct LocalLock<T>(RefCell<T>);
impl<T> StateLock<T> for LocalLock<T> {
    type Guard<'a>
        = RefMut<'a, T>
    where
        T: 'a;
    fn new(val: T) -> Self {
        Self(RefCell::new(val))
    }
    fn lock(&self) -> Self::Guard<'_> {
        self.0.borrow_mut()
    }
}

pub struct LocalWakerQueue(RefCell<Vec<Waker>>);
impl StateWakerQueue for LocalWakerQueue {
    fn new() -> Self {
        Self(RefCell::new(Vec::new()))
    }

    fn register(&self, waker: &Waker) {
        let mut wakers = self.0.borrow_mut();
        if !wakers.iter().any(|registered| registered.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn take_all(&self) -> Vec<Waker> {
        take(&mut *self.0.borrow_mut())
    }
}

pub struct Usize(Cell<usize>);

impl_state_int!(
    Usize, self, _order, val, curr, new, success, failure,
    new(v) { Self(Cell::new(v)) },
    load() { self.0.get() },
    store(v) { self.0.set(v) },
    fetch_add(v) {
        let old = self.0.get();
        self.0.set(old + v);
        old
    },
    fetch_sub(v) {
        let old = self.0.get();
        self.0.set(old - v);
        old
    },
    fetch_and(v) {
        let old = self.0.get();
        self.0.set(old & v);
        old
    },
    fetch_or(v) {
        let old = self.0.get();
        self.0.set(old | v);
        old
    },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == c {
            self.0.set(n);
            Ok(old)
        } else {
            Err(old)
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) }
);

// ==================== Pointer Helpers & Local Pointer Wrappers ====================

// OptionPtr helpers
fn opt_to_raw<T>(ptr: Option<NonNull<T>>) -> *mut T {
    ptr.map(|p| p.as_ptr()).unwrap_or(null_mut())
}
fn opt_from_raw<T>(ptr: *mut T) -> Option<NonNull<T>> {
    NonNull::new(ptr)
}

impl_ptr_state_wrapper!(
    OptionPtr, StateOptionPtr, Option<NonNull<T>>, Cell<*mut T>, self, _order,
    new(p) { Self(Cell::new(opt_to_raw(p))) },
    load() { opt_from_raw(self.0.get()) },
    store(p) { self.0.set(opt_to_raw(p)) },
    swap(p) {
        let old = self.0.get();
        self.0.set(opt_to_raw(p));
        opt_from_raw(old)
    },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == opt_to_raw(c) {
            self.0.set(opt_to_raw(n));
            Ok(opt_from_raw(old))
        } else {
            Err(opt_from_raw(old))
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) },
);

impl_ptr_state_wrapper!(
    NonNullPtr, StateNonNullPtr, NonNull<T>, Cell<NonNull<T>>, self, _order,
    new(p) { Self(Cell::new(p)) },
    load() { self.0.get() },
    store(p) { self.0.set(p) },
    swap(p) { self.0.replace(p) },
    compare_exchange(c, n, _s, _f) {
        let old = self.0.get();
        if old == c {
            self.0.set(n);
            Ok(old)
        } else {
            Err(old)
        }
    },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange(c, n, s, f) },
);

// ==================== Option Box & Arc ====================

pub struct OptionBox<T: ?Sized>(Cell<Option<Box<T>>>);

impl<T: ?Sized + Send> StateOptionBox<T> for OptionBox<T> {
    impl_cell_opt_methods!(Box<T>);
    fn swap(&self, new: Option<Box<T>>, _order: Ordering) -> Option<Box<T>> {
        self.0.replace(new)
    }
}

pub struct OptionArc<T: ?Sized>(Cell<Option<Arc<T>>>);

impl<T: ?Sized + Send + Sync> StateOptionArc<T> for OptionArc<T> {
    impl_cell_opt_methods!(Arc<T>);
    fn load_clone(&self, _order: Ordering) -> Option<Arc<T>> {
        let opt = self.0.take();
        let cloned = opt.clone();
        self.0.set(opt);
        cloned
    }
}
