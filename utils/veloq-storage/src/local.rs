use std::cell::{Cell, RefCell};
use std::task::Waker;

use crate::{LocalOnlyStorage, StateLock, StateWakerQueue, Storage, StrategyType, sealed};
use crate::{NonNullPtr, OptionArc, OptionBox, OptionPtr};

pub struct LocalStorage(std::marker::PhantomData<std::rc::Rc<()>>);
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
    type OptionBox<T: ?Sized + Send> = OptionBox<T>;
    type OptionArc<T: ?Sized + Send + Sync> = OptionArc<T>;
}

pub struct LocalLock<T>(RefCell<T>);
impl<T> StateLock<T> for LocalLock<T> {
    type Guard<'a>
        = std::cell::RefMut<'a, T>
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
        std::mem::take(&mut *self.0.borrow_mut())
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
