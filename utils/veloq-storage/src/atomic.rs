use std::sync::atomic::AtomicUsize;
use std::task::Waker;

use crate::{AtomicNonNullPtr, AtomicOptionArc, AtomicOptionBox, AtomicOptionPtr};
use crate::{StateLock, StateWakerQueue, Storage, StrategyType, ThreadSafeStorage, sealed};

pub struct AtomicStorage;
impl sealed::Sealed for AtomicStorage {}
impl ThreadSafeStorage for AtomicStorage {}

impl Storage for AtomicStorage {
    fn strategy_type() -> StrategyType {
        StrategyType::Atomic
    }
    type Usize = AtomicUsize;
    type OptionPtr<T> = AtomicOptionPtr<T>;
    type NonNullPtr<T> = AtomicNonNullPtr<T>;
    type Lock<T> = AtomicLock<T>;
    type WakerQueue = AtomicWakerQueue;
    type OptionBox<T: ?Sized + Send> = AtomicOptionBox<T>;
    type OptionArc<T: ?Sized + Send + Sync> = AtomicOptionArc<T>;
}

pub struct AtomicLock<T>(parking_lot::Mutex<T>);
impl<T> StateLock<T> for AtomicLock<T> {
    type Guard<'a>
        = parking_lot::MutexGuard<'a, T>
    where
        T: 'a;
    fn new(val: T) -> Self {
        Self(parking_lot::Mutex::new(val))
    }
    fn lock(&self) -> Self::Guard<'_> {
        self.0.lock()
    }
}
unsafe impl<T> Send for AtomicLock<T> {}
unsafe impl<T> Sync for AtomicLock<T> {}

pub struct AtomicWakerQueue(parking_lot::Mutex<Vec<Waker>>);
impl StateWakerQueue for AtomicWakerQueue {
    fn new() -> Self {
        Self(parking_lot::Mutex::new(Vec::new()))
    }

    fn register(&self, waker: &Waker) {
        let mut wakers = self.0.lock();
        if !wakers.iter().any(|registered| registered.will_wake(waker)) {
            wakers.push(waker.clone());
        }
    }

    fn take_all(&self) -> Vec<Waker> {
        std::mem::take(&mut *self.0.lock())
    }
}
unsafe impl Send for AtomicWakerQueue {}
unsafe impl Sync for AtomicWakerQueue {}

impl_state_int!(
    AtomicUsize, self, order, val, curr, new, success, failure,
    new(v) { Self::new(v) },
    load() { self.load(order) },
    store(v) { self.store(v, order) },
    fetch_add(v) { self.fetch_add(v, order) },
    fetch_sub(v) { self.fetch_sub(v, order) },
    fetch_and(v) { self.fetch_and(v, order) },
    fetch_or(v) { self.fetch_or(v, order) },
    compare_exchange(c, n, s, f) { self.compare_exchange(c, n, s, f) },
    compare_exchange_weak(c, n, s, f) { self.compare_exchange_weak(c, n, s, f) }
);
