use crate::{
    sync::{
        atomic::{AtomicU32, Ordering},
        sys,
    },
    time::{Duration, Instant},
};
use core::ops::Deref;
use lock_api::{
    GuardSend, RawRwLock as RawRwLockTrait, RawRwLockDowngrade, RawRwLockFair, RawRwLockTimed,
};
#[cfg(feature = "loom")]
use std::sync::OnceLock;

const READER_MASK: u32 = 0x0000_FFFF;
const WRITER_WAITING_SHIFT: u32 = 16;
const WRITER_WAITING_MASK: u32 = 0x3FFF_0000;
const WRITER_WAITING_ONE: u32 = 1 << WRITER_WAITING_SHIFT;
const WRITER_WAITING_MAX: u32 = WRITER_WAITING_MASK >> WRITER_WAITING_SHIFT;
const WRITER_BIT: u32 = 0x8000_0000;

#[cfg(not(feature = "loom"))]
struct StateCell(AtomicU32);

#[cfg(feature = "loom")]
struct StateCell(OnceLock<AtomicU32>);

impl StateCell {
    const fn new() -> Self {
        #[cfg(not(feature = "loom"))]
        {
            Self(AtomicU32::new(0))
        }
        #[cfg(feature = "loom")]
        {
            Self(OnceLock::new())
        }
    }
}

impl Deref for StateCell {
    type Target = AtomicU32;

    fn deref(&self) -> &Self::Target {
        #[cfg(not(feature = "loom"))]
        {
            &self.0
        }
        #[cfg(feature = "loom")]
        {
            self.0.get_or_init(|| AtomicU32::new(0))
        }
    }
}

/// The state word is also the platform wait address.
///
/// The low bits contain readers, the middle bits contain registered writers, and the
/// high bit identifies the writer which owns the lock. A writer waiter is registered
/// before it sleeps and is consumed by the CAS which acquires the lock. No operation
/// relies on the value it observed before a failed CAS: every failure returns the new
/// value to the loop and only that value may be used as a wait expectation.
pub struct RawRwLock {
    state: StateCell,
}

impl RawRwLock {
    #[inline]
    fn reader_count(state: u32) -> u32 {
        state & READER_MASK
    }

    #[inline]
    fn writer_waiter_count(state: u32) -> u32 {
        (state & WRITER_WAITING_MASK) >> WRITER_WAITING_SHIFT
    }

    #[inline]
    fn can_acquire_shared(state: u32) -> bool {
        state & (WRITER_BIT | WRITER_WAITING_MASK) == 0
    }

    #[inline]
    fn can_acquire_exclusive(state: u32) -> bool {
        state & (WRITER_BIT | READER_MASK) == 0
    }

    #[inline]
    fn add_reader(state: u32) -> u32 {
        let readers = Self::reader_count(state);
        assert!(readers < READER_MASK, "RwLock reader count overflow");
        state.checked_add(1).expect("RwLock reader state overflow")
    }

    #[inline]
    fn register_exclusive_waiter(&self) -> u32 {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            let waiters = Self::writer_waiter_count(observed);
            assert!(
                waiters < WRITER_WAITING_MAX,
                "RwLock writer waiter count overflow"
            );
            let current = observed
                .checked_add(WRITER_WAITING_ONE)
                .expect("RwLock writer waiter state overflow");
            match self.state.compare_exchange_weak(
                observed,
                current,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return current,
                Err(actual) => observed = actual,
            }
        }
    }

    /// Remove one writer registration using the latest state.
    ///
    /// The caller owns exactly one registration. The CAS loop is required because a
    /// release or another waiter may change unrelated bits while the timeout expires.
    #[inline]
    fn unregister_exclusive_waiter(&self) {
        let mut observed = self.state.load(Ordering::Acquire);
        loop {
            let waiters = Self::writer_waiter_count(observed);
            assert!(waiters != 0, "RwLock writer waiter count underflow");
            let current = observed
                .checked_sub(WRITER_WAITING_ONE)
                .expect("RwLock writer waiter state underflow");
            match self.state.compare_exchange_weak(
                observed,
                current,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    // Removing the last writer waiter can make every sleeping reader
                    // eligible. Broadcasting is intentional because readers and writers
                    // share one wait address.
                    self.notify_after_transition(observed, current);
                    return;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    #[inline]
    fn try_acquire_shared(&self, observed: u32) -> Result<u32, u32> {
        if !Self::can_acquire_shared(observed) {
            return Err(observed);
        }
        let current = Self::add_reader(observed);
        self.state
            .compare_exchange_weak(observed, current, Ordering::Acquire, Ordering::Relaxed)
    }

    #[inline]
    fn try_acquire_exclusive(&self, observed: u32, registered: bool) -> Result<u32, u32> {
        if !Self::can_acquire_exclusive(observed) {
            return Err(observed);
        }
        if registered {
            assert!(
                Self::writer_waiter_count(observed) != 0,
                "registered writer has no waiter registration"
            );
        }
        let current = if registered {
            observed
                .checked_sub(WRITER_WAITING_ONE)
                .expect("RwLock writer waiter state underflow")
                | WRITER_BIT
        } else {
            observed | WRITER_BIT
        };
        self.state
            .compare_exchange_weak(observed, current, Ordering::Acquire, Ordering::Relaxed)
    }

    #[inline]
    fn notify_after_transition(&self, previous: u32, current: u32) {
        let released_writer = previous & WRITER_BIT != 0 && current & WRITER_BIT == 0;
        let released_last_reader = Self::reader_count(previous) != 0
            && Self::reader_count(current) == 0
            && Self::writer_waiter_count(current) != 0;
        let removed_writer_waiter =
            Self::writer_waiter_count(previous) > Self::writer_waiter_count(current);
        let downgraded = previous & WRITER_BIT != 0 && current & WRITER_BIT == 0;

        if released_writer || released_last_reader || removed_writer_waiter || downgraded {
            sys::wake_all_by_address(&self.state);
        }
    }

    #[inline]
    fn release_shared(&self) {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            let readers = Self::reader_count(observed);
            assert!(readers != 0, "RwLock reader count underflow");
            let current = observed - 1;
            match self.state.compare_exchange_weak(
                observed,
                current,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.notify_after_transition(observed, current);
                    return;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    #[inline]
    fn release_exclusive(&self) {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            assert!(observed & WRITER_BIT != 0, "RwLock writer bit is not held");
            let current = observed & !WRITER_BIT;
            match self.state.compare_exchange_weak(
                observed,
                current,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.notify_after_transition(observed, current);
                    return;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    #[inline]
    fn downgrade_exclusive(&self) {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            assert!(
                observed & WRITER_BIT != 0 && Self::reader_count(observed) == 0,
                "RwLock downgrade requires an exclusive guard"
            );
            let current = (observed & !WRITER_BIT) | 1;
            match self.state.compare_exchange_weak(
                observed,
                current,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.notify_after_transition(observed, current);
                    return;
                }
                Err(actual) => observed = actual,
            }
        }
    }

    fn lock_shared_until(&self, timeout: Option<Instant>) -> bool {
        let observed = self.state.load(Ordering::Relaxed);
        self.lock_shared_from_observed(observed, timeout)
    }

    fn lock_shared_from_observed(&self, mut observed: u32, timeout: Option<Instant>) -> bool {
        loop {
            match self.try_acquire_shared(observed) {
                Ok(_) => return true,
                Err(actual) => {
                    observed = actual;
                    if Self::can_acquire_shared(observed) {
                        continue;
                    }
                }
            }

            let now = Instant::now();
            let Some(deadline) = timeout else {
                sys::wait_on_address(&self.state, observed);
                observed = self.state.load(Ordering::Relaxed);
                continue;
            };
            if now >= deadline {
                return false;
            }
            let remaining = deadline.duration_since(now);
            sys::wait_on_address_timeout(&self.state, observed, Some(remaining));
            observed = self.state.load(Ordering::Relaxed);
        }
    }

    fn lock_exclusive_until(&self, timeout: Option<Instant>) -> bool {
        let mut observed = self.state.load(Ordering::Relaxed);
        let mut registered = false;
        loop {
            match self.try_acquire_exclusive(observed, registered) {
                Ok(current) => {
                    self.notify_after_transition(observed, current);
                    return true;
                }
                Err(actual) => observed = actual,
            }

            if !registered {
                if let Some(deadline) = timeout {
                    let now = Instant::now();
                    if now >= deadline {
                        return false;
                    }
                }
                observed = self.register_exclusive_waiter();
                registered = true;
                continue;
            }

            let Some(deadline) = timeout else {
                sys::wait_on_address(&self.state, observed);
                observed = self.state.load(Ordering::Relaxed);
                continue;
            };
            let now = Instant::now();
            if now >= deadline {
                self.unregister_exclusive_waiter();
                return false;
            }
            let remaining = deadline.duration_since(now);
            sys::wait_on_address_timeout(&self.state, observed, Some(remaining));
            observed = self.state.load(Ordering::Relaxed);
        }
    }
}

unsafe impl RawRwLockTrait for RawRwLock {
    const INIT: Self = Self {
        state: StateCell::new(),
    };

    type GuardMarker = GuardSend;

    #[inline]
    fn lock_shared(&self) {
        let _ = self.lock_shared_until(None);
    }

    #[inline]
    fn try_lock_shared(&self) -> bool {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            match self.try_acquire_shared(observed) {
                Ok(_) => return true,
                Err(actual) => {
                    if !Self::can_acquire_shared(actual) {
                        return false;
                    }
                    observed = actual;
                }
            }
        }
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        self.release_shared();
    }

    #[inline]
    fn lock_exclusive(&self) {
        let _ = self.lock_exclusive_until(None);
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        let mut observed = self.state.load(Ordering::Relaxed);
        loop {
            match self.try_acquire_exclusive(observed, false) {
                Ok(_) => return true,
                Err(actual) => {
                    if !Self::can_acquire_exclusive(actual) {
                        return false;
                    }
                    observed = actual;
                }
            }
        }
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        self.release_exclusive();
    }

    #[inline]
    fn is_locked(&self) -> bool {
        let state = self.state.load(Ordering::Relaxed);
        state & (WRITER_BIT | READER_MASK) != 0
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        self.state.load(Ordering::Relaxed) & WRITER_BIT != 0
    }
}

unsafe impl RawRwLockFair for RawRwLock {
    #[inline]
    unsafe fn unlock_shared_fair(&self) {
        self.release_shared();
    }

    #[inline]
    unsafe fn unlock_exclusive_fair(&self) {
        self.release_exclusive();
    }

    #[inline]
    unsafe fn bump_shared(&self) {
        self.release_shared();
        self.lock_shared();
    }

    #[inline]
    unsafe fn bump_exclusive(&self) {
        self.release_exclusive();
        self.lock_exclusive();
    }
}

unsafe impl RawRwLockDowngrade for RawRwLock {
    #[inline]
    unsafe fn downgrade(&self) {
        self.downgrade_exclusive();
    }
}

unsafe impl RawRwLockTimed for RawRwLock {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_shared_for(&self, timeout: Self::Duration) -> bool {
        self.try_lock_shared_until(Instant::now() + timeout)
    }

    #[inline]
    fn try_lock_shared_until(&self, timeout: Self::Instant) -> bool {
        self.lock_shared_until(Some(timeout))
    }

    #[inline]
    fn try_lock_exclusive_for(&self, timeout: Self::Duration) -> bool {
        self.try_lock_exclusive_until(Instant::now() + timeout)
    }

    #[inline]
    fn try_lock_exclusive_until(&self, timeout: Self::Instant) -> bool {
        self.lock_exclusive_until(Some(timeout))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(not(feature = "loom"))]
    use std::{
        sync::{Arc, mpsc::channel},
        thread,
    };

    #[test]
    fn failed_reader_cas_uses_actual_state() {
        let lock = RawRwLock::INIT;
        assert!(lock.try_lock_shared());
        unsafe { lock.unlock_shared() };

        let actual = lock.state.load(Ordering::Acquire);
        assert_eq!(actual, 0);
        assert!(lock.try_lock_shared());
        unsafe { lock.unlock_shared() };
    }

    /// Fixes the exact stale-expected interleaving from the diagnosis: the reader
    /// observes a writer, another reader briefly changes the state back to zero,
    /// and the reader's CAS then fails with `actual == 0`. It must retry the new
    /// value instead of waiting on the old writer state forever.
    #[cfg(not(feature = "loom"))]
    #[test]
    fn reader_progresses_after_stale_cas_observation() {
        let lock = Arc::new(RawRwLock::INIT);
        assert!(lock.try_lock_exclusive());
        let stale = lock.state.load(Ordering::Acquire);
        let control = Arc::clone(&lock);
        let (done_tx, done_rx) = channel();

        let reader = thread::spawn(move || {
            let acquired = control.lock_shared_from_observed(stale, None);
            done_tx.send(acquired).unwrap();
            if acquired {
                control.release_shared();
            }
        });

        unsafe { lock.unlock_exclusive() };
        assert!(lock.try_lock_shared());
        unsafe { lock.unlock_shared() };

        let completed = done_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("reader must report its result");
        if !completed {
            assert!(lock.try_lock_exclusive());
            unsafe { lock.unlock_exclusive() };
        }
        assert!(completed, "reader must not sleep on stale state");
        reader.join().unwrap();
    }

    /// The timed reader uses the same CAS loop and must not spend its whole
    /// deadline waiting after a competing transition made the lock available.
    #[cfg(not(feature = "loom"))]
    #[test]
    fn timed_reader_retries_after_stale_cas_observation() {
        let lock = Arc::new(RawRwLock::INIT);
        assert!(lock.try_lock_exclusive());
        let stale = lock.state.load(Ordering::Acquire);
        let control = Arc::clone(&lock);
        let (done_tx, done_rx) = channel();

        let reader = thread::spawn(move || {
            let acquired = control.lock_shared_from_observed(
                stale,
                Some(Instant::now() + Duration::from_millis(500)),
            );
            done_tx.send(acquired).unwrap();
            if acquired {
                control.release_shared();
            }
        });

        unsafe { lock.unlock_exclusive() };
        assert!(lock.try_lock_shared());
        unsafe { lock.unlock_shared() };

        let completed = done_rx
            .recv_timeout(Duration::from_millis(200))
            .expect("timed reader must report its result");
        if !completed {
            assert!(lock.try_lock_exclusive());
            unsafe { lock.unlock_exclusive() };
        }
        assert!(completed, "timed reader must retry immediately");
        reader.join().unwrap();
    }

    #[test]
    fn timed_writer_unregisters_and_wakes_readers() {
        let lock = RawRwLock::INIT;
        assert!(lock.try_lock_exclusive());
        let waiter_state = lock.register_exclusive_waiter();
        assert_eq!(RawRwLock::writer_waiter_count(waiter_state), 1);
        lock.unregister_exclusive_waiter();
        assert_eq!(lock.state.load(Ordering::Acquire), WRITER_BIT);
        unsafe { lock.unlock_exclusive() };
        assert!(lock.try_lock_shared());
        unsafe { lock.unlock_shared() };
    }

    /// A reader blocked by writer preference must be woken when the last timed
    /// writer unregisters, and again when the writer releases the lock. This
    /// catches the old missing-notification path without relying on a sleep.
    #[cfg(not(feature = "loom"))]
    #[test]
    fn timed_writer_unregistration_wakes_blocked_reader() {
        let lock = Arc::new(RawRwLock::INIT);
        assert!(lock.try_lock_exclusive());
        let registered_state = lock.register_exclusive_waiter();
        assert_eq!(RawRwLock::writer_waiter_count(registered_state), 1);

        let reader_lock = Arc::clone(&lock);
        let (started_tx, started_rx) = channel();
        let (done_tx, done_rx) = channel();
        let reader = thread::spawn(move || {
            started_tx.send(()).unwrap();
            let acquired =
                reader_lock.lock_shared_until(Some(Instant::now() + Duration::from_secs(1)));
            done_tx.send(acquired).unwrap();
            if acquired {
                reader_lock.release_shared();
            }
        });

        started_rx.recv().unwrap();
        lock.unregister_exclusive_waiter();
        assert!(done_rx.try_recv().is_err(), "writer still owns the lock");
        unsafe { lock.unlock_exclusive() };

        assert!(
            done_rx
                .recv_timeout(Duration::from_millis(200))
                .expect("reader must be woken after writer release")
        );
        reader.join().unwrap();
    }

    #[test]
    #[should_panic(expected = "RwLock reader count overflow")]
    fn reader_count_overflow_is_rejected() {
        let lock = RawRwLock::INIT;
        lock.state.store(READER_MASK, Ordering::Relaxed);
        let _ = lock.try_lock_shared();
    }

    #[test]
    #[should_panic(expected = "RwLock writer waiter count overflow")]
    fn writer_waiter_overflow_is_rejected() {
        let lock = RawRwLock::INIT;
        lock.state.store(WRITER_WAITING_MASK, Ordering::Relaxed);
        let _ = lock.register_exclusive_waiter();
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_uses_production_raw_lock() {
        loom::model(|| {
            let raw = RawRwLock::INIT;
            let _ = raw.state.load(Ordering::Relaxed);
            let lock = loom::sync::Arc::new(lock_api::RwLock::const_new(raw, 0usize));
            let reader_lock = lock.clone();
            let reader = loom::thread::spawn(move || {
                let guard = reader_lock.read();
                assert!(*guard <= 1);
            });
            let writer_lock = lock.clone();
            let writer = loom::thread::spawn(move || {
                let mut guard = writer_lock.write();
                *guard += 1;
            });
            reader.join().unwrap();
            writer.join().unwrap();
            assert_eq!(*lock.read(), 1);
        });
    }
}
