use crate::{
    sync::{
        atomic::{NativeAtomicU32, Ordering},
        sys::native,
    },
    time::{Duration, Instant},
};

use lock_api::{
    GuardSend, RawRwLock as RawRwLockTrait, RawRwLockDowngrade, RawRwLockFair, RawRwLockTimed,
};

#[cfg(feature = "loom")]
use crate::sync::{atomic::LoomAtomicU32, sys::loom::WaitChannel};

const READER_MASK: u32 = 0x0000_FFFF;
const WRITER_WAITING_SHIFT: u32 = 16;
const WRITER_WAITING_MASK: u32 = 0x3FFF_0000;
const WRITER_WAITING_ONE: u32 = 1 << WRITER_WAITING_SHIFT;
const WRITER_WAITING_MAX: u32 = WRITER_WAITING_MASK >> WRITER_WAITING_SHIFT;
const WRITER_BIT: u32 = 0x8000_0000;

macro_rules! impl_raw_rwlock {
    (
        $(#[$meta:meta])*
        struct $name:ident,
        atomic: $atomic_ty:ty,
        $(channel: $channel_field:ident: $channel_ty:ty, init_channel: $init_channel:expr,)?
        wake_all: |$s_wake:ident| $wake_all_expr:expr,
        wait: |$s_wait:ident, $obs_wait:ident| $wait_expr:expr,
        wait_timeout: |$s_wt:ident, $obs_wt:ident, $rem_wt:ident| $wait_timeout_expr:expr,
        const_new: $is_const:ident
    ) => {
        $(#[$meta])*
        pub struct $name {
            state: $atomic_ty,
            $($channel_field: $channel_ty,)?
        }

        impl $name {
            impl_raw_rwlock!(@new_fn $is_const, $atomic_ty $(, $channel_field: $init_channel)?);

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
                    let $s_wake = self;
                    let _ = $wake_all_expr;
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

            pub fn lock_shared_until(&self, timeout: Option<Instant>) -> bool {
                let observed = self.state.load(Ordering::Relaxed);
                self.lock_shared_from_observed(observed, timeout)
            }

            pub fn lock_shared_from_observed(&self, mut observed: u32, timeout: Option<Instant>) -> bool {
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
                        let $s_wait = self;
                        let $obs_wait = observed;
                        let _ = $wait_expr;

                        observed = self.state.load(Ordering::Relaxed);
                        continue;
                    };
                    if now >= deadline {
                        return false;
                    }
                    let remaining = deadline.duration_since(now);

                    let $s_wt = self;
                    let $obs_wt = observed;
                    let $rem_wt = remaining;
                    let _ = $wait_timeout_expr;

                    observed = self.state.load(Ordering::Relaxed);
                }
            }

            pub fn lock_exclusive_until(&self, timeout: Option<Instant>) -> bool {
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
                        let $s_wait = self;
                        let $obs_wait = observed;
                        let _ = $wait_expr;

                        observed = self.state.load(Ordering::Relaxed);
                        continue;
                    };
                    let now = Instant::now();
                    if now >= deadline {
                        self.unregister_exclusive_waiter();
                        return false;
                    }
                    let remaining = deadline.duration_since(now);

                    let $s_wt = self;
                    let $obs_wt = observed;
                    let $rem_wt = remaining;
                    let _ = $wait_timeout_expr;

                    observed = self.state.load(Ordering::Relaxed);
                }
            }

            #[inline]
            pub fn lock_shared(&self) {
                let _ = self.lock_shared_until(None);
            }

            #[inline]
            pub fn try_lock_shared(&self) -> bool {
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

            /// Unlocks the shared reader lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold a shared reader lock.
            #[inline]
            pub unsafe fn unlock_shared(&self) {
                self.release_shared();
            }

            #[inline]
            pub fn lock_exclusive(&self) {
                let _ = self.lock_exclusive_until(None);
            }

            #[inline]
            pub fn try_lock_exclusive(&self) -> bool {
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

            /// Unlocks the exclusive writer lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold the exclusive writer lock.
            #[inline]
            pub unsafe fn unlock_exclusive(&self) {
                self.release_exclusive();
            }

            /// Downgrades the exclusive writer lock to a shared reader lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold the exclusive writer lock.
            #[inline]
            pub unsafe fn downgrade(&self) {
                self.downgrade_exclusive();
            }

            #[inline]
            pub fn is_locked(&self) -> bool {
                let state = self.state.load(Ordering::Relaxed);
                state & (WRITER_BIT | READER_MASK) != 0
            }

            #[inline]
            pub fn is_locked_exclusive(&self) -> bool {
                self.state.load(Ordering::Relaxed) & WRITER_BIT != 0
            }

            #[inline]
            pub fn try_lock_shared_for(&self, timeout: Duration) -> bool {
                self.try_lock_shared_until(Instant::now() + timeout)
            }

            #[inline]
            pub fn try_lock_shared_until(&self, timeout: Instant) -> bool {
                self.lock_shared_until(Some(timeout))
            }

            #[inline]
            pub fn try_lock_exclusive_for(&self, timeout: Duration) -> bool {
                self.try_lock_exclusive_until(Instant::now() + timeout)
            }

            #[inline]
            pub fn try_lock_exclusive_until(&self, timeout: Instant) -> bool {
                self.lock_exclusive_until(Some(timeout))
            }

            /// Fairly unlocks the shared reader lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold a shared reader lock.
            #[inline]
            pub unsafe fn unlock_shared_fair(&self) {
                self.release_shared();
            }

            /// Fairly unlocks the exclusive writer lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold the exclusive writer lock.
            #[inline]
            pub unsafe fn unlock_exclusive_fair(&self) {
                self.release_exclusive();
            }

            /// Atomically releases and re-acquires the shared reader lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold a shared reader lock.
            #[inline]
            pub unsafe fn bump_shared(&self) {
                self.release_shared();
                self.lock_shared();
            }

            /// Atomically releases and re-acquires the exclusive writer lock.
            ///
            /// # Safety
            ///
            /// The caller must currently hold the exclusive writer lock.
            #[inline]
            pub unsafe fn bump_exclusive(&self) {
                self.release_exclusive();
                self.lock_exclusive();
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }
    };

    (@new_fn const, $atomic_ty:ty) => {
        #[inline]
        pub const fn new() -> Self {
            Self {
                state: <$atomic_ty>::new(0),
            }
        }
    };

    (@new_fn non_const, $atomic_ty:ty, $channel_field:ident: $init_channel:expr) => {
        #[inline]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                state: <$atomic_ty>::new(0),
                $channel_field: $init_channel,
            }
        }
    };
}

impl_raw_rwlock!(
    #[derive(Debug)]
    struct NativeRawRwLock,
    atomic: NativeAtomicU32,
    wake_all: |s| native::wake_all_by_address(&s.state),
    wait: |s, obs| native::wait_on_address(&s.state, obs),
    wait_timeout: |s, obs, rem| {
        native::wait_on_address_timeout(&s.state, obs, Some(rem))
    },
    const_new: const
);

unsafe impl RawRwLockTrait for NativeRawRwLock {
    const INIT: Self = Self::new();

    type GuardMarker = GuardSend;

    #[inline]
    fn lock_shared(&self) {
        self.lock_shared();
    }

    #[inline]
    fn try_lock_shared(&self) -> bool {
        self.try_lock_shared()
    }

    #[inline]
    unsafe fn unlock_shared(&self) {
        unsafe { self.unlock_shared() };
    }

    #[inline]
    fn lock_exclusive(&self) {
        self.lock_exclusive();
    }

    #[inline]
    fn try_lock_exclusive(&self) -> bool {
        self.try_lock_exclusive()
    }

    #[inline]
    unsafe fn unlock_exclusive(&self) {
        unsafe { self.unlock_exclusive() };
    }

    #[inline]
    fn is_locked(&self) -> bool {
        self.is_locked()
    }

    #[inline]
    fn is_locked_exclusive(&self) -> bool {
        self.is_locked_exclusive()
    }
}

unsafe impl RawRwLockFair for NativeRawRwLock {
    #[inline]
    unsafe fn unlock_shared_fair(&self) {
        unsafe { self.unlock_shared_fair() };
    }

    #[inline]
    unsafe fn unlock_exclusive_fair(&self) {
        unsafe { self.unlock_exclusive_fair() };
    }

    #[inline]
    unsafe fn bump_shared(&self) {
        unsafe { self.bump_shared() };
    }

    #[inline]
    unsafe fn bump_exclusive(&self) {
        unsafe { self.bump_exclusive() };
    }
}

unsafe impl RawRwLockDowngrade for NativeRawRwLock {
    #[inline]
    unsafe fn downgrade(&self) {
        unsafe { self.downgrade() };
    }
}

unsafe impl RawRwLockTimed for NativeRawRwLock {
    type Duration = Duration;
    type Instant = Instant;

    #[inline]
    fn try_lock_shared_for(&self, timeout: Self::Duration) -> bool {
        self.try_lock_shared_for(timeout)
    }

    #[inline]
    fn try_lock_shared_until(&self, timeout: Self::Instant) -> bool {
        self.try_lock_shared_until(timeout)
    }

    #[inline]
    fn try_lock_exclusive_for(&self, timeout: Self::Duration) -> bool {
        self.try_lock_exclusive_for(timeout)
    }

    #[inline]
    fn try_lock_exclusive_until(&self, timeout: Self::Instant) -> bool {
        self.try_lock_exclusive_until(timeout)
    }
}

#[cfg(feature = "loom")]
impl_raw_rwlock!(
    struct LoomRawRwLock,
    atomic: LoomAtomicU32,
    channel: channel: WaitChannel, init_channel: WaitChannel::new(),
    wake_all: |s| s.channel.wake_all(),
    wait: |s, obs| s.channel.wait(&s.state, obs),
    wait_timeout: |s, obs, rem| s.channel.wait_timeout(&s.state, obs, rem),
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type RawRwLock = NativeRawRwLock;

#[cfg(feature = "loom")]
pub type RawRwLock = LoomRawRwLock;

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    #[cfg(not(feature = "loom"))]
    use std::{
        sync::{Arc, mpsc::channel},
        thread,
    };

    #[cfg(not(feature = "loom"))]
    #[test]
    fn failed_reader_cas_uses_actual_state() {
        let lock = RawRwLock::new();
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
        let lock = Arc::new(RawRwLock::new());
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
        let lock = Arc::new(RawRwLock::new());
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

    #[cfg(not(feature = "loom"))]
    #[test]
    fn timed_writer_unregisters_and_wakes_readers() {
        let lock = RawRwLock::new();
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
        let lock = Arc::new(RawRwLock::new());
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

    #[cfg(not(feature = "loom"))]
    #[test]
    #[should_panic(expected = "RwLock reader count overflow")]
    fn reader_count_overflow_is_rejected() {
        let lock = RawRwLock::new();
        lock.state.store(READER_MASK, Ordering::Relaxed);
        let _ = lock.try_lock_shared();
    }

    #[cfg(not(feature = "loom"))]
    #[test]
    #[should_panic(expected = "RwLock writer waiter count overflow")]
    fn writer_waiter_overflow_is_rejected() {
        let lock = RawRwLock::new();
        lock.state.store(WRITER_WAITING_MASK, Ordering::Relaxed);
        let _ = lock.register_exclusive_waiter();
    }

    #[cfg(feature = "loom")]
    #[test]
    fn loom_uses_production_raw_lock() {
        loom::model(|| {
            let lock = loom::sync::Arc::new(RawRwLock::new());
            let reader_lock = lock.clone();
            let reader = loom::thread::spawn(move || {
                reader_lock.lock_shared();
                unsafe { reader_lock.unlock_shared() };
            });
            let writer_lock = lock.clone();
            let writer = loom::thread::spawn(move || {
                writer_lock.lock_exclusive();
                unsafe { writer_lock.unlock_exclusive() };
            });
            reader.join().unwrap();
            writer.join().unwrap();
            assert!(!lock.is_locked());
        });
    }
}
