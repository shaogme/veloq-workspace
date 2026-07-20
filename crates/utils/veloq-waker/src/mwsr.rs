use veloq_std::{
    cell::UnsafeCell,
    fmt,
    sync::atomic::{
        AtomicUsize,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::Waker,
};

/// Idle state.
const WAITING: usize = 0;

/// A new waker value is being registered with the `MwsrWaker` cell.
const REGISTERING: usize = 0b01;

/// The waker currently registered with the `MwsrWaker` cell is being woken.
const WAKING: usize = 0b10;

/// A specialized synchronization primitive for task wakeup, optimized for
/// Single-Register (单注册者) and Multi-Wake (多唤醒者) scenarios.
///
/// Unlike `AtomicWaker`, `MwsrWaker` requires that at most one thread/task
/// calls `register` concurrently. This allows for simpler state transitions
/// and better performance. Because of this, `MwsrWaker::register` is marked
/// as `unsafe`.
pub struct MwsrWaker {
    state: AtomicUsize,
    waker: UnsafeCell<Option<Waker>>,
}

impl MwsrWaker {
    /// Create an `MwsrWaker`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        // Make sure that task is Sync
        #[allow(dead_code)]
        trait AssertSync: Sync {}
        impl AssertSync for Waker {}

        MwsrWaker {
            state: AtomicUsize::new(WAITING),
            waker: UnsafeCell::new(None),
        }
    }

    /// Create an `MwsrWaker`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        // Make sure that task is Sync
        #[allow(dead_code)]
        trait AssertSync: Sync {}
        impl AssertSync for Waker {}

        MwsrWaker {
            state: AtomicUsize::new(WAITING),
            waker: UnsafeCell::new(None),
        }
    }

    /// Registers the waker to be notified on calls to `wake`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there are **no concurrent calls** to `register`.
    /// Calling this function concurrently from multiple threads/tasks is undefined behavior.
    /// However, it is fully safe to call `register` concurrently with `wake`.
    pub unsafe fn register(&self, waker: &Waker) {
        if self.state.load(Relaxed) == WAKING {
            waker.wake_by_ref();
            return;
        }

        match self
            .state
            .compare_exchange(WAITING, REGISTERING, Acquire, Acquire)
        {
            Ok(_) => {
                // Lock acquired. Update the waker cell.
                // SAFETY: We acquired the REGISTERING lock. By the safety contract of this function,
                // there is no concurrent call to `register`, ensuring exclusive access to the cell.
                unsafe {
                    self.waker.with_mut(|w| match w {
                        Some(old_waker) if old_waker.will_wake(waker) => (),
                        _ => *w = Some(waker.clone()),
                    });
                }

                // Release the lock.
                let res = self
                    .state
                    .compare_exchange(REGISTERING, WAITING, AcqRel, Acquire);

                match res {
                    Ok(_) => {}
                    Err(actual) => {
                        // A concurrent wake occurred while we were registering.
                        debug_assert_eq!(actual, REGISTERING | WAKING);

                        // Take the waker.
                        // SAFETY: We hold the REGISTERING lock, and WAKING is set, so we have exclusive access.
                        let waker = unsafe {
                            self.waker
                                .with_mut(|w| w.take().expect("invariant broken: waker must exist"))
                        };

                        // Reset back to WAITING.
                        self.state.swap(WAITING, AcqRel);

                        waker.wake();
                    }
                }
            }
            Err(WAKING) => {
                // A concurrent wake is in progress. Wake the new waker immediately to avoid losing the wake event.
                waker.wake_by_ref();
            }
            Err(_) => {
                // Since there is no concurrent register, the state can never be REGISTERING or REGISTERING | WAKING.
                #[cfg(debug_assertions)]
                unreachable!("concurrent register detected on MwsrWaker");
                #[cfg(not(debug_assertions))]
                unsafe {
                    std::hint::unreachable_unchecked()
                }
            }
        }
    }

    /// Calls `wake` on the last `Waker` passed to `register`.
    ///
    /// If `register` has not been called yet, then this does nothing.
    pub fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    /// Returns the last `Waker` passed to `register`, so that the user can wake it.
    ///
    /// If a waker has not been registered, this returns `None`.
    pub fn take(&self) -> Option<Waker> {
        if self.state.load(Relaxed) & WAKING != 0 {
            return None;
        }

        match self.state.fetch_or(WAKING, AcqRel) {
            WAITING => {
                // The waking lock has been acquired.
                // SAFETY: We successfully set WAKING while state was WAITING, so we have exclusive access.
                let waker = unsafe { self.waker.with_mut(|w| w.take()) };

                // Release the lock.
                self.state.fetch_and(!WAKING, Release);

                waker
            }
            state => {
                // A concurrent wake is in progress, or a register is ongoing.
                debug_assert!(
                    state == REGISTERING || state == REGISTERING | WAKING || state == WAKING
                );
                None
            }
        }
    }
}

impl Default for MwsrWaker {
    fn default() -> Self {
        MwsrWaker::new()
    }
}

impl fmt::Debug for MwsrWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MwsrWaker")
    }
}

unsafe impl Send for MwsrWaker {}
unsafe impl Sync for MwsrWaker {}
