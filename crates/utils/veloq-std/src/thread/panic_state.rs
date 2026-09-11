//! Thread-local panic state for targets without `std::thread::panicking`.

#[cfg(not(feature = "std"))]
use core::cell::Cell;

/// The panic state visible to destructors on the current thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PanicState {
    /// No panic is currently being unwound.
    NotPanicking,
    /// The target panic backend has started unwinding.
    Panicking,
    /// The state could not be tracked because the TLS backend was unavailable.
    Untracked,
}

#[cfg(not(feature = "std"))]
static CURRENT_PANIC_STATE: veloq_tls::Tls<Cell<PanicState>> = veloq_tls::Tls::new();

/// Returns the state of the current thread.
#[inline]
pub fn panic_state() -> PanicState {
    #[cfg(feature = "std")]
    {
        if std::thread::panicking() {
            PanicState::Panicking
        } else {
            PanicState::NotPanicking
        }
    }

    #[cfg(not(feature = "std"))]
    {
        CURRENT_PANIC_STATE
            .try_with_or_init(|state| state.get(), || Cell::new(PanicState::NotPanicking))
            .unwrap_or(PanicState::Untracked)
    }
}

/// Returns whether the current thread is known to be panicking.
#[inline]
pub fn panicking() -> bool {
    matches!(panic_state(), PanicState::Panicking)
}

/// Marks the current thread as panicking for a target panic backend.
///
/// A backend must call [`PanicStateGuard::finish`] on every normal return
/// path. If control flow unwinds through the guard, dropping it intentionally
/// leaves the state as `Panicking` so later destructors do not mistake the
/// unwind for normal execution. A backend that catches that unwind must reset
/// the state before continuing.
#[must_use = "call finish on the backend's normal return path"]
pub struct PanicStateGuard {
    #[cfg(not(feature = "std"))]
    previous: PanicState,
    #[cfg(not(feature = "std"))]
    finished: bool,
}

/// Enters the `Panicking` state on the current thread.
#[inline]
pub fn enter_panicking() -> PanicStateGuard {
    #[cfg(feature = "std")]
    {
        PanicStateGuard {}
    }

    #[cfg(not(feature = "std"))]
    {
        let previous = CURRENT_PANIC_STATE
            .try_with_or_init(
                |state| {
                    let previous = state.get();
                    state.set(PanicState::Panicking);
                    previous
                },
                || Cell::new(PanicState::Panicking),
            )
            .unwrap_or(PanicState::Untracked);
        PanicStateGuard {
            previous,
            finished: false,
        }
    }
}

impl PanicStateGuard {
    /// Restores the state that was active before [`enter_panicking`].
    #[inline]
    pub fn finish(self) {
        #[cfg(not(feature = "std"))]
        {
            let mut this = self;
            if !this.finished {
                let previous = this.previous;
                let _ = CURRENT_PANIC_STATE.try_with(|state| state.set(previous));
                this.finished = true;
            }
        }
    }
}

impl Drop for PanicStateGuard {
    fn drop(&mut self) {
        // Do not restore state implicitly. Stable no_std code cannot tell a
        // normal drop from a drop caused by unwinding; restoring here would
        // hide the panic from destructors that run later in the unwind.
    }
}
