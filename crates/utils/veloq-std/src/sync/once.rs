use crate::{cell, fmt, sync, sys};

use core::panic::{RefUnwindSafe, UnwindSafe};

use cell::Cell;
use sync::atomic::{
    AtomicU32,
    Ordering::{Acquire, Relaxed, Release},
};

const INCOMPLETE: u32 = 3;
const POISONED: u32 = 2;
const RUNNING: u32 = 1;
const COMPLETE: u32 = 0;
const QUEUED: u32 = 4;
const STATE_MASK: u32 = 0b11;

pub struct Once {
    state_and_queued: AtomicU32,
}

impl UnwindSafe for Once {}
impl RefUnwindSafe for Once {}

pub struct OnceState {
    poisoned: bool,
    set_state_to: Cell<u32>,
}

pub(crate) enum OnceExclusiveState {
    Incomplete,
    Poisoned,
    Complete,
}

impl OnceState {
    #[inline]
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    #[inline]
    pub(crate) fn poison(&self) {
        self.set_state_to.set(POISONED);
    }
}

struct CompletionGuard<'a> {
    state_and_queued: &'a AtomicU32,
    set_state_on_drop_to: u32,
}

impl<'a> Drop for CompletionGuard<'a> {
    fn drop(&mut self) {
        if self
            .state_and_queued
            .swap(self.set_state_on_drop_to, Release)
            & QUEUED
            != 0
        {
            sys::wake_all_by_address(self.state_and_queued);
        }
    }
}

impl Default for Once {
    #[inline]
    fn default() -> Once {
        Once::new()
    }
}

impl Once {
    #[inline]
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Once {
        Once {
            state_and_queued: AtomicU32::new(INCOMPLETE),
        }
    }

    #[inline]
    #[cfg(feature = "loom")]
    pub fn new() -> Once {
        Once {
            state_and_queued: AtomicU32::new(INCOMPLETE),
        }
    }

    #[inline]
    pub fn is_completed(&self) -> bool {
        self.state_and_queued.load(Acquire) == COMPLETE
    }

    #[inline]
    pub(crate) fn state(&mut self) -> OnceExclusiveState {
        #[cfg(not(feature = "loom"))]
        let val = *self.state_and_queued.get_mut();
        #[cfg(feature = "loom")]
        let val = self.state_and_queued.load(Acquire);

        match val {
            INCOMPLETE => OnceExclusiveState::Incomplete,
            POISONED => OnceExclusiveState::Poisoned,
            COMPLETE => OnceExclusiveState::Complete,
            _ => unreachable!("invalid Once state"),
        }
    }

    #[cold]
    #[track_caller]
    pub fn wait(&self) {
        self.wait_inner(false);
    }

    #[cold]
    #[track_caller]
    pub fn wait_force(&self) {
        self.wait_inner(true);
    }

    #[inline]
    fn wait_inner(&self, ignore_poisoning: bool) {
        let mut state_and_queued = self.state_and_queued.load(Acquire);
        loop {
            let state = state_and_queued & STATE_MASK;
            let queued = state_and_queued & QUEUED != 0;
            match state {
                COMPLETE => return,
                POISONED if !ignore_poisoning => {
                    panic!("Once instance has previously been poisoned");
                }
                _ => {
                    if !queued {
                        let next = state_and_queued + QUEUED;
                        if let Err(new) = self.state_and_queued.compare_exchange_weak(
                            state_and_queued,
                            next,
                            Relaxed,
                            Acquire,
                        ) {
                            state_and_queued = new;
                            continue;
                        }
                        state_and_queued = next;
                    }
                    sys::wait_on_address(&self.state_and_queued, state_and_queued);
                    state_and_queued = self.state_and_queued.load(Acquire);
                }
            }
        }
    }

    #[inline]
    pub fn call_once<F>(&self, f: F)
    where
        F: FnOnce(),
    {
        if self.is_completed() {
            return;
        }
        let mut f = Some(f);
        self.call(false, &mut |_| f.take().unwrap()());
    }

    #[inline]
    pub fn call_once_force<F>(&self, f: F)
    where
        F: FnOnce(&OnceState),
    {
        if self.is_completed() {
            return;
        }
        let mut f = Some(f);
        self.call(true, &mut |p| f.take().unwrap()(p));
    }

    #[cold]
    #[track_caller]
    fn call(&self, ignore_poisoning: bool, f: &mut dyn FnMut(&OnceState)) {
        let mut state_and_queued = self.state_and_queued.load(Acquire);
        loop {
            let state = state_and_queued & STATE_MASK;
            let queued = state_and_queued & QUEUED != 0;
            match state {
                COMPLETE => return,
                POISONED if !ignore_poisoning => {
                    panic!("Once instance has previously been poisoned");
                }
                INCOMPLETE | POISONED => {
                    let next = RUNNING + if queued { QUEUED } else { 0 };
                    if let Err(new) = self.state_and_queued.compare_exchange_weak(
                        state_and_queued,
                        next,
                        Acquire,
                        Acquire,
                    ) {
                        state_and_queued = new;
                        continue;
                    }

                    let mut waiter_queue = CompletionGuard {
                        state_and_queued: &self.state_and_queued,
                        set_state_on_drop_to: POISONED,
                    };

                    let f_state = OnceState {
                        poisoned: state == POISONED,
                        set_state_to: Cell::new(COMPLETE),
                    };
                    f(&f_state);
                    waiter_queue.set_state_on_drop_to = f_state.set_state_to.get();
                    return;
                }
                _ => {
                    assert!(state == RUNNING);
                    if !queued {
                        let next = state_and_queued + QUEUED;
                        if let Err(new) = self.state_and_queued.compare_exchange_weak(
                            state_and_queued,
                            next,
                            Relaxed,
                            Acquire,
                        ) {
                            state_and_queued = new;
                            continue;
                        }
                        state_and_queued = next;
                    }
                    sys::wait_on_address(&self.state_and_queued, state_and_queued);
                    state_and_queued = self.state_and_queued.load(Acquire);
                }
            }
        }
    }
}

impl fmt::Debug for Once {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Once").finish_non_exhaustive()
    }
}

impl fmt::Debug for OnceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnceState")
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}
