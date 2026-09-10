use core::panic::{RefUnwindSafe, UnwindSafe};

use crate::{
    cell::Cell,
    fmt,
    sync::{
        atomic::{
            NativeAtomicU32,
            Ordering::{Acquire, Relaxed, Release},
        },
        sys::native::{
            wait_on_address as native_wait_on_address,
            wake_all_by_address as native_wake_all_by_address,
        },
    },
};

#[cfg(feature = "loom")]
use crate::sync::{
    atomic::LoomAtomicU32,
    sys::loom_sys::{
        wait_on_address as loom_wait_on_address, wake_all_by_address as loom_wake_all_by_address,
    },
};

const INCOMPLETE: u32 = 3;
const POISONED: u32 = 2;
const RUNNING: u32 = 1;
const COMPLETE: u32 = 0;
const QUEUED: u32 = 4;
const STATE_MASK: u32 = 0b11;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum OnceExclusiveState {
    Incomplete,
    Poisoned,
    Complete,
}

pub struct OnceState {
    poisoned: bool,
    set_state_to: Cell<u32>,
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

impl fmt::Debug for OnceState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OnceState")
            .field("poisoned", &self.is_poisoned())
            .finish()
    }
}

macro_rules! impl_once {
    (
        $(#[$meta:meta])*
        struct $name:ident,
        atomic: $atomic_ty:ty,
        guard: $guard_name:ident,
        wait_fn: $wait_fn:path,
        wake_fn: $wake_fn:path,
        exclusive_load: |$self_var:ident| $exclusive_load:expr,
        $(const_new: $is_const:ident)?
    ) => {
        $(#[$meta])*
        pub struct $name {
            state_and_queued: $atomic_ty,
        }

        impl UnwindSafe for $name {}
        impl RefUnwindSafe for $name {}

        struct $guard_name<'a> {
            state_and_queued: &'a $atomic_ty,
            set_state_on_drop_to: u32,
        }

        impl<'a> Drop for $guard_name<'a> {
            fn drop(&mut self) {
                if self
                    .state_and_queued
                    .swap(self.set_state_on_drop_to, Release)
                    & QUEUED
                    != 0
                {
                    $wake_fn(self.state_and_queued);
                }
            }
        }

        impl Default for $name {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            $(impl_once!(@new_fn $is_const, $atomic_ty);)?

            $(impl_once!(@new_complete_fn $is_const, $atomic_ty);)?

            #[inline]
            pub fn is_completed(&self) -> bool {
                self.state_and_queued.load(Acquire) == COMPLETE
            }

            #[inline]
            pub(crate) fn state(&mut self) -> OnceExclusiveState {
                let $self_var = self;
                let val = $exclusive_load;
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
                            $wait_fn(&self.state_and_queued, state_and_queued);
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

                            let mut waiter_queue = $guard_name {
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
                            $wait_fn(&self.state_and_queued, state_and_queued);
                            state_and_queued = self.state_and_queued.load(Acquire);
                        }
                    }
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($name)).finish_non_exhaustive()
            }
        }
    };

    (@new_fn const, $atomic_ty:ty) => {
        #[inline]
        pub const fn new() -> Self {
            Self {
                state_and_queued: <$atomic_ty>::new(INCOMPLETE),
            }
        }
    };

    (@new_fn non_const, $atomic_ty:ty) => {
        #[inline]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                state_and_queued: <$atomic_ty>::new(INCOMPLETE),
            }
        }
    };

    (@new_complete_fn const, $atomic_ty:ty) => {
        #[inline]
        pub(crate) const fn new_complete() -> Self {
            Self {
                state_and_queued: <$atomic_ty>::new(COMPLETE),
            }
        }
    };

    (@new_complete_fn non_const, $atomic_ty:ty) => {
        #[inline]
        pub(crate) fn new_complete() -> Self {
            Self {
                state_and_queued: <$atomic_ty>::new(COMPLETE),
            }
        }
    };
}

impl_once!(
    #[derive()]
    struct NativeOnce,
    atomic: NativeAtomicU32,
    guard: NativeCompletionGuard,
    wait_fn: native_wait_on_address,
    wake_fn: native_wake_all_by_address,
    exclusive_load: |s| *s.state_and_queued.get_mut(),
    const_new: const
);

#[cfg(feature = "loom")]
impl_once!(
    #[derive()]
    struct LoomOnce,
    atomic: LoomAtomicU32,
    guard: LoomCompletionGuard,
    wait_fn: loom_wait_on_address,
    wake_fn: loom_wake_all_by_address,
    exclusive_load: |s| s.state_and_queued.load(Acquire),
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type Once = NativeOnce;

#[cfg(feature = "loom")]
pub type Once = LoomOnce;
