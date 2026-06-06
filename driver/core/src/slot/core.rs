use crate::SlotSidecar;
use crate::DriverResult;
use bilge::prelude::*;
use std::marker::PhantomData;
use veloq_atomic_waker::AtomicWaker;
use veloq_shim::atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use veloq_shim::sync::Mutex;

#[bitsize(8)]
#[derive(FromBits, Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotState {
    Idle,
    Reserved,
    InFlightWaiting,
    InFlightReady,
    InFlightOrphaned,
    Finalizing,
    #[fallback]
    ReservedValue,
}

#[bitsize(64)]
#[derive(FromBits, DebugBits, Clone, Copy, PartialEq, Eq)]
pub struct PackedCoreState {
    pub generation: u32,
    pub state: SlotState,
    pub flags: u24,
}

impl PackedCoreState {
    #[inline]
    pub fn with_state(mut self, state: SlotState) -> Self {
        self.set_state(state);
        self
    }

    #[inline]
    pub fn with_generation(mut self, generation: u32) -> Self {
        self.set_generation(generation);
        self
    }
}

pub struct AtomicPackedCoreState(AtomicU64);

impl AtomicPackedCoreState {
    #[inline]
    pub fn new(state: PackedCoreState) -> Self {
        Self(AtomicU64::new(u64::from(state)))
    }

    #[inline]
    pub fn load(&self, order: Ordering) -> PackedCoreState {
        PackedCoreState::from(self.0.load(order))
    }

    #[inline]
    pub fn store(&self, state: PackedCoreState, order: Ordering) {
        self.0.store(u64::from(state), order);
    }

    #[inline]
    pub fn compare_exchange(
        &self,
        current: PackedCoreState,
        new: PackedCoreState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<PackedCoreState, PackedCoreState> {
        self.0
            .compare_exchange(u64::from(current), u64::from(new), success, failure)
            .map(PackedCoreState::from)
            .map_err(PackedCoreState::from)
    }

    #[inline]
    pub fn compare_exchange_weak(
        &self,
        current: PackedCoreState,
        new: PackedCoreState,
        success: Ordering,
        failure: Ordering,
    ) -> Result<PackedCoreState, PackedCoreState> {
        self.0
            .compare_exchange_weak(u64::from(current), u64::from(new), success, failure)
            .map(PackedCoreState::from)
            .map_err(PackedCoreState::from)
    }
}

pub struct SlotStorage<Op, UP, S: SlotSidecar, E, R = usize> {
    pub op: Option<Op>,
    pub result: Option<DriverResult<R, E>>,
    pub payload: Option<UP>,
    pub sidecar: S,
}

impl<Op, UP, S: SlotSidecar, E, R> SlotStorage<Op, UP, S, E, R> {
    #[inline]
    pub fn new() -> Self {
        Self {
            op: None,
            result: None,
            payload: None,
            sidecar: S::default(),
        }
    }

    #[inline]
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    #[inline]
    pub fn with_mut<F, X>(&mut self, f: F) -> X
    where
        F: FnOnce(
            &mut Option<Op>,
            &mut Option<DriverResult<R, E>>,
            &mut Option<UP>,
            &mut S,
        ) -> X,
    {
        f(
            &mut self.op,
            &mut self.result,
            &mut self.payload,
            &mut self.sidecar,
        )
    }
}

impl<Op, UP, S: SlotSidecar, E, R> Default for SlotStorage<Op, UP, S, E, R> {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

type SlotMarker<Op, S, UP> = PhantomData<fn() -> (Op, S, UP)>;

pub struct SlotData<Op, UP, S: SlotSidecar, E, R = usize> {
    pub(crate) core_state: AtomicPackedCoreState,
    pub next_free: AtomicUsize,
    pub(crate) completion_res: AtomicI32,
    pub(crate) completion_flags: AtomicU32,
    pub(crate) completion_data: Mutex<CompletionData<UP, E, R>>,
    pub(crate) completion_waker: AtomicWaker,
    marker: SlotMarker<Op, S, UP>,
}

#[derive(Debug)]
pub(crate) struct CompletionData<UP, E, R = usize> {
    pub payload: Option<UP>,
    pub detail: Option<DriverResult<R, E>>,
}

impl<UP, E, R> Default for CompletionData<UP, E, R> {
    fn default() -> Self {
        Self {
            payload: None,
            detail: None,
        }
    }
}

impl<Op, UP, S: SlotSidecar, E, R> SlotData<Op, UP, S, E, R> {
    pub(crate) const NULL_INDEX: usize = usize::MAX;

    pub fn new() -> Self {
        Self {
            core_state: AtomicPackedCoreState::new(PackedCoreState::new(
                0,
                SlotState::Idle,
                u24::new(0),
            )),
            next_free: AtomicUsize::new(Self::NULL_INDEX),
            completion_res: AtomicI32::new(0),
            completion_flags: AtomicU32::new(0),
            completion_data: Mutex::new(CompletionData::<UP, E, R>::default()),
            completion_waker: AtomicWaker::new(),
            marker: PhantomData,
        }
    }

    #[inline]
    pub(crate) fn state(&self, ordering: Ordering) -> SlotState {
        self.core_state.load(ordering).state()
    }

    #[inline]
    pub fn generation(&self, ordering: Ordering) -> u32 {
        self.core_state.load(ordering).generation()
    }

    #[inline]
    pub(crate) fn load_core_state(&self, ordering: Ordering) -> PackedCoreState {
        self.core_state.load(ordering)
    }

    #[inline]
    pub(crate) fn set_state(&self, state: SlotState, ordering: Ordering) {
        let mut current = self.core_state.load(Ordering::Acquire);
        loop {
            let new = current.with_state(state);
            match self
                .core_state
                .compare_exchange_weak(current, new, ordering, Ordering::Acquire)
            {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    pub(crate) fn reset(&self, generation: u32) {
        self.core_state.store(
            PackedCoreState::new(generation, SlotState::Idle, u24::new(0)),
            Ordering::Release,
        );
    }

    pub(crate) fn free(&self) {
        let mut current = self.core_state.load(Ordering::Acquire);
        loop {
            // Preserve READY state so detached completion can still be consumed.
            let target = if current.state() == SlotState::InFlightReady {
                SlotState::InFlightReady
            } else {
                SlotState::Idle
            };
            let new = current.with_state(target);
            match self.core_state.compare_exchange_weak(
                current,
                new,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }

    #[inline]
    pub(crate) fn completion_with_data<F, X>(&self, f: F) -> X
    where
        F: FnOnce(&mut Option<UP>, &mut Option<DriverResult<R, E>>) -> X,
    {
        let mut data = self.completion_data.lock();
        let CompletionData { payload, detail } = &mut *data;
        f(payload, detail)
    }
}

impl<Op, UP, S: SlotSidecar, E, R> Default for SlotData<Op, UP, S, E, R> {
    fn default() -> Self {
        Self::new()
    }
}
