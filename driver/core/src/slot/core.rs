use super::{SlotCompletion, SlotError, SlotPayload, SlotSidecarData, SlotSpec};
use crate::{
    DriverResult,
    driver::{AnomalyAttach, CompletionAnomalyKind, CompletionCleanupGuard, UserCompletionEvent},
};
use bilge::prelude::*;
use std::{
    fmt::{self, Debug},
    marker::PhantomData,
};
use veloq_atomic_waker::AtomicWaker;
use veloq_shim::{
    atomic::{AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    sync::Mutex,
};

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
    pub fn with_state(mut self, state: SlotState) -> Self {
        self.set_state(state);
        self
    }

    pub fn with_generation(mut self, generation: u32) -> Self {
        self.set_generation(generation);
        self
    }
}

pub struct AtomicPackedCoreState(AtomicU64);

impl AtomicPackedCoreState {
    pub fn new(state: PackedCoreState) -> Self {
        Self(AtomicU64::new(u64::from(state)))
    }

    pub fn load(&self, order: Ordering) -> PackedCoreState {
        PackedCoreState::from(self.0.load(order))
    }

    pub fn store(&self, state: PackedCoreState, order: Ordering) {
        self.0.store(u64::from(state), order);
    }

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

pub struct SlotStorage<Spec: SlotSpec> {
    pub result: Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
    pub payload: Option<SlotPayload<Spec>>,
    pub sidecar: SlotSidecarData<Spec>,
}

impl<Spec: SlotSpec> SlotStorage<Spec> {
    pub fn new() -> Self {
        Self {
            result: None,
            payload: None,
            sidecar: SlotSidecarData::<Spec>::default(),
        }
    }

    pub fn reset(&mut self) {
        *self = Self::new();
    }

    pub fn with_mut<F, X>(&mut self, f: F) -> X
    where
        F: FnOnce(
            &mut Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
            &mut Option<SlotPayload<Spec>>,
            &mut SlotSidecarData<Spec>,
        ) -> X,
    {
        f(&mut self.result, &mut self.payload, &mut self.sidecar)
    }
}

impl<Spec: SlotSpec> Default for SlotStorage<Spec> {
    fn default() -> Self {
        Self::new()
    }
}

type SlotMarker<Spec> = PhantomData<fn() -> Spec>;

pub struct SlotData<Spec: SlotSpec> {
    pub(crate) core_state: AtomicPackedCoreState,
    pub next_free: AtomicUsize,
    pub(crate) completion_res: AtomicI32,
    pub(crate) completion_flags: AtomicU32,
    pub(crate) completion_data: Mutex<CompletionData<Spec>>,
    pub(crate) completion_waker: AtomicWaker,
    marker: SlotMarker<Spec>,
}

pub(crate) enum CompletionData<Spec: SlotSpec> {
    Empty,
    User {
        event: UserCompletionEvent,
        payload: SlotPayload<Spec>,
        detail: Option<DriverResult<SlotCompletion<Spec>, SlotError<Spec>>>,
        cleanup: CompletionCleanupGuard,
    },
    Lost {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
        cleanup: CompletionCleanupGuard,
    },
}

impl<Spec: SlotSpec> Default for CompletionData<Spec> {
    fn default() -> Self {
        Self::Empty
    }
}

impl<Spec: SlotSpec> fmt::Debug for CompletionData<Spec>
where
    SlotPayload<Spec>: fmt::Debug,
    SlotCompletion<Spec>: fmt::Debug,
    SlotError<Spec>: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("Empty"),
            Self::User {
                event,
                payload,
                detail,
                cleanup,
            } => f
                .debug_struct("User")
                .field("event", event)
                .field("payload", payload)
                .field("detail", detail)
                .field("cleanup", cleanup)
                .finish(),
            Self::Lost {
                kind,
                attach,
                cleanup,
            } => f
                .debug_struct("Lost")
                .field("kind", kind)
                .field("attach", attach)
                .field("cleanup", cleanup)
                .finish(),
        }
    }
}

impl<Spec: SlotSpec> SlotData<Spec> {
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
            completion_data: Mutex::new(CompletionData::<Spec>::default()),
            completion_waker: AtomicWaker::new(),
            marker: PhantomData,
        }
    }

    pub(crate) fn state(&self, ordering: Ordering) -> SlotState {
        self.core_state.load(ordering).state()
    }

    pub fn generation(&self, ordering: Ordering) -> u32 {
        self.core_state.load(ordering).generation()
    }

    pub(crate) fn load_core_state(&self, ordering: Ordering) -> PackedCoreState {
        self.core_state.load(ordering)
    }

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

    pub(crate) fn completion_with_record_data<F, X>(&self, f: F) -> X
    where
        F: FnOnce(&mut CompletionData<Spec>) -> X,
    {
        let mut data = self.completion_data.lock();
        f(&mut *data)
    }
}

impl<Spec: SlotSpec> Default for SlotData<Spec> {
    fn default() -> Self {
        Self::new()
    }
}
