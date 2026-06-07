use std::time::Instant;

use crate::error::IocpError;
use crate::op::{IocpOp, IocpUserPayload, OverlappedEntry};
use veloq_driver_core::driver::registry::OpRegistry as CoreOpRegistry;
use veloq_driver_core::slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec};

/// State associated with an IOCP operation.
#[derive(Default)]
pub struct IocpOpState {
    pub(crate) generation: u32,
    pub(crate) timer_id: Option<veloq_wheel::TaskId>,
    pub(crate) timer_deadline: Option<Instant>,
    pub(crate) is_background: bool,
    pub(crate) rio_cancel_requested: bool,
}

pub enum IocpSlotSpec {}

impl CoreSlotSpec for IocpSlotSpec {
    type Op = IocpOp;
    type UserPayload = IocpUserPayload;
    type PlatformData = IocpOpState;
    type Sidecar = OverlappedEntry;
    type Error = IocpError;
    type Completion = usize;
}

pub(crate) type IocpOpRegistry = CoreOpRegistry<IocpSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpSlotSpec>;
