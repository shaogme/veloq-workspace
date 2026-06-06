use crate::driver::IocpOpState;
use crate::error::IocpError;
use crate::op::{IocpOp, IocpUserPayload, OverlappedEntry};
use veloq_driver_core::driver::registry::{
    OpRegistry as CoreOpRegistry, SlotRegistrySpec as CoreSlotRegistrySpec,
};
use veloq_driver_core::slot::Slot as CoreSlot;

pub enum IocpSlotSpec {}

impl CoreSlotRegistrySpec for IocpSlotSpec {
    type Op = IocpOp;
    type UserPayload = IocpUserPayload;
    type PlatformData = IocpOpState;
    type Sidecar = OverlappedEntry;
    type Error = IocpError;
    type Completion = usize;
}

pub(crate) type IocpOpRegistry = CoreOpRegistry<IocpSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpSlotSpec>;
