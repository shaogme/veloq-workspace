use crate::driver::UringOpState;
use crate::error::UringError;
use crate::op::{UringOp, UringUserPayload};
use veloq_driver_core::driver::registry::OpRegistry as CoreOpRegistry;
use veloq_driver_core::slot::{Slot as CoreSlot, SlotSpec as CoreSlotSpec};

pub enum UringSlotSpec {}

impl CoreSlotSpec for UringSlotSpec {
    type Op = UringOp;
    type UserPayload = UringUserPayload;
    type PlatformData = UringOpState;
    type Sidecar = ();
    type Error = UringError;
    type Completion = usize;
}

pub(crate) type UringOpRegistry = CoreOpRegistry<UringSlotSpec>;
pub(crate) type Slot<'a, State> = CoreSlot<'a, State, UringSlotSpec>;

pub(crate) use veloq_driver_core::slot::{
    CheckedSlotView, Reserved, SlotMarker as SlotState, SlotRegistryExt as UringOpRegistryExt,
    SlotSnapshot, SlotView,
};
