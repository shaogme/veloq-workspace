use crate::driver::UringOpState;
use crate::error::UringError;
use crate::op::{UringOp, UringUserPayload};
use veloq_driver_core::slot::Slot as CoreSlot;

pub(crate) type Slot<'a, State> =
    CoreSlot<'a, State, UringOp, UringUserPayload, UringOpState, (), UringError>;
pub(crate) use veloq_driver_core::slot::{
    Reserved, SlotMarker as SlotState, SlotRegistryExt as UringOpRegistryExt, SlotView,
};
