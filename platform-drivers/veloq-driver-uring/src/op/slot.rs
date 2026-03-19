use crate::driver::UringOpState;
use crate::op::UringOp;
use veloq_driver_core::slot::Slot as CoreSlot;

pub(crate) type Slot<'a, State> = CoreSlot<'a, State, UringOp, UringOpState, ()>;
pub(crate) use veloq_driver_core::slot::{
    Initialized, SlotMarker as SlotState, SlotRegistryExt as UringOpRegistryExt, SlotView,
};
