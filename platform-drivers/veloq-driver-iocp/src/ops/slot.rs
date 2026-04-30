use crate::driver::IocpOpState;
use crate::ops::{IocpOp, OverlappedEntry};
use veloq_driver_core::slot::Slot as CoreSlot;

pub(crate) type Slot<'a, State> = CoreSlot<'a, State, IocpOp, IocpOpState, OverlappedEntry>;
