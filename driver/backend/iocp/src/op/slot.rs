use crate::driver::IocpOpState;
use crate::error::IocpError;
use crate::op::{IocpOp, IocpUserPayload, OverlappedEntry};
use veloq_driver_core::slot::Slot as CoreSlot;

pub(crate) type Slot<'a, State, UP = IocpUserPayload> =
    CoreSlot<'a, State, IocpOp, UP, IocpOpState, OverlappedEntry, IocpError>;
