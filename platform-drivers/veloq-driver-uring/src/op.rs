//! io_uring Platform-Specific Operation Definitions

use crate::driver::UringDriver;
use io_uring::squeue;
use std::io;
use std::time::Duration;
use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::op::{IntoPlatformOp, OpKind};

mod payload;
mod submit;

pub(crate) use payload::UringOpPayload;
pub(crate) use payload::{
    Accept, Close, Connect, Fallocate, Fsync, OpSend, Open, ReadFixed, Recv, SendTo, SyncFileRange,
    Timeout, UdpRecvStream, UdpRefill, Wakeup, WriteFixed,
};

// ============================================================================
// VTable Definition
// ============================================================================

pub(crate) type MakeSqeFn =
    unsafe fn(op: &mut UringKernelOp, driver: &mut UringDriver) -> squeue::Entry;
pub(crate) type OnCompleteFn = unsafe fn(op: &mut UringKernelOp, result: i32) -> io::Result<usize>;
pub(crate) type DropFn = unsafe fn(op: &mut UringKernelOp);
pub(crate) type GetTimeoutFn = unsafe fn(op: &UringKernelOp) -> Option<Duration>;
pub(crate) type ResolveChunksFn = unsafe fn(op: &UringKernelOp, chunks: &mut [u16]) -> usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
    /// Only for background operations (e.g. Close)
    BackgroundOnly,
}

pub(crate) struct OpVTable {
    pub(crate) make_sqe: MakeSqeFn,
    pub(crate) on_complete: OnCompleteFn,
    pub(crate) drop: DropFn,
    pub(crate) strategy: SubmissionStrategy,
    pub(crate) get_timeout: GetTimeoutFn,
    pub(crate) resolve_chunks: ResolveChunksFn,
}

// ============================================================================
// UringKernelOp Struct & Union (Type-Erased)
// ============================================================================

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: &'static OpVTable,

    /// Type-erased payload
    pub(crate) payload: UringOpPayload,
}

impl PlatformOp for UringKernelOp {}

impl Drop for UringKernelOp {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self) };
    }
}

pub type UringOp = UringKernelOp;

// ============================================================================
// Macro Definition
// ============================================================================

macro_rules! define_uring_ops {
    (
        $(
            $OpType:ident {
                field: $field:ident,
                $(payload: $Payload:ty,)?
                kind: $kind:expr,
                make_sqe: $make_sqe:path,
                on_complete: $complete:path,
                drop: $drop:path,
                $(strategy: $strategy:expr,)?
                $(get_timeout: $get_timeout:expr,)?
                $(resolve_chunks: $resolve_chunks:expr,)?
                $(construct: $construct:expr,)?
                $(destruct: $destruct:expr,)?
            }
        ),+ $(,)?
    ) => {
        $(
            impl IntoPlatformOp<UringOp> for $OpType {
                type UserPayload = Box<$OpType>;
                const PAYLOAD_KIND: OpKind = $kind;

                fn into_kernel_and_payload(self) -> (UringKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        make_sqe: $make_sqe,
                        on_complete: $complete,
                        drop: $drop,
                        strategy: define_uring_ops!(@strategy $($strategy)?),
                        get_timeout: define_uring_ops!(@get_timeout $($get_timeout)?),
                        resolve_chunks: define_uring_ops!(@resolve_chunks $($resolve_chunks)?),
                    };

                    let mut user = Box::new(self);
                    let user_ptr = std::ptr::NonNull::from(user.as_mut());
                    let payload = define_uring_ops!(@construct user_ptr, $($construct)?, $OpType $(, $Payload)?);

                    let op = UringKernelOp {
                        vtable: &TABLE,
                        payload: UringOpPayload {
                            $field: std::mem::ManuallyDrop::new(payload),
                        },
                    };
                    (op, user)
                }

                fn from_user_payload(payload: Self::UserPayload) -> Self {
                    define_uring_ops!(@destruct payload, $($destruct)?)
                }

                fn payload_into_erased(payload: Self::UserPayload) -> veloq_driver_core::slot::ErasedPayload {
                    veloq_driver_core::slot::ErasedPayload {
                        ptr: Box::into_raw(payload) as *mut (),
                        kind: <$OpType as IntoPlatformOp<UringOp>>::PAYLOAD_KIND as u16,
                        drop_fn: define_uring_ops!(@drop_raw_fn $OpType),
                    }
                }

                unsafe fn payload_from_raw(ptr: *mut ()) -> Self::UserPayload {
                    unsafe { Box::from_raw(ptr as *mut $OpType) }
                }
            }
        )+
    };

    (@payload_type $OpType:ty) => { crate::op::payload::KernelRef<$OpType> };
    (@payload_type $OpType:ty, $Payload:ty) => { $Payload };

    (@strategy ) => { SubmissionStrategy::SubmitSqe };
    (@strategy $strategy:expr) => { $strategy };

    (@get_timeout ) => { crate::op::submit::get_timeout_none };
    (@get_timeout $func:expr) => { $func };

    (@resolve_chunks ) => { crate::op::submit::resolve_chunks_none };
    (@resolve_chunks $func:expr) => { $func };

    (@construct $user_ptr:expr, , $OpType:ty) => { crate::op::payload::KernelRef { user: $user_ptr } };
    (@construct $user_ptr:expr, $construct:expr, $OpType:ty, $Payload:ty) => { ($construct)($user_ptr) };

    (@destruct $user_payload:expr, ) => { *$user_payload };
    (@destruct $user_payload:expr, $destruct:expr) => { ($destruct)($user_payload) };

    (@drop_raw_fn $OpType:ty) => {{
        unsafe fn drop_raw(ptr: *mut ()) {
            unsafe { drop(Box::from_raw(ptr as *mut $OpType)) };
        }
        drop_raw
    }};
}

// ============================================================================
// Op Definitions
// ============================================================================

define_uring_ops! {
    ReadFixed {
        field: read,
        kind: OpKind::ReadFixed,
        make_sqe: submit::make_sqe_read_fixed,
        on_complete: submit::on_complete_read_fixed,
        drop: submit::drop_read_fixed,
        resolve_chunks: submit::resolve_chunks_read_fixed,
    },
    WriteFixed {
        field: write,
        kind: OpKind::WriteFixed,
        make_sqe: submit::make_sqe_write_fixed,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        resolve_chunks: submit::resolve_chunks_write_fixed,
    },
    Recv {
        field: recv,
        kind: OpKind::Recv,
        make_sqe: submit::make_sqe_recv,
        on_complete: submit::on_complete_recv,
        drop: submit::drop_recv,
    },
    OpSend {
        field: send,
        kind: OpKind::Send,
        make_sqe: submit::make_sqe_send,
        on_complete: submit::on_complete_send,
        drop: submit::drop_send,
    },
    Connect {
        field: connect,
        kind: OpKind::Connect,
        make_sqe: submit::make_sqe_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
    },
    Close {
        field: close,
        kind: OpKind::Close,
        make_sqe: submit::make_sqe_close,
        on_complete: submit::on_complete_close,
        drop: submit::drop_close,
        strategy: SubmissionStrategy::BackgroundOnly,
    },
    Fsync {
        field: fsync,
        kind: OpKind::Fsync,
        make_sqe: submit::make_sqe_fsync,
        on_complete: submit::on_complete_fsync,
        drop: submit::drop_fsync,
    },
    SyncFileRange {
        field: sync_range,
        kind: OpKind::SyncFileRange,
        make_sqe: submit::make_sqe_sync_range,
        on_complete: submit::on_complete_sync_range,
        drop: submit::drop_sync_range,
    },
    Fallocate {
        field: fallocate,
        kind: OpKind::Fallocate,
        make_sqe: submit::make_sqe_fallocate,
        on_complete: submit::on_complete_fallocate,
        drop: submit::drop_fallocate,
    },
    Accept {
        field: accept,
        payload: payload::AcceptPayload,
        kind: OpKind::Accept,
        make_sqe: submit::make_sqe_accept,
        on_complete: submit::on_complete_accept,
        drop: submit::drop_accept,
        construct: |user| payload::AcceptPayload { user },
        destruct: |user: Box<Accept>| *user,
    },
    SendTo {
        field: send_to,
        payload: payload::SendToPayload,
        kind: OpKind::SendTo,
        make_sqe: submit::make_sqe_send_to,
        on_complete: submit::on_complete_send_to,
        drop: submit::drop_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            let op = unsafe { user.as_ref() };
            let (msg_name, msg_namelen) = crate::net::socket_addr_to_storage(op.addr);
            payload::SendToPayload {
                user,
                msg_name: msg_name.0,
                msg_namelen: msg_namelen as libc::socklen_t,
                iovec: [unsafe { std::mem::zeroed() }],
                msghdr: unsafe { std::mem::zeroed() },
            }
        },
        destruct: |user: Box<SendTo>| *user,
    },
    UdpRecvStream {
        field: udp_recv_stream,
        payload: payload::UdpRecvStreamPayload,
        kind: OpKind::UdpRecvStream,
        make_sqe: submit::make_sqe_udp_recv_stream,
        on_complete: submit::on_complete_udp_recv_stream,
        drop: submit::drop_udp_recv_stream,
        construct: |user| payload::UdpRecvStreamPayload {
            user,
            msg_name: unsafe { std::mem::zeroed() },
            iovec: [unsafe { std::mem::zeroed() }],
            msghdr: unsafe { std::mem::zeroed() },
        },
        destruct: |user: Box<UdpRecvStream>| *user,
    },
    UdpRefill {
        field: udp_refill,
        kind: OpKind::UdpRefill,
        make_sqe: submit::make_sqe_udp_refill,
        on_complete: submit::on_complete_udp_refill,
        drop: submit::drop_udp_refill,
    },
    Open {
        field: open,
        payload: payload::OpenPayload,
        kind: OpKind::Open,
        make_sqe: submit::make_sqe_open,
        on_complete: submit::on_complete_open,
        drop: submit::drop_open,
        construct: |user| payload::OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    },
    Wakeup {
        field: wakeup,
        payload: payload::WakeupPayload,
        kind: OpKind::Wakeup,
        make_sqe: submit::make_sqe_wakeup,
        on_complete: submit::on_complete_wakeup,
        drop: submit::drop_wakeup,
        construct: |user| payload::WakeupPayload { user, buf: [0; 8] },
        destruct: |user: Box<Wakeup>| *user,
    },
    Timeout {
        field: timeout,
        payload: payload::TimeoutPayload,
        kind: OpKind::Timeout,
        make_sqe: submit::make_sqe_timeout,
        on_complete: submit::on_complete_timeout,
        drop: submit::drop_timeout,
        strategy: SubmissionStrategy::SoftwareTimer,
        get_timeout: submit::get_timeout_timeout,
        construct: |user| payload::TimeoutPayload { user, ts: [0; 2] },
        destruct: |user: Box<Timeout>| *user,
    },
}
