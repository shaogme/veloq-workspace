//! io_uring Platform-Specific Operation Definitions

use crate::config::UringRawHandle;
use crate::driver::UringDriver;
use crate::{OwnedRawHandle, RawHandle};
use io_uring::squeue;
use std::time::Duration;
use veloq_driver_core::DriverResult;
use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::op::{IntoPlatformOp, OpKind};

mod payload;
pub(crate) mod slot;
mod submit;

pub(crate) use payload::UringOpPayload;
pub(crate) use payload::{
    Accept, Close, Connect, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, Open, ReadFixed,
    ReadRaw, Recv, SendTo, SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecv,
    UdpRecvStream, UdpSend, Wakeup, WriteFixed, WriteRaw,
};

// ============================================================================
// VTable Definition
// ============================================================================

pub(crate) type MakeSqeFn = unsafe fn(
    op: &mut UringKernelOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry>;
pub(crate) type OnCompleteFn = unsafe fn(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize>;
pub(crate) type DropFn = unsafe fn(op: &mut UringKernelOp);
pub(crate) type GetTimeoutFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload) -> Option<Duration>;
pub(crate) type ResolveChunksFn =
    unsafe fn(op: &UringKernelOp, payload: &UringUserPayload, chunks: &mut [u16]) -> usize;

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
// UringKernelOp Struct & Payload (Type-Erased)
// ============================================================================

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: &'static OpVTable,

    /// Type-erased payload (kernel-side data)
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
                $(on_complete: $complete:path,)?
                $(completion: $completion:ty,)?
                $(map_completion: $map_completion:expr,)?
                drop: $drop:path,
                $(strategy: $strategy:expr,)?
                $(get_timeout: $get_timeout:expr,)?
                $(resolve_chunks: $resolve_chunks:expr,)?
                $(construct: $construct:expr,)?
                $(destruct: $destruct:expr,)?
            }
        ),+ $(,)?
    ) => {
        pub enum UringUserPayload {
            $( $OpType($OpType), )+
        }

        unsafe impl Send for UringUserPayload {}

        $(
            impl IntoPlatformOp<UringOp> for $OpType {
                type UserPayload = $OpType;
                type ErasedPayload = UringUserPayload;
                type Completion = define_uring_ops!(@completion_type $($completion)?);
                type DriverCompletion = usize;
                const PAYLOAD_KIND: OpKind = $kind;

                fn into_kernel_and_payload(self) -> (UringKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        make_sqe: $make_sqe,
                        on_complete: define_uring_ops!(@on_complete $($complete)?),
                        drop: $drop,
                        strategy: define_uring_ops!(@strategy $($strategy)?),
                        get_timeout: define_uring_ops!(@get_timeout $($get_timeout)?),
                        resolve_chunks: define_uring_ops!(@resolve_chunks $($resolve_chunks)?),
                    };

                    let user = self;
                    // We can't get the pointer yet because it's not in the slot.
                    // The vtable functions will get it via driver + user_data.
                    let payload = define_uring_ops!(@construct_dummy $OpType $(, $Payload)?);

                    let op = UringKernelOp {
                        vtable: &TABLE,
                        payload: UringOpPayload::$field(payload),
                    };
                    (op, user)
                }

                fn from_user_payload(payload: Self::UserPayload) -> Self {
                    payload
                }

                fn payload_into_erased(payload: Self::UserPayload) -> UringUserPayload {
                    UringUserPayload::$OpType(payload)
                }

                fn payload_from_erased(erased: UringUserPayload) -> Self::UserPayload {
                    match erased {
                        UringUserPayload::$OpType(p) => p,
                        #[allow(unreachable_patterns)]
                        _ => panic!("wrong payload type for {}", stringify!($OpType)),
                    }
                }

                fn map_completion_result(
                    &self,
                    res: DriverResult<usize>,
                ) -> DriverResult<Self::Completion> {
                    define_uring_ops!(@map_completion self, res, $($map_completion)?)
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

    (@construct_dummy $OpType:ty) => { crate::op::payload::KernelRef { _marker: std::marker::PhantomData } };
    (@construct_dummy $OpType:ty, $Payload:ty) => { unsafe { std::mem::zeroed() } };

    (@on_complete ) => { crate::op::submit::on_complete_default };
    (@on_complete $func:path) => { $func };

    (@completion_type ) => { usize };
    (@completion_type $ty:ty) => { $ty };

    (@map_completion $this:ident, $res:ident, ) => { $res };
    (@map_completion $this:ident, $res:ident, $expr:expr) => { ($expr)($this, $res) };
}

// ============================================================================
// Op Definitions
// ============================================================================

define_uring_ops! {
    ReadFixed {
        field: Read,
        kind: OpKind::ReadFixed,
        make_sqe: submit::make_sqe_read_fixed,
        on_complete: submit::on_complete_read_fixed,
        drop: submit::drop_read_fixed,
        resolve_chunks: submit::resolve_chunks_read_fixed,
    },
    ReadRaw {
        field: ReadRaw,
        kind: OpKind::ReadFixed,
        make_sqe: submit::make_sqe_read_raw,
        on_complete: submit::on_complete_read_fixed,
        drop: submit::drop_read_fixed,
        resolve_chunks: submit::resolve_chunks_read_raw,
    },
    WriteFixed {
        field: Write,
        kind: OpKind::WriteFixed,
        make_sqe: submit::make_sqe_write_fixed,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        resolve_chunks: submit::resolve_chunks_write_fixed,
    },
    WriteRaw {
        field: WriteRaw,
        kind: OpKind::WriteFixed,
        make_sqe: submit::make_sqe_write_raw,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        resolve_chunks: submit::resolve_chunks_write_raw,
    },
    Recv {
        field: Recv,
        kind: OpKind::Recv,
        make_sqe: submit::make_sqe_recv,
        on_complete: submit::on_complete_recv,
        drop: submit::drop_recv,
    },
    OpSend {
        field: Send,
        kind: OpKind::Send,
        make_sqe: submit::make_sqe_send,
        on_complete: submit::on_complete_send,
        drop: submit::drop_send,
    },
    UdpRecv {
        field: UdpRecv,
        kind: OpKind::UdpRecv,
        make_sqe: submit::make_sqe_udp_recv,
        on_complete: submit::on_complete_udp_recv,
        drop: submit::drop_udp_recv,
    },
    UdpSend {
        field: UdpSend,
        kind: OpKind::UdpSend,
        make_sqe: submit::make_sqe_udp_send,
        on_complete: submit::on_complete_udp_send,
        drop: submit::drop_udp_send,
    },
    Connect {
        field: Connect,
        kind: OpKind::Connect,
        make_sqe: submit::make_sqe_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
    },
    UdpConnect {
        field: UdpConnect,
        kind: OpKind::UdpConnect,
        make_sqe: submit::make_sqe_udp_connect,
        on_complete: submit::on_complete_udp_connect,
        drop: submit::drop_udp_connect,
    },
    Close {
        field: Close,
        kind: OpKind::Close,
        make_sqe: submit::make_sqe_close,
        on_complete: submit::on_complete_close,
        drop: submit::drop_close,
        strategy: SubmissionStrategy::BackgroundOnly,
    },
    Fsync {
        field: Fsync,
        kind: OpKind::Fsync,
        make_sqe: submit::make_sqe_fsync,
        on_complete: submit::on_complete_fsync,
        drop: submit::drop_fsync,
    },
    FsyncRaw {
        field: FsyncRaw,
        kind: OpKind::Fsync,
        make_sqe: submit::make_sqe_fsync_raw,
        on_complete: submit::on_complete_fsync,
        drop: submit::drop_fsync,
    },
    SyncFileRange {
        field: SyncRange,
        kind: OpKind::SyncFileRange,
        make_sqe: submit::make_sqe_sync_range,
        on_complete: submit::on_complete_sync_range,
        drop: submit::drop_sync_range,
    },
    SyncFileRangeRaw {
        field: SyncRangeRaw,
        kind: OpKind::SyncFileRange,
        make_sqe: submit::make_sqe_sync_range_raw,
        on_complete: submit::on_complete_sync_range,
        drop: submit::drop_sync_range,
    },
    Fallocate {
        field: Fallocate,
        kind: OpKind::Fallocate,
        make_sqe: submit::make_sqe_fallocate,
        on_complete: submit::on_complete_fallocate,
        drop: submit::drop_fallocate,
    },
    FallocateRaw {
        field: FallocateRaw,
        kind: OpKind::Fallocate,
        make_sqe: submit::make_sqe_fallocate_raw,
        on_complete: submit::on_complete_fallocate,
        drop: submit::drop_fallocate,
    },
    Accept {
        field: Accept,
        payload: payload::AcceptPayload,
        kind: OpKind::Accept,
        make_sqe: submit::make_sqe_accept,
        on_complete: submit::on_complete_accept,
        completion: OwnedRawHandle,
        map_completion: |_op: &Accept, res: DriverResult<usize>| {
            res.map(|raw| unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(
                    raw as i32,
                )))
            })
        },
        drop: submit::drop_accept,
        construct: |user| payload::AcceptPayload { user },
        destruct: |user: Box<Accept>| *user,
    },
    SendTo {
        field: SendTo,
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
        field: UdpRecvStream,
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
    Open {
        field: Open,
        payload: payload::OpenPayload,
        kind: OpKind::Open,
        make_sqe: submit::make_sqe_open,
        completion: OwnedRawHandle,
        map_completion: |_op: &Open, res: DriverResult<usize>| {
            res.map(|raw| unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(
                    raw as i32,
                )))
            })
        },
        drop: submit::drop_open,
        construct: |user| payload::OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    },
    Wakeup {
        field: Wakeup,
        payload: payload::WakeupPayload,
        kind: OpKind::Wakeup,
        make_sqe: submit::make_sqe_wakeup,
        on_complete: submit::on_complete_wakeup,
        drop: submit::drop_wakeup,
        construct: |user| payload::WakeupPayload { user, buf: [0; 8] },
        destruct: |user: Box<Wakeup>| *user,
    },
    Timeout {
        field: Timeout,
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
