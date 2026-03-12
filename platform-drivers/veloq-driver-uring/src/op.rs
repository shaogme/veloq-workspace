//! io_uring Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `UringKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

use crate::{UringDriver, submit};
use io_uring::squeue;
use std::io;
use std::mem::ManuallyDrop;
use std::time::Duration;
use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::op::{
    Accept as CoreAccept, Close as CoreClose, Connect as CoreConnect, Fallocate as CoreFallocate,
    Fsync as CoreFsync, IntoPlatformOp, OpKind, Open, ReadFixed as CoreReadFixed, Recv as CoreRecv,
    Send as CoreSend, SendTo as CoreSendTo, SyncFileRange as CoreSyncFileRange, Timeout,
    UdpRecvStream as CoreUdpRecvStream, UdpRefill as CoreUdpRefill, Wakeup as CoreWakeup,
    WriteFixed as CoreWriteFixed,
};

type ReadFixed = CoreReadFixed<crate::RawHandle>;
type WriteFixed = CoreWriteFixed<crate::RawHandle>;
type Recv = CoreRecv<crate::RawHandle>;
type OpSend = CoreSend<crate::RawHandle>;
type Connect = CoreConnect<crate::RawHandle, crate::SockAddrStorage>;
type Close = CoreClose<crate::RawHandle>;
type Fsync = CoreFsync<crate::RawHandle>;
type SyncFileRange = CoreSyncFileRange<crate::RawHandle>;
type Fallocate = CoreFallocate<crate::RawHandle>;
type Accept = CoreAccept<crate::RawHandle, crate::SockAddrStorage>;
type SendTo = CoreSendTo<crate::RawHandle>;
type UdpRecvStream = CoreUdpRecvStream<crate::RawHandle>;
type UdpRefill = CoreUdpRefill<crate::RawHandle>;
type Wakeup = CoreWakeup<crate::RawHandle>;

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

use std::ptr::NonNull;

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: NonNull<OpVTable>,

    /// Type-erased payload
    pub(crate) payload: UringOpPayload,
}

impl PlatformOp for UringKernelOp {}

impl Drop for UringKernelOp {
    fn drop(&mut self) {
        unsafe { (self.vtable.as_ref().drop)(self) };
    }
}

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
        // Ensure proper alignment
        #[repr(C)]
        pub(crate) union UringOpPayload {
            $(
                pub(crate) $field: ManuallyDrop< define_uring_ops!(@payload_type $OpType $(, $Payload)?) >,
            )+
        }

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
                        vtable: unsafe { NonNull::new_unchecked(&TABLE as *const _ as *mut _) },
                        payload: UringOpPayload {
                            $field: ManuallyDrop::new(payload),
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

    (@payload_type $OpType:ty) => { KernelRef<$OpType> };
    (@payload_type $OpType:ty, $Payload:ty) => { $Payload };

    // Default strategy: SubmitSqe
    (@strategy ) => { SubmissionStrategy::SubmitSqe };
    (@strategy $strategy:expr) => { $strategy };

    // Default get_timeout: return None
    (@get_timeout ) => { submit::get_timeout_none };
    (@get_timeout $func:expr) => { $func };

    // Default resolve_chunks: return 0
    (@resolve_chunks ) => { submit::resolve_chunks_none };
    (@resolve_chunks $func:expr) => { $func };

    // Default construct: keep only a pointer to user payload
    (@construct $user_ptr:expr, , $OpType:ty) => { KernelRef { user: $user_ptr } };
    // Custom construct
    (@construct $user_ptr:expr, $construct:expr, $OpType:ty, $Payload:ty) => { ($construct)($user_ptr) };

    // Default destruct: return user payload
    (@destruct $user_payload:expr, ) => { *$user_payload };
    // Custom destruct
    (@destruct $user_payload:expr, $destruct:expr) => { ($destruct)($user_payload) };

    (@drop_raw_fn $OpType:ty) => {{
        unsafe fn drop_raw(ptr: *mut ()) {
            unsafe { drop(Box::from_raw(ptr as *mut $OpType)) };
        }
        drop_raw
    }};
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

pub(crate) struct KernelRef<T> {
    pub(crate) user: NonNull<T>,
}

pub(crate) struct AcceptPayload {
    pub(crate) user: NonNull<Accept>,
}

pub(crate) struct SendToPayload {
    pub(crate) user: NonNull<SendTo>,
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) msg_namelen: libc::socklen_t,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct UdpRecvStreamPayload {
    pub(crate) user: NonNull<UdpRecvStream>,
    pub(crate) msg_name: libc::sockaddr_storage,
    pub(crate) iovec: [libc::iovec; 1],
    pub(crate) msghdr: libc::msghdr,
}

pub(crate) struct OpenPayload {
    pub(crate) user: NonNull<Open>,
}

pub(crate) struct WakeupPayload {
    pub(crate) user: NonNull<Wakeup>,
    pub(crate) buf: [u8; 8],
}

pub(crate) struct TimeoutPayload {
    pub(crate) user: NonNull<Timeout>,
    pub(crate) ts: [i64; 2],
}

pub(crate) type UringOp = UringKernelOp;

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
    }, // Kernel 5.1+
    WriteFixed {
        field: write,
        kind: OpKind::WriteFixed,
        make_sqe: submit::make_sqe_write_fixed,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        resolve_chunks: submit::resolve_chunks_write_fixed,
    }, // Kernel 5.1+
    Recv {
        field: recv,
        kind: OpKind::Recv,
        make_sqe: submit::make_sqe_recv,
        on_complete: submit::on_complete_recv,
        drop: submit::drop_recv,
    }, // Kernel 5.6+
    OpSend {
        field: send,
        kind: OpKind::Send,
        make_sqe: submit::make_sqe_send,
        on_complete: submit::on_complete_send,
        drop: submit::drop_send,
    }, // Kernel 5.6+
    Connect {
        field: connect,
        kind: OpKind::Connect,
        make_sqe: submit::make_sqe_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
    }, // Kernel 5.5+
    Close {
        field: close,
        kind: OpKind::Close,
        make_sqe: submit::make_sqe_close,
        on_complete: submit::on_complete_close,
        drop: submit::drop_close,
        strategy: SubmissionStrategy::BackgroundOnly,
    }, // Kernel 5.6+
    Fsync {
        field: fsync,
        kind: OpKind::Fsync,
        make_sqe: submit::make_sqe_fsync,
        on_complete: submit::on_complete_fsync,
        drop: submit::drop_fsync,
    }, // Kernel 5.1+
    SyncFileRange {
        field: sync_range,
        kind: OpKind::SyncFileRange,
        make_sqe: submit::make_sqe_sync_range,
        on_complete: submit::on_complete_sync_range,
        drop: submit::drop_sync_range,
    }, // Kernel 5.2+
    Fallocate {
        field: fallocate,
        kind: OpKind::Fallocate,
        make_sqe: submit::make_sqe_fallocate,
        on_complete: submit::on_complete_fallocate,
        drop: submit::drop_fallocate,
    }, // Kernel 5.6+
    Accept {
        field: accept,
        payload: AcceptPayload,
        kind: OpKind::Accept,
        make_sqe: submit::make_sqe_accept,
        on_complete: submit::on_complete_accept,
        drop: submit::drop_accept,
        construct: |user| AcceptPayload { user },
        destruct: |user: Box<Accept>| *user,
    }, // Kernel 5.5+
    SendTo {
        field: send_to,
        payload: SendToPayload,
        kind: OpKind::SendTo,
        make_sqe: submit::make_sqe_send_to,
        on_complete: submit::on_complete_send_to,
        drop: submit::drop_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            let op = unsafe { user.as_ref() };
            let (msg_name, msg_namelen) = crate::socket_addr_to_storage(op.addr);
            SendToPayload {
                user,
                msg_name: msg_name.0,
                msg_namelen: msg_namelen as libc::socklen_t,
                iovec: [unsafe { std::mem::zeroed() }],
                msghdr: unsafe { std::mem::zeroed() },
            }
        },
        destruct: |user: Box<SendTo>| *user,
    }, // Kernel 5.1+ (via SendMsg)
    UdpRecvStream {
        field: udp_recv_stream,
        payload: UdpRecvStreamPayload,
        kind: OpKind::UdpRecvStream,
        make_sqe: submit::make_sqe_udp_recv_stream,
        on_complete: submit::on_complete_udp_recv_stream,
        drop: submit::drop_udp_recv_stream,
        construct: |user| UdpRecvStreamPayload {
            user,
            msg_name: unsafe { std::mem::zeroed() },
            iovec: [unsafe { std::mem::zeroed() }],
            msghdr: unsafe { std::mem::zeroed() },
        },
        destruct: |user: Box<UdpRecvStream>| *user,
    }, // Kernel 5.1+ (via RecvMsg)
    UdpRefill {
        field: udp_refill,
        kind: OpKind::UdpRefill,
        make_sqe: submit::make_sqe_udp_refill,
        on_complete: submit::on_complete_udp_refill,
        drop: submit::drop_udp_refill,
    }, // No-op on io_uring, kept for cross-platform API parity
    Open {
        field: open,
        payload: OpenPayload,
        kind: OpKind::Open,
        make_sqe: submit::make_sqe_open,
        on_complete: submit::on_complete_open,
        drop: submit::drop_open,
        construct: |user| OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    }, // Kernel 5.6+ (via OpenAt)
    Wakeup {
        field: wakeup,
        payload: WakeupPayload,
        kind: OpKind::Wakeup,
        make_sqe: submit::make_sqe_wakeup,
        on_complete: submit::on_complete_wakeup,
        drop: submit::drop_wakeup,
        construct: |user| WakeupPayload { user, buf: [0; 8] },
        destruct: |user: Box<Wakeup>| *user,
    }, // Kernel 5.6+ (via Read)
    Timeout {
        field: timeout,
        payload: TimeoutPayload,
        kind: OpKind::Timeout,
        make_sqe: submit::make_sqe_timeout,
        on_complete: submit::on_complete_timeout,
        drop: submit::drop_timeout,
        strategy: SubmissionStrategy::SoftwareTimer,
        get_timeout: submit::get_timeout_timeout,
        construct: |user| TimeoutPayload { user, ts: [0; 2] },
        destruct: |user: Box<Timeout>| *user,
    }, // Kernel 5.4+
}
