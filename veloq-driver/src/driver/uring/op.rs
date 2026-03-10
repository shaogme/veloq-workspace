//! io_uring Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `UringKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

use crate::driver::PlatformOp;
use crate::driver::uring::UringDriver;
use crate::driver::uring::submit;
use crate::op::{
    Accept, Close, Connect, Fallocate, Fsync, IntoPlatformOp, IoFd, Open, ReadFixed, Recv,
    Send as OpSend, SendTo, SharedUserPayload, SyncFileRange, Timeout, UdpRecvStream, UdpRefill,
    Wakeup, WriteFixed,
};
use io_uring::squeue;
use std::io;
use std::mem::ManuallyDrop;
use std::time::Duration;

// ============================================================================
// VTable Definition
// ============================================================================

pub type MakeSqeFn = unsafe fn(op: &mut UringKernelOp, driver: &mut UringDriver) -> squeue::Entry;
pub type OnCompleteFn = unsafe fn(op: &mut UringKernelOp, result: i32) -> io::Result<usize>;
pub type DropFn = unsafe fn(op: &mut UringKernelOp);
pub type GetFdFn = unsafe fn(op: &UringKernelOp) -> Option<IoFd>;
pub type GetTimeoutFn = unsafe fn(op: &UringKernelOp) -> Option<Duration>;
pub type ResolveChunksFn = unsafe fn(op: &UringKernelOp, chunks: &mut [u16]) -> usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmissionStrategy {
    /// Submit a Standard SQE to the ring
    SubmitSqe,
    /// Handled by software timer wheel (no SQE submitted)
    SoftwareTimer,
    /// Only for background operations (e.g. Close)
    BackgroundOnly,
}

pub struct OpVTable {
    pub make_sqe: MakeSqeFn,
    pub on_complete: OnCompleteFn,
    pub drop: DropFn,
    pub get_fd: GetFdFn,
    pub strategy: SubmissionStrategy,
    pub get_timeout: GetTimeoutFn,
    pub resolve_chunks: ResolveChunksFn,
}

// ============================================================================
// UringKernelOp Struct & Union (Type-Erased)
// ============================================================================

use std::ptr::NonNull;

#[repr(C)]
pub struct UringKernelOp {
    /// Virtual Table for dynamic dispatch
    pub vtable: NonNull<OpVTable>,

    /// Type-erased payload
    pub payload: UringOpPayload,
}

impl PlatformOp for UringKernelOp {}

impl UringKernelOp {
    pub fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.as_ref().get_fd)(self) }
    }
}

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
                make_sqe: $make_sqe:path,
                on_complete: $complete:path,
                drop: $drop:path,
                get_fd: $get_fd:path,
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
        pub union UringOpPayload {
            $(
                pub $field: ManuallyDrop< define_uring_ops!(@payload_type $OpType $(, $Payload)?) >,
            )+
        }

        $(
            impl IntoPlatformOp<UringDriver> for $OpType {
                type UserPayload = SharedUserPayload<$OpType>;

                fn into_kernel_and_payload(self) -> (UringKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        make_sqe: $make_sqe,
                        on_complete: $complete,
                        drop: $drop,
                        get_fd: $get_fd,
                        strategy: define_uring_ops!(@strategy $($strategy)?),
                        get_timeout: define_uring_ops!(@get_timeout $($get_timeout)?),
                        resolve_chunks: define_uring_ops!(@resolve_chunks $($resolve_chunks)?),
                    };

                    let user = SharedUserPayload::new(self);
                    let user_ptr = user.user_ptr();
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
    (@destruct $user_payload:expr, ) => { $user_payload.into_inner() };
    // Custom destruct
    (@destruct $user_payload:expr, $destruct:expr) => { ($destruct)($user_payload) };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

pub struct KernelRef<T> {
    pub user: NonNull<T>,
}

pub struct AcceptPayload {
    pub user: NonNull<Accept>,
}

pub struct SendToPayload {
    pub user: NonNull<SendTo>,
    pub msg_name: libc::sockaddr_storage,
    pub msg_namelen: libc::socklen_t,
    pub iovec: [libc::iovec; 1],
    pub msghdr: libc::msghdr,
}

pub struct UdpRecvStreamPayload {
    pub user: NonNull<UdpRecvStream>,
    pub msg_name: libc::sockaddr_storage,
    pub msg_namelen: libc::socklen_t,
    pub iovec: [libc::iovec; 1],
    pub msghdr: libc::msghdr,
}

pub struct OpenPayload {
    pub user: NonNull<Open>,
}

pub struct WakeupPayload {
    pub user: NonNull<Wakeup>,
    pub buf: [u8; 8],
}

pub struct TimeoutPayload {
    pub user: NonNull<Timeout>,
    pub ts: [i64; 2],
}

pub type UringOp = UringKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

define_uring_ops! {
    ReadFixed {
        field: read,
        make_sqe: submit::make_sqe_read_fixed,
        on_complete: submit::on_complete_read_fixed,
        drop: submit::drop_read_fixed,
        get_fd: submit::get_fd_read_fixed,
        resolve_chunks: submit::resolve_chunks_read_fixed,
    }, // Kernel 5.1+
    WriteFixed {
        field: write,
        make_sqe: submit::make_sqe_write_fixed,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        get_fd: submit::get_fd_write_fixed,
        resolve_chunks: submit::resolve_chunks_write_fixed,
    }, // Kernel 5.1+
    Recv {
        field: recv,
        make_sqe: submit::make_sqe_recv,
        on_complete: submit::on_complete_recv,
        drop: submit::drop_recv,
        get_fd: submit::get_fd_recv,
    }, // Kernel 5.6+
    OpSend {
        field: send,
        make_sqe: submit::make_sqe_send,
        on_complete: submit::on_complete_send,
        drop: submit::drop_send,
        get_fd: submit::get_fd_send,
    }, // Kernel 5.6+
    Connect {
        field: connect,
        make_sqe: submit::make_sqe_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
        get_fd: submit::get_fd_connect,
    }, // Kernel 5.5+
    Close {
        field: close,
        make_sqe: submit::make_sqe_close,
        on_complete: submit::on_complete_close,
        drop: submit::drop_close,
        get_fd: submit::get_fd_close,
        strategy: SubmissionStrategy::BackgroundOnly,
    }, // Kernel 5.6+
    Fsync {
        field: fsync,
        make_sqe: submit::make_sqe_fsync,
        on_complete: submit::on_complete_fsync,
        drop: submit::drop_fsync,
        get_fd: submit::get_fd_fsync,
    }, // Kernel 5.1+
    SyncFileRange {
        field: sync_range,
        make_sqe: submit::make_sqe_sync_range,
        on_complete: submit::on_complete_sync_range,
        drop: submit::drop_sync_range,
        get_fd: submit::get_fd_sync_range,
    }, // Kernel 5.2+
    Fallocate {
        field: fallocate,
        make_sqe: submit::make_sqe_fallocate,
        on_complete: submit::on_complete_fallocate,
        drop: submit::drop_fallocate,
        get_fd: submit::get_fd_fallocate,
    }, // Kernel 5.6+
    Accept {
        field: accept,
        payload: AcceptPayload,
        make_sqe: submit::make_sqe_accept,
        on_complete: submit::on_complete_accept,
        drop: submit::drop_accept,
        get_fd: submit::get_fd_accept,
        construct: |user| AcceptPayload { user },
        destruct: |user: SharedUserPayload<Accept>| user.into_inner(),
    }, // Kernel 5.5+
    SendTo {
        field: send_to,
        payload: SendToPayload,
        make_sqe: submit::make_sqe_send_to,
        on_complete: submit::on_complete_send_to,
        drop: submit::drop_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            let op = unsafe { user.as_ref() };
            let (msg_name, msg_namelen) = crate::socket_addr_to_storage(op.addr);
            SendToPayload {
                user,
                msg_name,
                msg_namelen: msg_namelen as libc::socklen_t,
                iovec: [unsafe { std::mem::zeroed() }],
                msghdr: unsafe { std::mem::zeroed() },
            }
        },
        destruct: |user: SharedUserPayload<SendTo>| user.into_inner(),
    }, // Kernel 5.1+ (via SendMsg)
    UdpRecvStream {
        field: udp_recv_stream,
        payload: UdpRecvStreamPayload,
        make_sqe: submit::make_sqe_udp_recv_stream,
        on_complete: submit::on_complete_udp_recv_stream,
        drop: submit::drop_udp_recv_stream,
        get_fd: submit::get_fd_udp_recv_stream,
        construct: |user| UdpRecvStreamPayload {
            user,
            msg_name: unsafe { std::mem::zeroed() },
            msg_namelen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            iovec: [unsafe { std::mem::zeroed() }],
            msghdr: unsafe { std::mem::zeroed() },
        },
        destruct: |user: SharedUserPayload<UdpRecvStream>| user.into_inner(),
    }, // Kernel 5.1+ (via RecvMsg)
    UdpRefill {
        field: udp_refill,
        make_sqe: submit::make_sqe_udp_refill,
        on_complete: submit::on_complete_udp_refill,
        drop: submit::drop_udp_refill,
        get_fd: submit::get_fd_udp_refill,
    }, // No-op on io_uring, kept for cross-platform API parity
    Open {
        field: open,
        payload: OpenPayload,
        make_sqe: submit::make_sqe_open,
        on_complete: submit::on_complete_open,
        drop: submit::drop_open,
        get_fd: submit::get_fd_open,
        construct: |user| OpenPayload { user },
        destruct: |user: SharedUserPayload<Open>| user.into_inner(),
    }, // Kernel 5.6+ (via OpenAt)
    Wakeup {
        field: wakeup,
        payload: WakeupPayload,
        make_sqe: submit::make_sqe_wakeup,
        on_complete: submit::on_complete_wakeup,
        drop: submit::drop_wakeup,
        get_fd: submit::get_fd_wakeup,
        construct: |user| WakeupPayload { user, buf: [0; 8] },
        destruct: |user: SharedUserPayload<Wakeup>| user.into_inner(),
    }, // Kernel 5.6+ (via Read)
    Timeout {
        field: timeout,
        payload: TimeoutPayload,
        make_sqe: submit::make_sqe_timeout,
        on_complete: submit::on_complete_timeout,
        drop: submit::drop_timeout,
        get_fd: submit::get_fd_timeout,
        strategy: SubmissionStrategy::SoftwareTimer,
        get_timeout: submit::get_timeout_timeout,
        construct: |user| TimeoutPayload { user, ts: [0; 2] },
        destruct: |user: SharedUserPayload<Timeout>| user.into_inner(),
    }, // Kernel 5.4+
}
