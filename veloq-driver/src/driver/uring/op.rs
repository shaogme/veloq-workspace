//! io_uring Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `UringOp`: The Type-Erased operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations using blind casting

use crate::driver::PlatformOp;
use crate::driver::uring::UringDriver;
use crate::driver::uring::submit;
use crate::op::{
    Accept, Close, Connect, Fallocate, Fsync, IntoPlatformOp, IoFd, Open, ReadFixed, Recv,
    RecvFrom, Send as OpSend, SendTo, SyncFileRange, Timeout, Wakeup, WriteFixed,
};
use io_uring::squeue;
use std::io;
use std::mem::ManuallyDrop;
use std::time::Duration;

// ============================================================================
// VTable Definition
// ============================================================================

pub type MakeSqeFn = unsafe fn(op: &mut UringOp, driver: &UringDriver) -> squeue::Entry;
pub type OnCompleteFn = unsafe fn(op: &mut UringOp, result: i32) -> io::Result<usize>;
pub type DropFn = unsafe fn(op: &mut UringOp);
pub type GetFdFn = unsafe fn(op: &UringOp) -> Option<IoFd>;
pub type GetTimeoutFn = unsafe fn(op: &UringOp) -> Option<Duration>;

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
}

// ============================================================================
// UringOp Struct & Union (Type-Erased)
// ============================================================================

use std::ptr::NonNull;

#[repr(C)]
pub struct UringOp {
    /// Virtual Table for dynamic dispatch
    pub vtable: NonNull<OpVTable>,

    /// Type-erased payload
    pub payload: UringOpPayload,
}

impl PlatformOp for UringOp {}

impl UringOp {
    pub fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.as_ref().get_fd)(self) }
    }
}

impl Drop for UringOp {
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
                fn into_platform_op(self) -> UringOp {
                    static TABLE: OpVTable = OpVTable {
                        make_sqe: $make_sqe,
                        on_complete: $complete,
                        drop: $drop,
                        get_fd: $get_fd,
                        strategy: define_uring_ops!(@strategy $($strategy)?),
                        get_timeout: define_uring_ops!(@get_timeout $($get_timeout)?),
                    };

                    let payload = define_uring_ops!(@construct self, $($construct)?, $OpType $(, $Payload)?);

                    UringOp {
                        vtable: unsafe { NonNull::new_unchecked(&TABLE as *const _ as *mut _) },
                        payload: UringOpPayload {
                            $field: ManuallyDrop::new(payload),
                        },
                    }
                }

                fn from_platform_op(op: UringOp) -> Self {
                    let op = ManuallyDrop::new(op);
                    let payload = unsafe {
                        ManuallyDrop::into_inner(std::ptr::read(&op.payload.$field))
                    };
                    define_uring_ops!(@destruct payload, $($destruct)?)
                }
            }
        )+
    };

    (@payload_type $OpType:ty) => { $OpType };
    (@payload_type $OpType:ty, $Payload:ty) => { $Payload };

    // Default strategy: SubmitSqe
    (@strategy ) => { SubmissionStrategy::SubmitSqe };
    (@strategy $strategy:expr) => { $strategy };

    // Default get_timeout: return None
    (@get_timeout ) => { submit::get_timeout_none };
    (@get_timeout $func:expr) => { $func };

    // Default construct: return self
    (@construct $self:expr, , $OpType:ty) => { $self };
    // Custom construct
    (@construct $self:expr, $construct:expr, $OpType:ty, $Payload:ty) => { ($construct)($self) };

    // Default destruct: return payload (assumes payload is OpType)
    (@destruct $payload:expr, ) => { $payload };
    // Custom destruct
    (@destruct $payload:expr, $destruct:expr) => { ($destruct)($payload) };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

pub struct AcceptPayload {
    pub op: Accept,
}

pub struct SendToPayload {
    pub op: SendTo,
    pub msg_name: libc::sockaddr_storage,
    pub msg_namelen: libc::socklen_t,
    pub iovec: [libc::iovec; 1],
    pub msghdr: libc::msghdr,
}

pub struct RecvFromPayload {
    pub op: RecvFrom,
    pub msg_name: libc::sockaddr_storage,
    pub msg_namelen: libc::socklen_t,
    pub iovec: [libc::iovec; 1],
    pub msghdr: libc::msghdr,
}

pub struct OpenPayload {
    pub op: Open,
}

pub struct WakeupPayload {
    pub op: Wakeup,
    pub buf: [u8; 8],
}

pub struct TimeoutPayload {
    pub op: Timeout,
    pub ts: [i64; 2],
}

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
    }, // Kernel 5.1+
    WriteFixed {
        field: write,
        make_sqe: submit::make_sqe_write_fixed,
        on_complete: submit::on_complete_write_fixed,
        drop: submit::drop_write_fixed,
        get_fd: submit::get_fd_write_fixed,
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
        construct: |op| AcceptPayload { op },
        destruct: |p: AcceptPayload| p.op,
    }, // Kernel 5.5+
    SendTo {
        field: send_to,
        payload: SendToPayload,
        make_sqe: submit::make_sqe_send_to,
        on_complete: submit::on_complete_send_to,
        drop: submit::drop_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |op: SendTo| {
            let (msg_name, msg_namelen) = crate::socket_addr_to_storage(op.addr);
            SendToPayload {
                op,
                msg_name,
                msg_namelen: msg_namelen as libc::socklen_t,
                iovec: [unsafe { std::mem::zeroed() }],
                msghdr: unsafe { std::mem::zeroed() },
            }
        },
        destruct: |p: SendToPayload| p.op,
    }, // Kernel 5.1+ (via SendMsg)
    RecvFrom {
        field: recv_from,
        payload: RecvFromPayload,
        make_sqe: submit::make_sqe_recv_from,
        on_complete: submit::on_complete_recv_from,
        drop: submit::drop_recv_from,
        get_fd: submit::get_fd_recv_from,
        construct: |op| RecvFromPayload {
            op,
            msg_name: unsafe { std::mem::zeroed() },
            msg_namelen: std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            iovec: [unsafe { std::mem::zeroed() }],
            msghdr: unsafe { std::mem::zeroed() },
        },
        destruct: |p: RecvFromPayload| p.op,
    }, // Kernel 5.1+ (via RecvMsg)
    Open {
        field: open,
        payload: OpenPayload,
        make_sqe: submit::make_sqe_open,
        on_complete: submit::on_complete_open,
        drop: submit::drop_open,
        get_fd: submit::get_fd_open,
        construct: |op| OpenPayload { op },
        destruct: |p: OpenPayload| p.op,
    }, // Kernel 5.6+ (via OpenAt)
    Wakeup {
        field: wakeup,
        payload: WakeupPayload,
        make_sqe: submit::make_sqe_wakeup,
        on_complete: submit::on_complete_wakeup,
        drop: submit::drop_wakeup,
        get_fd: submit::get_fd_wakeup,
        construct: |op| WakeupPayload { op, buf: [0; 8] },
        destruct: |p: WakeupPayload| p.op,
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
        construct: |op| TimeoutPayload { op, ts: [0; 2] },
        destruct: |p: TimeoutPayload| p.op,
    }, // Kernel 5.4+
}
