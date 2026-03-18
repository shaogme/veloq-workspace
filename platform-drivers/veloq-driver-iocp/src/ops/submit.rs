pub(crate) mod common;
pub(crate) mod file;
pub(crate) mod net;

use crate::config::IoFd;
use crate::ops::{
    AcceptPayload, Close, Connect, Fallocate, Fsync, KernelRef, OpenPayload, ReadFixed, Recv,
    SendToPayload, SubmitContext, SyncFileRange, Timeout, UdpRecvStream, UdpRefill, Wakeup,
    WriteFixed,
};
use std::io;

pub(crate) use common::*;
pub(crate) use file::*;
pub(crate) use net::*;

use veloq_driver_core::op::Send as OpSend;

// ============================================================================
// Macros for Lifecycle & FD Access
// ============================================================================

macro_rules! impl_get_fd {
    ($fn_name:ident, $payload_type:ty, direct_fd) => {
        pub(crate) fn $fn_name(payload: &$payload_type) -> Option<IoFd> {
            // SAFETY: Dereferencing the user pointer in KernelRef.
            let u = unsafe { payload.user.as_ref() };
            Some(u.fd)
        }
    };
    ($fn_name:ident, $payload_type:ty, nested_fd) => {
        pub(crate) fn $fn_name(payload: &$payload_type) -> Option<IoFd> {
            // SAFETY: Dereferencing the user pointer in the payload.
            let u = unsafe { payload.user.as_ref() };
            Some(u.fd)
        }
    };
    ($fn_name:ident, $payload_type:ty, no_fd) => {
        pub(crate) fn $fn_name(_payload: &$payload_type) -> Option<IoFd> {
            None
        }
    };
}

// Generate standard get_fd functions
impl_get_fd!(get_fd_read_fixed, KernelRef<ReadFixed>, direct_fd);
impl_get_fd!(get_fd_write_fixed, KernelRef<WriteFixed>, direct_fd);
impl_get_fd!(get_fd_recv, KernelRef<Recv>, direct_fd);
impl_get_fd!(
    get_fd_send,
    KernelRef<OpSend<crate::config::RawHandle>>,
    direct_fd
);
impl_get_fd!(get_fd_connect, KernelRef<Connect>, direct_fd);
impl_get_fd!(get_fd_accept, AcceptPayload, nested_fd);
impl_get_fd!(get_fd_send_to, SendToPayload, nested_fd);
impl_get_fd!(get_fd_udp_recv_stream, KernelRef<UdpRecvStream>, direct_fd);
impl_get_fd!(get_fd_udp_refill, KernelRef<UdpRefill>, direct_fd);
impl_get_fd!(get_fd_open, OpenPayload, no_fd);
impl_get_fd!(get_fd_close, KernelRef<Close>, direct_fd);
impl_get_fd!(get_fd_fsync, KernelRef<Fsync>, direct_fd);
impl_get_fd!(get_fd_sync_range, KernelRef<SyncFileRange>, direct_fd);
impl_get_fd!(get_fd_fallocate, KernelRef<Fallocate>, direct_fd);
impl_get_fd!(get_fd_timeout, KernelRef<Timeout>, no_fd);
impl_get_fd!(get_fd_wakeup, KernelRef<Wakeup>, no_fd);

// ============================================================================
// Other Operations
// ============================================================================

pub(crate) unsafe fn submit_wakeup(
    _header: &mut crate::ops::OverlappedEntry,
    _payload: &mut KernelRef<Wakeup>,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

pub(crate) unsafe fn submit_timeout(
    _header: &mut crate::ops::OverlappedEntry,
    payload: &mut KernelRef<Timeout>,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: Dereferencing the user pointer in KernelRef.
    let u = unsafe { payload.user.as_ref() };
    Ok(SubmissionResult::Timer(u.duration))
}
