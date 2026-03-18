pub(crate) mod common;
pub(crate) mod file;
pub(crate) mod net;

use crate::ops::{
    AcceptPayload, Close, Connect, Fallocate, Fsync, KernelRef, OpSend, OpenPayload, Recv,
    SendToPayload, SubmitContext, SyncFileRange, Timeout, UdpRecvStream, UdpRefill, Wakeup,
};
use std::io;

pub(crate) use common::SubmissionResult;
pub(crate) use common::resolve_fd;
pub(crate) use file::*;
pub(crate) use net::*;

macro_rules! impl_get_fd {
    ($fn_name:ident, $payload:ty, direct_fd) => {
        pub(crate) unsafe fn $fn_name(payload: &$payload) -> Option<crate::config::IoFd> {
            // SAFETY: the caller guarantees the payload pointer is valid.
            let user = unsafe { payload.user.as_ref() };
            Some(user.fd)
        }
    };
    ($fn_name:ident, $payload:ty, no_fd) => {
        pub(crate) unsafe fn $fn_name(_payload: &$payload) -> Option<crate::config::IoFd> {
            None
        }
    };
}

impl_get_fd!(
    get_fd_read_fixed,
    KernelRef<crate::ops::ReadFixed>,
    direct_fd
);
impl_get_fd!(
    get_fd_write_fixed,
    KernelRef<crate::ops::WriteFixed>,
    direct_fd
);
impl_get_fd!(get_fd_recv, KernelRef<Recv>, direct_fd);
impl_get_fd!(get_fd_send, KernelRef<OpSend>, direct_fd);
impl_get_fd!(get_fd_connect, KernelRef<Connect>, direct_fd);
impl_get_fd!(get_fd_accept, AcceptPayload, direct_fd);
impl_get_fd!(get_fd_send_to, SendToPayload, direct_fd);
impl_get_fd!(get_fd_open, OpenPayload, no_fd); // Open does not have a direct fd in payload
impl_get_fd!(get_fd_udp_recv_stream, KernelRef<UdpRecvStream>, direct_fd);
impl_get_fd!(get_fd_udp_refill, KernelRef<UdpRefill>, direct_fd);

impl_get_fd!(get_fd_close, KernelRef<Close>, direct_fd);
impl_get_fd!(get_fd_fsync, KernelRef<Fsync>, direct_fd);
impl_get_fd!(get_fd_sync_range, KernelRef<SyncFileRange>, direct_fd);
impl_get_fd!(get_fd_fallocate, KernelRef<Fallocate>, direct_fd);
impl_get_fd!(get_fd_timeout, KernelRef<Timeout>, no_fd);
impl_get_fd!(get_fd_wakeup, KernelRef<Wakeup>, no_fd);

// ============================================================================
// Other Operations
// ============================================================================

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_wakeup(
    _header: &mut crate::ops::OverlappedEntry,
    _payload: &mut KernelRef<Wakeup>,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid.
pub(crate) fn submit_timeout(
    _header: &mut crate::ops::OverlappedEntry,
    payload: &mut KernelRef<Timeout>,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: Dereferencing the user pointer in KernelRef.
    let u = unsafe { payload.user.as_ref() };
    Ok(SubmissionResult::Timer(u.duration))
}
