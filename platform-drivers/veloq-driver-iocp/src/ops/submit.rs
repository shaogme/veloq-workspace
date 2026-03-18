pub(crate) mod common;
pub(crate) mod file;
pub(crate) mod net;

use std::io;
use std::mem::ManuallyDrop;
use crate::config::IoFd;
use crate::ops::IocpOp;

pub(crate) use common::*;
pub(crate) use file::*;
pub(crate) use net::*;

// ============================================================================
// Macros for Lifecycle & FD Access
// ============================================================================

macro_rules! impl_lifecycle {
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, direct_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            // SAFETY: Calling ManuallyDrop::drop on the union field.
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &IocpOp) -> Option<IoFd> {
            // SAFETY: Accessing the union field with the correct variant.
            let k = unsafe { &*op.payload.$variant };
            // SAFETY: Dereferencing the user pointer in KernelRef.
            let u = unsafe { k.user.as_ref() };
            Some(u.fd)
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, nested_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            // SAFETY: Calling ManuallyDrop::drop on the union field.
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &IocpOp) -> Option<IoFd> {
            // SAFETY: Accessing the union field with the correct variant.
            let k = unsafe { &*op.payload.$variant };
            // SAFETY: Dereferencing the user pointer in the payload.
            let u = unsafe { k.user.as_ref() };
            Some(u.fd)
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, no_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            // SAFETY: Calling ManuallyDrop::drop on the union field.
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(_op: &IocpOp) -> Option<IoFd> {
            None
        }
    };
}

// Generate standard lifecycle functions
impl_lifecycle!(drop_read_fixed, get_fd_read_fixed, read, direct_fd);
impl_lifecycle!(drop_write_fixed, get_fd_write_fixed, write, direct_fd);
impl_lifecycle!(drop_recv, get_fd_recv, recv, direct_fd);
impl_lifecycle!(drop_send, get_fd_send, send, direct_fd);
impl_lifecycle!(drop_connect, get_fd_connect, connect, direct_fd);
impl_lifecycle!(drop_accept, get_fd_accept, accept, nested_fd);
impl_lifecycle!(drop_send_to, get_fd_send_to, send_to, nested_fd);
impl_lifecycle!(
    drop_udp_recv_stream,
    get_fd_udp_recv_stream,
    udp_recv_stream,
    direct_fd
);
impl_lifecycle!(drop_udp_refill, get_fd_udp_refill, udp_refill, direct_fd);
impl_lifecycle!(drop_open, get_fd_open, open, no_fd);
impl_lifecycle!(drop_close, get_fd_close, close, direct_fd);
impl_lifecycle!(drop_fsync, get_fd_fsync, fsync, direct_fd);
impl_lifecycle!(drop_sync_range, get_fd_sync_range, sync_range, direct_fd);
impl_lifecycle!(drop_fallocate, get_fd_fallocate, fallocate, direct_fd);
impl_lifecycle!(drop_timeout, get_fd_timeout, timeout, no_fd);
impl_lifecycle!(drop_wakeup, get_fd_wakeup, wakeup, no_fd);

// ============================================================================
// Other Operations
// ============================================================================

pub(crate) unsafe fn submit_wakeup(
    _op: &mut IocpOp,
    _ctx: &mut crate::ops::SubmitContext,
) -> io::Result<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

pub(crate) unsafe fn submit_timeout(
    op: &mut IocpOp,
    _ctx: &mut crate::ops::SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: Accessing the union field with the correct variant.
    let kernel = unsafe { &op.payload.timeout };
    // SAFETY: Dereferencing the user pointer in KernelRef.
    let u = unsafe { kernel.user.as_ref() };
    Ok(SubmissionResult::Timer(u.duration))
}
