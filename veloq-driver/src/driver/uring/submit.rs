//! io_uring Operation Submission Implementations (Static Functions)
//!
//! This module implements the logic for submitting operations and handling completions,
//! exposed as static functions for VTable construction.

use crate::driver::uring::UringDriver;
use crate::driver::uring::op::UringOp;
use crate::op::IoFd;
use io_uring::{opcode, squeue, types};
use std::io;
use std::mem::ManuallyDrop;

// ============================================================================
// Macros
// ============================================================================

macro_rules! impl_lifecycle {
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, direct_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut UringOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &UringOp) -> Option<IoFd> {
            unsafe { Some(op.payload.$variant.fd) }
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, nested_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut UringOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &UringOp) -> Option<IoFd> {
            unsafe { Some(op.payload.$variant.op.fd) }
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, no_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut UringOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(_op: &UringOp) -> Option<IoFd> {
            None
        }
    };
}

macro_rules! impl_default_completion {
    ($fn_name:ident) => {
        pub(crate) unsafe fn $fn_name(_op: &mut UringOp, result: i32) -> io::Result<usize> {
            if result >= 0 {
                Ok(result as usize)
            } else {
                Err(io::Error::from_raw_os_error(-result))
            }
        }
    };
}

// ============================================================================
// ReadFixed / WriteFixed
// ============================================================================

macro_rules! make_rw_fixed {
    ($fn_name:ident, $field:ident, $type_raw:path, $type_fixed:path) => {
        pub(crate) unsafe fn $fn_name(op: &mut UringOp, driver: &UringDriver) -> squeue::Entry {
            let rw_op = unsafe { &mut *op.payload.$field };
            let (region_idx, _offset) = rw_op.buf.resolve_region_info();
            let ptr = rw_op.buf.as_mut_ptr();
            let len = rw_op.buf.capacity() as u32;

            // Check if chunk is actually registered in kernel
            let is_registered = if region_idx != usize::MAX {
                driver.registered_chunks.get(region_idx).unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_idx as u16;
                match rw_op.fd {
                    IoFd::Raw(fd) => $type_fixed(types::Fd(fd.fd), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build(),
                    IoFd::Fixed(fd_idx) => $type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build(),
                }
            } else {
                // Fallback to standard IO (or if region is MAX)
                match rw_op.fd {
                    IoFd::Raw(fd) => $type_raw(types::Fd(fd.fd), ptr, len)
                        .offset(rw_op.offset)
                        .build(),
                    IoFd::Fixed(fd_idx) => $type_raw(types::Fixed(fd_idx), ptr, len)
                        .offset(rw_op.offset)
                        .build(),
                }
            }
        }
    };
    ($fn_name:ident, $field:ident, $type_raw:path, $type_fixed:path, write) => {
        pub(crate) unsafe fn $fn_name(op: &mut UringOp, driver: &UringDriver) -> squeue::Entry {
            let rw_op = unsafe { &mut *op.payload.$field };
            let (region_idx, _offset) = rw_op.buf.resolve_region_info();
            let ptr = rw_op.buf.as_slice().as_ptr();
            let len = rw_op.buf.len() as u32;

            // Check if chunk is actually registered in kernel
            let is_registered = if region_idx != usize::MAX {
                driver.registered_chunks.get(region_idx).unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_idx as u16;
                match rw_op.fd {
                    IoFd::Raw(fd) => $type_fixed(types::Fd(fd.fd), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build(),
                    IoFd::Fixed(fd_idx) => $type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build(),
                }
            } else {
                match rw_op.fd {
                    IoFd::Raw(fd) => $type_raw(types::Fd(fd.fd), ptr, len)
                        .offset(rw_op.offset)
                        .build(),
                    IoFd::Fixed(fd_idx) => $type_raw(types::Fixed(fd_idx), ptr, len)
                        .offset(rw_op.offset)
                        .build(),
                }
            }
        }
    };
}

make_rw_fixed!(
    make_sqe_read_fixed,
    read,
    opcode::Read::new,
    opcode::ReadFixed::new
);
make_rw_fixed!(
    make_sqe_write_fixed,
    write,
    opcode::Write::new,
    opcode::WriteFixed::new,
    write
);

impl_default_completion!(on_complete_read_fixed);
impl_lifecycle!(drop_read_fixed, get_fd_read_fixed, read, direct_fd);

impl_default_completion!(on_complete_write_fixed);
impl_lifecycle!(drop_write_fixed, get_fd_write_fixed, write, direct_fd);

// ============================================================================
// Recv / Send / Connect / Accept
// ============================================================================

macro_rules! make_buf_op {
    ($fn_name:ident, $field:ident, $opcode:path, recv_args) => {
        pub(crate) unsafe fn $fn_name(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
            let val = unsafe { &mut *op.payload.$field };
            match val.fd {
                IoFd::Raw(fd) => $opcode(
                    types::Fd(fd.fd),
                    val.buf.as_mut_ptr(),
                    val.buf.capacity() as u32,
                )
                .build(),
                IoFd::Fixed(idx) => $opcode(
                    types::Fixed(idx),
                    val.buf.as_mut_ptr(),
                    val.buf.capacity() as u32,
                )
                .build(),
            }
        }
    };
    ($fn_name:ident, $field:ident, $opcode:path, send_args) => {
        pub(crate) unsafe fn $fn_name(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
            let val = unsafe { &mut *op.payload.$field };
            match val.fd {
                IoFd::Raw(fd) => $opcode(
                    types::Fd(fd.fd),
                    val.buf.as_slice().as_ptr(),
                    val.buf.len() as u32,
                )
                .build(),
                IoFd::Fixed(idx) => $opcode(
                    types::Fixed(idx),
                    val.buf.as_slice().as_ptr(),
                    val.buf.len() as u32,
                )
                .build(),
            }
        }
    };
}

make_buf_op!(make_sqe_recv, recv, opcode::Recv::new, recv_args);
impl_default_completion!(on_complete_recv);
impl_lifecycle!(drop_recv, get_fd_recv, recv, direct_fd);

make_buf_op!(make_sqe_send, send, opcode::Send::new, send_args);
impl_default_completion!(on_complete_send);
impl_lifecycle!(drop_send, get_fd_send, send, direct_fd);

pub(crate) unsafe fn make_sqe_connect(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let val = unsafe { &mut *op.payload.connect };
    match val.fd {
        IoFd::Raw(fd) => opcode::Connect::new(
            types::Fd(fd.fd),
            &val.addr as *const _ as *const _,
            val.addr_len,
        )
        .build(),
        IoFd::Fixed(idx) => opcode::Connect::new(
            types::Fixed(idx),
            &val.addr as *const _ as *const _,
            val.addr_len,
        )
        .build(),
    }
}
impl_default_completion!(on_complete_connect);
impl_lifecycle!(drop_connect, get_fd_connect, connect, direct_fd);

pub(crate) unsafe fn make_sqe_accept(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let val = unsafe { &mut (*op.payload.accept).op };
    match val.fd {
        IoFd::Raw(fd) => opcode::Accept::new(
            types::Fd(fd.fd),
            &mut val.addr as *mut _ as *mut _,
            &mut val.addr_len as *mut _,
        )
        .build(),
        IoFd::Fixed(idx) => opcode::Accept::new(
            types::Fixed(idx),
            &mut val.addr as *mut _ as *mut _,
            &mut val.addr_len as *mut _,
        )
        .build(),
    }
}

pub(crate) unsafe fn on_complete_accept(op: &mut UringOp, result: i32) -> io::Result<usize> {
    if result >= 0 {
        let accept_op = unsafe { &mut (*op.payload.accept).op };
        // Try fallback parsing to populate remote_addr early
        let addr_bytes = unsafe {
            std::slice::from_raw_parts(
                &accept_op.addr as *const _ as *const u8,
                accept_op.addr_len as usize,
            )
        };
        if let Ok(addr) = crate::to_socket_addr(addr_bytes) {
            accept_op.remote_addr = Some(addr);
        }
        Ok(result as usize)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}

impl_lifecycle!(drop_accept, get_fd_accept, accept, nested_fd);

// ============================================================================
// SendTo
// ============================================================================

pub(crate) unsafe fn make_sqe_send_to(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let payload = unsafe { &mut *op.payload.send_to };

    // Initialize internal pointers
    payload.iovec[0].iov_base = payload.op.buf.as_slice().as_ptr() as *mut _;
    payload.iovec[0].iov_len = payload.op.buf.len();

    payload.msghdr.msg_name = &mut payload.msg_name as *mut _ as *mut libc::c_void;
    payload.msghdr.msg_namelen = payload.msg_namelen;
    payload.msghdr.msg_iov = payload.iovec.as_mut_ptr();
    payload.msghdr.msg_iovlen = 1;
    // msg_control already zeroed

    match payload.op.fd {
        IoFd::Raw(fd) => {
            opcode::SendMsg::new(types::Fd(fd.fd), &payload.msghdr as *const _).build()
        }
        IoFd::Fixed(idx) => {
            opcode::SendMsg::new(types::Fixed(idx), &payload.msghdr as *const _).build()
        }
    }
}

impl_default_completion!(on_complete_send_to);
impl_lifecycle!(drop_send_to, get_fd_send_to, send_to, nested_fd);

// ============================================================================
// RecvFrom
// ============================================================================

pub(crate) unsafe fn make_sqe_recv_from(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let payload = unsafe { &mut *op.payload.recv_from };

    // Initialize internal pointers
    payload.iovec[0].iov_base = payload.op.buf.as_mut_ptr() as *mut _;
    payload.iovec[0].iov_len = payload.op.buf.capacity();

    payload.msghdr.msg_name = &mut payload.msg_name as *mut _ as *mut libc::c_void;
    payload.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
    payload.msghdr.msg_iov = payload.iovec.as_mut_ptr();
    payload.msghdr.msg_iovlen = 1;

    match payload.op.fd {
        IoFd::Raw(fd) => {
            opcode::RecvMsg::new(types::Fd(fd.fd), &mut payload.msghdr as *mut _).build()
        }
        IoFd::Fixed(idx) => {
            opcode::RecvMsg::new(types::Fixed(idx), &mut payload.msghdr as *mut _).build()
        }
    }
}

pub(crate) unsafe fn on_complete_recv_from(op: &mut UringOp, result: i32) -> io::Result<usize> {
    if result >= 0 {
        let payload = unsafe { &mut *op.payload.recv_from };
        let len = payload.msghdr.msg_namelen as usize;
        let addr_bytes =
            unsafe { std::slice::from_raw_parts(&payload.msg_name as *const _ as *const u8, len) };
        if let Ok(addr) = crate::to_socket_addr(addr_bytes) {
            payload.op.addr = Some(addr);
        }
        Ok(result as usize)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    }
}

impl_lifecycle!(drop_recv_from, get_fd_recv_from, recv_from, nested_fd);

// ============================================================================
// Close
// ============================================================================

pub(crate) unsafe fn make_sqe_close(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let close_op = unsafe { &mut *op.payload.close };
    match close_op.fd {
        IoFd::Raw(fd) => opcode::Close::new(types::Fd(fd.fd)).build(),
        IoFd::Fixed(idx) => opcode::Close::new(types::Fixed(idx)).build(),
    }
}

impl_default_completion!(on_complete_close);
impl_lifecycle!(drop_close, get_fd_close, close, direct_fd);

// ============================================================================
// Fsync
// ============================================================================

pub(crate) unsafe fn make_sqe_fsync(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let fsync_op = unsafe { &mut *op.payload.fsync };
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    match fsync_op.fd {
        IoFd::Raw(fd) => opcode::Fsync::new(types::Fd(fd.fd)).flags(flags).build(),
        IoFd::Fixed(idx) => opcode::Fsync::new(types::Fixed(idx)).flags(flags).build(),
    }
}

impl_default_completion!(on_complete_fsync);
impl_lifecycle!(drop_fsync, get_fd_fsync, fsync, direct_fd);

// ============================================================================
// SyncFileRange
// ============================================================================

pub(crate) unsafe fn make_sqe_sync_range(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let sync_op = unsafe { &mut *op.payload.sync_range };
    match sync_op.fd {
        IoFd::Raw(fd) => opcode::SyncFileRange::new(types::Fd(fd.fd), sync_op.nbytes as u32)
            .offset(sync_op.offset)
            .flags(sync_op.flags)
            .build(),
        IoFd::Fixed(idx) => opcode::SyncFileRange::new(types::Fixed(idx), sync_op.nbytes as u32)
            .offset(sync_op.offset)
            .flags(sync_op.flags)
            .build(),
    }
}

impl_default_completion!(on_complete_sync_range);
impl_lifecycle!(drop_sync_range, get_fd_sync_range, sync_range, direct_fd);

// ============================================================================
// Fallocate
// ============================================================================

pub(crate) unsafe fn make_sqe_fallocate(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let fallocate_op = unsafe { &mut *op.payload.fallocate };
    match fallocate_op.fd {
        IoFd::Raw(fd) => opcode::Fallocate::new(types::Fd(fd.fd), fallocate_op.len)
            .offset(fallocate_op.offset)
            .mode(fallocate_op.mode)
            .build(),
        IoFd::Fixed(idx) => opcode::Fallocate::new(types::Fixed(idx), fallocate_op.len)
            .offset(fallocate_op.offset)
            .mode(fallocate_op.mode)
            .build(),
    }
}

impl_default_completion!(on_complete_fallocate);
impl_lifecycle!(drop_fallocate, get_fd_fallocate, fallocate, direct_fd);

// ============================================================================
// Open
// ============================================================================

pub(crate) unsafe fn make_sqe_open(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let payload = unsafe { &mut *op.payload.open };
    let path_ptr = payload.op.path.as_slice().as_ptr() as *const _;
    opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
        .flags(payload.op.flags)
        .mode(payload.op.mode)
        .build()
}

impl_default_completion!(on_complete_open);
impl_lifecycle!(drop_open, get_fd_open, open, no_fd);

// ============================================================================
// Timeout
// ============================================================================

pub(crate) unsafe fn make_sqe_timeout(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let payload = unsafe { &mut *op.payload.timeout };

    payload.ts[0] = payload.op.duration.as_secs() as i64;
    payload.ts[1] = payload.op.duration.subsec_nanos() as i64;
    let ts_ptr = payload.ts.as_ptr() as *const types::Timespec;

    opcode::Timeout::new(ts_ptr).build()
}

impl_default_completion!(on_complete_timeout);
impl_lifecycle!(drop_timeout, get_fd_timeout, timeout, no_fd);

// ============================================================================
// Wakeup
// ============================================================================

pub(crate) unsafe fn make_sqe_wakeup(op: &mut UringOp, _driver: &UringDriver) -> squeue::Entry {
    let payload = unsafe { &mut *op.payload.wakeup };
    match payload.op.fd {
        IoFd::Raw(fd) => opcode::Read::new(types::Fd(fd.fd), payload.buf.as_mut_ptr(), 8).build(),
        _ => panic!("Wakeup only supports raw fd"),
    }
}

impl_default_completion!(on_complete_wakeup);
impl_lifecycle!(drop_wakeup, get_fd_wakeup, wakeup, no_fd);

// ============================================================================
// VTable Helpers
// ============================================================================

pub(crate) unsafe fn get_timeout_timeout(op: &UringOp) -> Option<std::time::Duration> {
    let payload = unsafe { &*op.payload.timeout };
    Some(payload.op.duration)
}

pub(crate) unsafe fn get_timeout_none(_op: &UringOp) -> Option<std::time::Duration> {
    None
}
