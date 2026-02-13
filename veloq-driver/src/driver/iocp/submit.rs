//! IOCP Operation Submission Logic (Static Functions)
//!
//! This module implements the logic for submitting operations, handling completions,
//! and accessing FDs, exposed as static functions for VTable construction.

use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::op::{IocpOp, SubmitContext};
use crate::op::IoFd;
use std::io;
use std::mem::ManuallyDrop;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;
use windows_sys::Win32::Foundation::{ERROR_IO_PENDING, GetLastError, HANDLE};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, SO_UPDATE_ACCEPT_CONTEXT, SO_UPDATE_CONNECT_CONTEXT, SOCKADDR, SOCKADDR_IN,
    SOCKADDR_IN6, SOCKADDR_STORAGE, SOCKET, SOCKET_ERROR, SOL_SOCKET, WSAGetLastError, WSARecvFrom,
    WSASendTo, bind, getsockname, setsockopt,
};
use windows_sys::Win32::Storage::FileSystem::{ReadFile, WriteFile};
use windows_sys::Win32::System::IO::CreateIoCompletionPort;

use veloq_blocking::BlockingTask;
use veloq_blocking::blocking_ops::windows::{BlockingOps, CompletionInfo};
use veloq_buf::FixedBuf;

// ============================================================================
// Macros
// ============================================================================

macro_rules! impl_lifecycle {
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, direct_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &IocpOp) -> Option<IoFd> {
            unsafe { Some(op.payload.$variant.fd) }
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, nested_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(op: &IocpOp) -> Option<IoFd> {
            unsafe { Some(op.payload.$variant.op.fd) }
        }
    };
    ($drop_fn:ident, $get_fd_fn:ident, $variant:ident, no_fd) => {
        pub(crate) unsafe fn $drop_fn(op: &mut IocpOp) {
            unsafe {
                ManuallyDrop::drop(&mut op.payload.$variant);
            }
        }

        pub(crate) unsafe fn $get_fd_fn(_op: &IocpOp) -> Option<IoFd> {
            None
        }
    };
}

macro_rules! impl_blocking_offload {
    ($fn_name:ident, $variant:ident, $enum_variant:ident) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut IocpOp,
            ctx: &mut SubmitContext,
        ) -> io::Result<SubmissionResult> {
            let payload = unsafe { &*op.payload.$variant };
            let handle = resolve_fd(payload.fd, ctx.registered_files)?;

            let entry = &op.header;
            let user_data = entry.user_data;

            // CompletionInfo now uses ctx.overlapped address which is from Slot
            let completion = CompletionInfo {
                port: ctx.port as usize,
                user_data,
                overlapped: ctx.overlapped as usize,
            };

            let op = BlockingOps::$enum_variant {
                handle: handle as usize,
                completion,
            };
            Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
        }
    };
}

// ============================================================================
// Submission Result
// ============================================================================

pub enum SubmissionResult {
    Pending,
    PostToQueue,
    Offload(BlockingTask),
    Timer(Duration),
}

// ============================================================================
// Helper Functions
// ============================================================================

pub(crate) fn resolve_fd(fd: IoFd, registered_files: &[Option<HANDLE>]) -> io::Result<HANDLE> {
    match fd {
        IoFd::Raw(h) => Ok(h.into()),
        IoFd::Fixed(idx) => {
            if let Some(Some(h)) = registered_files.get(idx as usize) {
                Ok(*h)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Invalid registered file descriptor",
                ))
            }
        }
    }
}

// ============================================================================
// Read/Write
// ============================================================================

macro_rules! submit_io_op {
    ($fn_name:ident, $field:ident, $win_api:ident, offset, $ptr_fn:expr) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut IocpOp,
            ctx: &mut SubmitContext,
        ) -> io::Result<SubmissionResult> {
            let val = unsafe { &mut *op.payload.$field };
            // Using ctx.overlapped (Slot Overlapped)
            let overlapped = unsafe { &mut *ctx.overlapped };

            overlapped.Anonymous.Anonymous.Offset = val.offset as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (val.offset >> 32) as u32;

            let handle = resolve_fd(val.fd, ctx.registered_files)?;
            unsafe {
                CreateIoCompletionPort(handle, ctx.port, 0, 0);
            }

            let mut bytes = 0;
            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            let ptr = get_ptr(&mut val.buf);

            let ret = unsafe {
                $win_api(
                    handle,
                    ptr as _,
                    val.buf.len() as u32, // using len() which is common for buf/slice
                    &mut bytes,
                    ctx.overlapped,
                )
            };

            if ret == 0 {
                let err = unsafe { GetLastError() };
                if err != ERROR_IO_PENDING {
                    return Err(io::Error::from_raw_os_error(err as i32));
                }
            }
            Ok(SubmissionResult::Pending)
        }
    };
}

submit_io_op!(
    submit_read_fixed,
    read,
    ReadFile,
    offset,
    |b: &mut FixedBuf| b.as_mut_ptr()
);
impl_lifecycle!(drop_read_fixed, get_fd_read_fixed, read, direct_fd);

submit_io_op!(
    submit_write_fixed,
    write,
    WriteFile,
    offset,
    |b: &mut FixedBuf| b.as_slice().as_ptr() as *mut u8
);
impl_lifecycle!(drop_write_fixed, get_fd_write_fixed, write, direct_fd);

pub(crate) unsafe fn submit_recv(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let val = unsafe { &mut *op.payload.recv };

    let overlapped = unsafe { &mut *ctx.overlapped };
    overlapped.Anonymous.Anonymous.Offset = 0;
    overlapped.Anonymous.Anonymous.OffsetHigh = 0;

    let handle = resolve_fd(val.fd, ctx.registered_files)?;

    // Try RIO upgrade
    if let Some(rio) = &mut ctx.rio {
        if let Some(res) = rio.try_submit_recv(val.fd, handle, &mut val.buf, op.header.user_data)? {
            return Ok(res);
        }
    }

    // Fallback to normal IOCP
    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    let mut bytes = 0;
    let ptr = val.buf.as_mut_ptr();

    let ret = unsafe {
        ReadFile(
            handle,
            ptr as _,
            val.buf.len() as u32,
            &mut bytes,
            ctx.overlapped,
        )
    };

    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}
impl_lifecycle!(drop_recv, get_fd_recv, recv, direct_fd);

pub(crate) unsafe fn submit_send(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let val = unsafe { &mut *op.payload.send };

    let overlapped = unsafe { &mut *ctx.overlapped };
    overlapped.Anonymous.Anonymous.Offset = 0;
    overlapped.Anonymous.Anonymous.OffsetHigh = 0;

    let handle = resolve_fd(val.fd, ctx.registered_files)?;

    // Try RIO upgrade
    if let Some(rio) = &mut ctx.rio {
        if let Some(res) = rio.try_submit_send(val.fd, handle, &val.buf, op.header.user_data)? {
            return Ok(res);
        }
    }

    // Fallback
    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    let mut bytes = 0;
    let ptr = val.buf.as_slice().as_ptr() as *mut u8;

    let ret = unsafe {
        WriteFile(
            handle,
            ptr as _,
            val.buf.len() as u32,
            &mut bytes,
            ctx.overlapped,
        )
    };

    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}
impl_lifecycle!(drop_send, get_fd_send, send, direct_fd);

// ============================================================================
// Connect
// ============================================================================

pub(crate) unsafe fn submit_connect(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let connect_op = unsafe { &mut *op.payload.connect };
    let handle = resolve_fd(connect_op.fd, ctx.registered_files)?;
    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    let mut need_bind = true;
    let mut name: SOCKADDR_STORAGE = unsafe { std::mem::zeroed() };
    let mut namelen = std::mem::size_of::<SOCKADDR_STORAGE>() as i32;

    let ret = unsafe {
        getsockname(
            handle as SOCKET,
            &mut name as *mut _ as *mut SOCKADDR,
            &mut namelen,
        )
    };

    if ret == 0 {
        let family = name.ss_family;
        if family == AF_INET {
            let addr_in = unsafe { &*(&name as *const _ as *const SOCKADDR_IN) };
            if addr_in.sin_port != 0 {
                need_bind = false;
            }
        } else if family == AF_INET6 {
            let addr_in6 = unsafe { &*(&name as *const _ as *const SOCKADDR_IN6) };
            if addr_in6.sin6_port != 0 {
                need_bind = false;
            }
        }
    }

    if need_bind {
        let family = connect_op.addr.ss_family;
        let mut bind_addr: SOCKADDR_IN = unsafe { std::mem::zeroed() };
        bind_addr.sin_family = AF_INET;
        let mut bind_addr6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
        bind_addr6.sin6_family = AF_INET6;

        let (ptr, len) = if family == AF_INET {
            (
                &bind_addr as *const _ as *const SOCKADDR,
                std::mem::size_of::<SOCKADDR_IN>() as i32,
            )
        } else {
            (
                &bind_addr6 as *const _ as *const SOCKADDR,
                std::mem::size_of::<SOCKADDR_IN6>() as i32,
            )
        };
        unsafe {
            bind(handle as SOCKET, ptr, len);
        }
    }

    let mut bytes_sent = 0;
    let ret = unsafe {
        (ctx.ext.connect_ex)(
            handle as SOCKET,
            &connect_op.addr as *const _ as *const SOCKADDR,
            connect_op.addr_len as i32,
            std::ptr::null(),
            0,
            &mut bytes_sent,
            ctx.overlapped,
        )
    };

    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

pub(crate) unsafe fn on_complete_connect(
    op: &mut IocpOp,
    result: usize,
    _ext: &Extensions,
) -> io::Result<usize> {
    let connect_op = unsafe { &*op.payload.connect };
    if let Some(fd) = connect_op.fd.raw() {
        let ret = unsafe {
            setsockopt(
                fd.into(),
                SOL_SOCKET,
                SO_UPDATE_CONNECT_CONTEXT,
                std::ptr::null(),
                0,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(result)
}

impl_lifecycle!(drop_connect, get_fd_connect, connect, direct_fd);

// ============================================================================
// Accept
// ============================================================================

pub(crate) unsafe fn submit_accept(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &mut *op.payload.accept };
    let handle = resolve_fd(payload.op.fd, ctx.registered_files)?;
    let accept_socket = payload.op.accept_socket;

    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    const MIN_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
    let split = MIN_ADDR_LEN;
    let mut bytes_received = 0;

    let ret = unsafe {
        (ctx.ext.accept_ex)(
            handle as SOCKET,
            accept_socket.into(),
            payload.accept_buffer.as_mut_ptr() as *mut _,
            0,
            split as u32,
            split as u32,
            &mut bytes_received,
            ctx.overlapped,
        )
    };

    if ret == 0 {
        let err = unsafe { GetLastError() };
        if err != ERROR_IO_PENDING {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

pub(crate) unsafe fn on_complete_accept(
    op: &mut IocpOp,
    result: usize,
    ext: &Extensions,
) -> io::Result<usize> {
    let payload = unsafe { &mut *op.payload.accept };
    let accept_socket = payload.op.accept_socket;
    let listen_handle = payload.op.fd.raw().ok_or(io::Error::from_raw_os_error(0))?;

    let ret = unsafe {
        setsockopt(
            accept_socket.into(),
            SOL_SOCKET,
            SO_UPDATE_ACCEPT_CONTEXT,
            &listen_handle as *const _ as *const _,
            std::mem::size_of_val(&listen_handle) as i32,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }

    const MIN_ADDR_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
    let split = MIN_ADDR_LEN;

    let mut local_sockaddr: *mut SOCKADDR = std::ptr::null_mut();
    let mut remote_sockaddr: *mut SOCKADDR = std::ptr::null_mut();
    let mut local_len: i32 = 0;
    let mut remote_len: i32 = 0;

    unsafe {
        (ext.get_accept_ex_sockaddrs)(
            payload.accept_buffer.as_ptr() as *const _,
            0,
            split as u32,
            split as u32,
            &mut local_sockaddr,
            &mut local_len,
            &mut remote_sockaddr,
            &mut remote_len,
        );
    }

    if !remote_sockaddr.is_null() && remote_len > 0 {
        let family = unsafe { (*remote_sockaddr).sa_family };
        if family == AF_INET {
            let addr_in = unsafe { &*(remote_sockaddr as *const SOCKADDR_IN) };
            let ip = Ipv4Addr::from(unsafe { addr_in.sin_addr.S_un.S_addr.to_ne_bytes() });
            let port = u16::from_be(addr_in.sin_port);
            payload.op.remote_addr = Some(SocketAddr::V4(SocketAddrV4::new(ip, port)));
        } else if family == AF_INET6 {
            let addr_in6 = unsafe { &*(remote_sockaddr as *const SOCKADDR_IN6) };
            let ip = Ipv6Addr::from(unsafe { addr_in6.sin6_addr.u.Byte });
            let port = u16::from_be(addr_in6.sin6_port);
            let flowinfo = addr_in6.sin6_flowinfo;
            let scope_id = unsafe { addr_in6.Anonymous.sin6_scope_id };
            payload.op.remote_addr = Some(SocketAddr::V6(SocketAddrV6::new(
                ip, port, flowinfo, scope_id,
            )));
        }
    }
    Ok(result)
}

impl_lifecycle!(drop_accept, get_fd_accept, accept, nested_fd);

// ============================================================================
// SendTo
// ============================================================================

pub(crate) unsafe fn submit_send_to(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &mut *op.payload.send_to };
    let handle = resolve_fd(payload.op.fd, ctx.registered_files)?;

    // Try RIO upgrade logic
    if let Some(rio) = &mut ctx.rio {
        if let Some(res) = rio.try_submit_send_to(
            payload.op.fd,
            handle,
            &payload.op.buf,
            &payload.addr as *const _ as *const std::ffi::c_void,
            payload.addr_len,
            op.header.user_data,
        )? {
            return Ok(res);
        }
    }

    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    payload.wsabuf.len = payload.op.buf.len() as u32;
    payload.wsabuf.buf = payload.op.buf.as_slice().as_ptr() as *mut u8;

    let mut bytes = 0;
    let ret = unsafe {
        WSASendTo(
            handle as SOCKET,
            &payload.wsabuf,
            1,
            &mut bytes,
            0,
            &payload.addr as *const _ as *const SOCKADDR,
            payload.addr_len,
            ctx.overlapped,
            None,
        )
    };

    if ret == SOCKET_ERROR {
        let err = unsafe { WSAGetLastError() };
        if err != ERROR_IO_PENDING as i32 {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

impl_lifecycle!(drop_send_to, get_fd_send_to, send_to, nested_fd);

// ============================================================================
// RecvFrom
// ============================================================================

pub(crate) unsafe fn submit_recv_from(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &mut *op.payload.recv_from };
    let handle = resolve_fd(payload.op.fd, ctx.registered_files)?;

    // Try RIO upgrade
    if let Some(rio) = &mut ctx.rio {
        if let Some(res) = rio.try_submit_recv_from(
            payload.op.fd,
            handle,
            &mut payload.op.buf,
            &payload.addr as *const _ as *const std::ffi::c_void,
            &payload.addr_len,
            op.header.user_data,
        )? {
            return Ok(res);
        }
    }

    unsafe {
        CreateIoCompletionPort(handle, ctx.port, 0, 0);
    }

    payload.wsabuf.len = payload.op.buf.capacity() as u32;
    payload.wsabuf.buf = payload.op.buf.as_mut_ptr();

    let mut bytes = 0;
    let ret = unsafe {
        WSARecvFrom(
            handle as SOCKET,
            &payload.wsabuf,
            1,
            &mut bytes,
            &mut payload.flags,
            &mut payload.addr as *mut _ as *mut SOCKADDR,
            &mut payload.addr_len,
            ctx.overlapped,
            None,
        )
    };

    if ret == SOCKET_ERROR {
        let err = unsafe { WSAGetLastError() };
        if err != ERROR_IO_PENDING as i32 {
            return Err(io::Error::from_raw_os_error(err as i32));
        }
    }
    Ok(SubmissionResult::Pending)
}

impl_lifecycle!(drop_recv_from, get_fd_recv_from, recv_from, nested_fd);

// ============================================================================
// Open
// ============================================================================

pub(crate) unsafe fn submit_open(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &*op.payload.open };
    let path_ptr = payload.op.path.as_slice().as_ptr() as usize;

    let entry = &op.header;
    let user_data = entry.user_data;

    let completion = CompletionInfo {
        port: ctx.port as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Open {
        path_ptr,
        flags: payload.op.flags,
        mode: payload.op.mode,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

impl_lifecycle!(drop_open, get_fd_open, open, no_fd);

// ============================================================================
// Close
// ============================================================================

impl_blocking_offload!(submit_close, close, Close);
impl_lifecycle!(drop_close, get_fd_close, close, direct_fd);

// ============================================================================
// Fsync
// ============================================================================

impl_blocking_offload!(submit_fsync, fsync, Fsync);
impl_lifecycle!(drop_fsync, get_fd_fsync, fsync, direct_fd);

// ============================================================================
// SyncFileRange
// ============================================================================

impl_blocking_offload!(submit_sync_range, sync_range, SyncFileRange);
impl_lifecycle!(drop_sync_range, get_fd_sync_range, sync_range, direct_fd);

// ============================================================================
// Fallocate
// ============================================================================

pub(crate) unsafe fn submit_fallocate(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &*op.payload.fallocate };
    let handle = resolve_fd(payload.fd, ctx.registered_files)?;

    let entry = &op.header;
    let user_data = entry.user_data;

    let completion = CompletionInfo {
        port: ctx.port as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Fallocate {
        handle: handle as usize,
        mode: payload.mode,
        offset: payload.offset,
        len: payload.len,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}
impl_lifecycle!(drop_fallocate, get_fd_fallocate, fallocate, direct_fd);

// ============================================================================
// Wakeup
// ============================================================================

pub(crate) unsafe fn submit_wakeup(
    _op: &mut IocpOp,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    Ok(SubmissionResult::PostToQueue)
}

impl_lifecycle!(drop_wakeup, get_fd_wakeup, wakeup, no_fd);

// ============================================================================
// Timeout
// ============================================================================

pub(crate) unsafe fn submit_timeout(
    op: &mut IocpOp,
    _ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let duration = unsafe { op.payload.timeout.duration };
    Ok(SubmissionResult::Timer(duration))
}

impl_lifecycle!(drop_timeout, get_fd_timeout, timeout, no_fd);
