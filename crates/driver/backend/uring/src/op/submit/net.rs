use crate::{
    OwnedRawHandle, RawHandle,
    config::UringRawHandle,
    driver::{SqeEnv, SqeFd},
    error::{UringError, UringResult},
    net::{socket_addr_to_storage, to_socket_addr},
    op::{
        Accept, AcceptMulti, Connect, OpSend, Recv, RecvMulti, RecvProvided, SendTo, UdpConnect,
        UdpRecv, UdpRecvFrom, UdpSend,
        payload::{AcceptPayload, KernelRef, SendToPayload, UdpRecvFromPayload},
    },
};
use io_uring::{opcode, squeue, types};
use veloq_driver_core::driver::SubmitTokenContext;
use veloq_std::{mem::size_of, ptr, slice::from_raw_parts};

use super::{invalid_buf_io_range, resolve_socket_fd, resolve_socket_fd_direct, sqe_with_fd};

pub(crate) unsafe fn make_sqe_recv(
    _kernel: &mut KernelRef<Recv>,
    val: &mut Recv,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_read_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_recv", err))?;
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_recv")?;
    Ok(sqe_with_fd!(fd, |f| opcode::Recv::new(f, ptr, len).build()))
}

/// A recv whose buffer the kernel picks out of the provided-buffer ring.
///
/// `len` is the buffer size rather than `0`: the "0 means use the whole buffer" shortcut only
/// arrived after 5.19, and on a kernel without it a zero-length recv is exactly what you get.
/// Every buffer in the ring has the same capacity, so naming it costs nothing and behaves the
/// same everywhere.
pub(crate) unsafe fn make_sqe_recv_provided(
    _kernel: &mut KernelRef<RecvProvided>,
    val: &mut RecvProvided,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    const SCOPE: &str = "uring.op.submit.make_sqe_recv_provided";
    let (bgid, len) = env.provided_buf_info(SCOPE)?;
    let fd = resolve_socket_fd(env.file_table, val.fd, SCOPE)?;
    Ok(sqe_with_fd!(fd, |f| opcode::Recv::new(
        f,
        ptr::null_mut(),
        len
    )
    .buf_group(bgid)
    .build()
    .flags(squeue::Flags::BUFFER_SELECT)))
}

/// A recv that stays armed, each completion drawing its own buffer from the ring.
///
/// `len` is deliberately left at the opcode's default of `0`, unlike
/// [`make_sqe_recv_provided`]: multishot completions are sized by the kernel one at a time, so
/// there is no single length this submission could name.
///
/// `RecvMulti::build` sets `IOSQE_BUFFER_SELECT` unconditionally — the kernel refuses a
/// multishot recv without buffer selection, which is why this operation exists only once a ring
/// is registered.
pub(crate) unsafe fn make_sqe_recv_multi(
    _kernel: &mut KernelRef<RecvMulti>,
    val: &mut RecvMulti,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    const SCOPE: &str = "uring.op.submit.make_sqe_recv_multi";
    let (bgid, _buf_size) = env.provided_buf_info(SCOPE)?;
    let fd = resolve_socket_fd_direct(env.file_table, val.fd, SCOPE)?;
    Ok(sqe_with_fd!(fd, |f| opcode::RecvMulti::new(f, bgid).build()))
}

pub(crate) unsafe fn make_sqe_send(
    _kernel: &mut KernelRef<OpSend>,
    val: &mut OpSend,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_write_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_send", err))?;
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_send")?;
    Ok(sqe_with_fd!(fd, |f| opcode::Send::new(f, ptr, len).build()))
}

pub(crate) unsafe fn make_sqe_udp_recv(
    _kernel: &mut KernelRef<UdpRecv>,
    val: &mut UdpRecv,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_read_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_recv", err))?;
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_udp_recv")?;
    Ok(sqe_with_fd!(fd, |f| opcode::Recv::new(f, ptr, len).build()))
}

pub(crate) unsafe fn make_sqe_udp_send(
    _kernel: &mut KernelRef<UdpSend>,
    val: &mut UdpSend,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_write_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_send", err))?;
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_udp_send")?;
    Ok(sqe_with_fd!(fd, |f| opcode::Send::new(f, ptr, len).build()))
}

pub(crate) unsafe fn make_sqe_connect(
    _kernel: &mut KernelRef<Connect>,
    val: &mut Connect,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_connect")?;
    let addr = &val.addr.0 as *const _ as *const _;
    Ok(sqe_with_fd!(fd, |f| opcode::Connect::new(
        f,
        addr,
        val.addr_len
    )
    .build()))
}

pub(crate) unsafe fn make_sqe_udp_connect(
    _kernel: &mut KernelRef<UdpConnect>,
    val: &mut UdpConnect,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_socket_fd(
        env.file_table,
        val.fd,
        "uring.op.submit.make_sqe_udp_connect",
    )?;
    let addr = &val.addr.0 as *const _ as *const _;
    Ok(sqe_with_fd!(fd, |f| opcode::Connect::new(
        f,
        addr,
        val.addr_len
    )
    .build()))
}

pub(crate) unsafe fn make_sqe_accept(
    _kernel: &mut AcceptPayload,
    val: &mut Accept,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_socket_fd(env.file_table, val.fd, "uring.op.submit.make_sqe_accept")?;
    let addr = &mut val.addr.0 as *mut _ as *mut _;
    let addr_len = &mut val.addr_len as *mut _;
    Ok(sqe_with_fd!(fd, |f| opcode::Accept::new(f, addr, addr_len).build()))
}

/// 把 accept 完成的结果（一个裸 fd）变成一个拥有所有权的句柄。
///
/// 单发 `Accept` 与 multishot `AcceptMulti` 共用，两者的 `Completion` 是同一个东西。
pub(crate) fn accepted_handle_from_res(res: UringResult<usize>) -> UringResult<OwnedRawHandle> {
    res.map(|raw| unsafe {
        OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(raw as i32)))
    })
}

pub(crate) unsafe fn make_sqe_accept_multi(
    _kernel: &mut KernelRef<AcceptMulti>,
    val: &mut AcceptMulti,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_socket_fd_direct(
        env.file_table,
        val.fd,
        "uring.op.submit.make_sqe_accept_multi",
    )?;
    // `AcceptMulti` 没有 addr/addrlen 字段：内核不回填对端地址，因为多条完成共享一个
    // 地址缓冲会互相覆盖。地址由门面层在拿到 fd 之后用 `getpeername` 补。
    Ok(sqe_with_fd!(fd, |f| opcode::AcceptMulti::new(f).build()))
}

pub(crate) unsafe fn on_complete_accept(
    _kernel: &mut AcceptPayload,
    accept_op: &mut Accept,
    result: i32,
) -> UringResult<usize> {
    if result < 0 {
        return Err(UringError::CompletionWait
            .report(
                "uring.op.submit.on_complete_accept",
                "kernel completion returned error",
            )
            .set_error_code(-result));
    }

    let addr_bytes = unsafe {
        from_raw_parts(
            &accept_op.addr.0 as *const _ as *const u8,
            accept_op.addr_len as usize,
        )
    };
    if let Ok(addr) = to_socket_addr(addr_bytes) {
        accept_op.remote_addr = Some(addr);
    }
    Ok(result as usize)
}

pub(crate) unsafe fn make_sqe_send_to(
    kernel: &mut SendToPayload,
    user: &mut SendTo,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let (ptr, len) = user
        .buf
        .checked_write_range(user.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_send_to", err))?;
    kernel.iovec[0].iov_base = ptr as *mut _;
    kernel.iovec[0].iov_len = len as usize;

    let (msg_name, msg_namelen) = socket_addr_to_storage(user.addr);
    kernel.msg_name = msg_name.0;
    kernel.msg_namelen = msg_namelen;

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = kernel.msg_namelen;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let fd = resolve_socket_fd(env.file_table, user.fd, "uring.op.submit.make_sqe_send_to")?;
    let msghdr = &kernel.msghdr as *const _;
    Ok(sqe_with_fd!(fd, |f| opcode::SendMsg::new(f, msghdr).build()))
}

pub(crate) unsafe fn make_sqe_udp_recv_from(
    kernel: &mut UdpRecvFromPayload,
    user: &mut UdpRecvFrom,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = user.fd;
    let recv_buf = &mut user.buf;

    let (ptr, len) = recv_buf
        .checked_read_range(user.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_recv_from", err))?;
    kernel.iovec[0].iov_base = ptr as *mut _;
    kernel.iovec[0].iov_len = len as usize;

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = size_of::<libc::sockaddr_storage>() as _;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let sqe_fd = resolve_socket_fd(env.file_table, fd, "uring.op.submit.make_sqe_udp_recv_from")?;
    let msghdr = &mut kernel.msghdr as *mut _;
    Ok(sqe_with_fd!(sqe_fd, |f| opcode::RecvMsg::new(f, msghdr).build()))
}

pub(crate) unsafe fn on_complete_udp_recv_from(
    kernel: &mut UdpRecvFromPayload,
    user: &mut UdpRecvFrom,
    result: i32,
) -> UringResult<usize> {
    if result < 0 {
        return Err(UringError::CompletionWait
            .report(
                "uring.op.submit.on_complete_udp_recv_from",
                "kernel completion returned error",
            )
            .set_error_code(-result));
    }

    let len = kernel.msghdr.msg_namelen as usize;
    let addr_bytes = unsafe { from_raw_parts(&kernel.msg_name as *const _ as *const u8, len) };
    if let Ok(addr) = to_socket_addr(addr_bytes) {
        user.addr = Some(addr);
    }
    Ok(result as usize)
}
