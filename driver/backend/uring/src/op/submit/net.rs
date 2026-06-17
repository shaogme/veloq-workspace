use crate::{
    driver::UringDriver,
    error::{UringDriverResult as DriverResult, UringError},
    net::{socket_addr_to_storage, to_socket_addr},
    op::{
        Accept, Connect, OpSend, Recv, SendTo, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend,
        payload::{AcceptPayload, KernelRef, SendToPayload, UdpRecvFromPayload},
    },
};
use io_uring::{opcode, squeue};
use std::{mem::size_of, slice::from_raw_parts};
use veloq_driver_core::driver::SubmitTokenContext;

use super::{invalid_buf_io_range, resolve_socket_fd};

pub(crate) unsafe fn make_sqe_recv(
    _kernel: &mut KernelRef<Recv>,
    val: &mut Recv,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_read_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_recv", err))?;
    let fixed_fd = resolve_socket_fd(&driver.file_slots, val.fd, "uring.op.submit.make_sqe_recv")?;
    Ok(opcode::Recv::new(fixed_fd, ptr, len).build())
}

pub(crate) unsafe fn make_sqe_send(
    _kernel: &mut KernelRef<OpSend>,
    val: &mut OpSend,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_write_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_send", err))?;
    let fixed_fd = resolve_socket_fd(&driver.file_slots, val.fd, "uring.op.submit.make_sqe_send")?;
    Ok(opcode::Send::new(fixed_fd, ptr, len).build())
}

pub(crate) unsafe fn make_sqe_udp_recv(
    _kernel: &mut KernelRef<UdpRecv>,
    val: &mut UdpRecv,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_read_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_recv", err))?;
    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        val.fd,
        "uring.op.submit.make_sqe_udp_recv",
    )?;
    Ok(opcode::Recv::new(fixed_fd, ptr, len).build())
}

pub(crate) unsafe fn make_sqe_udp_send(
    _kernel: &mut KernelRef<UdpSend>,
    val: &mut UdpSend,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let (ptr, len) = val
        .buf
        .checked_write_range(val.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_send", err))?;
    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        val.fd,
        "uring.op.submit.make_sqe_udp_send",
    )?;
    Ok(opcode::Send::new(fixed_fd, ptr, len).build())
}

pub(crate) unsafe fn make_sqe_connect(
    _kernel: &mut KernelRef<Connect>,
    val: &mut Connect,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        val.fd,
        "uring.op.submit.make_sqe_connect",
    )?;
    Ok(opcode::Connect::new(fixed_fd, &val.addr.0 as *const _ as *const _, val.addr_len).build())
}

pub(crate) unsafe fn make_sqe_udp_connect(
    _kernel: &mut KernelRef<UdpConnect>,
    val: &mut UdpConnect,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        val.fd,
        "uring.op.submit.make_sqe_udp_connect",
    )?;
    Ok(opcode::Connect::new(fixed_fd, &val.addr.0 as *const _ as *const _, val.addr_len).build())
}

pub(crate) unsafe fn make_sqe_accept(
    _kernel: &mut AcceptPayload,
    val: &mut Accept,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        val.fd,
        "uring.op.submit.make_sqe_accept",
    )?;
    Ok(opcode::Accept::new(
        fixed_fd,
        &mut val.addr.0 as *mut _ as *mut _,
        &mut val.addr_len as *mut _,
    )
    .build())
}

pub(crate) unsafe fn on_complete_accept(
    _kernel: &mut AcceptPayload,
    accept_op: &mut Accept,
    result: i32,
) -> DriverResult<usize> {
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
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
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

    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        user.fd,
        "uring.op.submit.make_sqe_send_to",
    )?;
    Ok(opcode::SendMsg::new(fixed_fd, &kernel.msghdr as *const _).build())
}

pub(crate) unsafe fn make_sqe_udp_recv_from(
    kernel: &mut UdpRecvFromPayload,
    user: &mut UdpRecvFrom,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
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

    let fixed_fd = resolve_socket_fd(
        &driver.file_slots,
        fd,
        "uring.op.submit.make_sqe_udp_recv_from",
    )?;
    Ok(opcode::RecvMsg::new(fixed_fd, &mut kernel.msghdr as *mut _).build())
}

pub(crate) unsafe fn on_complete_udp_recv_from(
    kernel: &mut UdpRecvFromPayload,
    user: &mut UdpRecvFrom,
    result: i32,
) -> DriverResult<usize> {
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
