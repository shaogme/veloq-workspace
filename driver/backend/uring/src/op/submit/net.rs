use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{UringOp, UringOpPayload, UringUserPayload};
use io_uring::{opcode, squeue};
use veloq_driver_core::op::{checked_read_buf_range, checked_write_buf_range};

use super::{invalid_buf_io_range, payload_variant_mismatch, resolve_socket_fd};

macro_rules! make_buf_op {
    ($fn_name:ident, $OpType:ident, $opcode:path, recv_args) => {
        pub(crate) unsafe fn $fn_name(
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let val = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
            let (ptr, len) =
                checked_read_buf_range(&mut val.buf, val.buf_offset).map_err(|err| {
                    invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
                })?;
            let fixed_fd = resolve_socket_fd(
                &driver.registered_files,
                &driver.file_generations,
                val.fd,
                concat!("uring.op.submit.", stringify!($fn_name)),
            )?;
            Ok($opcode(fixed_fd, ptr, len).build())
        }
    };
    ($fn_name:ident, $OpType:ident, $opcode:path, send_args) => {
        pub(crate) unsafe fn $fn_name(
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let val = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
            let (ptr, len) = checked_write_buf_range(&val.buf, val.buf_offset).map_err(|err| {
                invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
            })?;
            let fixed_fd = resolve_socket_fd(
                &driver.registered_files,
                &driver.file_generations,
                val.fd,
                concat!("uring.op.submit.", stringify!($fn_name)),
            )?;
            Ok($opcode(fixed_fd, ptr, len).build())
        }
    };
}

make_buf_op!(make_sqe_recv, Recv, opcode::Recv::new, recv_args);
impl_default_completion!(on_complete_recv);
impl_lifecycle!(drop_recv, Recv, direct_fd);

make_buf_op!(make_sqe_send, OpSend, opcode::Send::new, send_args);
impl_default_completion!(on_complete_send);
impl_lifecycle!(drop_send, Send, direct_fd);

make_buf_op!(make_sqe_udp_recv, UdpRecv, opcode::Recv::new, recv_args);
impl_default_completion!(on_complete_udp_recv);
impl_lifecycle!(drop_udp_recv, UdpRecv, direct_fd);

make_buf_op!(make_sqe_udp_send, UdpSend, opcode::Send::new, send_args);
impl_default_completion!(on_complete_udp_send);
impl_lifecycle!(drop_udp_send, UdpSend, direct_fd);

pub(crate) unsafe fn make_sqe_connect(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_connect"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_connect"))?;
    let val = match payload {
        crate::op::UringUserPayload::Connect(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_connect")),
    };
    let fixed_fd = resolve_socket_fd(
        &driver.registered_files,
        &driver.file_generations,
        val.fd,
        "uring.op.submit.make_sqe_connect",
    )?;
    Ok(opcode::Connect::new(fixed_fd, &val.addr.0 as *const _ as *const _, val.addr_len).build())
}
impl_default_completion!(on_complete_connect);
impl_lifecycle!(drop_connect, Connect, direct_fd);

pub(crate) unsafe fn make_sqe_udp_connect(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_connect"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_connect"))?;
    let val = match payload {
        crate::op::UringUserPayload::UdpConnect(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_udp_connect",
            ));
        }
    };
    let fixed_fd = resolve_socket_fd(
        &driver.registered_files,
        &driver.file_generations,
        val.fd,
        "uring.op.submit.make_sqe_udp_connect",
    )?;
    Ok(opcode::Connect::new(fixed_fd, &val.addr.0 as *const _ as *const _, val.addr_len).build())
}
impl_default_completion!(on_complete_udp_connect);
impl_lifecycle!(drop_udp_connect, UdpConnect, direct_fd);

pub(crate) unsafe fn make_sqe_accept(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_accept"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_accept"))?;
    let val = match payload {
        crate::op::UringUserPayload::Accept(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_accept")),
    };
    let fixed_fd = resolve_socket_fd(
        &driver.registered_files,
        &driver.file_generations,
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
    _op: &mut UringOp,
    payload: &mut UringUserPayload,
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

    let accept_op = match payload {
        crate::op::UringUserPayload::Accept(p) => p,
        _ => {
            return Err(UringError::InvalidState.report(
                "uring.op.submit.on_complete_accept",
                "payload variant mismatch for accept",
            ));
        }
    };

    let addr_bytes = unsafe {
        std::slice::from_raw_parts(
            &accept_op.addr.0 as *const _ as *const u8,
            accept_op.addr_len as usize,
        )
    };
    if let Ok(addr) = crate::net::to_socket_addr(addr_bytes) {
        accept_op.remote_addr = Some(addr);
    }
    Ok(result as usize)
}

impl_lifecycle!(drop_accept, Accept, nested_fd);

pub(crate) unsafe fn make_sqe_send_to(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_send_to"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_send_to"))?;
    let user = match payload {
        crate::op::UringUserPayload::SendTo(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_send_to")),
    };

    let kernel = match &mut op.payload {
        UringOpPayload::SendTo(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_send_to")),
    };

    let (ptr, len) = checked_write_buf_range(&user.buf, user.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_send_to", err))?;
    kernel.iovec[0].iov_base = ptr as *mut _;
    kernel.iovec[0].iov_len = len as usize;

    let (msg_name, msg_namelen) = crate::net::socket_addr_to_storage(user.addr);
    kernel.msg_name = msg_name.0;
    kernel.msg_namelen = msg_namelen;

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = kernel.msg_namelen;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let fixed_fd = resolve_socket_fd(
        &driver.registered_files,
        &driver.file_generations,
        user.fd,
        "uring.op.submit.make_sqe_send_to",
    )?;
    Ok(opcode::SendMsg::new(fixed_fd, &kernel.msghdr as *const _).build())
}

impl_default_completion!(on_complete_send_to);
impl_lifecycle!(drop_send_to, SendTo, nested_fd);

pub(crate) unsafe fn make_sqe_udp_recv_from(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_recv_from"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_recv_from"))?;
    let user = match payload {
        crate::op::UringUserPayload::UdpRecvFrom(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_udp_recv_from",
            ));
        }
    };

    let kernel = match &mut op.payload {
        UringOpPayload::UdpRecvFrom(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_udp_recv_from",
            ));
        }
    };

    let fd = user.fd;
    let recv_buf = &mut user.buf;

    let (ptr, len) = checked_read_buf_range(recv_buf, user.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_udp_recv_from", err))?;
    kernel.iovec[0].iov_base = ptr as *mut _;
    kernel.iovec[0].iov_len = len as usize;

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let fixed_fd = resolve_socket_fd(
        &driver.registered_files,
        &driver.file_generations,
        fd,
        "uring.op.submit.make_sqe_udp_recv_from",
    )?;
    Ok(opcode::RecvMsg::new(fixed_fd, &mut kernel.msghdr as *mut _).build())
}

pub(crate) unsafe fn on_complete_udp_recv_from(
    op: &mut UringOp,
    payload: &mut UringUserPayload,
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

    let user = match payload {
        crate::op::UringUserPayload::UdpRecvFrom(p) => p,
        _ => {
            return Err(UringError::InvalidState.report(
                "uring.op.submit.on_complete_udp_recv_from",
                "payload variant mismatch for udp_recv_from",
            ));
        }
    };

    let kernel = match &mut op.payload {
        UringOpPayload::UdpRecvFrom(p) => p,
        _ => return Err(payload_variant_mismatch("on_complete_udp_recv_from")),
    };

    let len = kernel.msghdr.msg_namelen as usize;
    let addr_bytes =
        unsafe { std::slice::from_raw_parts(&kernel.msg_name as *const _ as *const u8, len) };
    if let Ok(addr) = crate::net::to_socket_addr(addr_bytes) {
        user.addr = Some(addr);
    }
    Ok(result as usize)
}

impl_lifecycle!(drop_udp_recv_from, UdpRecvFrom, direct_fd);
