use crate::driver::UringDriver;
use crate::op::{UringOp, UringOpPayload, UringUserPayload};
use diagweave::report::Report;
use io_uring::{opcode, squeue, types};
use veloq_buf::PoolKind;
use veloq_driver_core::{DriverErrorKind, DriverResult, driver_error, driver_os_error};

#[inline]
fn payload_variant_mismatch(scope: &'static str) -> Report<DriverErrorKind> {
    driver_error(
        DriverErrorKind::InvalidState,
        scope,
        "UringOpPayload variant mismatch",
    )
}

macro_rules! impl_lifecycle {
    ($drop_fn:ident, $variant:ident, direct_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
    ($drop_fn:ident, $variant:ident, nested_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
    ($drop_fn:ident, $variant:ident, no_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
}

macro_rules! impl_default_completion {
    ($fn_name:ident) => {
        pub(crate) unsafe fn $fn_name(
            _op: &mut UringOp,
            _payload: &mut UringUserPayload,
            result: i32,
        ) -> DriverResult<usize> {
            if result >= 0 {
                Ok(result as usize)
            } else {
                Err(driver_os_error(
                    DriverErrorKind::Completion,
                    concat!("uring.op.submit.", stringify!($fn_name)),
                    -result,
                    "kernel completion returned error",
                ))
            }
        }
    };
}

pub(crate) unsafe fn on_complete_default(
    _op: &mut UringOp,
    _payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize> {
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(driver_os_error(
            DriverErrorKind::Completion,
            "uring.op.submit.on_complete_default",
            -result,
            "kernel completion returned error",
        ))
    }
}

macro_rules! make_rw_fixed {
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path) => {
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
            let rw_op = match payload {
                crate::op::UringUserPayload::$variant(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };

            let region_info = rw_op.buf.resolve_region_info();
            let ptr = unsafe { rw_op.buf.as_mut_ptr().add(rw_op.buf_offset) };
            let len = (rw_op.buf.capacity() - rw_op.buf_offset) as u32;

            let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
                driver
                    .registered_chunks
                    .get(region_info.id as usize)
                    .unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_info.id;
                let fd_idx = rw_op.fd.fixed_index();
                Ok($type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                    .offset(rw_op.offset)
                    .build())
            } else {
                let fd_idx = rw_op.fd.fixed_index();
                Ok($type_raw(types::Fixed(fd_idx), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path, write) => {
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
            let rw_op = match payload {
                crate::op::UringUserPayload::$variant(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
            let region_info = rw_op.buf.resolve_region_info();
            let ptr = unsafe { rw_op.buf.as_slice().as_ptr().add(rw_op.buf_offset) };
            let len = (rw_op.buf.len() - rw_op.buf_offset) as u32;

            let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
                driver
                    .registered_chunks
                    .get(region_info.id as usize)
                    .unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_info.id;
                let fd_idx = rw_op.fd.fixed_index();
                Ok($type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                    .offset(rw_op.offset)
                    .build())
            } else {
                let fd_idx = rw_op.fd.fixed_index();
                Ok($type_raw(types::Fixed(fd_idx), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
}

macro_rules! make_rw_raw {
    ($fn_name:ident, $OpType:ident, $type_raw:path, write) => {
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
            let rw_op = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
            let region_info = rw_op.buf.resolve_region_info();
            let ptr = unsafe { rw_op.buf.as_slice().as_ptr().add(rw_op.buf_offset) };
            let len = (rw_op.buf.len() - rw_op.buf_offset) as u32;
            let fd = rw_op.fd.as_fd();

            let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
                driver
                    .registered_chunks
                    .get(region_info.id as usize)
                    .unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_info.id;
                Ok(opcode::WriteFixed::new(types::Fd(fd), ptr, len, fixed_idx)
                    .offset(rw_op.offset)
                    .build())
            } else {
                Ok($type_raw(types::Fd(fd), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
    ($fn_name:ident, $OpType:ident, $type_raw:path) => {
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
            let rw_op = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
            let region_info = rw_op.buf.resolve_region_info();
            let ptr = unsafe { rw_op.buf.as_mut_ptr().add(rw_op.buf_offset) };
            let len = (rw_op.buf.capacity() - rw_op.buf_offset) as u32;
            let fd = rw_op.fd.as_fd();

            let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
                driver
                    .registered_chunks
                    .get(region_info.id as usize)
                    .unwrap_or(false)
            } else {
                false
            };

            if is_registered {
                let fixed_idx = region_info.id;
                Ok(opcode::ReadFixed::new(types::Fd(fd), ptr, len, fixed_idx)
                    .offset(rw_op.offset)
                    .build())
            } else {
                Ok($type_raw(types::Fd(fd), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
}

make_rw_fixed!(
    make_sqe_read_fixed,
    ReadFixed,
    opcode::Read::new,
    opcode::ReadFixed::new
);
make_rw_raw!(make_sqe_read_raw, ReadRaw, opcode::Read::new);
make_rw_fixed!(
    make_sqe_write_fixed,
    WriteFixed,
    opcode::Write::new,
    opcode::WriteFixed::new,
    write
);
make_rw_raw!(make_sqe_write_raw, WriteRaw, opcode::Write::new, write);

impl_default_completion!(on_complete_read_fixed);
impl_lifecycle!(drop_read_fixed, Read, direct_fd);

impl_default_completion!(on_complete_write_fixed);
impl_lifecycle!(drop_write_fixed, Write, direct_fd);
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
            let ptr = unsafe { val.buf.as_mut_ptr().add(val.buf_offset) };
            let len = (val.buf.capacity() - val.buf_offset) as u32;
            let idx = val.fd.fixed_index();
            Ok($opcode(types::Fixed(idx), ptr, len).build())
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
            let ptr = unsafe { val.buf.as_slice().as_ptr().add(val.buf_offset) };
            let len = (val.buf.len() - val.buf_offset) as u32;
            let idx = val.fd.fixed_index();
            Ok($opcode(types::Fixed(idx), ptr, len).build())
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
    let idx = val.fd.fixed_index();
    Ok(opcode::Connect::new(
        types::Fixed(idx),
        &val.addr.0 as *const _ as *const _,
        val.addr_len,
    )
    .build())
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
    let idx = val.fd.fixed_index();
    Ok(opcode::Connect::new(
        types::Fixed(idx),
        &val.addr.0 as *const _ as *const _,
        val.addr_len,
    )
    .build())
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
    let idx = val.fd.fixed_index();
    Ok(opcode::Accept::new(
        types::Fixed(idx),
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
        return Err(driver_os_error(
            DriverErrorKind::Completion,
            "uring.op.submit.on_complete_accept",
            -result,
            "kernel completion returned error",
        ));
    }

    let accept_op = match payload {
        crate::op::UringUserPayload::Accept(p) => p,
        _ => {
            return Err(driver_error(
                DriverErrorKind::InvalidState,
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

    kernel.iovec[0].iov_base =
        unsafe { user.buf.as_slice().as_ptr().add(user.buf_offset) } as *mut _;
    kernel.iovec[0].iov_len = user.buf.len() - user.buf_offset;

    let (msg_name, msg_namelen) = crate::net::socket_addr_to_storage(user.addr);
    kernel.msg_name = msg_name.0;
    kernel.msg_namelen = msg_namelen;

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = kernel.msg_namelen;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let idx = user.fd.fixed_index();
    Ok(opcode::SendMsg::new(types::Fixed(idx), &kernel.msghdr as *const _).build())
}

impl_default_completion!(on_complete_send_to);
impl_lifecycle!(drop_send_to, SendTo, nested_fd);

pub(crate) unsafe fn make_sqe_udp_recv_stream(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_recv_stream"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_udp_recv_stream"))?;
    let user = match payload {
        crate::op::UringUserPayload::UdpRecvStream(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_udp_recv_stream",
            ));
        }
    };

    let kernel = match &mut op.payload {
        UringOpPayload::UdpRecvStream(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_udp_recv_stream",
            ));
        }
    };

    let fd = user.fd;
    let recv_buf = match user.buf.as_mut() {
        Some(buf) => buf,
        None => {
            return Err(driver_error(
                DriverErrorKind::InvalidInput,
                "uring.op.submit.make_sqe_udp_recv_stream",
                "UdpRecvStream buffer missing",
            ));
        }
    };

    kernel.iovec[0].iov_base = recv_buf.as_mut_ptr() as *mut _;
    kernel.iovec[0].iov_len = recv_buf.capacity();

    kernel.msghdr.msg_name = &mut kernel.msg_name as *mut _ as *mut libc::c_void;
    kernel.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
    kernel.msghdr.msg_iov = kernel.iovec.as_mut_ptr();
    kernel.msghdr.msg_iovlen = 1;

    let idx = fd.fixed_index();
    Ok(opcode::RecvMsg::new(types::Fixed(idx), &mut kernel.msghdr as *mut _).build())
}

pub(crate) unsafe fn on_complete_udp_recv_stream(
    op: &mut UringOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize> {
    if result < 0 {
        return Err(driver_os_error(
            DriverErrorKind::Completion,
            "uring.op.submit.on_complete_udp_recv_stream",
            -result,
            "kernel completion returned error",
        ));
    }

    let user = match payload {
        crate::op::UringUserPayload::UdpRecvStream(p) => p,
        _ => {
            return Err(driver_error(
                DriverErrorKind::InvalidState,
                "uring.op.submit.on_complete_udp_recv_stream",
                "payload variant mismatch for udp_recv_stream",
            ));
        }
    };

    let kernel = match &mut op.payload {
        UringOpPayload::UdpRecvStream(p) => p,
        _ => return Err(payload_variant_mismatch("on_complete_udp_recv_stream")),
    };

    let len = kernel.msghdr.msg_namelen as usize;
    let addr_bytes =
        unsafe { std::slice::from_raw_parts(&kernel.msg_name as *const _ as *const u8, len) };
    if let Ok(addr) = crate::net::to_socket_addr(addr_bytes) {
        user.addr = Some(addr);
    }
    Ok(result as usize)
}

impl_lifecycle!(drop_udp_recv_stream, UdpRecvStream, direct_fd);

pub(crate) unsafe fn make_sqe_close(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_close"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_close"))?;
    let close_op = match payload {
        crate::op::UringUserPayload::Close(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_close")),
    };
    let idx = close_op.fd.fixed_index();
    Ok(opcode::Close::new(types::Fixed(idx)).build())
}

impl_default_completion!(on_complete_close);
impl_lifecycle!(drop_close, Close, direct_fd);

pub(crate) unsafe fn make_sqe_fsync(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync"))?;
    let fsync_op = match payload {
        crate::op::UringUserPayload::Fsync(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_fsync")),
    };
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let idx = fsync_op.fd.fixed_index();
    Ok(opcode::Fsync::new(types::Fixed(idx)).flags(flags).build())
}

impl_default_completion!(on_complete_fsync);
impl_lifecycle!(drop_fsync, Fsync, direct_fd);

pub(crate) unsafe fn make_sqe_fsync_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync_raw"))?;
    let fsync_op = match payload {
        crate::op::UringUserPayload::FsyncRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fsync_raw",
            ));
        }
    };
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let fd = fsync_op.fd.as_fd();
    Ok(opcode::Fsync::new(types::Fd(fd)).flags(flags).build())
}

pub(crate) unsafe fn make_sqe_sync_range(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range"))?;
    let sync_op = match payload {
        crate::op::UringUserPayload::SyncFileRange(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_sync_range",
            ));
        }
    };
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return Err(driver_error(
                DriverErrorKind::InvalidInput,
                "uring.op.submit.make_sqe_sync_range",
                format!(
                    "sync_file_range: nbytes ({}) exceeds 32-bit limit and is not u64::MAX (0)",
                    sync_op.nbytes
                ),
            ));
        }
    } else {
        sync_op.nbytes as u32
    };

    let idx = sync_op.fd.fixed_index();
    Ok(opcode::SyncFileRange::new(types::Fixed(idx), nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build())
}

impl_default_completion!(on_complete_sync_range);
impl_lifecycle!(drop_sync_range, SyncRange, direct_fd);

pub(crate) unsafe fn make_sqe_sync_range_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range_raw"))?;
    let sync_op = match payload {
        crate::op::UringUserPayload::SyncFileRangeRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_sync_range_raw",
            ));
        }
    };
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return Err(driver_error(
                DriverErrorKind::InvalidInput,
                "uring.op.submit.make_sqe_sync_range_raw",
                format!(
                    "sync_file_range: nbytes ({}) exceeds 32-bit limit and is not u64::MAX (0)",
                    sync_op.nbytes
                ),
            ));
        }
    } else {
        sync_op.nbytes as u32
    };

    let fd = sync_op.fd.as_fd();
    Ok(opcode::SyncFileRange::new(types::Fd(fd), nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build())
}

pub(crate) unsafe fn make_sqe_fallocate(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate"))?;
    let fallocate_op = match payload {
        crate::op::UringUserPayload::Fallocate(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fallocate",
            ));
        }
    };
    let idx = fallocate_op.fd.fixed_index();
    Ok(opcode::Fallocate::new(types::Fixed(idx), fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

impl_default_completion!(on_complete_fallocate);
impl_lifecycle!(drop_fallocate, Fallocate, direct_fd);

pub(crate) unsafe fn make_sqe_fallocate_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate_raw"))?;
    let fallocate_op = match payload {
        crate::op::UringUserPayload::FallocateRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fallocate_raw",
            ));
        }
    };
    let fd = fallocate_op.fd.as_fd();
    Ok(opcode::Fallocate::new(types::Fd(fd), fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_open(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_open"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_open"))?;
    let user = match payload {
        crate::op::UringUserPayload::Open(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_open")),
    };
    let path_ptr = user.path.as_slice().as_ptr() as *const _;
    Ok(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
        .flags(user.flags)
        .mode(user.mode)
        .build())
}

impl_lifecycle!(drop_open, Open, no_fd);

pub(crate) unsafe fn make_sqe_timeout(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_timeout"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_timeout"))?;
    let user = match payload {
        crate::op::UringUserPayload::Timeout(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_timeout")),
    };

    let kernel = match &mut op.payload {
        UringOpPayload::Timeout(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_timeout")),
    };

    kernel.ts[0] = user.duration.as_secs() as i64;
    kernel.ts[1] = user.duration.subsec_nanos() as i64;
    let ts_ptr = kernel.ts.as_ptr() as *const types::Timespec;

    Ok(opcode::Timeout::new(ts_ptr).build())
}

impl_default_completion!(on_complete_timeout);
impl_lifecycle!(drop_timeout, Timeout, no_fd);

pub(crate) unsafe fn make_sqe_wakeup(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_wakeup"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_wakeup"))?;
    let user = match payload {
        crate::op::UringUserPayload::Wakeup(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_wakeup")),
    };

    let kernel = match &mut op.payload {
        UringOpPayload::Wakeup(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_wakeup")),
    };

    let idx = user.fd.fixed_index();
    Ok(opcode::Read::new(types::Fixed(idx), kernel.buf.as_mut_ptr(), 8).build())
}

impl_default_completion!(on_complete_wakeup);
impl_lifecycle!(drop_wakeup, Wakeup, no_fd);

pub(crate) unsafe fn get_timeout_timeout(
    _op: &UringOp,
    payload: &UringUserPayload,
) -> Option<std::time::Duration> {
    match payload {
        crate::op::UringUserPayload::Timeout(p) => Some(p.duration),
        _ => None,
    }
}

pub(crate) unsafe fn get_timeout_none(
    _op: &UringOp,
    _payload: &UringUserPayload,
) -> Option<std::time::Duration> {
    None
}

pub(crate) unsafe fn resolve_chunks_none(
    _op: &UringOp,
    _payload: &UringUserPayload,
    _chunks: &mut [u16],
) -> usize {
    0
}

pub(crate) unsafe fn resolve_chunks_read_fixed(
    _op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [u16],
) -> usize {
    let rw_op = match payload {
        crate::op::UringUserPayload::ReadFixed(p) => p,
        _ => return 0,
    };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) unsafe fn resolve_chunks_read_raw(
    op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [u16],
) -> usize {
    unsafe { resolve_chunks_read_fixed(op, payload, chunks) }
}

pub(crate) unsafe fn resolve_chunks_write_fixed(
    _op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [u16],
) -> usize {
    let rw_op = match payload {
        crate::op::UringUserPayload::WriteFixed(p) => p,
        _ => return 0,
    };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) unsafe fn resolve_chunks_write_raw(
    op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [u16],
) -> usize {
    unsafe { resolve_chunks_write_fixed(op, payload, chunks) }
}
