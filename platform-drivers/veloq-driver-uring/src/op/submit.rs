use crate::config::IoFd;
use crate::driver::UringDriver;
use crate::op::{UringOp, UringOpPayload};
use io_uring::{opcode, squeue, types};
use std::io;
use veloq_buf::PoolKind;

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
        pub(crate) unsafe fn $fn_name(_op: &mut UringOp, result: i32) -> io::Result<usize> {
            if result >= 0 {
                Ok(result as usize)
            } else {
                Err(io::Error::from_raw_os_error(-result))
            }
        }
    };
}

macro_rules! make_rw_fixed {
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut UringOp,
            driver: &mut UringDriver,
        ) -> io::Result<squeue::Entry> {
            let kernel = match &mut op.payload {
                UringOpPayload::$variant(kernel) => kernel,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UringOpPayload variant mismatch",
                    ))
                }
            };
            let rw_op = unsafe { kernel.user.as_mut() };
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
                match rw_op.fd {
                    IoFd::Raw(fd) => Ok($type_fixed(types::Fd(fd.fd), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build()),
                    IoFd::Fixed(fd_idx) => {
                        Ok($type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                            .offset(rw_op.offset)
                            .build())
                    }
                }
            } else {
                match rw_op.fd {
                    IoFd::Raw(fd) => Ok($type_raw(types::Fd(fd.fd), ptr, len)
                        .offset(rw_op.offset)
                        .build()),
                    IoFd::Fixed(fd_idx) => Ok($type_raw(types::Fixed(fd_idx), ptr, len)
                        .offset(rw_op.offset)
                        .build()),
                }
            }
        }
    };
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path, write) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut UringOp,
            driver: &mut UringDriver,
        ) -> io::Result<squeue::Entry> {
            let kernel = match &mut op.payload {
                UringOpPayload::$variant(kernel) => kernel,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UringOpPayload variant mismatch",
                    ))
                }
            };
            let rw_op = unsafe { kernel.user.as_mut() };
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
                match rw_op.fd {
                    IoFd::Raw(fd) => Ok($type_fixed(types::Fd(fd.fd), ptr, len, fixed_idx)
                        .offset(rw_op.offset)
                        .build()),
                    IoFd::Fixed(fd_idx) => {
                        Ok($type_fixed(types::Fixed(fd_idx), ptr, len, fixed_idx)
                            .offset(rw_op.offset)
                            .build())
                    }
                }
            } else {
                match rw_op.fd {
                    IoFd::Raw(fd) => Ok($type_raw(types::Fd(fd.fd), ptr, len)
                        .offset(rw_op.offset)
                        .build()),
                    IoFd::Fixed(fd_idx) => Ok($type_raw(types::Fixed(fd_idx), ptr, len)
                        .offset(rw_op.offset)
                        .build()),
                }
            }
        }
    };
}

make_rw_fixed!(
    make_sqe_read_fixed,
    Read,
    opcode::Read::new,
    opcode::ReadFixed::new
);
make_rw_fixed!(
    make_sqe_write_fixed,
    Write,
    opcode::Write::new,
    opcode::WriteFixed::new,
    write
);

impl_default_completion!(on_complete_read_fixed);
impl_lifecycle!(drop_read_fixed, Read, direct_fd);

impl_default_completion!(on_complete_write_fixed);
impl_lifecycle!(drop_write_fixed, Write, direct_fd);

macro_rules! make_buf_op {
    ($fn_name:ident, $variant:ident, $opcode:path, recv_args) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut UringOp,
            _driver: &mut UringDriver,
        ) -> io::Result<squeue::Entry> {
            let kernel = match &mut op.payload {
                UringOpPayload::$variant(kernel) => kernel,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UringOpPayload variant mismatch",
                    ))
                }
            };
            let val = unsafe { kernel.user.as_mut() };
            let ptr = unsafe { val.buf.as_mut_ptr().add(val.buf_offset) };
            let len = (val.buf.capacity() - val.buf_offset) as u32;
            match val.fd {
                IoFd::Raw(fd) => Ok($opcode(types::Fd(fd.fd), ptr, len).build()),
                IoFd::Fixed(idx) => Ok($opcode(types::Fixed(idx), ptr, len).build()),
            }
        }
    };
    ($fn_name:ident, $variant:ident, $opcode:path, send_args) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut UringOp,
            _driver: &mut UringDriver,
        ) -> io::Result<squeue::Entry> {
            let kernel = match &mut op.payload {
                UringOpPayload::$variant(kernel) => kernel,
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "UringOpPayload variant mismatch",
                    ))
                }
            };
            let val = unsafe { kernel.user.as_mut() };
            let ptr = unsafe { val.buf.as_slice().as_ptr().add(val.buf_offset) };
            let len = (val.buf.len() - val.buf_offset) as u32;
            match val.fd {
                IoFd::Raw(fd) => Ok($opcode(types::Fd(fd.fd), ptr, len).build()),
                IoFd::Fixed(idx) => Ok($opcode(types::Fixed(idx), ptr, len).build()),
            }
        }
    };
}

make_buf_op!(make_sqe_recv, Recv, opcode::Recv::new, recv_args);
impl_default_completion!(on_complete_recv);
impl_lifecycle!(drop_recv, Recv, direct_fd);

make_buf_op!(make_sqe_send, Send, opcode::Send::new, send_args);
impl_default_completion!(on_complete_send);
impl_lifecycle!(drop_send, Send, direct_fd);

make_buf_op!(make_sqe_udp_recv, UdpRecv, opcode::Recv::new, recv_args);
impl_default_completion!(on_complete_udp_recv);
impl_lifecycle!(drop_udp_recv, UdpRecv, direct_fd);

make_buf_op!(make_sqe_udp_send, UdpSend, opcode::Send::new, send_args);
impl_default_completion!(on_complete_udp_send);
impl_lifecycle!(drop_udp_send, UdpSend, direct_fd);

pub(crate) unsafe fn make_sqe_connect(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let kernel = match &mut op.payload {
        UringOpPayload::Connect(kernel) => kernel,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let val = unsafe { kernel.user.as_mut() };
    match val.fd {
        IoFd::Raw(fd) => Ok(opcode::Connect::new(
            types::Fd(fd.fd),
            &val.addr.0 as *const _ as *const _,
            val.addr_len,
        )
        .build()),
        IoFd::Fixed(idx) => Ok(opcode::Connect::new(
            types::Fixed(idx),
            &val.addr.0 as *const _ as *const _,
            val.addr_len,
        )
        .build()),
    }
}
impl_default_completion!(on_complete_connect);
impl_lifecycle!(drop_connect, Connect, direct_fd);

pub(crate) unsafe fn make_sqe_accept(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::Accept(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let val = unsafe { payload.user.as_mut() };
    match val.fd {
        IoFd::Raw(fd) => Ok(opcode::Accept::new(
            types::Fd(fd.fd),
            &mut val.addr.0 as *mut _ as *mut _,
            &mut val.addr_len as *mut _,
        )
        .build()),
        IoFd::Fixed(idx) => Ok(opcode::Accept::new(
            types::Fixed(idx),
            &mut val.addr.0 as *mut _ as *mut _,
            &mut val.addr_len as *mut _,
        )
        .build()),
    }
}

pub(crate) unsafe fn on_complete_accept(op: &mut UringOp, result: i32) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }

    let payload = match &mut op.payload {
        UringOpPayload::Accept(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload variant mismatch for accept",
            ));
        }
    };

    let accept_op = unsafe { payload.user.as_mut() };
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
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::SendTo(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let user = unsafe { payload.user.as_ref() };

    payload.iovec[0].iov_base =
        unsafe { user.buf.as_slice().as_ptr().add(user.buf_offset) } as *mut _;
    payload.iovec[0].iov_len = user.buf.len() - user.buf_offset;

    payload.msghdr.msg_name = &mut payload.msg_name as *mut _ as *mut libc::c_void;
    payload.msghdr.msg_namelen = payload.msg_namelen;
    payload.msghdr.msg_iov = payload.iovec.as_mut_ptr();
    payload.msghdr.msg_iovlen = 1;

    match user.fd {
        IoFd::Raw(fd) => {
            Ok(opcode::SendMsg::new(types::Fd(fd.fd), &payload.msghdr as *const _).build())
        }
        IoFd::Fixed(idx) => {
            Ok(opcode::SendMsg::new(types::Fixed(idx), &payload.msghdr as *const _).build())
        }
    }
}

impl_default_completion!(on_complete_send_to);
impl_lifecycle!(drop_send_to, SendTo, nested_fd);

pub(crate) unsafe fn make_sqe_udp_recv_stream(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::UdpRecvStream(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let user = unsafe { payload.user.as_mut() };
    let fd = user.fd;
    let recv_buf = match user.buf.as_mut() {
        Some(buf) => buf,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UdpRecvStream buffer missing",
            ));
        }
    };

    payload.iovec[0].iov_base = recv_buf.as_mut_ptr() as *mut _;
    payload.iovec[0].iov_len = recv_buf.capacity();

    payload.msghdr.msg_name = &mut payload.msg_name as *mut _ as *mut libc::c_void;
    payload.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as _;
    payload.msghdr.msg_iov = payload.iovec.as_mut_ptr();
    payload.msghdr.msg_iovlen = 1;

    match fd {
        IoFd::Raw(fd) => {
            Ok(opcode::RecvMsg::new(types::Fd(fd.fd), &mut payload.msghdr as *mut _).build())
        }
        IoFd::Fixed(idx) => {
            Ok(opcode::RecvMsg::new(types::Fixed(idx), &mut payload.msghdr as *mut _).build())
        }
    }
}

pub(crate) unsafe fn on_complete_udp_recv_stream(
    op: &mut UringOp,
    result: i32,
) -> io::Result<usize> {
    if result < 0 {
        return Err(io::Error::from_raw_os_error(-result));
    }

    let payload = match &mut op.payload {
        UringOpPayload::UdpRecvStream(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "payload variant mismatch for udp_recv_stream",
            ));
        }
    };
    let user = unsafe { payload.user.as_mut() };
    let len = payload.msghdr.msg_namelen as usize;
    let addr_bytes =
        unsafe { std::slice::from_raw_parts(&payload.msg_name as *const _ as *const u8, len) };
    if let Ok(addr) = crate::net::to_socket_addr(addr_bytes) {
        user.addr = Some(addr);
    }
    Ok(result as usize)
}

impl_lifecycle!(drop_udp_recv_stream, UdpRecvStream, direct_fd);

pub(crate) unsafe fn make_sqe_close(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let kernel = match &mut op.payload {
        UringOpPayload::Close(kernel) => kernel,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let close_op = unsafe { kernel.user.as_mut() };
    match close_op.fd {
        IoFd::Raw(fd) => Ok(opcode::Close::new(types::Fd(fd.fd)).build()),
        IoFd::Fixed(idx) => Ok(opcode::Close::new(types::Fixed(idx)).build()),
    }
}

impl_default_completion!(on_complete_close);
impl_lifecycle!(drop_close, Close, direct_fd);

pub(crate) unsafe fn make_sqe_fsync(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let kernel = match &mut op.payload {
        UringOpPayload::Fsync(kernel) => kernel,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let fsync_op = unsafe { kernel.user.as_mut() };
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    match fsync_op.fd {
        IoFd::Raw(fd) => Ok(opcode::Fsync::new(types::Fd(fd.fd)).flags(flags).build()),
        IoFd::Fixed(idx) => Ok(opcode::Fsync::new(types::Fixed(idx)).flags(flags).build()),
    }
}

impl_default_completion!(on_complete_fsync);
impl_lifecycle!(drop_fsync, Fsync, direct_fd);

pub(crate) unsafe fn make_sqe_sync_range(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let kernel = match &mut op.payload {
        UringOpPayload::SyncRange(kernel) => kernel,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let sync_op = unsafe { kernel.user.as_mut() };
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "sync_file_range: nbytes ({}) exceeds 32-bit limit and is not u64::MAX (0)",
                    sync_op.nbytes
                ),
            ));
        }
    } else {
        sync_op.nbytes as u32
    };

    match sync_op.fd {
        IoFd::Raw(fd) => Ok(opcode::SyncFileRange::new(types::Fd(fd.fd), nbytes)
            .offset(sync_op.offset)
            .flags(sync_op.flags)
            .build()),
        IoFd::Fixed(idx) => Ok(opcode::SyncFileRange::new(types::Fixed(idx), nbytes)
            .offset(sync_op.offset)
            .flags(sync_op.flags)
            .build()),
    }
}

impl_default_completion!(on_complete_sync_range);
impl_lifecycle!(drop_sync_range, SyncRange, direct_fd);

pub(crate) unsafe fn make_sqe_fallocate(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let kernel = match &mut op.payload {
        UringOpPayload::Fallocate(kernel) => kernel,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let fallocate_op = unsafe { kernel.user.as_mut() };
    match fallocate_op.fd {
        IoFd::Raw(fd) => Ok(opcode::Fallocate::new(types::Fd(fd.fd), fallocate_op.len)
            .offset(fallocate_op.offset)
            .mode(fallocate_op.mode)
            .build()),
        IoFd::Fixed(idx) => Ok(opcode::Fallocate::new(types::Fixed(idx), fallocate_op.len)
            .offset(fallocate_op.offset)
            .mode(fallocate_op.mode)
            .build()),
    }
}

impl_default_completion!(on_complete_fallocate);
impl_lifecycle!(drop_fallocate, Fallocate, direct_fd);

pub(crate) unsafe fn make_sqe_open(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::Open(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let user = unsafe { payload.user.as_ref() };
    let path_ptr = user.path.as_slice().as_ptr() as *const _;
    Ok(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
        .flags(user.flags)
        .mode(user.mode)
        .build())
}

impl_default_completion!(on_complete_open);
impl_lifecycle!(drop_open, Open, no_fd);

pub(crate) unsafe fn make_sqe_timeout(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::Timeout(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let user = unsafe { payload.user.as_ref() };

    payload.ts[0] = user.duration.as_secs() as i64;
    payload.ts[1] = user.duration.subsec_nanos() as i64;
    let ts_ptr = payload.ts.as_ptr() as *const types::Timespec;

    Ok(opcode::Timeout::new(ts_ptr).build())
}

impl_default_completion!(on_complete_timeout);
impl_lifecycle!(drop_timeout, Timeout, no_fd);

pub(crate) unsafe fn make_sqe_wakeup(
    op: &mut UringOp,
    _driver: &mut UringDriver,
) -> io::Result<squeue::Entry> {
    let payload = match &mut op.payload {
        UringOpPayload::Wakeup(payload) => payload,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "UringOpPayload variant mismatch",
            ));
        }
    };
    let user = unsafe { payload.user.as_ref() };
    match user.fd {
        IoFd::Raw(fd) => {
            Ok(opcode::Read::new(types::Fd(fd.fd), payload.buf.as_mut_ptr(), 8).build())
        }
        IoFd::Fixed(idx) => {
            Ok(opcode::Read::new(types::Fixed(idx), payload.buf.as_mut_ptr(), 8).build())
        }
    }
}

impl_default_completion!(on_complete_wakeup);
impl_lifecycle!(drop_wakeup, Wakeup, no_fd);

pub(crate) unsafe fn get_timeout_timeout(op: &UringOp) -> Option<std::time::Duration> {
    match &op.payload {
        UringOpPayload::Timeout(payload) => {
            let user = unsafe { payload.user.as_ref() };
            Some(user.duration)
        }
        _ => None,
    }
}

pub(crate) unsafe fn get_timeout_none(_op: &UringOp) -> Option<std::time::Duration> {
    None
}

pub(crate) unsafe fn resolve_chunks_none(_op: &UringOp, _chunks: &mut [u16]) -> usize {
    0
}

pub(crate) unsafe fn resolve_chunks_read_fixed(op: &UringOp, chunks: &mut [u16]) -> usize {
    let kernel = match &op.payload {
        UringOpPayload::Read(kernel) => kernel,
        _ => return 0,
    };
    let rw_op = unsafe { kernel.user.as_ref() };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) unsafe fn resolve_chunks_write_fixed(op: &UringOp, chunks: &mut [u16]) -> usize {
    let kernel = match &op.payload {
        UringOpPayload::Write(kernel) => kernel,
        _ => return 0,
    };
    let rw_op = unsafe { kernel.user.as_ref() };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}
