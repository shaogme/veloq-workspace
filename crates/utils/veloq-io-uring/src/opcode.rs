//! Minimal opcode builders used by the Veloq io_uring backend.
//!
//! Builder methods only copy pointer values into SQEs; they never dereference them. The caller
//! must keep every pointed-to buffer, socket address, message header, pathname, or timespec alive
//! and at the same address until the kernel has consumed the request and emitted its CQE.

use crate::{
    squeue::{self, Entry},
    sys,
    types::{Fd, FsyncFlags, Timespec, UseFixed},
};

macro_rules! assign_fd {
    ($sqe:expr, $fd:expr) => {{
        let fd = $fd;
        if let Some(raw) = fd.raw_fd() {
            $sqe.fd = raw;
        } else if let Some(index) = fd.fixed_index() {
            $sqe.fd = index as i32;
            $sqe.flags |= squeue::Flags::FIXED_FILE.bits();
        }
    }};
}

#[inline(always)]
fn sqe_zeroed() -> sys::IoUringSqe {
    // All SQE fields are integer or byte storage, so zero is a valid bit pattern.
    unsafe { core::mem::zeroed() }
}

#[inline]
fn entry_with_fd<T: UseFixed>(opcode: u8, fd: T) -> sys::IoUringSqe {
    let mut sqe = sqe_zeroed();
    sqe.opcode = opcode;
    assign_fd!(sqe, fd);
    sqe
}

#[inline]
fn set_buffer_group(sqe: &mut sys::IoUringSqe, group: u16) {
    sqe.buf_index_group = group.to_ne_bytes();
}

/// Read from a file into a userspace buffer.
#[derive(Debug)]
pub struct Read<T: UseFixed = Fd> {
    fd: T,
    buf: *mut u8,
    len: u32,
    offset: u64,
    rw_flags: i32,
    buf_group: u16,
}

impl<T: UseFixed> Read<T> {
    #[inline]
    pub fn new(fd: T, buf: *mut u8, len: u32) -> Self {
        Self {
            fd,
            buf,
            len,
            offset: 0,
            rw_flags: 0,
            buf_group: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn rw_flags(mut self, flags: i32) -> Self {
        self.rw_flags = flags;
        self
    }

    #[inline]
    pub const fn buf_group(mut self, group: u16) -> Self {
        self.buf_group = group;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_READ;
}

/// Read from a file into a previously registered fixed buffer.
#[derive(Debug)]
pub struct ReadFixed<T: UseFixed = Fd> {
    fd: T,
    buf: *mut u8,
    len: u32,
    buf_index: u16,
    offset: u64,
    rw_flags: i32,
}

impl<T: UseFixed> ReadFixed<T> {
    #[inline]
    pub fn new(fd: T, buf: *mut u8, len: u32, buf_index: u16) -> Self {
        Self {
            fd,
            buf,
            len,
            buf_index,
            offset: 0,
            rw_flags: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn rw_flags(mut self, flags: i32) -> Self {
        self.rw_flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        sqe.buf_index_group = self.buf_index.to_ne_bytes();
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_READ_FIXED;
}

/// Write a userspace buffer to a file.
#[derive(Debug)]
pub struct Write<T: UseFixed = Fd> {
    fd: T,
    buf: *const u8,
    len: u32,
    offset: u64,
    rw_flags: i32,
}

impl<T: UseFixed> Write<T> {
    #[inline]
    pub fn new(fd: T, buf: *const u8, len: u32) -> Self {
        Self {
            fd,
            buf,
            len,
            offset: 0,
            rw_flags: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn rw_flags(mut self, flags: i32) -> Self {
        self.rw_flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_WRITE;
}

/// Write a userspace buffer using a previously registered fixed buffer.
#[derive(Debug)]
pub struct WriteFixed<T: UseFixed = Fd> {
    fd: T,
    buf: *const u8,
    len: u32,
    buf_index: u16,
    offset: u64,
    rw_flags: i32,
}

impl<T: UseFixed> WriteFixed<T> {
    #[inline]
    pub fn new(fd: T, buf: *const u8, len: u32, buf_index: u16) -> Self {
        Self {
            fd,
            buf,
            len,
            buf_index,
            offset: 0,
            rw_flags: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn rw_flags(mut self, flags: i32) -> Self {
        self.rw_flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        sqe.buf_index_group = self.buf_index.to_ne_bytes();
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_WRITE_FIXED;
}

/// Synchronize a file.
#[derive(Debug)]
pub struct Fsync<T: UseFixed = Fd> {
    fd: T,
    flags: FsyncFlags,
}

impl<T: UseFixed> Fsync<T> {
    #[inline]
    pub fn new(fd: T) -> Self {
        Self {
            fd,
            flags: FsyncFlags::empty(),
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: FsyncFlags) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.op_flags = self.flags.bits();
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_FSYNC;
}

/// Synchronize a range of a file.
#[derive(Debug)]
pub struct SyncFileRange<T: UseFixed = Fd> {
    fd: T,
    len: u32,
    offset: u64,
    flags: u32,
}

impl<T: UseFixed> SyncFileRange<T> {
    #[inline]
    pub fn new(fd: T, len: u32) -> Self {
        Self {
            fd,
            len,
            offset: 0,
            flags: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.flags;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_SYNC_FILE_RANGE;
}

/// Preallocate or deallocate file space.
#[derive(Debug)]
pub struct Fallocate<T: UseFixed = Fd> {
    fd: T,
    len: u64,
    offset: u64,
    mode: i32,
}

impl<T: UseFixed> Fallocate<T> {
    #[inline]
    pub fn new(fd: T, len: u64) -> Self {
        Self {
            fd,
            len,
            offset: 0,
            mode: 0,
        }
    }

    #[inline]
    pub const fn offset(mut self, offset: u64) -> Self {
        self.offset = offset;
        self
    }

    #[inline]
    pub const fn mode(mut self, mode: i32) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.len;
        sqe.len = self.mode as u32;
        sqe.off_or_addr2 = self.offset;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_FALLOCATE;
}

/// Open a file relative to a directory descriptor.
#[derive(Debug)]
pub struct OpenAt {
    dirfd: Fd,
    pathname: *const libc::c_char,
    flags: i32,
    mode: libc::mode_t,
}

impl OpenAt {
    #[inline]
    pub fn new(dirfd: Fd, pathname: *const libc::c_char) -> Self {
        Self {
            dirfd,
            pathname,
            flags: 0,
            mode: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub const fn mode(mut self, mode: libc::mode_t) -> Self {
        self.mode = mode;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = self.dirfd.0;
        sqe.addr_or_splice_off_in = self.pathname as usize as u64;
        sqe.len = self.mode;
        sqe.op_flags = self.flags as u32;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_OPENAT;
}

/// Close a raw or registered file descriptor.
#[derive(Debug)]
pub struct Close<T: UseFixed = Fd> {
    fd: T,
}

impl<T: UseFixed> Close<T> {
    #[inline]
    pub fn new(fd: T) -> Self {
        Self { fd }
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        if let Some(raw) = self.fd.raw_fd() {
            sqe.fd = raw;
        } else if let Some(index) = self.fd.fixed_index() {
            sqe.splice_fd_in_or_file_index = index.wrapping_add(1);
        }
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_CLOSE;
}

/// Receive from a socket into a userspace buffer.
#[derive(Debug)]
pub struct Recv<T: UseFixed = Fd> {
    fd: T,
    buf: *mut u8,
    len: u32,
    flags: i32,
    buf_group: u16,
}

impl<T: UseFixed> Recv<T> {
    #[inline]
    pub fn new(fd: T, buf: *mut u8, len: u32) -> Self {
        Self {
            fd,
            buf,
            len,
            flags: 0,
            buf_group: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub const fn buf_group(mut self, group: u16) -> Self {
        self.buf_group = group;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_RECV;
}

/// Receive repeatedly, selecting buffers from a provided-buffer ring.
#[derive(Debug)]
pub struct RecvMulti<T: UseFixed = Fd> {
    fd: T,
    buf_group: u16,
    flags: i32,
    len: u32,
}

impl<T: UseFixed> RecvMulti<T> {
    #[inline]
    pub fn new(fd: T, buf_group: u16) -> Self {
        Self {
            fd,
            buf_group,
            flags: 0,
            len: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub const fn len(mut self, len: u32) -> Self {
        self.len = len;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        sqe.flags |= squeue::Flags::BUFFER_SELECT.bits();
        sqe.ioprio = sys::IORING_RECV_MULTISHOT as u16;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_RECV;
}

/// Send a userspace buffer on a socket.
#[derive(Debug)]
pub struct Send<T: UseFixed = Fd> {
    fd: T,
    buf: *const u8,
    len: u32,
    flags: i32,
}

impl<T: UseFixed> Send<T> {
    #[inline]
    pub fn new(fd: T, buf: *const u8, len: u32) -> Self {
        Self {
            fd,
            buf,
            len,
            flags: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_SEND;
}

/// Connect a socket.
#[derive(Debug)]
pub struct Connect<T: UseFixed = Fd> {
    fd: T,
    addr: *const libc::sockaddr,
    addrlen: libc::socklen_t,
}

impl<T: UseFixed> Connect<T> {
    #[inline]
    pub fn new(fd: T, addr: *const libc::sockaddr, addrlen: libc::socklen_t) -> Self {
        Self { fd, addr, addrlen }
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.addr as usize as u64;
        sqe.off_or_addr2 = self.addrlen as u64;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_CONNECT;
}

/// Accept one connection from a listening socket.
#[derive(Debug)]
pub struct Accept<T: UseFixed = Fd> {
    fd: T,
    addr: *mut libc::sockaddr,
    addrlen: *mut libc::socklen_t,
    flags: i32,
}

impl<T: UseFixed> Accept<T> {
    #[inline]
    pub fn new(fd: T, addr: *mut libc::sockaddr, addrlen: *mut libc::socklen_t) -> Self {
        Self {
            fd,
            addr,
            addrlen,
            flags: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.addr as usize as u64;
        sqe.off_or_addr2 = self.addrlen as usize as u64;
        sqe.op_flags = self.flags as u32;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_ACCEPT;
}

/// Accept connections repeatedly.
#[derive(Debug)]
pub struct AcceptMulti<T: UseFixed = Fd> {
    fd: T,
    flags: i32,
}

impl<T: UseFixed> AcceptMulti<T> {
    #[inline]
    pub fn new(fd: T) -> Self {
        Self { fd, flags: 0 }
    }

    #[inline]
    pub const fn flags(mut self, flags: i32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.ioprio = sys::IORING_ACCEPT_MULTISHOT as u16;
        sqe.op_flags = self.flags as u32;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_ACCEPT;
}

/// Send a socket message described by an `msghdr`.
#[derive(Debug)]
pub struct SendMsg<T: UseFixed = Fd> {
    fd: T,
    msg: *const libc::msghdr,
    flags: u32,
}

impl<T: UseFixed> SendMsg<T> {
    #[inline]
    pub fn new(fd: T, msg: *const libc::msghdr) -> Self {
        Self { fd, msg, flags: 0 }
    }

    #[inline]
    pub const fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.msg as usize as u64;
        sqe.len = 1;
        sqe.op_flags = self.flags;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_SENDMSG;
}

/// Receive a socket message described by an `msghdr`.
#[derive(Debug)]
pub struct RecvMsg<T: UseFixed = Fd> {
    fd: T,
    msg: *mut libc::msghdr,
    flags: u32,
    buf_group: u16,
}

impl<T: UseFixed> RecvMsg<T> {
    #[inline]
    pub fn new(fd: T, msg: *mut libc::msghdr) -> Self {
        Self {
            fd,
            msg,
            flags: 0,
            buf_group: 0,
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub const fn buf_group(mut self, group: u16) -> Self {
        self.buf_group = group;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = entry_with_fd(Self::CODE, self.fd);
        sqe.addr_or_splice_off_in = self.msg as usize as u64;
        sqe.len = 1;
        sqe.op_flags = self.flags;
        set_buffer_group(&mut sqe, self.buf_group);
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_RECVMSG;
}

/// Register a timeout operation.
#[derive(Debug)]
pub struct Timeout {
    timespec: *const Timespec,
    count: u32,
    flags: u32,
}

impl Timeout {
    #[inline]
    pub fn new(timespec: *const Timespec) -> Self {
        Self {
            timespec,
            count: 0,
            flags: 0,
        }
    }

    #[inline]
    pub const fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    #[inline]
    pub const fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.timespec as usize as u64;
        sqe.len = 1;
        sqe.off_or_addr2 = self.count as u64;
        sqe.op_flags = self.flags;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_TIMEOUT;
}

/// Cancel an existing request identified by its user data.
#[derive(Debug)]
pub struct AsyncCancel {
    user_data: u64,
}

impl AsyncCancel {
    #[inline]
    pub fn new(user_data: u64) -> Self {
        Self { user_data }
    }

    #[inline]
    pub fn build(self) -> Entry {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.user_data;
        Entry { inner: sqe }
    }

    pub const CODE: u8 = sys::IORING_OP_ASYNC_CANCEL;
}

#[cfg(test)]
mod tests {
    use core::{
        mem::{offset_of, size_of},
        ptr,
    };

    use super::*;
    use crate::types::{self, Fixed};

    fn raw(entry: &Entry) -> &sys::IoUringSqe {
        &entry.inner
    }

    #[test]
    fn file_opcode_layouts_match_kernel_fields() {
        let entry = ReadFixed::new(Fixed(7), ptr::null_mut(), 32, 9)
            .offset(11)
            .build();
        let sqe = raw(&entry);
        assert_eq!(sqe.opcode, ReadFixed::<types::Fixed>::CODE);
        assert_eq!(sqe.fd, 7);
        assert_eq!(sqe.flags, squeue::Flags::FIXED_FILE.bits());
        assert_eq!(sqe.len, 32);
        assert_eq!(sqe.off_or_addr2, 11);
        assert_eq!(sqe.buf_index_group, 9_u16.to_ne_bytes());
        assert_eq!(offset_of!(sys::IoUringSqe, user_data), 32);
    }

    #[test]
    fn multishot_recv_selects_provided_buffers() {
        let entry = RecvMulti::new(Fd(4), 13).build();
        let sqe = raw(&entry);
        assert_eq!(sqe.opcode, RecvMulti::<types::Fd>::CODE);
        assert_eq!(sqe.ioprio, sys::IORING_RECV_MULTISHOT as u16);
        assert_eq!(sqe.flags, squeue::Flags::BUFFER_SELECT.bits());
        assert_eq!(sqe.buf_index_group, 13_u16.to_ne_bytes());
    }

    #[test]
    fn every_builder_starts_with_zero_user_data() {
        let entries = [
            Read::new(Fd(1), ptr::null_mut(), 0).build(),
            Write::new(Fd(1), ptr::null(), 0).build(),
            Fsync::new(Fd(1)).build(),
            SyncFileRange::new(Fd(1), 0).build(),
            Fallocate::new(Fd(1), 0).build(),
            OpenAt::new(Fd(-100), ptr::null()).build(),
            Close::new(Fd(1)).build(),
            Recv::new(Fd(1), ptr::null_mut(), 0).build(),
            Send::new(Fd(1), ptr::null(), 0).build(),
            Connect::new(Fd(1), ptr::null(), 0).build(),
            Accept::new(Fd(1), ptr::null_mut(), ptr::null_mut()).build(),
            AcceptMulti::new(Fd(1)).build(),
            SendMsg::new(Fd(1), ptr::null()).build(),
            RecvMsg::new(Fd(1), ptr::null_mut()).build(),
            Timeout::new(ptr::null()).build(),
            AsyncCancel::new(1).build(),
        ];
        assert!(entries.iter().all(|entry| entry.get_user_data() == 0));
        assert_eq!(size_of::<Entry>(), 64);
    }
}
