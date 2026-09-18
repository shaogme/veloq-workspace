//! Opcode builders used by the Veloq io_uring backend.
//!
//! Builders validate the control fields that are owned by io_uring before an SQE is produced.
//! Pointer-bearing constructors are unsafe: the caller must keep every pointed-to object alive,
//! initialized, and at the same address until the kernel has consumed the request and emitted its
//! CQE (or the request has been otherwise proven to be gone).

use core::convert::TryFrom;

use veloq_std::io::{Error, ErrorKind, Result};

use crate::{
    squeue::{self, Entry},
    sys,
    types::{AsyncCancelFlags, Fd, FsyncFlags, TimeoutFlags, Timespec, UseFixed},
};

const ACCEPT_FLAGS: u32 = libc::SOCK_CLOEXEC as u32
    | libc::SOCK_NONBLOCK as u32
    | sys::IORING_ACCEPT_DONTWAIT
    | sys::IORING_ACCEPT_POLL_FIRST;
const SYNC_FILE_RANGE_FLAGS: u32 = 1 | 2 | 4;

#[inline]
fn invalid_input() -> Error {
    Error::from(ErrorKind::InvalidInput)
}

#[inline]
fn unsupported() -> Error {
    Error::from_raw_os_error(libc::EOPNOTSUPP)
}

#[inline]
fn overflow() -> Error {
    Error::from_raw_os_error(libc::EOVERFLOW)
}

#[inline]
fn validate_known_flags(flags: u32, known: u32) -> Result<()> {
    if flags & !known != 0 {
        Err(unsupported())
    } else {
        Ok(())
    }
}

#[inline]
fn validate_accept_flags(flags: i32) -> Result<()> {
    validate_known_flags(flags as u32, ACCEPT_FLAGS)
}

#[inline]
fn sqe_zeroed() -> sys::IoUringSqe {
    // All SQE fields are integer or byte storage, so zero is a valid bit pattern.
    unsafe { core::mem::zeroed() }
}

#[inline]
fn set_fd<T: UseFixed>(sqe: &mut sys::IoUringSqe, fd: T) -> Result<()> {
    if let Some(raw) = fd.raw_fd() {
        sqe.fd = raw;
        return Ok(());
    }

    let Some(index) = fd.fixed_index() else {
        return Err(invalid_input());
    };
    sqe.fd = i32::try_from(index).map_err(|_| overflow())?;
    sqe.flags |= squeue::Flags::FIXED_FILE.bits();
    Ok(())
}

#[inline]
fn entry_with_fd<T: UseFixed>(opcode: u8, fd: T) -> Result<sys::IoUringSqe> {
    let mut sqe = sqe_zeroed();
    sqe.opcode = opcode;
    set_fd(&mut sqe, fd)?;
    Ok(sqe)
}

#[inline]
fn set_buffer_group(sqe: &mut sys::IoUringSqe, group: u16) {
    sqe.buf_index_group = group.to_ne_bytes();
}

/// Do not perform any I/O.
#[derive(Debug)]
pub struct Nop;

impl Default for Nop {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl Nop {
    /// The NOP opcode.
    pub const CODE: u8 = sys::IORING_OP_NOP;

    #[inline]
    pub const fn new() -> Self {
        Self
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        Ok(Entry { inner: sqe })
    }
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
    /// Create a read request.
    ///
    /// # Safety
    ///
    /// `buf` must point to a writable region of at least `len` bytes and that region must remain
    /// valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *mut u8, len: u32) -> Self {
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

    /// Set flags passed to the underlying `preadv2`-compatible operation.
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        Ok(Entry { inner: sqe })
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
    /// Create a fixed-buffer read request.
    ///
    /// # Safety
    ///
    /// `buf` must point into the registered buffer selected by `buf_index`, cover at least `len`
    /// bytes, and remain valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *mut u8, len: u32, buf_index: u16) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        sqe.buf_index_group = self.buf_index.to_ne_bytes();
        Ok(Entry { inner: sqe })
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
    /// Create a write request.
    ///
    /// # Safety
    ///
    /// `buf` must point to a readable region of at least `len` bytes and that region must remain
    /// valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *const u8, len: u32) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        Ok(Entry { inner: sqe })
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
    /// Create a fixed-buffer write request.
    ///
    /// # Safety
    ///
    /// `buf` must point into the registered buffer selected by `buf_index`, cover at least `len`
    /// bytes, and remain valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *const u8, len: u32, buf_index: u16) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.rw_flags as u32;
        sqe.buf_index_group = self.buf_index.to_ne_bytes();
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        validate_known_flags(self.flags.bits(), sys::IORING_FSYNC_DATASYNC)?;
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.op_flags = self.flags.bits();
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        validate_known_flags(self.flags, SYNC_FILE_RANGE_FLAGS)?;
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.len = self.len;
        sqe.off_or_addr2 = self.offset;
        sqe.op_flags = self.flags;
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.len;
        sqe.len = self.mode as u32;
        sqe.off_or_addr2 = self.offset;
        Ok(Entry { inner: sqe })
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
    /// Create an `openat` request.
    ///
    /// # Safety
    ///
    /// `pathname` must point to a NUL-terminated path that remains valid and unmoved until the
    /// kernel has finished the request.
    #[inline]
    pub unsafe fn new(dirfd: Fd, pathname: *const libc::c_char) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = self.dirfd.0;
        sqe.addr_or_splice_off_in = self.pathname as usize as u64;
        sqe.len = self.mode;
        sqe.op_flags = self.flags as u32;
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        if let Some(raw) = self.fd.raw_fd() {
            sqe.fd = raw;
        } else if let Some(index) = self.fd.fixed_index() {
            sqe.splice_fd_in_or_file_index = index.checked_add(1).ok_or_else(overflow)?;
        } else {
            return Err(invalid_input());
        }
        Ok(Entry { inner: sqe })
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
    /// Create a receive request.
    ///
    /// # Safety
    ///
    /// `buf` must point to a writable region of at least `len` bytes, or be a valid null pointer
    /// only when the caller will add buffer selection before submission. The pointed-to state must
    /// remain valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *mut u8, len: u32) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        if self.flags & libc::MSG_WAITALL != 0 {
            return Err(invalid_input());
        }
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        set_buffer_group(&mut sqe, self.buf_group);
        sqe.flags |= squeue::Flags::BUFFER_SELECT.bits();
        sqe.ioprio = sys::IORING_RECV_MULTISHOT as u16;
        Ok(Entry { inner: sqe })
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
    /// Create a send request.
    ///
    /// # Safety
    ///
    /// `buf` must point to a readable region of at least `len` bytes and remain valid and unmoved
    /// until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, buf: *const u8, len: u32) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.buf as usize as u64;
        sqe.len = self.len;
        sqe.op_flags = self.flags as u32;
        Ok(Entry { inner: sqe })
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
    /// Create a connect request.
    ///
    /// # Safety
    ///
    /// `addr` must point to an initialized socket address containing at least `addrlen` bytes and
    /// must remain valid and unmoved until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, addr: *const libc::sockaddr, addrlen: libc::socklen_t) -> Self {
        Self { fd, addr, addrlen }
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.addr as usize as u64;
        sqe.off_or_addr2 = self.addrlen as u64;
        Ok(Entry { inner: sqe })
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
    /// Create an accept request.
    ///
    /// # Safety
    ///
    /// `addr` and `addrlen` must point to writable storage valid until the kernel has finished the
    /// request. `addrlen` must be initialized as required by `accept4(2)`.
    #[inline]
    pub unsafe fn new(fd: T, addr: *mut libc::sockaddr, addrlen: *mut libc::socklen_t) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        validate_accept_flags(self.flags)?;
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.addr as usize as u64;
        sqe.off_or_addr2 = self.addrlen as usize as u64;
        sqe.op_flags = self.flags as u32;
        Ok(Entry { inner: sqe })
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
    pub fn build(self) -> Result<Entry> {
        validate_accept_flags(self.flags)?;
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.ioprio = sys::IORING_ACCEPT_MULTISHOT as u16;
        sqe.op_flags = self.flags as u32;
        Ok(Entry { inner: sqe })
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
    /// Create a sendmsg request.
    ///
    /// # Safety
    ///
    /// `msg` and every buffer reachable through the message must remain initialized, valid, and at
    /// the same addresses until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, msg: *const libc::msghdr) -> Self {
        Self { fd, msg, flags: 0 }
    }

    #[inline]
    pub const fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.msg as usize as u64;
        sqe.len = 1;
        sqe.op_flags = self.flags;
        Ok(Entry { inner: sqe })
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
    /// Create a recvmsg request.
    ///
    /// # Safety
    ///
    /// `msg` and every writable buffer reachable through the message must remain initialized,
    /// valid, and at the same addresses until the kernel has finished the request.
    #[inline]
    pub unsafe fn new(fd: T, msg: *mut libc::msghdr) -> Self {
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
    pub fn build(self) -> Result<Entry> {
        let mut sqe = entry_with_fd(Self::CODE, self.fd)?;
        sqe.addr_or_splice_off_in = self.msg as usize as u64;
        sqe.len = 1;
        sqe.op_flags = self.flags;
        set_buffer_group(&mut sqe, self.buf_group);
        Ok(Entry { inner: sqe })
    }

    pub const CODE: u8 = sys::IORING_OP_RECVMSG;
}

/// Register a timeout operation.
#[derive(Debug)]
pub struct Timeout {
    timespec: *const Timespec,
    count: u32,
    flags: TimeoutFlags,
}

impl Timeout {
    /// Create a timeout request.
    ///
    /// # Safety
    ///
    /// `timespec` must point to an initialized [`Timespec`] that remains valid and unmoved until
    /// the kernel has finished the request.
    #[inline]
    pub unsafe fn new(timespec: *const Timespec) -> Self {
        Self {
            timespec,
            count: 0,
            flags: TimeoutFlags::empty(),
        }
    }

    #[inline]
    pub const fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    #[inline]
    pub const fn flags(mut self, flags: TimeoutFlags) -> Self {
        self.flags = flags;
        self
    }

    fn validate_flags(flags: TimeoutFlags) -> Result<()> {
        let bits = flags.bits();
        let known = sys::IORING_TIMEOUT_ABS
            | sys::IORING_TIMEOUT_UPDATE
            | sys::IORING_TIMEOUT_BOOTTIME
            | sys::IORING_TIMEOUT_REALTIME
            | sys::IORING_LINK_TIMEOUT_UPDATE
            | sys::IORING_TIMEOUT_ETIME_SUCCESS
            | sys::IORING_TIMEOUT_MULTISHOT;
        validate_known_flags(bits, known)?;
        if (bits & sys::IORING_TIMEOUT_CLOCK_MASK).count_ones() > 1 {
            return Err(invalid_input());
        }
        if bits & sys::IORING_TIMEOUT_UPDATE_MASK != 0 {
            return Err(unsupported());
        }
        Ok(())
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        Self::validate_flags(self.flags)?;
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.timespec as usize as u64;
        sqe.len = 1;
        sqe.off_or_addr2 = self.count as u64;
        sqe.op_flags = self.flags.bits();
        Ok(Entry { inner: sqe })
    }

    pub const CODE: u8 = sys::IORING_OP_TIMEOUT;
}

/// Cancel an existing request identified by its user data.
#[derive(Debug)]
pub struct AsyncCancel {
    user_data: u64,
    flags: AsyncCancelFlags,
}

impl AsyncCancel {
    #[inline]
    pub fn new(user_data: u64) -> Self {
        Self {
            user_data,
            flags: AsyncCancelFlags::empty(),
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: AsyncCancelFlags) -> Self {
        self.flags = flags;
        self
    }

    fn validate_flags(flags: AsyncCancelFlags) -> Result<()> {
        let bits = flags.bits();
        let known = sys::IORING_ASYNC_CANCEL_ALL
            | sys::IORING_ASYNC_CANCEL_FD
            | sys::IORING_ASYNC_CANCEL_ANY
            | sys::IORING_ASYNC_CANCEL_FD_FIXED
            | sys::IORING_ASYNC_CANCEL_USERDATA
            | sys::IORING_ASYNC_CANCEL_OP;
        validate_known_flags(bits, known)?;
        let selectors = sys::IORING_ASYNC_CANCEL_FD
            | sys::IORING_ASYNC_CANCEL_FD_FIXED
            | sys::IORING_ASYNC_CANCEL_USERDATA
            | sys::IORING_ASYNC_CANCEL_OP;
        if bits & sys::IORING_ASYNC_CANCEL_ANY != 0 && bits & selectors != 0 {
            return Err(invalid_input());
        }
        if bits & sys::IORING_ASYNC_CANCEL_FD != 0 && bits & sys::IORING_ASYNC_CANCEL_FD_FIXED != 0
        {
            return Err(invalid_input());
        }
        if bits & (sys::IORING_ASYNC_CANCEL_FD | sys::IORING_ASYNC_CANCEL_FD_FIXED) != 0 {
            // This builder deliberately has no fd selector; accepting these flags would encode
            // -1 as a real selector and cancel an unrelated request.
            return Err(unsupported());
        }
        Ok(())
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        Self::validate_flags(self.flags)?;
        let mut sqe = sqe_zeroed();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.user_data;
        sqe.op_flags = self.flags.bits();
        Ok(Entry { inner: sqe })
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
    fn nop_has_only_the_kernel_required_fields() {
        let entry = Nop::new().build().expect("NOP must always build");
        let sqe = raw(&entry);
        assert_eq!(sqe.opcode, Nop::CODE);
        assert_eq!(sqe.fd, -1);
        assert_eq!(sqe.flags, 0);
        assert_eq!(sqe.ioprio, 0);
        assert_eq!(sqe.user_data, 0);
        assert_eq!(sqe.command, [0, 0]);
    }

    #[test]
    fn file_opcode_layouts_match_kernel_fields() {
        let mut buffer = [0_u8; 32];
        let entry = unsafe { ReadFixed::new(Fixed(7), buffer.as_mut_ptr(), 32, 9) }
            .offset(11)
            .build()
            .expect("valid fixed descriptor");
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
    fn fixed_fd_encoding_rejects_values_that_do_not_fit_sqes() {
        let error = Close::new(Fixed(u32::MAX))
            .build()
            .expect_err("index overflow");
        assert_eq!(error.raw_os_error(), Some(libc::EOVERFLOW));

        let mut buffer = [0_u8; 1];
        let error = unsafe { Read::new(Fixed(u32::MAX), buffer.as_mut_ptr(), 0) }
            .build()
            .expect_err("index overflow");
        assert_eq!(error.raw_os_error(), Some(libc::EOVERFLOW));
    }

    #[test]
    fn multishot_recv_selects_provided_buffers() {
        let entry = RecvMulti::new(Fd(4), 13)
            .build()
            .expect("valid multishot request");
        let sqe = raw(&entry);
        assert_eq!(sqe.opcode, RecvMulti::<types::Fd>::CODE);
        assert_eq!(sqe.ioprio, sys::IORING_RECV_MULTISHOT as u16);
        assert_eq!(sqe.flags, squeue::Flags::BUFFER_SELECT.bits());
        assert_eq!(sqe.buf_index_group, 13_u16.to_ne_bytes());
    }

    #[test]
    fn multishot_recv_rejects_msg_waitall() {
        let error = RecvMulti::new(Fd(4), 13)
            .flags(libc::MSG_WAITALL)
            .build()
            .expect_err("MSG_WAITALL is incompatible with multishot recv");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn timeout_rejects_unknown_and_conflicting_flags() {
        let timespec = Timespec::new();
        let unknown = unsafe { Timeout::new(&timespec) }
            .flags(TimeoutFlags::from_bits_retain(1 << 31))
            .build()
            .expect_err("unknown timeout flags");
        assert_eq!(unknown.kind(), ErrorKind::Unsupported);

        let clocks = unsafe { Timeout::new(&timespec) }
            .flags(TimeoutFlags::BOOTTIME.union(TimeoutFlags::REALTIME))
            .build()
            .expect_err("two timeout clocks");
        assert_eq!(clocks.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn cancel_rejects_selector_without_a_matching_fd_field() {
        let error = AsyncCancel::new(42)
            .flags(AsyncCancelFlags::FD)
            .build()
            .expect_err("FD cancellation needs an fd selector");
        assert_eq!(error.kind(), ErrorKind::Unsupported);
    }

    #[test]
    fn every_pointer_builder_starts_with_zero_user_data() {
        let mut buffer = [0_u8; 8];
        let path = [0_i8];
        let mut address: libc::sockaddr_storage = unsafe { core::mem::zeroed() };
        let mut address_len = core::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        let mut message: libc::msghdr = unsafe { core::mem::zeroed() };
        let timespec = Timespec::new();
        let entries = [
            unsafe { Read::new(Fd(1), buffer.as_mut_ptr(), 0) }
                .build()
                .unwrap(),
            unsafe { Write::new(Fd(1), buffer.as_ptr(), 0) }
                .build()
                .unwrap(),
            Fsync::new(Fd(1)).build().unwrap(),
            SyncFileRange::new(Fd(1), 0).build().unwrap(),
            Fallocate::new(Fd(1), 0).build().unwrap(),
            unsafe { OpenAt::new(Fd(-100), path.as_ptr()) }
                .build()
                .unwrap(),
            Close::new(Fd(1)).build().unwrap(),
            unsafe { Recv::new(Fd(1), buffer.as_mut_ptr(), 0) }
                .build()
                .unwrap(),
            unsafe { Send::new(Fd(1), buffer.as_ptr(), 0) }
                .build()
                .unwrap(),
            unsafe {
                Connect::new(
                    Fd(1),
                    ptr::addr_of!(address).cast(),
                    core::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
                )
            }
            .build()
            .unwrap(),
            unsafe {
                Accept::new(
                    Fd(1),
                    ptr::addr_of_mut!(address).cast(),
                    ptr::addr_of_mut!(address_len),
                )
            }
            .build()
            .unwrap(),
            AcceptMulti::new(Fd(1)).build().unwrap(),
            unsafe { SendMsg::new(Fd(1), ptr::addr_of!(message)) }
                .build()
                .unwrap(),
            unsafe { RecvMsg::new(Fd(1), ptr::addr_of_mut!(message)) }
                .build()
                .unwrap(),
            unsafe { Timeout::new(&timespec) }.build().unwrap(),
            AsyncCancel::new(1).build().unwrap(),
        ];
        assert!(entries.iter().all(|entry| entry.get_user_data() == 0));
        assert_eq!(size_of::<Entry>(), 64);
    }
}
