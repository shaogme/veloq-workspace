use crate::{IoFd, RawHandleMeta, SockAddr};
use std::ptr::NonNull;
use std::sync::Arc;
use veloq_buf::FixedBuf;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpKind {
    ReadFixed = 1,
    WriteFixed = 2,
    Recv = 3,
    Send = 4,
    Connect = 5,
    Close = 6,
    Fsync = 7,
    SyncFileRange = 8,
    Fallocate = 9,
    Accept = 10,
    SendTo = 11,
    UdpRecvStream = 12,
    Open = 13,
    Wakeup = 14,
    Timeout = 15,
    UdpRecv = 16,
    UdpSend = 17,
    UdpConnect = 18,
}

/// Read from a file descriptor at a specific offset using a fixed buffer.
pub struct ReadFixed {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Read from a file handle using a platform raw handle.
pub struct ReadRaw<H: RawHandleMeta> {
    pub fd: H,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Write to a file descriptor at a specific offset using a fixed buffer.
pub struct WriteFixed {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Write to a file handle using a platform raw handle.
pub struct WriteRaw<H: RawHandleMeta> {
    pub fd: H,
    pub buf: FixedBuf,
    pub offset: u64,
    pub buf_offset: usize,
}

/// Receive data from a socket into a fixed buffer.
pub struct Recv {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Send data from a fixed buffer to a socket.
pub struct Send {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Receive data from a UDP socket into a fixed buffer.
pub struct UdpRecv {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Send data from a fixed buffer to a UDP socket.
pub struct UdpSend {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Connect a socket to a remote address.
pub struct Connect<A: SockAddr> {
    pub fd: IoFd,
    /// Raw address bytes (sockaddr representation), boxed to reduce struct size.
    pub addr: A,
    pub addr_len: u32,
}

/// Connect a UDP socket to a remote address.
pub struct UdpConnect<A: SockAddr> {
    pub fd: IoFd,
    /// Raw address bytes (sockaddr representation), boxed to reduce struct size.
    pub addr: A,
    pub addr_len: u32,
}

/// Open a file.
/// Path representation is platform-agnostic (raw bytes).
#[derive(Debug)]
pub struct Open {
    /// Path stored in a fixed buffer.
    /// - Unix: UTF-8 encoded, null-terminated.
    /// - Windows: UTF-16 encoded, null-terminated (stored as bytes).
    pub path: FixedBuf,
    pub flags: i32,
    pub mode: u32,
}

/// Close a file descriptor or handle.
pub struct Close {
    pub fd: IoFd,
}

/// Flush file buffers to disk.
pub struct Fsync {
    pub fd: IoFd,
    /// If true, only sync data (not metadata).
    pub datasync: bool,
}

/// Sync a raw file handle.
pub struct FsyncRaw<H: RawHandleMeta> {
    pub fd: H,
    /// If true, only sync data (not metadata).
    pub datasync: bool,
}

/// Timeout operation (platform-specific timing).
pub struct Timeout {
    pub duration: std::time::Duration,
}

/// Wake up the event loop.
pub struct Wakeup {
    pub fd: IoFd,
}

/// Accept a new connection on a listening socket.
/// Result includes the new socket handle and remote address.
pub struct Accept<A: SockAddr> {
    pub fd: IoFd,
    /// Buffer for storing the remote address.
    /// On Windows, we parse the result from the AcceptEx output buffer, so we don't need this storage.
    pub addr: A,
    /// Length of the address buffer.
    pub addr_len: u32,
    /// Parsed remote address (populated after completion).
    pub remote_addr: Option<std::net::SocketAddr>,
}

/// Send data to a specific address (UDP).
pub struct SendTo {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
    /// Target address.
    pub addr: std::net::SocketAddr,
}

/// Sync file range.
pub struct SyncFileRange {
    pub fd: IoFd,
    pub offset: u64,
    pub nbytes: u64,
    pub flags: u32,
}

/// Sync a raw file handle range.
pub struct SyncFileRangeRaw<H: RawHandleMeta> {
    pub fd: H,
    pub offset: u64,
    pub nbytes: u64,
    pub flags: u32,
}

/// Pre-allocate file space.
pub struct Fallocate {
    pub fd: IoFd,
    pub mode: i32,
    pub offset: u64,
    pub len: u64,
}

/// Pre-allocate space on a raw file handle.
pub struct FallocateRaw<H: RawHandleMeta> {
    pub fd: H,
    pub mode: i32,
    pub offset: u64,
    pub len: u64,
}

/// Receive data as UDP datagram stream.
pub struct UdpRecvStream {
    pub fd: IoFd,
    /// Unix io_uring path uses this provided buffer; Windows can leave it as None.
    pub buf: Option<FixedBuf>,
    /// Unix io_uring path: source address parsed from recvmsg.
    pub addr: Option<std::net::SocketAddr>,
    /// Windows RIO path: resulting datagram, populated on completion.
    pub result: Option<UdpRecvPacket>,
}

/// A received UDP datagram.
pub struct UdpRecvPacket {
    pub buf: UdpRecvPacketBuf,
    pub addr: std::net::SocketAddr,
}

pub enum UdpRecvPacketBuf {
    Owned(FixedBuf),
    Leased(UdpRecvPacketBufLease),
}

impl UdpRecvPacketBuf {
    #[inline]
    pub fn from_fixed_buf(buf: FixedBuf) -> Self {
        Self::Owned(buf)
    }

    /// # Safety
    /// `ptr..ptr+len` must remain readable until the returned buffer is dropped.
    /// `owner` must keep the backing allocation alive and make `release(idx)` safe.
    #[inline]
    pub unsafe fn from_leased_parts(
        ptr: NonNull<u8>,
        len: usize,
        capacity: usize,
        idx: u32,
        owner: Arc<dyn UdpRecvPacketBufLeaseOwner>,
    ) -> Self {
        Self::Leased(UdpRecvPacketBufLease::new(ptr, len, capacity, idx, owner))
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_slice(),
            Self::Leased(buf) => buf.as_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
            Self::Leased(buf) => buf.len(),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.capacity(),
            Self::Leased(buf) => buf.capacity(),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn into_fixed_buf(self) -> Option<FixedBuf> {
        match self {
            Self::Owned(buf) => Some(buf),
            Self::Leased(_) => None,
        }
    }
}

pub trait UdpRecvPacketBufLeaseOwner: std::marker::Send + Sync {
    fn release(&self, idx: u32);
}

pub struct UdpRecvPacketBufLease {
    ptr: NonNull<u8>,
    len: u32,
    capacity: u32,
    idx: u32,
    owner: Arc<dyn UdpRecvPacketBufLeaseOwner>,
}

unsafe impl std::marker::Send for UdpRecvPacketBufLease {}

impl UdpRecvPacketBufLease {
    #[inline]
    fn new(
        ptr: NonNull<u8>,
        len: usize,
        capacity: usize,
        idx: u32,
        owner: Arc<dyn UdpRecvPacketBufLeaseOwner>,
    ) -> Self {
        assert!(len <= capacity, "len must be <= capacity");
        assert!(
            capacity <= u32::MAX as usize,
            "UDP recv packet buffer only supports capacity <= u32::MAX"
        );

        Self {
            ptr,
            len: len as u32,
            capacity: capacity as u32,
            idx,
            owner,
        }
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len()) }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len as usize
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for UdpRecvPacketBufLease {
    fn drop(&mut self) {
        self.owner.release(self.idx);
    }
}
