use crate::{IoFd, RawHandleMeta, SockAddr};
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
    UdpRecvFrom = 12,
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

/// Receive a UDP datagram together with its source address.
pub struct UdpRecvFrom {
    pub fd: IoFd,
    pub buf: FixedBuf,
    pub buf_offset: usize,
    pub addr: Option<std::net::SocketAddr>,
}

/// A received UDP datagram.
pub struct UdpRecvPacket {
    pub buf: UdpRecvPacketBuf,
    pub addr: std::net::SocketAddr,
}

pub enum UdpRecvPacketBuf {
    Owned(FixedBuf),
}

impl UdpRecvPacketBuf {
    #[inline]
    pub fn from_fixed_buf(buf: FixedBuf) -> Self {
        Self::Owned(buf)
    }

    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_slice(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.capacity(),
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
        }
    }
}
