use crate::{Handle, IoFd, RawHandleMeta, SockAddr};
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
    AcceptMulti = 19,
    AcceptedSocket = 20,
    RecvProvided = 21,
    ProvidedBuf = 22,
    RecvMulti = 23,
}

/// Read from a file descriptor at a specific offset using a fixed buffer.
pub struct ReadFixed<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct WriteFixed<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct Recv<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Send data from a fixed buffer to a socket.
pub struct Send<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Receive data from a UDP socket into a fixed buffer.
pub struct UdpRecv<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Send data from a fixed buffer to a UDP socket.
pub struct UdpSend<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
}

/// Connect a socket to a remote address.
pub struct Connect<H: Handle, A: SockAddr> {
    pub fd: IoFd<H>,
    /// Raw address bytes (sockaddr representation), boxed to reduce struct size.
    pub addr: A,
    pub addr_len: u32,
}

/// Connect a UDP socket to a remote address.
pub struct UdpConnect<H: Handle, A: SockAddr> {
    pub fd: IoFd<H>,
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
pub struct Close<H: Handle> {
    pub fd: IoFd<H>,
}

/// Flush file buffers to disk.
pub struct Fsync<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct Wakeup<H: Handle> {
    pub fd: IoFd<H>,
}

/// Accept a new connection on a listening socket.
/// Result includes the new socket handle and remote address.
pub struct Accept<H: Handle, A: SockAddr> {
    pub fd: IoFd<H>,
    /// Buffer for storing the remote address.
    /// On Windows, we parse the result from the AcceptEx output buffer, so we don't need this storage.
    pub addr: A,
    /// Length of the address buffer.
    pub addr_len: u32,
    /// Parsed remote address (populated after completion).
    pub remote_addr: Option<std::net::SocketAddr>,
}

/// Accept connections on a listening socket until the operation is cancelled.
///
/// One submission, many completions (io_uring multishot). Each completion carries a new
/// connection; the operation stays armed until it is cancelled or the kernel terminates it.
pub struct AcceptMulti<H: Handle> {
    pub fd: IoFd<H>,
}

/// One connection produced by an [`AcceptMulti`] stream.
///
/// Deliberately carries no address: `IORING_OP_ACCEPT`'s multishot variant has no `addr` /
/// `addrlen` fields at all, because several completions sharing one address buffer would
/// overwrite each other. The accepted descriptor arrives as the operation's `Completion`
/// (from the CQE's result), and the peer address has to be recovered with `getpeername`
/// afterwards.
pub struct AcceptedSocket;

/// Receive from a socket into a buffer the kernel picks out of the driver's provided-buffer
/// ring.
///
/// Deliberately carries no buffer: with `IOSQE_BUFFER_SELECT` the buffer is bound to the
/// connection only once data actually arrives, which is the entire point of provided buffers —
/// ten thousand idle connections no longer pin ten thousand receive buffers.
pub struct RecvProvided<H: Handle> {
    pub fd: IoFd<H>,
}

/// Receive from a socket until the operation is cancelled, each completion carrying a buffer the
/// kernel picked out of the driver's provided-buffer ring.
///
/// The intersection of [`AcceptMulti`] and [`RecvProvided`], and it needs nothing either of them
/// did not already need: the submit payload stays in the slot for the kernel to keep receiving
/// into, and every completion's record payload is a [`ProvidedBuf`].
///
/// A provided-buffer ring is not optional here — it is the kernel's rule, not a design choice:
/// the multishot variant of `IORING_OP_RECV` forces `IOSQE_BUFFER_SELECT`, because a single
/// caller-supplied buffer could not possibly hold several completions' worth of data.
pub struct RecvMulti<H: Handle> {
    pub fd: IoFd<H>,
}

/// The buffer one provided-buffer completion hands over.
///
/// `None` means the kernel consumed no buffer for this completion — either the ring was empty
/// (`-ENOBUFS`) or the operation failed before a buffer was selected. That is a real state of
/// a real completion, not a placeholder waiting to be filled in.
pub struct ProvidedBuf {
    pub buf: Option<FixedBuf>,
}

/// Send data to a specific address (UDP).
pub struct SendTo<H: Handle> {
    pub fd: IoFd<H>,
    pub buf: FixedBuf,
    pub buf_offset: usize,
    /// Target address.
    pub addr: std::net::SocketAddr,
}

/// Sync file range.
pub struct SyncFileRange<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct Fallocate<H: Handle> {
    pub fd: IoFd<H>,
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
pub struct UdpRecvFrom<H: Handle> {
    pub fd: IoFd<H>,
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
    pub fn from_fixed_buf(buf: FixedBuf) -> Self {
        Self::Owned(buf)
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.capacity(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_fixed_buf(self) -> Option<FixedBuf> {
        match self {
            Self::Owned(buf) => Some(buf),
        }
    }
}
