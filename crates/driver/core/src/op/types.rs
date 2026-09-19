use crate::platform::receive_pump::{ReceivePermit, ReceivePumpState};
use crate::{Handle, IoFd, RawHandleMeta, SockAddr};
use veloq_buf::FixedBuf;
use veloq_std::{net::SocketAddr, time::Duration};

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
    Open = 13,
    Wakeup = 14,
    Timeout = 15,
    UdpSend = 17,
    UdpConnect = 18,
    AcceptMulti = 19,
    AcceptedSocket = 20,
    RecvProvided = 21,
    ProvidedBuf = 22,
    RecvMulti = 23,
    UdpRecvMulti = 24,
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
    pub duration: Duration,
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
    pub remote_addr: Option<SocketAddr>,
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

/// Receive UDP datagrams continuously through one logical driver operation.
///
/// The receive pump owns the fixed receive slots for the lifetime of the operation.  A backend
/// may submit one request per slot, but all completions still belong to this single logical op.
/// This operation intentionally has no [`SingleShotOp`](crate::op::SingleShotOp) implementation;
/// it can only be consumed as a completion stream.
pub struct UdpRecvMulti<H: Handle> {
    pub fd: IoFd<H>,
    pump: ReceivePumpState,
}

impl<H: Handle> UdpRecvMulti<H> {
    /// Construct a receive operation from a backend-owned pump.
    ///
    /// This constructor is intentionally named for backend use.  The facade receives an
    /// operation only through [`UdpReceiveOperationBuilder`](crate::driver::UdpReceiveOperationBuilder)
    /// and never owns or initializes the pump itself.
    pub fn from_backend(fd: IoFd<H>, pump: ReceivePumpState) -> Self {
        Self { fd, pump }
    }
}

/// Backend-only access to the pump carried by [`UdpRecvMulti`].
///
/// The operation keeps the pump field private so importing the operation alias does not expose
/// the state machine to the facade.  Backend crates use this contract when building SQEs or
/// routing native completions.
pub trait UdpRecvMultiBackend {
    fn receive_pump(&self) -> &ReceivePumpState;
    fn receive_pump_mut(&mut self) -> &mut ReceivePumpState;
}

impl<H: Handle> UdpRecvMultiBackend for UdpRecvMulti<H> {
    fn receive_pump(&self) -> &ReceivePumpState {
        &self.pump
    }

    fn receive_pump_mut(&mut self) -> &mut ReceivePumpState {
        &mut self.pump
    }
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
    pub addr: SocketAddr,
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

/// A received UDP datagram.
pub struct UdpRecvPacket {
    pub buf: UdpRecvPacketBuf,
    pub addr: SocketAddr,
}

pub enum UdpRecvPacketBuf {
    Owned(FixedBuf),
    OwnedWithPermit {
        buf: FixedBuf,
        permit: ReceivePermit,
    },
}

impl UdpRecvPacketBuf {
    pub fn from_fixed_buf(buf: FixedBuf) -> Self {
        Self::Owned(buf)
    }

    pub fn from_fixed_buf_with_permit(buf: FixedBuf, permit: ReceivePermit) -> Self {
        Self::OwnedWithPermit { buf, permit }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Owned(buf) => buf.as_slice(),
            Self::OwnedWithPermit { buf, .. } => buf.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.len(),
            Self::OwnedWithPermit { buf, .. } => buf.len(),
        }
    }

    pub fn capacity(&self) -> usize {
        match self {
            Self::Owned(buf) => buf.capacity(),
            Self::OwnedWithPermit { buf, .. } => buf.capacity(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn into_fixed_buf(self) -> Option<FixedBuf> {
        match self {
            Self::Owned(buf) => Some(buf),
            Self::OwnedWithPermit { buf, permit } => {
                drop(permit);
                Some(buf)
            }
        }
    }

    pub fn into_fixed_buf_with_permit(self) -> Option<(FixedBuf, ReceivePermit)> {
        match self {
            Self::Owned(buf) => Some((buf, ReceivePermit::detached())),
            Self::OwnedWithPermit { buf, permit } => Some((buf, permit)),
        }
    }
}
