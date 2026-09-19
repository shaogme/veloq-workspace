use veloq_std::{any::type_name, mem::size_of, ptr::NonNull};

use windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE;

use crate::{
    config::OwnedRawHandle,
    error::{IocpError, IocpResult},
    net::addr::SockAddrStorage,
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, Connect, Fallocate, FallocateRaw, Fsync,
        FsyncRaw, OpSend, Open, ProvidedBuf, ReadFixed, ReadRaw, Recv, RecvMulti, RecvProvided,
        SendTo, SyncFileRange, SyncFileRangeRaw, Timeout, UdpConnect, UdpRecvMulti, UdpRecvPacket,
        UdpSend, Wakeup, WriteFixed, WriteRaw, spec::PayloadBinding,
    },
};

use diagweave::prelude::*;
use veloq_buf::FixedBuf;
use veloq_driver_core::{platform::receive_pump::ReceivePumpState, slot::Generation};

pub enum IocpUserPayload {
    ReadFixed(ReadFixed),
    ReadRaw(ReadRaw),
    WriteFixed(WriteFixed),
    WriteRaw(WriteRaw),
    Recv(Recv),
    OpSend(OpSend),
    UdpSend(UdpSend),
    Close(Close),
    Fsync(Fsync),
    FsyncRaw(FsyncRaw),
    SyncFileRange(SyncFileRange),
    SyncFileRangeRaw(SyncFileRangeRaw),
    Fallocate(Fallocate),
    FallocateRaw(FallocateRaw),
    Timeout(Timeout),
    Connect(Connect),
    UdpConnect(UdpConnect),
    Accept(Accept),
    SendTo(SendTo),
    /// The persistent submit payload for the internal UDP receive pump.
    UdpRecvMulti(UdpRecvMulti),
    /// One datagram produced by the UDP receive pump.
    UdpRecvPacket(UdpRecvPacket),
    /// An accepted socket produced by an accept multishot operation.
    AcceptedSocket(AcceptedSocket),
    /// A provided receive buffer produced by a receive multishot operation.
    ProvidedBuf(ProvidedBuf),
    Open(Open),
    Wakeup(Wakeup),
    AcceptMulti(AcceptMulti),
    RecvProvided(RecvProvided),
    RecvMulti(RecvMulti),
}

pub(crate) enum IocpOpPayload {
    Read(KernelRef<ReadFixed>),
    ReadRaw(KernelRef<ReadRaw>),
    Write(KernelRef<WriteFixed>),
    WriteRaw(KernelRef<WriteRaw>),
    Recv(KernelRef<Recv>),
    Send(KernelRef<OpSend>),
    UdpSend(KernelRef<UdpSend>),
    Close(KernelRef<Close>),
    Fsync(KernelRef<Fsync>),
    FsyncRaw(KernelRef<FsyncRaw>),
    SyncRange(KernelRef<SyncFileRange>),
    SyncRangeRaw(KernelRef<SyncFileRangeRaw>),
    Fallocate(KernelRef<Fallocate>),
    FallocateRaw(KernelRef<FallocateRaw>),
    Timeout(KernelRef<Timeout>),
    Connect(KernelRef<Connect>),
    UdpConnect(KernelRef<UdpConnect>),
    Accept(AcceptPayload),
    AcceptMulti(AcceptMultiPayload),
    SendTo(SendToPayload),
    RecvProvided(RecvProvidedPayload),
    RecvMulti(RecvMultiPayload),
    UdpRecvMulti(UdpRecvMultiPayload),
    Open(OpenPayload),
    Wakeup(KernelRef<Wakeup>),
}

/// Reference to a kernel operation.
pub(crate) struct PayloadRef<T> {
    user: Option<NonNull<T>>,
}

impl<T> PayloadRef<T> {
    #[inline]
    pub(crate) const fn unbound() -> Self {
        Self { user: None }
    }

    #[inline]
    pub(crate) fn bind(&mut self, user: NonNull<T>) {
        self.user = Some(user);
    }

    #[inline]
    pub(crate) fn clear(&mut self) {
        self.user = None;
    }

    #[inline]
    pub(crate) unsafe fn as_ref(&self) -> IocpResult<&T> {
        let user = self.user.ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("payload_type", type_name::<T>())
                .attach_note("IOCP user payload used before binding")
        })?;
        // SAFETY: the payload is bound to the live slot payload before submission.
        Ok(unsafe { user.as_ref() })
    }

    #[inline]
    pub(crate) unsafe fn as_mut(&mut self) -> IocpResult<&mut T> {
        let mut user = self.user.ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .with_ctx("payload_type", type_name::<T>())
                .attach_note("IOCP user payload used before binding")
        })?;
        // SAFETY: the payload is bound to the live slot payload before submission.
        Ok(unsafe { user.as_mut() })
    }
}

pub(crate) struct KernelRef<T> {
    pub(crate) user: PayloadRef<T>,
}

/// Payload for the socket accept operation.
pub(crate) const ACCEPT_EX_ADDR_SECTION_LEN: usize = size_of::<SOCKADDR_STORAGE>() + 16;
pub(crate) const ACCEPT_EX_OUTPUT_BUFFER_LEN: usize = ACCEPT_EX_ADDR_SECTION_LEN * 2;

pub(crate) struct AcceptPayload {
    pub(crate) user: PayloadRef<Accept>,
    pub(crate) accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    pub(crate) accept_socket: Option<OwnedRawHandle>,
}

/// Persistent kernel state for an `AcceptMulti` operation.
///
/// The accepted socket is kept separate from the listening socket and is never reused for the
/// next request.  Later stages add the request-generation and inflight transitions around these
/// fields; keeping them in a dedicated payload now prevents the multishot path from falling back
/// to the one-shot `AcceptPayload` shape.
pub(crate) struct AcceptMultiPayload {
    pub(crate) user: PayloadRef<AcceptMulti>,
    pub(crate) accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    pub(crate) accept_socket: Option<OwnedRawHandle>,
    pub(crate) request_generation: Generation,
}

impl AcceptMultiPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            accept_buffer: [0; ACCEPT_EX_OUTPUT_BUFFER_LEN],
            accept_socket: None,
            request_generation: Generation::ZERO,
        }
    }
}

/// Persistent state for a one-record provided receive request.
///
/// The buffer is backend-owned.  It is intentionally optional until the receive submission
/// stage allocates and binds it, so construction cannot accidentally claim a buffer that the
/// kernel has not been given.
pub(crate) struct RecvProvidedPayload {
    pub(crate) user: PayloadRef<RecvProvided>,
    pub(crate) buffer: Option<FixedBuf>,
    pub(crate) request_generation: Generation,
}

impl RecvProvidedPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            buffer: None,
            request_generation: Generation::ZERO,
        }
    }
}

/// Persistent state for the TCP receive pump behind `RecvMulti`.
///
/// Request depth, leases and terminal state are deliberately owned by this payload rather than
/// by the facade.  The concrete pump transitions are added by the receive implementation stage.
pub(crate) struct RecvMultiPayload {
    pub(crate) user: PayloadRef<RecvMulti>,
    pub(crate) pump: Option<ReceivePumpState>,
}

impl RecvMultiPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            pump: None,
        }
    }
}

/// Payload for the socket send-to operation.
pub(crate) struct SendToPayload {
    pub(crate) user: PayloadRef<SendTo>,
    pub(crate) addr: SockAddrStorage,
    pub(crate) addr_len: i32,
}

/// Kernel-side binding for the persistent UDP receive pump.
///
/// The receive pump is kept in the slot's submit payload for the complete lifetime of the
/// logical operation.  Individual datagrams are represented by [`IocpUserPayload::UdpRecvPacket`]
/// and never borrow this binding.
pub(crate) struct UdpRecvMultiPayload {
    pub(crate) user: PayloadRef<UdpRecvMulti>,
}

/// Payload for the file open operation.
pub(crate) struct OpenPayload {
    pub(crate) user: PayloadRef<Open>,
}

pub(crate) fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
    KernelRef {
        user: PayloadRef::unbound(),
    }
}

impl<T> PayloadBinding<T> for KernelRef<T> {
    fn bind(&mut self, user: NonNull<T>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<Accept> for AcceptPayload {
    fn bind(&mut self, user: NonNull<Accept>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<AcceptMulti> for AcceptMultiPayload {
    fn bind(&mut self, user: NonNull<AcceptMulti>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<SendTo> for SendToPayload {
    fn bind(&mut self, user: NonNull<SendTo>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<RecvProvided> for RecvProvidedPayload {
    fn bind(&mut self, user: NonNull<RecvProvided>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<RecvMulti> for RecvMultiPayload {
    fn bind(&mut self, user: NonNull<RecvMulti>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<UdpRecvMulti> for UdpRecvMultiPayload {
    fn bind(&mut self, user: NonNull<UdpRecvMulti>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}

impl PayloadBinding<Open> for OpenPayload {
    fn bind(&mut self, user: NonNull<Open>) {
        self.user.bind(user);
    }

    fn clear(&mut self) {
        self.user.clear();
    }
}
