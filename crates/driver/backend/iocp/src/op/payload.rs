use veloq_std::{any::type_name, format, mem::size_of, ptr::NonNull, vec::Vec};

use windows_sys::Win32::Networking::WinSock::SOCKADDR_STORAGE;

use crate::{
    config::OwnedRawHandle,
    error::{IocpError, IocpResult},
    net::SockAddrStorage,
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, Connect, Fallocate, FallocateRaw, Fsync,
        FsyncRaw, OpSend, Open, OverlappedEntry, PayloadBinding, ProvidedBuf, ReadFixed, ReadRaw,
        Recv, RecvMulti, RecvProvided, SendTo, SyncFileRange, SyncFileRangeRaw, Timeout,
        UdpConnect, UdpRecvMulti, UdpRecvPacket, UdpSend, Wakeup, WriteFixed, WriteRaw,
    },
};

use diagweave::prelude::*;
use veloq_buf::FixedBuf;
use veloq_driver_core::{Generation, OpToken, ReceivePumpState};

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
    user: PayloadRef<T>,
}

impl<T> KernelRef<T> {
    pub(crate) fn new(_user: &T) -> Self {
        Self {
            user: PayloadRef::unbound(),
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<T> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<T> {
        &mut self.user
    }
}

/// Payload for the socket accept operation.
pub(crate) const ACCEPT_EX_ADDR_SECTION_LEN: usize = size_of::<SOCKADDR_STORAGE>() + 16;
pub(crate) const ACCEPT_EX_OUTPUT_BUFFER_LEN: usize = ACCEPT_EX_ADDR_SECTION_LEN * 2;

pub(crate) struct AcceptPayload {
    user: PayloadRef<Accept>,
    accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    accept_socket: Option<OwnedRawHandle>,
}

impl AcceptPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            accept_buffer: [0; ACCEPT_EX_OUTPUT_BUFFER_LEN],
            accept_socket: None,
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<Accept> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<Accept> {
        &mut self.user
    }

    pub(crate) fn accept_buffer(&self) -> &[u8; ACCEPT_EX_OUTPUT_BUFFER_LEN] {
        &self.accept_buffer
    }

    pub(crate) fn accept_buffer_mut(&mut self) -> &mut [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN] {
        &mut self.accept_buffer
    }

    pub(crate) fn accept_socket(&self) -> Option<&OwnedRawHandle> {
        self.accept_socket.as_ref()
    }

    pub(crate) fn accept_socket_mut(&mut self) -> &mut Option<OwnedRawHandle> {
        &mut self.accept_socket
    }
}

/// Persistent kernel state for an `AcceptMulti` operation.
///
/// The accepted socket is kept separate from the listening socket and is never reused for the
/// next request.  Later stages add the request-generation and inflight transitions around these
/// fields; keeping them in a dedicated payload now prevents the multishot path from falling back
/// to the one-shot `AcceptPayload` shape.
pub(crate) struct AcceptMultiPayload {
    user: PayloadRef<AcceptMulti>,
    accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    accept_socket: Option<OwnedRawHandle>,
    request_generation: Generation,
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

    pub(crate) fn user(&self) -> &PayloadRef<AcceptMulti> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<AcceptMulti> {
        &mut self.user
    }

    pub(crate) fn accept_buffer(&self) -> &[u8; ACCEPT_EX_OUTPUT_BUFFER_LEN] {
        &self.accept_buffer
    }

    pub(crate) fn accept_buffer_mut(&mut self) -> &mut [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN] {
        &mut self.accept_buffer
    }

    pub(crate) fn accept_socket(&self) -> Option<&OwnedRawHandle> {
        self.accept_socket.as_ref()
    }

    pub(crate) fn accept_socket_mut(&mut self) -> &mut Option<OwnedRawHandle> {
        &mut self.accept_socket
    }

    pub(crate) const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) fn set_request_generation(&mut self, generation: Generation) {
        self.request_generation = generation;
    }
}

/// Persistent state for a one-record provided receive request.
///
/// The buffer is backend-owned.  It is intentionally optional until the receive submission
/// stage allocates and binds it, so construction cannot accidentally claim a buffer that the
/// kernel has not been given.
pub(crate) struct RecvProvidedPayload {
    user: PayloadRef<RecvProvided>,
    buffer: Option<FixedBuf>,
    request_generation: Generation,
}

impl RecvProvidedPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            buffer: None,
            request_generation: Generation::ZERO,
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<RecvProvided> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<RecvProvided> {
        &mut self.user
    }

    pub(crate) fn buffer(&self) -> Option<&FixedBuf> {
        self.buffer.as_ref()
    }

    pub(crate) fn buffer_mut(&mut self) -> &mut Option<FixedBuf> {
        &mut self.buffer
    }

    pub(crate) const fn request_generation(&self) -> Generation {
        self.request_generation
    }

    pub(crate) fn set_request_generation(&mut self, generation: Generation) {
        self.request_generation = generation;
    }
}

/// Persistent state for the TCP receive pump behind `RecvMulti`.
///
/// Request depth, leases and terminal state are deliberately owned by this payload rather than
/// by the facade.  The concrete pump transitions are added by the receive implementation stage.
pub(crate) struct RecvMultiPayload {
    user: PayloadRef<RecvMulti>,
    pump: Option<ReceivePumpState>,
    raw_buffer: Option<FixedBuf>,
}

impl RecvMultiPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            pump: None,
            raw_buffer: None,
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<RecvMulti> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<RecvMulti> {
        &mut self.user
    }

    pub(crate) fn pump(&self) -> Option<&ReceivePumpState> {
        self.pump.as_ref()
    }

    pub(crate) fn pump_mut(&mut self) -> &mut Option<ReceivePumpState> {
        &mut self.pump
    }

    pub(crate) fn raw_buffer(&self) -> Option<&FixedBuf> {
        self.raw_buffer.as_ref()
    }

    pub(crate) fn raw_buffer_mut(&mut self) -> &mut Option<FixedBuf> {
        &mut self.raw_buffer
    }
}

/// Payload for the socket send-to operation.
pub(crate) struct SendToPayload {
    user: PayloadRef<SendTo>,
    addr: SockAddrStorage,
    addr_len: i32,
}

impl SendToPayload {
    pub(crate) fn new(addr: SockAddrStorage, addr_len: i32) -> Self {
        Self {
            user: PayloadRef::unbound(),
            addr,
            addr_len,
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<SendTo> {
        &self.user
    }

    pub(crate) fn addr(&self) -> &SockAddrStorage {
        &self.addr
    }

    pub(crate) const fn addr_len(&self) -> i32 {
        self.addr_len
    }
}

/// Kernel-side binding for the persistent UDP receive pump.
///
/// The receive pump is kept in the slot's submit payload for the complete lifetime of the
/// logical operation.  Individual datagrams are represented by [`IocpUserPayload::UdpRecvPacket`]
/// and never borrow this binding.
pub(crate) struct RawUdpReceiveRequest {
    slot_id: u32,
    header: OverlappedEntry,
    address: SockAddrStorage,
    address_len: i32,
}

pub(crate) struct UdpRecvMultiPayload {
    user: PayloadRef<UdpRecvMulti>,
    request_id: u64,
    raw_requests: Option<Vec<RawUdpReceiveRequest>>,
    terminal_error: Option<Report<IocpError>>,
}

impl RawUdpReceiveRequest {
    pub(crate) fn new(slot_id: u32, token: OpToken) -> Self {
        Self {
            slot_id,
            header: OverlappedEntry::new(token),
            address: SockAddrStorage::default(),
            address_len: 0,
        }
    }

    pub(crate) const fn slot_id(&self) -> u32 {
        self.slot_id
    }

    pub(crate) fn header_mut(&mut self) -> &mut OverlappedEntry {
        &mut self.header
    }

    pub(crate) fn header(&self) -> &OverlappedEntry {
        &self.header
    }

    pub(crate) fn address(&self) -> &SockAddrStorage {
        &self.address
    }

    pub(crate) fn address_len(&self) -> i32 {
        self.address_len
    }

    pub(crate) fn with_submit_fields<T>(
        &mut self,
        submit: impl FnOnce(&mut OverlappedEntry, &mut SockAddrStorage, &mut i32) -> T,
    ) -> T {
        submit(&mut self.header, &mut self.address, &mut self.address_len)
    }
}

impl UdpRecvMultiPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
            request_id: 0,
            raw_requests: None,
            terminal_error: None,
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<UdpRecvMulti> {
        &self.user
    }

    pub(crate) fn user_mut(&mut self) -> &mut PayloadRef<UdpRecvMulti> {
        &mut self.user
    }

    pub(crate) fn try_allocate_raw_requests(
        &mut self,
        token: OpToken,
        depth: usize,
    ) -> IocpResult<()> {
        if let Some(requests) = &self.raw_requests {
            if requests.len() == depth {
                return Ok(());
            }
            return IocpError::InvalidState
                .with_ctx("existing_depth", requests.len())
                .with_ctx("requested_depth", depth)
                .attach_note("raw UDP receive request owner depth changed after allocation");
        }

        let mut requests = Vec::new();
        requests.try_reserve_exact(depth).map_err(|error| {
            IocpError::Submission
                .to_report()
                .with_ctx("request_depth", depth)
                .attach_note(format!(
                    "failed to reserve raw UDP request contexts: {error}"
                ))
        })?;
        for slot_id in 0..depth {
            let slot_id = u32::try_from(slot_id).map_err(|_| {
                IocpError::InvalidInput
                    .to_report()
                    .with_ctx("request_depth", depth)
                    .attach_note("raw UDP receive slot id exceeds u32")
            })?;
            requests.push(RawUdpReceiveRequest::new(slot_id, token));
        }
        self.raw_requests = Some(requests);
        Ok(())
    }

    pub(crate) fn raw_requests(&self) -> Option<&[RawUdpReceiveRequest]> {
        self.raw_requests.as_deref()
    }

    pub(crate) fn raw_requests_mut(&mut self) -> Option<&mut [RawUdpReceiveRequest]> {
        self.raw_requests.as_deref_mut()
    }

    pub(crate) fn raw_request_mut(&mut self, slot_id: u32) -> Option<&mut RawUdpReceiveRequest> {
        self.raw_requests_mut()?.get_mut(slot_id as usize)
    }

    pub(crate) fn with_bound_parts_mut<T>(
        &mut self,
        access: impl FnOnce(
            &mut UdpRecvMulti,
            &mut [RawUdpReceiveRequest],
            &mut u64,
            &mut Option<Report<IocpError>>,
        ) -> IocpResult<T>,
    ) -> IocpResult<T> {
        let Self {
            user,
            request_id,
            raw_requests,
            terminal_error,
        } = self;
        let user = unsafe { user.as_mut()? };
        let requests = raw_requests.as_deref_mut().ok_or_else(|| {
            IocpError::InvalidState
                .to_report()
                .attach_note("raw UDP child request owners have not been allocated")
        })?;
        access(user, requests, request_id, terminal_error)
    }

    pub(crate) fn take_terminal_error(&mut self) -> Option<Report<IocpError>> {
        self.terminal_error.take()
    }
}

/// Payload for the file open operation.
pub(crate) struct OpenPayload {
    user: PayloadRef<Open>,
}

impl OpenPayload {
    pub(crate) fn new() -> Self {
        Self {
            user: PayloadRef::unbound(),
        }
    }

    pub(crate) fn user(&self) -> &PayloadRef<Open> {
        &self.user
    }
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

#[cfg(test)]
mod raw_udp_receive_request_tests {
    use super::UdpRecvMultiPayload;
    use veloq_driver_core::{Generation, OpToken};
    use veloq_std::vec::Vec;

    #[test]
    fn child_request_overlapped_and_address_storage_are_independent() {
        let token = OpToken::from_registry_parts(0, Generation::ZERO).expect("valid test token");
        let mut payload = UdpRecvMultiPayload::new();
        payload
            .try_allocate_raw_requests(token, 3)
            .expect("child request allocation should succeed");
        let requests = payload.raw_requests().expect("child owners should exist");

        let overlapped_addresses = requests
            .iter()
            .map(|request| request.header.inner() as *const _ as usize)
            .collect::<Vec<_>>();
        let peer_address_addresses = requests
            .iter()
            .map(|request| &request.address as *const _ as usize)
            .collect::<Vec<_>>();
        assert_eq!(overlapped_addresses.len(), 3);
        assert_ne!(overlapped_addresses[0], overlapped_addresses[1]);
        assert_ne!(overlapped_addresses[1], overlapped_addresses[2]);
        assert_ne!(peer_address_addresses[0], peer_address_addresses[1]);
        assert_ne!(peer_address_addresses[1], peer_address_addresses[2]);
    }

    #[test]
    fn child_request_capacity_overflow_returns_an_error() {
        let token = OpToken::from_registry_parts(0, Generation::ZERO).expect("valid test token");
        let mut payload = UdpRecvMultiPayload::new();

        assert!(
            payload
                .try_allocate_raw_requests(token, usize::MAX)
                .is_err()
        );
    }
}
