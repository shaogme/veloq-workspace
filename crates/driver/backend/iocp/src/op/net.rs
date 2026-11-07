use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RawHandle},
    error::IocpResult,
    ext::Extensions,
    net::SockAddrStorage,
    op::{
        Accept, AcceptMulti, AcceptMultiPayload, AcceptPayload, AcceptedSocket, Connect,
        IocpMultiShotSpec, IocpOpSpec, KernelRef, OpSend, OverlappedEntry, ProvidedBuf, Recv,
        RecvMulti, RecvMultiPayload, RecvProvided, RecvProvidedPayload, SendTo, SendToPayload,
        SubmitContext, UdpConnect, UdpRecvMulti, UdpRecvMultiPayload, UdpRecvPacket, UdpSend,
        submit::{
            AcceptSubmissionOps, ConnectSubmissionOps, NetSubmissionOps, SubmissionOps,
            SubmissionResult, UdpReceiveSubmit,
        },
    },
};

use veloq_driver_core::{CompletionCleanupGuard, OpKind};
use veloq_std::{mem::size_of, net::SocketAddr};
use windows_sys::Win32::Networking::WinSock::{SOCKADDR_IN, SOCKADDR_IN6};

use core::marker::PhantomData;

pub(super) struct NetworkOpSpec<T>(PhantomData<T>);

impl IocpOpSpec for NetworkOpSpec<Recv> {
    type User = Recv;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Recv;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_recv(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_recv(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<OpSend> {
    type User = OpSend;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Send;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_send(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<UdpSend> {
    type User = UdpSend;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpSend;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_udp_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_udp_send(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<Connect> {
    type User = Connect;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Connect;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        ConnectSubmissionOps::submit_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        _payload: &mut Self::KernelPayload,
        result: usize,
        _ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { ConnectSubmissionOps::on_complete_connect(header, result) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_connect(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<UdpConnect> {
    type User = UdpConnect;
    type KernelPayload = KernelRef<Self::User>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpConnect;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        KernelRef::new(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        ConnectSubmissionOps::submit_udp_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        _header: &mut OverlappedEntry,
        _payload: &mut Self::KernelPayload,
        result: usize,
        _ext: &Extensions,
    ) -> IocpResult<usize> {
        Ok(result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_udp_connect(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<Accept> {
    type User = Accept;
    type KernelPayload = AcceptPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Accept;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        AcceptPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        AcceptSubmissionOps::submit_accept(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { AcceptSubmissionOps::on_complete_accept(header, payload, result, ext) }
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        AcceptSubmissionOps::completion_cleanup_close_socket(result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_accept(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(raw as _)))
        })
    }
}

impl IocpMultiShotSpec for NetworkOpSpec<AcceptMulti> {
    type User = AcceptMulti;
    type KernelPayload = AcceptMultiPayload;
    type RecordPayload = AcceptedSocket;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::AcceptMulti;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        AcceptMultiPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        AcceptSubmissionOps::submit_accept_multi(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { AcceptSubmissionOps::on_complete_accept_multi(header, payload, result, ext) }
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        AcceptSubmissionOps::completion_cleanup_close_socket(result)
    }

    fn orphan_cleanup(
        payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        AcceptSubmissionOps::orphan_cleanup_accept_multi(payload, result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_accept_multi(payload) }
    }

    fn map_completion(
        _payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(raw as _)))
        })
    }
}

impl IocpMultiShotSpec for NetworkOpSpec<RecvProvided> {
    type User = RecvProvided;
    type KernelPayload = RecvProvidedPayload;
    type RecordPayload = ProvidedBuf;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::RecvProvided;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        RecvProvidedPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_recv_provided(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        _ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { NetSubmissionOps::on_complete_recv_provided(header, payload, result) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_recv_provided(payload) }
    }

    fn map_completion(
        _payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpMultiShotSpec for NetworkOpSpec<RecvMulti> {
    type User = RecvMulti;
    type KernelPayload = RecvMultiPayload;
    type RecordPayload = ProvidedBuf;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::RecvMulti;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        RecvMultiPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_recv_multi(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_recv_multi(payload) }
    }

    fn map_completion(
        _payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for NetworkOpSpec<SendTo> {
    type User = SendTo;
    type KernelPayload = SendToPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SendTo;

    fn new_kernel_payload(user: &Self::User) -> Self::KernelPayload {
        let (addr, _raw_addr_len) = SockAddrStorage::from_socket_addr(user.addr);
        let addr_len = match user.addr {
            SocketAddr::V4(_) => size_of::<SOCKADDR_IN>() as i32,
            SocketAddr::V6(_) => size_of::<SOCKADDR_IN6>() as i32,
        };
        SendToPayload::new(addr, addr_len)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        NetSubmissionOps::submit_send_to(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_send_to(payload) }
    }

    fn map_completion(
        _payload: &Self::User,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpMultiShotSpec for NetworkOpSpec<UdpRecvMulti> {
    type User = UdpRecvMulti;
    type KernelPayload = UdpRecvMultiPayload;
    type RecordPayload = UdpRecvPacket;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecvMulti;

    fn new_kernel_payload(_user: &Self::User) -> Self::KernelPayload {
        UdpRecvMultiPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        UdpReceiveSubmit::submit_initial(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { SubmissionOps::get_fd_udp_recv_multi(payload) }
    }

    fn map_completion(_payload: &Self::RecordPayload, res: IocpResult<usize>) -> IocpResult<usize> {
        res
    }
}
