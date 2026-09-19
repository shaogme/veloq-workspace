use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RawHandle},
    error::IocpResult,
    ext::Extensions,
    net::addr::socket_addr_to_storage,
    op::{
        ACCEPT_EX_OUTPUT_BUFFER_LEN, Accept, AcceptMulti, AcceptMultiPayload, AcceptPayload,
        Connect, KernelRef, OpSend, OverlappedEntry, PayloadRef, ProvidedBuf, Recv, RecvMulti,
        RecvMultiPayload, RecvProvided, RecvProvidedPayload, SendTo, SendToPayload, SubmitContext,
        UdpConnect, UdpRecvMulti, UdpRecvMultiPayload, UdpRecvPacket, UdpSend, kernel_ref,
        spec::{IocpMultiShotSpec, IocpOpSpec},
        submit,
    },
};

use veloq_driver_core::{driver::CompletionCleanupGuard, op::OpKind};
use veloq_std::{mem::size_of, net::SocketAddr};
use windows_sys::Win32::Networking::WinSock::{SOCKADDR_IN, SOCKADDR_IN6};

impl IocpOpSpec for Recv {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Recv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_recv(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_recv(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for OpSend {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Send;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_send(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpSend {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpSend;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_udp_send(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_send(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Connect {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Connect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_connect(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_connect(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for UdpConnect {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpConnect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_udp_connect(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_udp_connect(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_connect(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Accept {
    type KernelPayload = AcceptPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Accept;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        AcceptPayload {
            user: PayloadRef::unbound(),
            accept_buffer: [0; ACCEPT_EX_OUTPUT_BUFFER_LEN],
            accept_socket: None,
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_accept(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_accept(header, payload, result, ext) }
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_socket(result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_accept(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(raw as _)))
        })
    }
}

impl IocpMultiShotSpec for AcceptMulti {
    type KernelPayload = AcceptMultiPayload;
    type RecordPayload = crate::op::AcceptedSocket;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::AcceptMulti;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        AcceptMultiPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_accept_multi(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_accept_multi(header, payload, result, ext) }
    }

    fn completion_cleanup(
        _payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_socket(result)
    }

    fn orphan_cleanup(
        payload: &mut Self::KernelPayload,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        submit::orphan_cleanup_accept_multi(payload, result)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_accept_multi(payload) }
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

impl IocpMultiShotSpec for RecvProvided {
    type KernelPayload = RecvProvidedPayload;
    type RecordPayload = ProvidedBuf;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::RecvProvided;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        RecvProvidedPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_recv_provided(header, payload, ctx)
    }

    unsafe fn on_complete(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        result: usize,
        ext: &Extensions,
    ) -> IocpResult<usize> {
        unsafe { submit::on_complete_recv_provided(header, payload, result, ext) }
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_recv_provided(payload) }
    }

    fn map_completion(
        _payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpMultiShotSpec for RecvMulti {
    type KernelPayload = RecvMultiPayload;
    type RecordPayload = ProvidedBuf;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::RecvMulti;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        RecvMultiPayload::new()
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_recv_multi(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_recv_multi(payload) }
    }

    fn map_completion(
        _payload: &Self::RecordPayload,
        res: IocpResult<usize>,
    ) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for SendTo {
    type KernelPayload = SendToPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SendTo;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        let (addr, _raw_addr_len) = socket_addr_to_storage(user.addr);
        let addr_len = match user.addr {
            SocketAddr::V4(_) => size_of::<SOCKADDR_IN>() as i32,
            SocketAddr::V6(_) => size_of::<SOCKADDR_IN6>() as i32,
        };
        SendToPayload {
            user: PayloadRef::unbound(),
            addr,
            addr_len,
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_send_to(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_send_to(payload) }
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpMultiShotSpec for UdpRecvMulti {
    type KernelPayload = UdpRecvMultiPayload;
    type RecordPayload = UdpRecvPacket;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecvMulti;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        UdpRecvMultiPayload {
            user: PayloadRef::unbound(),
        }
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<submit::SubmissionResult> {
        submit::submit_udp_recv_multi(header, payload, ctx)
    }

    unsafe fn get_fd(payload: &Self::KernelPayload) -> Option<IoFd> {
        unsafe { submit::get_fd_udp_recv_multi(payload) }
    }

    fn map_completion(_payload: &Self::RecordPayload, res: IocpResult<usize>) -> IocpResult<usize> {
        res
    }
}
