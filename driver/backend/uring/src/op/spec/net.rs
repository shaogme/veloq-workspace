use crate::{
    OwnedRawHandle, RawHandle,
    config::UringRawHandle,
    driver::UringDriver,
    error::UringDriverResult as DriverResult,
    op::{
        Accept, Connect, OpSend, Recv, SendTo, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, payload,
        submit,
    },
};
use io_uring::squeue;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, SubmitTokenContext},
    op::OpKind,
};

use super::UringOpSpec;

impl UringOpSpec for Recv {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Recv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_recv(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for OpSend {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Send;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_send(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpRecv {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecv;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_recv(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpSend {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpSend;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_send(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Connect {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Connect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_connect(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpConnect {
    type KernelPayload = payload::KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpConnect;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_connect(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Accept {
    type KernelPayload = payload::AcceptPayload;
    type Completion = OwnedRawHandle;

    const PAYLOAD_KIND: OpKind = OpKind::Accept;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::AcceptPayload::new()
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_accept(kernel, payload, driver, token) }
    }

    unsafe fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> DriverResult<usize> {
        unsafe { submit::on_complete_accept(kernel, payload, result) }
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res.map(|raw| unsafe {
            OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_socket(raw as i32)))
        })
    }
}

impl UringOpSpec for SendTo {
    type KernelPayload = payload::SendToPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::SendTo;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::SendToPayload::new()
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_send_to(kernel, payload, driver, token) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for UdpRecvFrom {
    type KernelPayload = payload::UdpRecvFromPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::UdpRecvFrom;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::UdpRecvFromPayload::new()
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        driver: &mut UringDriver,
        token: SubmitTokenContext,
    ) -> DriverResult<squeue::Entry> {
        unsafe { submit::make_sqe_udp_recv_from(kernel, payload, driver, token) }
    }

    unsafe fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> DriverResult<usize> {
        unsafe { submit::on_complete_udp_recv_from(kernel, payload, result) }
    }

    fn map_completion(_payload: &Self, res: DriverResult<usize>) -> DriverResult<Self::Completion> {
        res
    }
}
