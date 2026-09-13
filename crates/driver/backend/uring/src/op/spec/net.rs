use crate::{
    driver::{CqeEnv, SqeEnv},
    error::UringResult,
    op::{
        Accept, AcceptMulti, AcceptedSocket, CompletionCleanupHintFn, Connect, OpSend, ProvidedBuf,
        Recv, RecvMulti, RecvProvided, SendTo, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend,
        UringRecordItem, UringUserPayload, payload, submit,
    },
};
use io_uring::squeue;
use veloq_driver_core::driver::{CompletionCleanupGuard, OpToken, SubmitTokenContext};

use super::{UringOpSpec, make_sqe_adapter, on_complete_adapter};

impl_uring_forwarding_spec!(Recv, submit::make_sqe_recv);
impl_uring_forwarding_spec!(OpSend, submit::make_sqe_send);
impl_uring_forwarding_spec!(UdpRecv, submit::make_sqe_udp_recv);
impl_uring_forwarding_spec!(UdpSend, submit::make_sqe_udp_send);
impl_uring_forwarding_spec!(Connect, submit::make_sqe_connect);
impl_uring_forwarding_spec!(UdpConnect, submit::make_sqe_udp_connect);

macro_rules! impl_provided_recv_spec {
    ($OpType:ty, $make_sqe:path) => {
        impl UringOpSpec for $OpType {
            type KernelPayload = payload::KernelRef<Self>;

            fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
                payload::kernel_ref(user)
            }

            fn make_sqe(
                kernel: &mut Self::KernelPayload,
                payload: &mut Self,
                env: &SqeEnv<'_>,
                token: SubmitTokenContext,
            ) -> UringResult<squeue::Entry> {
                make_sqe_adapter(kernel, payload, env, token, $make_sqe)
            }

            fn record_item(
                _kernel: &mut Self::KernelPayload,
                _payload: &mut Self,
                _token: OpToken,
                result: i32,
                flags: u32,
                env: &mut CqeEnv<'_>,
            ) -> UringResult<UringRecordItem> {
                let buf = env.take_provided_buf(flags, result)?;
                Ok(UringRecordItem::New(UringUserPayload::ProvidedBuf(
                    ProvidedBuf { buf },
                )))
            }
        }
    };
}

// Both operations create a record only after the kernel selects a buffer from the ring. The
// declaration differentiates their single-shot/multishot ownership policy.
impl_provided_recv_spec!(RecvProvided, submit::make_sqe_recv_provided);
impl_provided_recv_spec!(RecvMulti, submit::make_sqe_recv_multi);

impl UringOpSpec for Accept {
    type KernelPayload = payload::AcceptPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::AcceptPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_accept)
    }

    fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> UringResult<usize> {
        on_complete_adapter(kernel, payload, result, submit::on_complete_accept)
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    const COMPLETION_CLEANUP_HINT: Option<CompletionCleanupHintFn> =
        Some(submit::completion_cleanup_close_raw_fd);
}

impl UringOpSpec for AcceptMulti {
    type KernelPayload = payload::KernelRef<Self>;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        payload::kernel_ref(user)
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_accept_multi)
    }

    /// 每条完成产出一个新连接。提交 payload（`AcceptMulti { fd }`，监听 socket）必须
    /// 留在 slot 里——内核还要用它继续 accept。
    fn record_item(
        _kernel: &mut Self::KernelPayload,
        _payload: &mut Self,
        _token: OpToken,
        _result: i32,
        _flags: u32,
        _env: &mut CqeEnv<'_>,
    ) -> UringResult<UringRecordItem> {
        Ok(UringRecordItem::New(UringUserPayload::AcceptedSocket(
            AcceptedSocket,
        )))
    }

    /// 与单发 `Accept` 相同：一条被丢弃的完成里那个已经被内核创建出来的 fd 必须关掉，
    /// 否则每次取消泄漏一个描述符。
    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        result: i32,
    ) -> CompletionCleanupGuard {
        submit::completion_cleanup_close_raw_fd(result)
    }

    const COMPLETION_CLEANUP_HINT: Option<CompletionCleanupHintFn> =
        Some(submit::completion_cleanup_close_raw_fd);
}

impl UringOpSpec for SendTo {
    type KernelPayload = payload::SendToPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::SendToPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_send_to)
    }
}

impl UringOpSpec for UdpRecvFrom {
    type KernelPayload = payload::UdpRecvFromPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::UdpRecvFromPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_udp_recv_from)
    }

    fn on_complete(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        result: i32,
    ) -> UringResult<usize> {
        on_complete_adapter(kernel, payload, result, submit::on_complete_udp_recv_from)
    }
}
