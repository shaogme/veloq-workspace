mod file;
mod net;

use crate::{
    OwnedRawHandle,
    driver::{CqeEnv, SqeEnv},
    error::{UringError, UringResult},
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, Connect, Fallocate, FallocateRaw, Fsync,
        FsyncRaw, OpSend, OpVTable, Open, ProvidedBuf, ReadFixed, ReadRaw, Recv, RecvMulti,
        RecvProvided, SendTo, SubmissionStrategy, SyncFileRange, SyncFileRangeRaw, Timeout,
        UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, UringKernelOp, UringOpPayload, UringSlotSpec,
        UringUserPayload, Wakeup, WriteFixed, WriteRaw, payload, submit,
    },
};
use diagweave::prelude::*;
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, SubmitTokenContext},
    op::{
        IntoPlatformOp, LostReason, OpCompletion, OpError, OpKind, OpResult, SingleShotOp,
        payload_projection_mismatch_report,
    },
};
use veloq_std::{convert::identity, time::Duration};

pub(crate) trait UringOpSpec: Sized + Send + 'static {
    type KernelPayload;
    type Completion;

    const PAYLOAD_KIND: OpKind;
    const STRATEGY: SubmissionStrategy = SubmissionStrategy::SubmitSqe;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload;

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry>;

    unsafe fn on_complete(
        _kernel: &mut Self::KernelPayload,
        _payload: &mut Self,
        result: i32,
    ) -> UringResult<usize> {
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(UringError::CompletionWait
                .report(
                    "uring.op.spec.on_complete_default",
                    "kernel completion returned error",
                )
                .set_error_code(-result))
        }
    }

    fn completion_cleanup(
        _kernel: &mut Self::KernelPayload,
        _result: i32,
    ) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }

    fn orphan_cleanup(kernel: &mut Self::KernelPayload, result: i32) -> CompletionCleanupGuard {
        Self::completion_cleanup(kernel, result)
    }

    fn get_timeout(_kernel: &Self::KernelPayload, _payload: &Self) -> Option<Duration> {
        None
    }

    fn resolve_chunks(
        _kernel: &Self::KernelPayload,
        _payload: &Self,
        _chunks: &mut [ChunkId],
    ) -> usize {
        0
    }

    /// 为一条完成造出它自己的记录 payload。默认 `None`——绝大多数操作的记录 payload 就是
    /// 提交 payload 本身。
    ///
    /// 需要覆盖它的是两类操作：提交 payload 必须留在 slot 里的（multishot），以及产物在
    /// 提交时还不存在的（provided buffer）。见 [`RecordItemFn`]。
    fn record_item(
        _kernel: &mut Self::KernelPayload,
        _payload: &mut Self,
        _result: i32,
        _flags: u32,
        _env: &mut CqeEnv<'_>,
    ) -> UringResult<Option<UringUserPayload>> {
        Ok(None)
    }

    fn map_completion(payload: &Self, res: UringResult<usize>) -> UringResult<Self::Completion>;
}

pub(crate) trait UringOpErasure: UringOpSpec {
    fn erase_kernel_payload(payload: Self::KernelPayload) -> UringOpPayload;
    fn kernel_payload_ref(payload: &UringOpPayload) -> Option<&Self::KernelPayload>;
    fn kernel_payload_mut(payload: &mut UringOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self) -> UringUserPayload;
    fn try_user_payload(payload: UringUserPayload) -> UringResult<Self>;
    fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self>;
    fn user_payload_mut(payload: &mut UringUserPayload) -> Option<&mut Self>;

    fn vtable() -> &'static OpVTable;
}

pub(crate) unsafe fn make_sqe_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    env: &SqeEnv<'_>,
    token: SubmitTokenContext,
) -> UringResult<squeue::Entry>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.make_sqe", "kernel payload mismatch")
    })?;
    let user = S::user_payload_mut(payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.make_sqe", "user payload mismatch")
    })?;
    unsafe { S::make_sqe(kernel, user, env, token) }
}

pub(crate) unsafe fn on_complete_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
) -> UringResult<usize>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_mut(&mut op.payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.on_complete", "kernel payload mismatch")
    })?;
    let user = S::user_payload_mut(payload).ok_or_else(|| {
        UringError::InvalidState.report("uring.op.spec.on_complete", "user payload mismatch")
    })?;
    unsafe { S::on_complete(kernel, user, result) }
}

pub(crate) unsafe fn completion_cleanup_shim<S>(
    op: &mut UringKernelOp,
    result: i32,
) -> CompletionCleanupGuard
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_mut(&mut op.payload) else {
        return CompletionCleanupGuard::default();
    };
    S::completion_cleanup(kernel, result)
}

pub(crate) unsafe fn orphan_cleanup_shim<S>(
    op: &mut UringKernelOp,
    result: i32,
) -> CompletionCleanupGuard
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_mut(&mut op.payload) else {
        return CompletionCleanupGuard::default();
    };
    S::orphan_cleanup(kernel, result)
}

pub(crate) unsafe fn get_timeout_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
) -> Option<Duration>
where
    S: UringOpErasure,
{
    let kernel = S::kernel_payload_ref(&op.payload)?;
    let user = S::user_payload_ref(payload)?;
    S::get_timeout(kernel, user)
}

pub(crate) unsafe fn resolve_chunks_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_ref(&op.payload) else {
        return 0;
    };
    let Some(user) = S::user_payload_ref(payload) else {
        return 0;
    };
    S::resolve_chunks(kernel, user, chunks)
}

pub(crate) unsafe fn record_item_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    result: i32,
    flags: u32,
    env: &mut CqeEnv<'_>,
) -> UringResult<Option<UringUserPayload>>
where
    S: UringOpErasure,
{
    let Some(kernel) = S::kernel_payload_mut(&mut op.payload) else {
        return Ok(None);
    };
    let Some(user) = S::user_payload_mut(payload) else {
        return Ok(None);
    };
    S::record_item(kernel, user, result, flags, env)
}

macro_rules! impl_uring_op_erasure {
    // 只生成擦除层。提交 payload 与记录 payload 不是同一个类型的操作用这一支，
    // `IntoPlatformOp` 得手写。
    (@erasure_only $OpType:ty, $user_variant:ident, $kernel_variant:ident) => {
        impl_uring_op_erasure!(@erasure $OpType, $user_variant, $kernel_variant);
    };
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl_uring_op_erasure!(@erasure $OpType, $user_variant, $kernel_variant);
        impl_uring_single_shot_op!($OpType, $completion);
    };
    (@erasure $OpType:ty, $user_variant:ident, $kernel_variant:ident) => {
        impl UringOpErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> UringOpPayload {
                UringOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(payload: &UringOpPayload) -> Option<&Self::KernelPayload> {
                match payload {
                    UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(
                payload: &mut UringOpPayload,
            ) -> Option<&mut Self::KernelPayload> {
                match payload {
                    UringOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> UringUserPayload {
                UringUserPayload::$user_variant(payload)
            }

            fn try_user_payload(payload: UringUserPayload) -> UringResult<Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(payload_projection_mismatch_report::<UringError>(
                        stringify!($OpType),
                        "UringUserPayload",
                    )),
                }
            }

            fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn user_payload_mut(payload: &mut UringUserPayload) -> Option<&mut Self> {
                match payload {
                    UringUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static OpVTable {
                static TABLE: OpVTable = OpVTable {
                    make_sqe: make_sqe_shim::<$OpType>,
                    on_complete: on_complete_shim::<$OpType>,
                    completion_cleanup: completion_cleanup_shim::<$OpType>,
                    orphan_cleanup: orphan_cleanup_shim::<$OpType>,
                    strategy: <$OpType as UringOpSpec>::STRATEGY,
                    get_timeout: get_timeout_shim::<$OpType>,
                    resolve_chunks: resolve_chunks_shim::<$OpType>,
                    record_item: record_item_shim::<$OpType>,
                };
                &TABLE
            }
        }
    };
}

/// 记录 payload 与提交 payload **不是同一个类型**的操作的 [`IntoPlatformOp`]。
///
/// 走这一支的有两类，它们是正交的两件事，只是恰好都不能用上面那个宏：
///
/// - 提交 payload 必须留在 slot 里（multishot：`AcceptMulti` / `RecvMulti`），因为内核还要
///   拿它继续产出完成；
/// - 产物在提交时还不存在（provided buffer：`RecvProvided` / `RecvMulti`），因为 buffer 是
///   内核在数据到达时才从环里挑的。
///
/// `$map` 把 CQE 的结果变成这个操作的 `Completion`。它拿不到 payload——记录 payload 里没有
/// 提交时的信息，这正是两者分家的意思。
macro_rules! impl_uring_record_payload_op {
    ($OpType:ty, $record_variant:ident, $record:ty, $completion:ty, $map:expr) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as UringOpSpec>::new_kernel_payload(&self);
                let op = UringKernelOp {
                    vtable: <$OpType as UringOpErasure>::vtable(),
                    payload: <$OpType as UringOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                <$OpType as UringOpErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(
                erased: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                match erased {
                    UringUserPayload::$record_variant(item) => Ok(item),
                    _ => Err(payload_projection_mismatch_report::<UringError>(
                        stringify!($record_variant),
                        "UringUserPayload",
                    )),
                }
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                OpCompletion::new($map(res), payload)
            }

            /// 提交同步失败时 slot 里躺着的是**提交** payload，不是任何一条完成的产物——
            /// 没有 item 可以交给用户，也没有用户交出来的资源要还。默认实现会把它当记录
            /// payload 去投影，那必然失败并给出一个含义完全错误的 `PayloadTypeMismatch`。
            fn submit_failed(
                erased: UringUserPayload,
                report: Report<UringError>,
            ) -> OpResult<Self::Output, UringError, Self::Completion> {
                drop(erased);
                OpResult::ResourceLost(OpError::new(LostReason::Other, report))
            }
        }
    };
}

/// 单发操作的 [`IntoPlatformOp`]：提交 payload 就是记录 payload，就是操作自己。
macro_rules! impl_uring_single_shot_op {
    ($OpType:ty, $completion:ty) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $OpType;
            type Output = $OpType;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as UringOpSpec>::new_kernel_payload(&self);
                let op = UringKernelOp {
                    vtable: <$OpType as UringOpErasure>::vtable(),
                    payload: <$OpType as UringOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                <$OpType as UringOpErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(
                payload: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                <$OpType as UringOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let completion = <$OpType as UringOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
}

impl UringOpSpec for Wakeup {
    type KernelPayload = payload::WakeupPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::WakeupPayload::new()
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        unsafe { submit::make_sqe_wakeup(kernel, payload, env, token) }
    }

    fn map_completion(_payload: &Self, res: UringResult<usize>) -> UringResult<Self::Completion> {
        res
    }
}

impl UringOpSpec for Timeout {
    type KernelPayload = payload::TimeoutPayload;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Timeout;
    const STRATEGY: SubmissionStrategy = SubmissionStrategy::SoftwareTimer;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::TimeoutPayload::new()
    }

    unsafe fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        unsafe { submit::make_sqe_timeout(kernel, payload, env, token) }
    }

    fn get_timeout(_kernel: &Self::KernelPayload, payload: &Self) -> Option<Duration> {
        Some(payload.duration)
    }

    fn map_completion(_payload: &Self, res: UringResult<usize>) -> UringResult<Self::Completion> {
        res
    }
}

impl_uring_op_erasure!(ReadFixed, ReadFixed, Read, usize);
impl_uring_op_erasure!(ReadRaw, ReadRaw, ReadRaw, usize);
impl_uring_op_erasure!(WriteFixed, WriteFixed, Write, usize);
impl_uring_op_erasure!(WriteRaw, WriteRaw, WriteRaw, usize);
impl_uring_op_erasure!(Recv, Recv, Recv, usize);
impl_uring_op_erasure!(@erasure_only RecvProvided, RecvProvided, RecvProvided);
impl_uring_op_erasure!(@erasure_only RecvMulti, RecvMulti, RecvMulti);
impl_uring_op_erasure!(OpSend, OpSend, Send, usize);
impl_uring_op_erasure!(UdpRecv, UdpRecv, UdpRecv, usize);
impl_uring_op_erasure!(UdpSend, UdpSend, UdpSend, usize);
impl_uring_op_erasure!(Connect, Connect, Connect, usize);
impl_uring_op_erasure!(UdpConnect, UdpConnect, UdpConnect, usize);
impl_uring_op_erasure!(Close, Close, Close, usize);
impl_uring_op_erasure!(Fsync, Fsync, Fsync, usize);
impl_uring_op_erasure!(FsyncRaw, FsyncRaw, FsyncRaw, usize);
impl_uring_op_erasure!(SyncFileRange, SyncFileRange, SyncRange, usize);
impl_uring_op_erasure!(SyncFileRangeRaw, SyncFileRangeRaw, SyncRangeRaw, usize);
impl_uring_op_erasure!(Fallocate, Fallocate, Fallocate, usize);
impl_uring_op_erasure!(FallocateRaw, FallocateRaw, FallocateRaw, usize);
impl_uring_op_erasure!(Accept, Accept, Accept, OwnedRawHandle);
impl_uring_op_erasure!(@erasure_only AcceptMulti, AcceptMulti, AcceptMulti);

// 提交 payload 留在 slot 里（监听 socket，内核还要拿它继续 accept），每条完成的记录里是一
// 个 `AcceptedSocket`——新连接的 fd 走 `Completion`。
//
// 不实现 `SingleShotOp`：`await` 一个 `AcceptMulti` 只会取到第一条完成然后把其余的取消掉。
impl_uring_record_payload_op!(
    AcceptMulti,
    AcceptedSocket,
    AcceptedSocket,
    OwnedRawHandle,
    submit::accepted_handle_from_res
);

// 单发，但提交 payload 与记录 payload 仍然不同——**因为产物在提交时还不存在**。
//
// slot 里是 `RecvProvided { fd }`，记录里是内核在数据到达时才从环里挑出来的那个
// `ProvidedBuf`。这正是 `SubmitPayload` / `RecordPayload` 拆开之后多出来的表达力：「多条
// 完成」与「产物不是提交物」原本被 multishot 这一个词捆在一起，其实是两件事。
//
// 它**是** `SingleShotOp`：一次提交一条完成，`await` 得到的就是那一条。
impl_uring_record_payload_op!(RecvProvided, ProvidedBuf, ProvidedBuf, usize, identity);

impl SingleShotOp<UringSlotSpec> for RecvProvided {}

// 两件事同时发生：提交 payload 留在 slot 里，产物由内核从环里挑。
//
// 于是它与上面两个各共享一半，自己不需要任何新机制——`AcceptMulti` 的记录 payload 投影加
// `RecvProvided` 的 `record_item`，就是 multishot recv 的全部。
//
// 与 `AcceptMulti` 一样不实现 `SingleShotOp`。
impl_uring_record_payload_op!(RecvMulti, ProvidedBuf, ProvidedBuf, usize, identity);

impl_uring_op_erasure!(SendTo, SendTo, SendTo, usize);
impl_uring_op_erasure!(UdpRecvFrom, UdpRecvFrom, UdpRecvFrom, usize);
impl_uring_op_erasure!(Open, Open, Open, OwnedRawHandle);
impl_uring_op_erasure!(Wakeup, Wakeup, Wakeup, usize);
impl_uring_op_erasure!(Timeout, Timeout, Timeout, usize);
