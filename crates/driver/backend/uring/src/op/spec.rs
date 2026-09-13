use crate::{
    OwnedRawHandle,
    driver::{CqeEnv, SqeEnv},
    error::{UringError, UringResult},
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, CompletionCleanupHintFn, Connect, Fallocate,
        FallocateRaw, Fsync, FsyncRaw, OpSend, OpVTable, Open, ProvidedBuf, ReadFixed, ReadRaw,
        Recv, RecvMulti, RecvProvided, SendTo, SubmissionStrategy, SyncFileRange, SyncFileRangeRaw,
        Timeout, UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, UringKernelOp, UringOpPayload,
        UringPayloadTag, UringRecordItem, UringSlotSpec, UringUserPayload, Wakeup, WriteFixed,
        WriteRaw, payload, submit,
    },
};
use diagweave::prelude::*;
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, OpToken, SubmitTokenContext},
    op::{IntoPlatformOp, LostReason, OpCompletion, OpError, OpKind, OpResult, SingleShotOp},
};
use veloq_std::time::Duration;

macro_rules! impl_uring_forwarding_spec {
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
        }
    };
    ($OpType:ty, $make_sqe:path, $resolve_chunks:path) => {
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

            fn resolve_chunks(
                kernel: &Self::KernelPayload,
                payload: &Self,
                chunks: &mut [ChunkId],
            ) -> UringResult<usize> {
                Ok($resolve_chunks(kernel, payload, chunks))
            }
        }
    };
}

mod file;
mod net;

pub(crate) trait UringOpDeclaration: Sized + Send + 'static {
    type Completion;

    const PAYLOAD_KIND: OpKind;
    const STRATEGY: SubmissionStrategy;

    fn map_completion(res: UringResult<usize>) -> UringResult<Self::Completion>;
}

pub(crate) trait UringOpSpec: UringOpDeclaration {
    type KernelPayload;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload;

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry>;

    fn on_complete(
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

    const COMPLETION_CLEANUP_HINT: Option<CompletionCleanupHintFn> = None;

    fn orphan_cleanup(kernel: &mut Self::KernelPayload, result: i32) -> CompletionCleanupGuard {
        Self::completion_cleanup(kernel, result)
    }

    fn get_timeout(
        _kernel: &Self::KernelPayload,
        _payload: &Self,
    ) -> UringResult<Option<Duration>> {
        Ok(None)
    }

    fn resolve_chunks(
        _kernel: &Self::KernelPayload,
        _payload: &Self,
        _chunks: &mut [ChunkId],
    ) -> UringResult<usize> {
        Ok(0)
    }

    /// 为一条完成造出它自己的记录 payload。默认使用提交 payload——绝大多数操作的记录
    /// payload 就是提交 payload 本身。
    ///
    /// 需要覆盖它的是两类操作：提交 payload 必须留在 slot 里的（multishot），以及产物在
    /// 提交时还不存在的（provided buffer）。见 [`RecordItemFn`]。
    fn record_item(
        _kernel: &mut Self::KernelPayload,
        _payload: &mut Self,
        _token: OpToken,
        _result: i32,
        _flags: u32,
        _env: &mut CqeEnv<'_>,
    ) -> UringResult<UringRecordItem> {
        Ok(UringRecordItem::UseSubmitPayload)
    }
}

/// Calls an operation's unsafe SQE builder after the type-erased shim has restored its types.
///
/// The callback remains unsafe because it creates pointers whose validity lasts until the CQE;
/// keeping that boundary in one adapter prevents every operation spec from repeating the same
/// unsafe block.
pub(crate) fn make_sqe_adapter<S>(
    kernel: &mut S::KernelPayload,
    payload: &mut S,
    env: &SqeEnv<'_>,
    token: SubmitTokenContext,
    make_sqe: unsafe fn(
        &mut S::KernelPayload,
        &mut S,
        &SqeEnv<'_>,
        SubmitTokenContext,
    ) -> UringResult<squeue::Entry>,
) -> UringResult<squeue::Entry>
where
    S: UringOpSpec,
{
    // SAFETY: the typed callback is selected by the operation spec and receives its matching
    // kernel/user payloads. Its raw pointer invariants are local to the callback itself.
    unsafe { make_sqe(kernel, payload, env, token) }
}

/// Calls an operation's unsafe completion callback after the type-erased shim restored its types.
pub(crate) fn on_complete_adapter<S>(
    kernel: &mut S::KernelPayload,
    payload: &mut S,
    result: i32,
    on_complete: unsafe fn(&mut S::KernelPayload, &mut S, i32) -> UringResult<usize>,
) -> UringResult<usize>
where
    S: UringOpSpec,
{
    // SAFETY: the typed callback is selected by the operation spec and receives its matching
    // kernel/user payloads. Its raw pointer invariants are local to the callback itself.
    unsafe { on_complete(kernel, payload, result) }
}

pub(crate) trait UringOpErasure: UringOpSpec {
    const OPERATION_NAME: &'static str;
    const USER_PAYLOAD_TAG: UringPayloadTag;
    const KERNEL_PAYLOAD_TAG: UringPayloadTag;

    fn erase_kernel_payload(payload: Self::KernelPayload) -> UringOpPayload;
    fn kernel_payload_ref(payload: &UringOpPayload) -> Option<&Self::KernelPayload>;
    fn kernel_payload_mut(payload: &mut UringOpPayload) -> Option<&mut Self::KernelPayload>;

    fn erase_user_payload(payload: Self) -> UringUserPayload;
    fn try_user_payload(payload: UringUserPayload) -> UringResult<Self>;
    fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self>;
    fn user_payload_mut(payload: &mut UringUserPayload) -> Option<&mut Self>;

    fn vtable() -> &'static OpVTable;
}

fn projection_mismatch_report(
    scope: &'static str,
    operation: &'static str,
    token: Option<OpToken>,
    expected_kernel: Option<UringPayloadTag>,
    actual_kernel: Option<UringPayloadTag>,
    expected_user: Option<UringPayloadTag>,
    actual_user: Option<UringPayloadTag>,
) -> Report<UringError> {
    let mut report = UringError::Internal
        .report(scope, "operation payload projection mismatch")
        .with_ctx("operation", operation);
    if let Some(token) = token {
        report = report
            .with_ctx("token_index", token.index())
            .with_ctx("token_generation", token.generation());
    }
    if let Some(tag) = expected_kernel {
        report = report.with_ctx("expected_kernel_payload", tag.name());
    }
    if let Some(tag) = actual_kernel {
        report = report.with_ctx("actual_kernel_payload", tag.name());
    }
    if let Some(tag) = expected_user {
        report = report.with_ctx("expected_user_payload", tag.name());
    }
    if let Some(tag) = actual_user {
        report = report.with_ctx("actual_user_payload", tag.name());
    }
    report
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
    let kernel = match S::kernel_payload_mut(&mut op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.make_sqe",
                S::OPERATION_NAME,
                Some(token.op_token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    let user = match S::user_payload_mut(payload) {
        Some(user) => user,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.make_sqe",
                S::OPERATION_NAME,
                Some(token.op_token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    S::make_sqe(kernel, user, env, token)
}

pub(crate) unsafe fn on_complete_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    token: OpToken,
    result: i32,
) -> UringResult<usize>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_mut(&mut op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.on_complete",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    let user = match S::user_payload_mut(payload) {
        Some(user) => user,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.on_complete",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    S::on_complete(kernel, user, result)
}

pub(crate) unsafe fn completion_cleanup_shim<S>(
    op: &mut UringKernelOp,
    result: i32,
) -> UringResult<CompletionCleanupGuard>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_mut(&mut op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.completion_cleanup",
                S::OPERATION_NAME,
                None,
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                None,
                None,
            ));
        }
    };
    Ok(S::completion_cleanup(kernel, result))
}

pub(crate) unsafe fn orphan_cleanup_shim<S>(
    op: &mut UringKernelOp,
    result: i32,
) -> UringResult<CompletionCleanupGuard>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_mut(&mut op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.orphan_cleanup",
                S::OPERATION_NAME,
                None,
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                None,
                None,
            ));
        }
    };
    Ok(S::orphan_cleanup(kernel, result))
}

pub(crate) unsafe fn get_timeout_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    token: OpToken,
) -> UringResult<Option<Duration>>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_ref(&op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.get_timeout",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    let user = match S::user_payload_ref(payload) {
        Some(user) => user,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.get_timeout",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    S::get_timeout(kernel, user)
}

pub(crate) unsafe fn resolve_chunks_shim<S>(
    op: &UringKernelOp,
    payload: &UringUserPayload,
    token: OpToken,
    chunks: &mut [ChunkId],
) -> UringResult<usize>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_ref(&op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.resolve_chunks",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    let user = match S::user_payload_ref(payload) {
        Some(user) => user,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.resolve_chunks",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    S::resolve_chunks(kernel, user, chunks)
}

pub(crate) unsafe fn record_item_shim<S>(
    op: &mut UringKernelOp,
    payload: &mut UringUserPayload,
    token: OpToken,
    result: i32,
    flags: u32,
    env: &mut CqeEnv<'_>,
) -> UringResult<UringRecordItem>
where
    S: UringOpErasure,
{
    let kernel = match S::kernel_payload_mut(&mut op.payload) {
        Some(kernel) => kernel,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.record_item",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    let user = match S::user_payload_mut(payload) {
        Some(user) => user,
        None => {
            return Err(projection_mismatch_report(
                "uring.op.spec.record_item",
                S::OPERATION_NAME,
                Some(token),
                Some(S::KERNEL_PAYLOAD_TAG),
                Some(op.payload.tag()),
                Some(S::USER_PAYLOAD_TAG),
                Some(payload.tag()),
            ));
        }
    };
    S::record_item(kernel, user, token, result, flags, env)
}

macro_rules! impl_uring_op_erasure {
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident) => {
        impl UringOpErasure for $OpType {
            const OPERATION_NAME: &'static str = stringify!($OpType);
            const USER_PAYLOAD_TAG: UringPayloadTag = UringPayloadTag::$user_variant;
            const KERNEL_PAYLOAD_TAG: UringPayloadTag = UringPayloadTag::$kernel_variant;

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
                let actual = payload.tag();
                match payload {
                    UringUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(projection_mismatch_report(
                        "uring.op.spec.try_user_payload",
                        Self::OPERATION_NAME,
                        None,
                        None,
                        None,
                        Some(Self::USER_PAYLOAD_TAG),
                        Some(actual),
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
                    operation_name: <$OpType as UringOpErasure>::OPERATION_NAME,
                    make_sqe: make_sqe_shim::<$OpType>,
                    on_complete: on_complete_shim::<$OpType>,
                    completion_cleanup: completion_cleanup_shim::<$OpType>,
                    completion_cleanup_hint: <$OpType as UringOpSpec>::COMPLETION_CLEANUP_HINT,
                    orphan_cleanup: orphan_cleanup_shim::<$OpType>,
                    strategy: <$OpType as UringOpDeclaration>::STRATEGY,
                    get_timeout: get_timeout_shim::<$OpType>,
                    resolve_chunks: resolve_chunks_shim::<$OpType>,
                    record_item: record_item_shim::<$OpType>,
                };
                &TABLE
            }
        }
    };
}

macro_rules! impl_uring_op_declaration {
    ($OpType:ty, $kind:path, $completion:ty, $strategy:path) => {
        impl UringOpDeclaration for $OpType {
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = $kind;
            const STRATEGY: SubmissionStrategy = $strategy;

            fn map_completion(res: UringResult<usize>) -> UringResult<Self::Completion> {
                res
            }
        }
    };
    ($OpType:ty, $kind:path, $completion:ty, $strategy:path, $map:path) => {
        impl UringOpDeclaration for $OpType {
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = $kind;
            const STRATEGY: SubmissionStrategy = $strategy;

            fn map_completion(res: UringResult<usize>) -> UringResult<Self::Completion> {
                $map(res)
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
/// `UringOpDeclaration::map_completion` 把 CQE 的结果变成这个操作的 `Completion`。它拿不到
/// payload——记录 payload 里没有提交时的信息，这正是两者分家的意思。
macro_rules! impl_uring_record_payload_op {
    ($OpType:ty, $record_variant:ident, $record:ty, $completion:ty) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOpDeclaration>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as UringOpSpec>::new_kernel_payload(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
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
                    payload => Err(projection_mismatch_report(
                        "uring.op.spec.try_record_from_erased",
                        <$OpType as UringOpErasure>::OPERATION_NAME,
                        None,
                        None,
                        None,
                        Some(UringPayloadTag::$record_variant),
                        Some(payload.tag()),
                    )),
                }
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                OpCompletion::new(
                    <$OpType as UringOpDeclaration>::map_completion(res),
                    payload,
                )
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

            const PAYLOAD_KIND: OpKind = <$OpType as UringOpDeclaration>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as UringOpSpec>::new_kernel_payload(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
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
                let completion = <$OpType as UringOpDeclaration>::map_completion(res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
}

macro_rules! impl_uring_single_shot_marker {
    (yes, $OpType:ty) => {
        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
    (no, $OpType:ty) => {};
}

macro_rules! declare_uring_operations {
    () => {};
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            single_shot: yes,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_declaration!($OpType, $kind, $completion, $strategy);
        impl_uring_op_erasure!($OpType, $user_variant, $kernel_variant);
        impl_uring_single_shot_op!($OpType, $completion);
        declare_uring_operations!($($rest)*);
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            single_shot: yes,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_declaration!($OpType, $kind, $completion, $strategy, $map);
        impl_uring_op_erasure!($OpType, $user_variant, $kernel_variant);
        impl_uring_single_shot_op!($OpType, $completion);
        declare_uring_operations!($($rest)*);
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            single_shot: $single_shot:ident,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_declaration!($OpType, $kind, $completion, $strategy);
        impl_uring_op_erasure!($OpType, $user_variant, $kernel_variant);
        impl_uring_record_payload_op!($OpType, $record_variant, $record, $completion);
        impl_uring_single_shot_marker!($single_shot, $OpType);
        declare_uring_operations!($($rest)*);
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            single_shot: $single_shot:ident,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_declaration!($OpType, $kind, $completion, $strategy, $map);
        impl_uring_op_erasure!($OpType, $user_variant, $kernel_variant);
        impl_uring_record_payload_op!($OpType, $record_variant, $record, $completion);
        impl_uring_single_shot_marker!($single_shot, $OpType);
        declare_uring_operations!($($rest)*);
    };
}

impl UringOpSpec for Wakeup {
    type KernelPayload = payload::WakeupPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::WakeupPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_wakeup)
    }
}

impl UringOpSpec for Timeout {
    type KernelPayload = payload::TimeoutPayload;

    fn new_kernel_payload(_user: &Self) -> Self::KernelPayload {
        payload::TimeoutPayload::new()
    }

    fn make_sqe(
        kernel: &mut Self::KernelPayload,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        make_sqe_adapter(kernel, payload, env, token, submit::make_sqe_timeout)
    }

    fn get_timeout(_kernel: &Self::KernelPayload, payload: &Self) -> UringResult<Option<Duration>> {
        Ok(Some(payload.duration))
    }
}

declare_uring_operations! {
    ReadFixed {
        user: ReadFixed,
        kernel: Read,
        kind: OpKind::ReadFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    ReadRaw {
        user: ReadRaw,
        kernel: ReadRaw,
        kind: OpKind::ReadFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    WriteFixed {
        user: WriteFixed,
        kernel: Write,
        kind: OpKind::WriteFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    WriteRaw {
        user: WriteRaw,
        kernel: WriteRaw,
        kind: OpKind::WriteFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Recv {
        user: Recv,
        kernel: Recv,
        kind: OpKind::Recv,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    RecvProvided {
        user: RecvProvided,
        kernel: RecvProvided,
        kind: OpKind::RecvProvided,
        completion: usize,
        record: ProvidedBuf(ProvidedBuf),
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    RecvMulti {
        user: RecvMulti,
        kernel: RecvMulti,
        kind: OpKind::RecvMulti,
        completion: usize,
        record: ProvidedBuf(ProvidedBuf),
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: no,
    };
    OpSend {
        user: OpSend,
        kernel: Send,
        kind: OpKind::Send,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    UdpRecv {
        user: UdpRecv,
        kernel: UdpRecv,
        kind: OpKind::UdpRecv,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    UdpSend {
        user: UdpSend,
        kernel: UdpSend,
        kind: OpKind::UdpSend,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Connect {
        user: Connect,
        kernel: Connect,
        kind: OpKind::Connect,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    UdpConnect {
        user: UdpConnect,
        kernel: UdpConnect,
        kind: OpKind::UdpConnect,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Close {
        user: Close,
        kernel: Close,
        kind: OpKind::Close,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Fsync {
        user: Fsync,
        kernel: Fsync,
        kind: OpKind::Fsync,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    FsyncRaw {
        user: FsyncRaw,
        kernel: FsyncRaw,
        kind: OpKind::Fsync,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    SyncFileRange {
        user: SyncFileRange,
        kernel: SyncRange,
        kind: OpKind::SyncFileRange,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    SyncFileRangeRaw {
        user: SyncFileRangeRaw,
        kernel: SyncRangeRaw,
        kind: OpKind::SyncFileRange,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Fallocate {
        user: Fallocate,
        kernel: Fallocate,
        kind: OpKind::Fallocate,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    FallocateRaw {
        user: FallocateRaw,
        kernel: FallocateRaw,
        kind: OpKind::Fallocate,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Accept {
        user: Accept,
        kernel: Accept,
        kind: OpKind::Accept,
        completion: OwnedRawHandle,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
        map: submit::accepted_handle_from_res,
    };
    AcceptMulti {
        user: AcceptMulti,
        kernel: AcceptMulti,
        kind: OpKind::AcceptMulti,
        completion: OwnedRawHandle,
        record: AcceptedSocket(AcceptedSocket),
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: no,
        map: submit::accepted_handle_from_res,
    };
    SendTo {
        user: SendTo,
        kernel: SendTo,
        kind: OpKind::SendTo,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    UdpRecvFrom {
        user: UdpRecvFrom,
        kernel: UdpRecvFrom,
        kind: OpKind::UdpRecvFrom,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Open {
        user: Open,
        kernel: Open,
        kind: OpKind::Open,
        completion: OwnedRawHandle,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
        map: submit::opened_handle_from_res,
    };
    Wakeup {
        user: Wakeup,
        kernel: Wakeup,
        kind: OpKind::Wakeup,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        single_shot: yes,
    };
    Timeout {
        user: Timeout,
        kernel: Timeout,
        kind: OpKind::Timeout,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SoftwareTimer,
        single_shot: yes,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{FileTableExhaustion, IoFd, SockAddrStorage, UringRawHandle},
        diagnostics::UringCompletionDiagnostics,
        driver::{CqeEnv, FileTable, SqeEnv},
        net::socket_addr_to_storage,
        op::{UringOp, UringOpErasure},
    };
    use io_uring::squeue;
    use veloq_buf::{FixedBuf, NoopRegistrar, heap::ChunkId};
    use veloq_driver_core::{
        driver::{OpToken, SubmitTokenContext},
        op::IntoPlatformOp,
        slot::Generation,
    };
    use veloq_std::{
        mem::size_of,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        num::NonZeroUsize,
    };

    fn buffer(len: usize) -> FixedBuf {
        FixedBuf::alloc_heap(
            NonZeroUsize::new(16).expect("test capacity is non-zero"),
            len,
        )
        .expect("test buffer allocation failed")
    }

    fn file_fd() -> IoFd {
        IoFd::direct(UringRawHandle::for_file(-1))
    }

    fn socket_fd() -> IoFd {
        IoFd::direct(UringRawHandle::for_socket(-1))
    }

    fn socket_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 12345))
    }

    fn storage_addr() -> (SockAddrStorage, u32) {
        socket_addr_to_storage(socket_addr())
    }

    fn test_token() -> SubmitTokenContext {
        let token = OpToken::from_registry_parts(0, Generation::new(1))
            .expect("test token should be encodable");
        SubmitTokenContext::user(token)
    }

    fn test_sqe_env<'a>(file_table: &'a FileTable, registrar: &'a NoopRegistrar) -> SqeEnv<'a> {
        SqeEnv::for_test(file_table, registrar)
    }

    fn assert_internal_error<T>(result: UringResult<T>) {
        let Err(report) = result else {
            panic!("projection mismatch should return an internal error");
        };
        assert_eq!(*report.inner(), UringError::Internal);
    }

    fn assert_erasure_mapping<S, K, U>(
        name: &str,
        operation: S,
        expected_kind: OpKind,
        expected_kernel_variant: K,
        expected_user_variant: U,
    ) where
        S: UringOpErasure + IntoPlatformOp<UringSlotSpec>,
        K: Fn(&UringOpPayload) -> bool,
        U: Fn(&UringUserPayload) -> bool,
    {
        let (op, payload) =
            <S as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(operation);
        let payload = <S as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload);

        assert_eq!(
            <S as UringOpDeclaration>::PAYLOAD_KIND,
            expected_kind,
            "{name}: spec kind"
        );
        assert_eq!(
            <S as IntoPlatformOp<UringSlotSpec>>::PAYLOAD_KIND,
            expected_kind,
            "{name}: platform kind"
        );
        assert!(
            core::ptr::eq(op.vtable(), <S as UringOpErasure>::vtable()),
            "{name}: vtable pointer"
        );
        assert!(
            expected_kernel_variant(&op.payload),
            "{name}: kernel payload variant"
        );
        assert!(
            expected_user_variant(&payload),
            "{name}: user payload variant"
        );
        assert_eq!(
            op.vtable().strategy,
            <S as UringOpDeclaration>::STRATEGY,
            "{name}: submission strategy"
        );
    }

    macro_rules! assert_mapping {
        ($name:literal, $ty:ty, $operation:expr, $kind:expr, $kernel:ident, $user:ident) => {
            assert_erasure_mapping::<$ty, _, _>(
                $name,
                $operation,
                $kind,
                |payload| matches!(payload, UringOpPayload::$kernel(_)),
                |payload| matches!(payload, UringUserPayload::$user(_)),
            );
        };
    }

    #[test]
    fn all_operation_erasure_mappings_match_the_baseline_descriptor() {
        let (addr, addr_len) = storage_addr();

        assert_mapping!(
            "ReadFixed",
            ReadFixed,
            ReadFixed {
                fd: file_fd(),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            OpKind::ReadFixed,
            Read,
            ReadFixed
        );
        assert_mapping!(
            "ReadRaw",
            ReadRaw,
            ReadRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            OpKind::ReadFixed,
            ReadRaw,
            ReadRaw
        );
        assert_mapping!(
            "WriteFixed",
            WriteFixed,
            WriteFixed {
                fd: file_fd(),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            OpKind::WriteFixed,
            Write,
            WriteFixed
        );
        assert_mapping!(
            "WriteRaw",
            WriteRaw,
            WriteRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            OpKind::WriteFixed,
            WriteRaw,
            WriteRaw
        );
        assert_mapping!(
            "Recv",
            Recv,
            Recv {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
            },
            OpKind::Recv,
            Recv,
            Recv
        );
        assert_mapping!(
            "RecvProvided",
            RecvProvided,
            RecvProvided { fd: socket_fd() },
            OpKind::RecvProvided,
            RecvProvided,
            RecvProvided
        );
        assert_mapping!(
            "RecvMulti",
            RecvMulti,
            RecvMulti { fd: socket_fd() },
            OpKind::RecvMulti,
            RecvMulti,
            RecvMulti
        );
        assert_mapping!(
            "OpSend",
            OpSend,
            OpSend {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
            },
            OpKind::Send,
            Send,
            OpSend
        );
        assert_mapping!(
            "UdpRecv",
            UdpRecv,
            UdpRecv {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
            },
            OpKind::UdpRecv,
            UdpRecv,
            UdpRecv
        );
        assert_mapping!(
            "UdpSend",
            UdpSend,
            UdpSend {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
            },
            OpKind::UdpSend,
            UdpSend,
            UdpSend
        );
        assert_mapping!(
            "Connect",
            Connect,
            Connect {
                fd: socket_fd(),
                addr,
                addr_len,
            },
            OpKind::Connect,
            Connect,
            Connect
        );
        assert_mapping!(
            "UdpConnect",
            UdpConnect,
            UdpConnect {
                fd: socket_fd(),
                addr,
                addr_len,
            },
            OpKind::UdpConnect,
            UdpConnect,
            UdpConnect
        );
        assert_mapping!(
            "Close",
            Close,
            Close { fd: file_fd() },
            OpKind::Close,
            Close,
            Close
        );
        assert_mapping!(
            "Fsync",
            Fsync,
            Fsync {
                fd: file_fd(),
                datasync: false,
            },
            OpKind::Fsync,
            Fsync,
            Fsync
        );
        assert_mapping!(
            "FsyncRaw",
            FsyncRaw,
            FsyncRaw {
                fd: UringRawHandle::for_file(-1),
                datasync: false,
            },
            OpKind::Fsync,
            FsyncRaw,
            FsyncRaw
        );
        assert_mapping!(
            "SyncFileRange",
            SyncFileRange,
            SyncFileRange {
                fd: file_fd(),
                offset: 0,
                nbytes: 4,
                flags: 0,
            },
            OpKind::SyncFileRange,
            SyncRange,
            SyncFileRange
        );
        assert_mapping!(
            "SyncFileRangeRaw",
            SyncFileRangeRaw,
            SyncFileRangeRaw {
                fd: UringRawHandle::for_file(-1),
                offset: 0,
                nbytes: 4,
                flags: 0,
            },
            OpKind::SyncFileRange,
            SyncRangeRaw,
            SyncFileRangeRaw
        );
        assert_mapping!(
            "Fallocate",
            Fallocate,
            Fallocate {
                fd: file_fd(),
                mode: 0,
                offset: 0,
                len: 4,
            },
            OpKind::Fallocate,
            Fallocate,
            Fallocate
        );
        assert_mapping!(
            "FallocateRaw",
            FallocateRaw,
            FallocateRaw {
                fd: UringRawHandle::for_file(-1),
                mode: 0,
                offset: 0,
                len: 4,
            },
            OpKind::Fallocate,
            FallocateRaw,
            FallocateRaw
        );
        assert_mapping!(
            "Accept",
            Accept,
            Accept {
                fd: socket_fd(),
                addr,
                addr_len,
                remote_addr: None,
            },
            OpKind::Accept,
            Accept,
            Accept
        );
        assert_mapping!(
            "AcceptMulti",
            AcceptMulti,
            AcceptMulti { fd: socket_fd() },
            OpKind::AcceptMulti,
            AcceptMulti,
            AcceptMulti
        );
        assert_mapping!(
            "SendTo",
            SendTo,
            SendTo {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
                addr: socket_addr(),
            },
            OpKind::SendTo,
            SendTo,
            SendTo
        );
        assert_mapping!(
            "UdpRecvFrom",
            UdpRecvFrom,
            UdpRecvFrom {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
                addr: None,
            },
            OpKind::UdpRecvFrom,
            UdpRecvFrom,
            UdpRecvFrom
        );
        assert_mapping!(
            "Open",
            Open,
            Open {
                path: buffer(1),
                flags: 0,
                mode: 0,
            },
            OpKind::Open,
            Open,
            Open
        );
        assert_mapping!(
            "Wakeup",
            Wakeup,
            Wakeup { fd: file_fd() },
            OpKind::Wakeup,
            Wakeup,
            Wakeup
        );
        assert_mapping!(
            "Timeout",
            Timeout,
            Timeout {
                duration: Duration::from_secs(1),
            },
            OpKind::Timeout,
            Timeout,
            Timeout
        );
    }

    #[test]
    fn separated_record_payload_mappings_match_the_baseline() {
        assert!(matches!(
            <AcceptMulti as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(
                UringUserPayload::AcceptedSocket(AcceptedSocket)
            ),
            Ok(AcceptedSocket)
        ));
        assert!(matches!(
            <RecvProvided as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(
                UringUserPayload::ProvidedBuf(ProvidedBuf { buf: None })
            ),
            Ok(ProvidedBuf { buf: None })
        ));
        assert!(matches!(
            <RecvMulti as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(
                UringUserPayload::ProvidedBuf(ProvidedBuf { buf: None })
            ),
            Ok(ProvidedBuf { buf: None })
        ));
    }

    fn read_raw_parts() -> (UringOp, UringUserPayload) {
        let (op, payload) =
            <ReadRaw as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(ReadRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            });
        (
            op,
            <ReadRaw as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload),
        )
    }

    fn write_raw_payload() -> UringUserPayload {
        <WriteRaw as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(WriteRaw {
            fd: UringRawHandle::for_file(-1),
            buf: buffer(4),
            offset: 0,
            buf_offset: 0,
        })
    }

    fn wrong_kernel_op() -> UringOp {
        let (op, _) =
            <WriteRaw as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(WriteRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            });
        op.with_vtable_for_test(<ReadRaw as UringOpErasure>::vtable())
    }

    #[test]
    fn projection_mismatch_shims_return_explicit_internal_errors() {
        let (mut op, _) = read_raw_parts();
        let mut user = write_raw_payload();
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);

        // SQE construction and completion now report the same internal mismatch class.
        assert_internal_error(unsafe {
            make_sqe_shim::<ReadRaw>(&mut op, &mut user, &env, test_token())
        });
        assert_internal_error(unsafe {
            on_complete_shim::<ReadRaw>(&mut op, &mut user, test_token().op_token, 0)
        });

        // Cleanup keeps the core guard contract, but its checked backend path is explicit too.
        let mut wrong_op = wrong_kernel_op();
        assert_internal_error(unsafe { completion_cleanup_shim::<ReadRaw>(&mut wrong_op, 0) });

        let mut wrong_op = wrong_kernel_op();
        assert_internal_error(unsafe { orphan_cleanup_shim::<ReadRaw>(&mut wrong_op, 0) });

        let wrong_op = wrong_kernel_op();
        let (_, valid_user) = read_raw_parts();
        assert_internal_error(unsafe {
            get_timeout_shim::<ReadRaw>(&wrong_op, &valid_user, test_token().op_token)
        });
        let (valid_op, _) = read_raw_parts();
        let user = write_raw_payload();
        assert_internal_error(unsafe {
            get_timeout_shim::<ReadRaw>(&valid_op, &user, test_token().op_token)
        });

        let mut chunks = [ChunkId::ZERO; 1];
        let wrong_op = wrong_kernel_op();
        let (_, valid_user) = read_raw_parts();
        assert_internal_error(unsafe {
            resolve_chunks_shim::<ReadRaw>(
                &wrong_op,
                &valid_user,
                test_token().op_token,
                &mut chunks,
            )
        });
        let (valid_op, _) = read_raw_parts();
        let user = write_raw_payload();
        assert_internal_error(unsafe {
            resolve_chunks_shim::<ReadRaw>(&valid_op, &user, test_token().op_token, &mut chunks)
        });

        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        let mut wrong_op = wrong_kernel_op();
        let (_, mut valid_user) = read_raw_parts();
        assert_internal_error(unsafe {
            record_item_shim::<ReadRaw>(
                &mut wrong_op,
                &mut valid_user,
                test_token().op_token,
                0,
                0,
                &mut cqe_env,
            )
        });

        let (mut valid_op, _) = read_raw_parts();
        let mut user = write_raw_payload();
        assert_internal_error(unsafe {
            record_item_shim::<ReadRaw>(
                &mut valid_op,
                &mut user,
                test_token().op_token,
                0,
                0,
                &mut cqe_env,
            )
        });
    }

    /// Reads `io_uring_sqe.addr`, which is the second 64-bit union field in the C ABI SQE.
    /// The offset and size are asserted here so this test fails if the dependency changes the
    /// layout it exposes to the opcode builders.
    fn sqe_addr(entry: &squeue::Entry) -> usize {
        const ADDR_OFFSET: usize = 16;
        assert_eq!(size_of::<squeue::Entry>(), 64);
        // SAFETY: `Entry` is `#[repr(C)]` around the dependency's 64-byte `io_uring_sqe`, and
        // the binding places its `addr` union at byte offset 16. `read_unaligned` permits the
        // byte-level inspection without imposing an alignment assumption on the test mirror.
        unsafe {
            core::ptr::read_unaligned(
                (entry as *const squeue::Entry)
                    .cast::<u8>()
                    .add(ADDR_OFFSET)
                    .cast::<u64>(),
            ) as usize
        }
    }

    #[test]
    fn send_to_payload_keeps_self_references_after_submission_setup() {
        let user = SendTo {
            fd: socket_fd(),
            buf: buffer(4),
            buf_offset: 0,
            addr: socket_addr(),
        };
        // Moving this operation through the erasure layer is valid before make_sqe establishes
        // any self-reference.
        let (mut op, user) =
            <SendTo as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(user);
        let mut user = <SendTo as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(user);
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);
        let entry = unsafe {
            (<SendTo as UringOpErasure>::vtable().make_sqe)(&mut op, &mut user, &env, test_token())
        }
        .expect("send_to SQE should be built with a direct socket descriptor");

        let UringOpPayload::SendTo(kernel) = &mut op.payload else {
            panic!("SendTo must use the SendTo kernel payload variant");
        };
        let (msg_name, iovec, msghdr) = kernel.test_pointers();
        assert_eq!(unsafe { (*msghdr).msg_name }, msg_name);
        assert_eq!(unsafe { (*msghdr).msg_iov }, iovec);
        assert_eq!(sqe_addr(&entry), msghdr as usize);
    }

    #[test]
    fn udp_recv_from_payload_keeps_self_references_and_decodes_address() {
        let user = UdpRecvFrom {
            fd: socket_fd(),
            buf: buffer(4),
            buf_offset: 0,
            addr: None,
        };
        // As above, the operation is moved before the kernel pointers are initialized.
        let (mut op, user) =
            <UdpRecvFrom as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(user);
        let mut user = <UdpRecvFrom as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(user);
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);
        let entry = unsafe {
            (<UdpRecvFrom as UringOpErasure>::vtable().make_sqe)(
                &mut op,
                &mut user,
                &env,
                test_token(),
            )
        }
        .expect("udp_recv_from SQE should be built with a direct socket descriptor");

        let expected_addr = socket_addr();
        let (storage, len) = storage_addr();
        let UringOpPayload::UdpRecvFrom(kernel) = &mut op.payload else {
            panic!("UdpRecvFrom must use the UdpRecvFrom kernel payload variant");
        };
        let (msg_name, iovec, msghdr) = kernel.test_pointers();
        assert_eq!(unsafe { (*msghdr).msg_name }, msg_name);
        assert_eq!(unsafe { (*msghdr).msg_iov }, iovec);
        assert_eq!(sqe_addr(&entry), msghdr as usize);

        // Writing the storage field in place does not move the payload; it models the kernel's
        // address write before the existing completion callback reads it.
        kernel.test_set_received_address(storage.0, len as usize);
        let result = unsafe {
            (<UdpRecvFrom as UringOpErasure>::vtable().on_complete)(
                &mut op,
                &mut user,
                test_token().op_token,
                4,
            )
        };
        assert_eq!(result.expect("valid UDP completion"), 4);
        assert!(matches!(
            user,
            UringUserPayload::UdpRecvFrom(UdpRecvFrom {
                addr: Some(addr),
                ..
            }) if addr == expected_addr
        ));
    }

    #[test]
    fn udp_recv_from_rejects_an_address_length_beyond_storage() {
        let user = UdpRecvFrom {
            fd: socket_fd(),
            buf: buffer(4),
            buf_offset: 0,
            addr: None,
        };
        let (mut op, user) =
            <UdpRecvFrom as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(user);
        let mut user = <UdpRecvFrom as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(user);
        let UringOpPayload::UdpRecvFrom(kernel) = &mut op.payload else {
            panic!("UdpRecvFrom must use the UdpRecvFrom kernel payload variant");
        };
        let (storage, _) = storage_addr();
        kernel.test_set_received_address(storage.0, size_of::<libc::sockaddr_storage>() + 1);

        let result = unsafe {
            (<UdpRecvFrom as UringOpErasure>::vtable().on_complete)(
                &mut op,
                &mut user,
                test_token().op_token,
                4,
            )
        };
        let Err(report) = result else {
            panic!("an oversized sockaddr length must be rejected");
        };
        assert_eq!(*report.inner(), UringError::InvalidState);
    }
}
