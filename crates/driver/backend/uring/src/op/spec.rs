use crate::{
    OwnedRawHandle,
    driver::env::{CqeEnv, SqeEnv},
    error::{UringError, UringResult},
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, CompletionCardinality, Connect,
        ErasedOperationDescriptor, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, Open,
        OperationDescriptor, ProvidedBuf, ReadFixed, ReadRaw, RecordPolicy, Recv, RecvMulti,
        RecvProvided, SendTo, SubmissionStrategy, SyncFileRange, SyncFileRangeRaw, Timeout,
        UdpConnect, UdpRecv, UdpRecvFrom, UdpSend, UringKernelOp, UringRecordItem, UringSlotSpec,
        UringUserPayload, Wakeup, WriteFixed, WriteRaw, payload, submit,
    },
};
use diagweave::prelude::*;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, OpToken, SubmitTokenContext},
    op::{IntoPlatformOp, LostReason, OpCompletion, OpError, OpKind, OpResult, SingleShotOp},
    slot::{SlotAccess, SlotAccessError},
};
use veloq_io_uring::squeue;
use veloq_std::{convert::identity, format, pin::Pin, time::Duration};

use submit::{
    completion_cleanup_close_raw_fd as cleanup_close_raw_fd,
    on_complete_accept as on_complete_accept_descriptor,
    on_complete_udp_recv_from as on_complete_udp_recv_from_descriptor,
    resolve_chunks_read_fixed as resolve_chunks_read_fixed_descriptor,
    resolve_chunks_read_raw as resolve_chunks_read_raw_descriptor,
    resolve_chunks_write_fixed as resolve_chunks_write_fixed_descriptor,
    resolve_chunks_write_raw as resolve_chunks_write_raw_descriptor,
};

pub(crate) trait UringOpSpec: Sized + Send + 'static {
    type KernelPayload;

    fn make_sqe(
        kernel: Pin<&mut Self::KernelPayload>,
        payload: &mut Self,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry>;

    fn on_complete(
        _kernel: Pin<&mut Self::KernelPayload>,
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

    fn get_timeout(
        _kernel: Pin<&Self::KernelPayload>,
        _payload: &Self,
    ) -> UringResult<Option<Duration>> {
        Ok(None)
    }

    fn resolve_chunks(
        _kernel: Pin<&Self::KernelPayload>,
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
        _kernel: Pin<&mut Self::KernelPayload>,
        _payload: &mut Self,
        _token: OpToken,
        _result: i32,
        _flags: u32,
        _env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem> {
        Ok(UringRecordItem::UseSubmitPayload)
    }
}

pub(crate) trait UringOperationDescriptor: UringOpSpec {
    type Completion;
    type RecordPayload;

    const PAYLOAD_KIND: OpKind;
    const NAME: &'static str;
    const USER_PAYLOAD_ID: PayloadId;
    const KERNEL_PAYLOAD_ID: PayloadId;

    fn descriptor() -> &'static OperationDescriptor<Self>;

    fn encode_kernel_payload(payload: Self::KernelPayload) -> UringKernelPayloadStorage;
    fn encode_user_payload(payload: Self) -> UringUserPayload;
    fn try_user_payload(payload: UringUserPayload) -> UringResult<Self>;
    fn try_record_from_erased(payload: UringUserPayload) -> UringResult<Self::RecordPayload>;

    #[cfg(test)]
    fn kernel_payload_ref(payload: &UringKernelPayloadStorage) -> Option<&Self::KernelPayload>;
    fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self>;

    unsafe fn with_projected_access<F, R>(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: Option<OpToken>,
        scope: &'static str,
        f: F,
    ) -> UringResult<R>
    where
        F: FnOnce(Pin<&mut Self::KernelPayload>, &mut Self) -> R;

    unsafe fn make_sqe_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        env: &SqeEnv<'_>,
        token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry>;
    unsafe fn on_complete_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: OpToken,
        result: i32,
    ) -> UringResult<usize>;
    unsafe fn get_timeout_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: OpToken,
    ) -> UringResult<Option<Duration>>;
    unsafe fn resolve_chunks_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: OpToken,
        chunks: &mut [ChunkId],
    ) -> UringResult<usize>;
    unsafe fn record_item_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: OpToken,
        result: i32,
        flags: u32,
        env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem>;
}

fn new_accept_kernel(_user: &Accept) -> payload::AcceptPayload {
    payload::AcceptPayload::new()
}

fn new_open_kernel(_user: &Open) -> payload::OpenPayload {
    payload::OpenPayload::new()
}

fn new_send_to_kernel(_user: &SendTo) -> payload::SendToPayload {
    payload::SendToPayload::new()
}

fn new_udp_recv_from_kernel(_user: &UdpRecvFrom) -> payload::UdpRecvFromPayload {
    payload::UdpRecvFromPayload::new()
}

fn new_wakeup_kernel(_user: &Wakeup) -> payload::WakeupPayload {
    payload::WakeupPayload::new()
}

fn new_timeout_kernel(_user: &Timeout) -> payload::TimeoutPayload {
    payload::TimeoutPayload::new()
}

unsafe fn descriptor_on_complete_default<S>(
    _kernel: Pin<&mut S::KernelPayload>,
    _payload: &mut S,
    result: i32,
) -> UringResult<usize>
where
    S: UringOpSpec,
{
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(UringError::CompletionWait
            .report(
                "uring.op.spec.descriptor_on_complete_default",
                "kernel completion returned error",
            )
            .set_error_code(-result))
    }
}

fn descriptor_get_timeout_default<S>(
    _kernel: Pin<&S::KernelPayload>,
    _payload: &S,
) -> UringResult<Option<Duration>>
where
    S: UringOpSpec,
{
    Ok(None)
}

fn descriptor_get_timeout_timeout(
    _kernel: Pin<&payload::TimeoutPayload>,
    payload: &Timeout,
) -> UringResult<Option<Duration>> {
    Ok(Some(payload.duration))
}

fn descriptor_resolve_chunks_default<S>(
    _kernel: Pin<&S::KernelPayload>,
    _payload: &S,
    _chunks: &mut [ChunkId],
) -> usize
where
    S: UringOpSpec,
{
    0
}

fn descriptor_record_item_submit<S>(
    _kernel: Pin<&mut S::KernelPayload>,
    _payload: &mut S,
    _token: OpToken,
    _result: i32,
    _flags: u32,
    _env: &mut CqeEnv<'_, '_>,
) -> UringResult<UringRecordItem>
where
    S: UringOpSpec,
{
    Ok(UringRecordItem::UseSubmitPayload)
}

fn descriptor_record_item_provided<S>(
    _kernel: Pin<&mut S::KernelPayload>,
    _payload: &mut S,
    _token: OpToken,
    result: i32,
    flags: u32,
    env: &mut CqeEnv<'_, '_>,
) -> UringResult<UringRecordItem>
where
    S: UringOpSpec,
{
    let buf = env.take_provided_buf(flags, result)?;
    Ok(UringRecordItem::New(UringUserPayload::from_storage(
        UringUserPayloadStorage::ProvidedBuf(ProvidedBuf { buf }),
    )))
}

fn descriptor_record_item_accepted<S>(
    _kernel: Pin<&mut S::KernelPayload>,
    _payload: &mut S,
    _token: OpToken,
    _result: i32,
    _flags: u32,
    _env: &mut CqeEnv<'_, '_>,
) -> UringResult<UringRecordItem>
where
    S: UringOpSpec,
{
    Ok(UringRecordItem::New(UringUserPayload::from_storage(
        UringUserPayloadStorage::AcceptedSocket(AcceptedSocket),
    )))
}

fn descriptor_cleanup_none(_result: i32) -> CompletionCleanupGuard {
    CompletionCleanupGuard::default()
}

fn projection_mismatch_report(
    scope: &'static str,
    operation: &'static str,
    token: Option<OpToken>,
    expected_kernel: Option<PayloadId>,
    actual_kernel: Option<PayloadId>,
    expected_user: Option<PayloadId>,
    actual_user: Option<PayloadId>,
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

fn projection_access_report(
    scope: &'static str,
    token: Option<OpToken>,
    error: SlotAccessError,
) -> Report<UringError> {
    let mut report = UringError::Internal
        .report(
            scope,
            "slot access failed during operation payload projection",
        )
        .with_ctx("slot_index", error.snapshot.index)
        .with_ctx("slot_generation", error.snapshot.generation)
        .with_ctx("slot_status", format!("{:?}", error.snapshot.status))
        .with_ctx("slot_has_op", error.snapshot.has_op)
        .with_ctx("slot_has_payload", error.snapshot.has_payload)
        .with_ctx("slot_access_action", format!("{:?}", error.action))
        .with_ctx("slot_access_reason", format!("{:?}", error.reason));
    if let Some(token) = token {
        report = report
            .with_ctx("token_index", token.index())
            .with_ctx("token_generation", token.generation());
    }
    report
}

macro_rules! descriptor_cardinality {
    (single) => {
        CompletionCardinality::Single
    };
    (multi) => {
        CompletionCardinality::Multi
    };
}

macro_rules! descriptor_resolve_chunks {
    (none) => {
        descriptor_resolve_chunks_default
    };
    ($resolve:ident) => {
        $resolve
    };
}

macro_rules! descriptor_on_complete {
    (default) => {
        descriptor_on_complete_default
    };
    ($on_complete:ident) => {
        $on_complete
    };
}

macro_rules! descriptor_get_timeout {
    (default) => {
        descriptor_get_timeout_default
    };
    ($get_timeout:ident) => {
        $get_timeout
    };
}

macro_rules! descriptor_record_factory {
    (use_submit) => {
        descriptor_record_item_submit
    };
    (provided_buffer) => {
        descriptor_record_item_provided
    };
    (accepted_socket) => {
        descriptor_record_item_accepted
    };
}

macro_rules! descriptor_cleanup {
    (none) => {
        descriptor_cleanup_none
    };
    ($cleanup:ident) => {
        $cleanup
    };
}

macro_rules! descriptor_cleanup_hint {
    (none) => {
        None
    };
    ($cleanup:ident) => {
        Some($cleanup)
    };
}

macro_rules! descriptor_try_record {
    (submit, $OpType:ty, $payload:ident) => {
        <$OpType as UringOperationDescriptor>::try_user_payload($payload)
    };
    ($record_variant:ident, $OpType:ty, $payload:ident) => {{
        let actual = $payload.payload_id();
        match $payload.into_storage() {
            UringUserPayloadStorage::$record_variant($payload) => Ok($payload),
            _ => Err(projection_mismatch_report(
                "uring.op.spec.try_record_from_erased",
                stringify!($OpType),
                None,
                None,
                None,
                Some(PayloadId::Record(stringify!($record_variant))),
                Some(actual),
            )),
        }
    }};
}

macro_rules! impl_uring_op_spec {
    (
        $OpType:ty,
        $kernel_type:ty,
        $make_sqe:path,
        $resolve_chunks:ident,
        $on_complete:ident,
        $get_timeout:ident,
        $record_factory:ident
    ) => {
        impl UringOpSpec for $OpType {
            type KernelPayload = $kernel_type;

            fn make_sqe(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                env: &SqeEnv<'_>,
                token: SubmitTokenContext,
            ) -> UringResult<squeue::Entry> {
                // SAFETY: the operation row selects the callback for this exact kernel and user
                // payload pair. The callback's pointer validity is upheld by the pinned slot.
                unsafe { $make_sqe(kernel, payload, env, token) }
            }

            fn on_complete(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                result: i32,
            ) -> UringResult<usize> {
                // SAFETY: the operation row selects the callback for this exact kernel and user
                // payload pair.
                unsafe { descriptor_on_complete!($on_complete)(kernel, payload, result) }
            }

            fn get_timeout(
                kernel: Pin<&Self::KernelPayload>,
                payload: &Self,
            ) -> UringResult<Option<Duration>> {
                descriptor_get_timeout!($get_timeout)(kernel, payload)
            }

            fn resolve_chunks(
                kernel: Pin<&Self::KernelPayload>,
                payload: &Self,
                chunks: &mut [ChunkId],
            ) -> UringResult<usize> {
                Ok(descriptor_resolve_chunks!($resolve_chunks)(
                    kernel, payload, chunks,
                ))
            }

            fn record_item(
                kernel: Pin<&mut Self::KernelPayload>,
                payload: &mut Self,
                token: OpToken,
                result: i32,
                flags: u32,
                env: &mut CqeEnv<'_, '_>,
            ) -> UringResult<UringRecordItem> {
                descriptor_record_factory!($record_factory)(
                    kernel, payload, token, result, flags, env,
                )
            }
        }
    };
}

macro_rules! impl_uring_operation_descriptor {
    (@impl
        $OpType:ty,
        $user_variant:ident,
        $kernel_variant:ident,
        $record:ty,
        $record_projection:ident,
        $record_policy:expr,
        $kind:path,
        $completion:ty,
        $strategy:path,
        $cardinality:ident,
        $new_kernel:path,
        $make_sqe:path,
        $resolve_chunks:ident,
        $on_complete:ident,
        $get_timeout:ident,
        $record_factory:ident,
        $cleanup:ident,
        $map:path
    ) => {
        impl UringOperationDescriptor for $OpType {
            type Completion = $completion;
            type RecordPayload = $record;

            const PAYLOAD_KIND: OpKind = $kind;
            const NAME: &'static str = stringify!($OpType);
            const USER_PAYLOAD_ID: PayloadId = PayloadId::User(stringify!($user_variant));
            const KERNEL_PAYLOAD_ID: PayloadId = PayloadId::Kernel(stringify!($kernel_variant));

            fn encode_kernel_payload(payload: Self::KernelPayload) -> UringKernelPayloadStorage {
                UringKernelPayloadStorage::$kernel_variant(payload)
            }

            fn encode_user_payload(payload: Self) -> UringUserPayload {
                UringUserPayload::from_storage(UringUserPayloadStorage::$user_variant(payload))
            }

            fn try_user_payload(payload: UringUserPayload) -> UringResult<Self> {
                let actual = payload.payload_id();
                match payload.into_storage() {
                    UringUserPayloadStorage::$user_variant(payload) => Ok(payload),
                    _ => Err(projection_mismatch_report(
                        "uring.op.spec.try_user_payload",
                        Self::NAME,
                        None,
                        None,
                        None,
                        Some(Self::USER_PAYLOAD_ID),
                        Some(actual),
                    )),
                }
            }

            fn try_record_from_erased(
                payload: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                descriptor_try_record!($record_projection, $OpType, payload)
            }

            #[cfg(test)]
            fn kernel_payload_ref(
                payload: &UringKernelPayloadStorage,
            ) -> Option<&Self::KernelPayload> {
                match payload {
                    UringKernelPayloadStorage::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn user_payload_ref(payload: &UringUserPayload) -> Option<&Self> {
                match &payload.storage {
                    UringUserPayloadStorage::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            unsafe fn with_projected_access<F, R>(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: Option<OpToken>,
                scope: &'static str,
                f: F,
            ) -> UringResult<R>
            where
                F: FnOnce(Pin<&mut Self::KernelPayload>, &mut Self) -> R,
            {
                let (mut op, payload) = access
                    .operation_and_payload_mut()
                    .map_err(|err: SlotAccessError| {
                        projection_access_report(scope, token, err)
                    })?;
                let actual_kernel = op.as_ref().get_ref().payload.payload_id();
                let actual_user = payload.payload_id();
                let op = unsafe { op.as_mut().get_unchecked_mut() };
                let kernel = match &mut op.payload {
                    UringKernelPayloadStorage::$kernel_variant(kernel) => {
                        // SAFETY: `op` is borrowed through `SlotAccess`, so this variant
                        // remains at its stable slot address for the duration of the callback.
                        unsafe { Pin::new_unchecked(kernel) }
                    }
                    _ => {
                        return Err(projection_mismatch_report(
                            scope,
                            Self::NAME,
                            token,
                            Some(Self::KERNEL_PAYLOAD_ID),
                            Some(actual_kernel),
                            Some(Self::USER_PAYLOAD_ID),
                            Some(actual_user),
                        ));
                    }
                };
                let user = match &mut payload.storage {
                    UringUserPayloadStorage::$user_variant(user) => user,
                    _ => {
                        return Err(projection_mismatch_report(
                            scope,
                            Self::NAME,
                            token,
                            Some(Self::KERNEL_PAYLOAD_ID),
                            Some(actual_kernel),
                            Some(Self::USER_PAYLOAD_ID),
                            Some(actual_user),
                        ));
                    }
                };
                Ok(f(kernel, user))
            }

            unsafe fn make_sqe_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                env: &SqeEnv<'_>,
                token: SubmitTokenContext,
            ) -> UringResult<squeue::Entry> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token.op_token),
                        "uring.op.spec.make_sqe",
                        |kernel, user| Self::make_sqe(kernel, user, env, token),
                    )
                }?
            }

            unsafe fn on_complete_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                result: i32,
            ) -> UringResult<usize> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.on_complete",
                        |kernel, user| Self::on_complete(kernel, user, result),
                    )
                }?
            }

            unsafe fn get_timeout_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
            ) -> UringResult<Option<Duration>> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.get_timeout",
                        |kernel, user| Self::get_timeout(kernel.as_ref(), user),
                    )
                }?
            }

            unsafe fn resolve_chunks_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                chunks: &mut [ChunkId],
            ) -> UringResult<usize> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.resolve_chunks",
                        |kernel, user| Self::resolve_chunks(kernel.as_ref(), user, chunks),
                    )
                }?
            }

            unsafe fn record_item_dispatch(
                access: &mut SlotAccess<'_, UringSlotSpec>,
                token: OpToken,
                result: i32,
                flags: u32,
                env: &mut CqeEnv<'_, '_>,
            ) -> UringResult<UringRecordItem> {
                unsafe {
                    Self::with_projected_access(
                        access,
                        Some(token),
                        "uring.op.spec.record_item",
                        |kernel, user| Self::record_item(kernel, user, token, result, flags, env),
                    )
                }?
            }

            fn descriptor() -> &'static OperationDescriptor<Self> {
                static DESCRIPTOR: OperationDescriptor<$OpType> = OperationDescriptor {
                    new_kernel: $new_kernel,
                    encode_kernel: <$OpType as UringOperationDescriptor>::encode_kernel_payload,
                    encode_submit: <$OpType as UringOperationDescriptor>::encode_user_payload,
                    try_record: <$OpType as UringOperationDescriptor>::try_record_from_erased,
                    map_completion: $map,
                    erased: ErasedOperationDescriptor {
                        name: <$OpType as UringOperationDescriptor>::NAME,
                        strategy: $strategy,
                        cardinality: descriptor_cardinality!($cardinality),
                        record_policy: $record_policy,
                        make_sqe: <$OpType as UringOperationDescriptor>::make_sqe_dispatch,
                        on_complete: <$OpType as UringOperationDescriptor>::on_complete_dispatch,
                        completion_cleanup: descriptor_cleanup!($cleanup),
                        completion_cleanup_hint: descriptor_cleanup_hint!($cleanup),
                        orphan_cleanup: descriptor_cleanup!($cleanup),
                        get_timeout: <$OpType as UringOperationDescriptor>::get_timeout_dispatch,
                        resolve_chunks:
                            <$OpType as UringOperationDescriptor>::resolve_chunks_dispatch,
                        record_item: <$OpType as UringOperationDescriptor>::record_item_dispatch,
                    },
                };
                &DESCRIPTOR
            }
        }
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
    ) => {
        impl_uring_operation_descriptor!(@impl
            $OpType,
            $user_variant,
            $kernel_variant,
            $OpType,
            submit,
            RecordPolicy::UseSubmitPayload,
            $kind,
            $completion,
            $strategy,
            $cardinality,
            $new_kernel,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory,
            $cleanup,
            $map
        );
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
    ) => {
        impl_uring_operation_descriptor!(@impl
            $OpType,
            $user_variant,
            $kernel_variant,
            $record,
            $record_variant,
            descriptor_record_policy!($record_variant),
            $kind,
            $completion,
            $strategy,
            $cardinality,
            $new_kernel,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory,
            $cleanup,
            $map
        );
    };
}

macro_rules! descriptor_record_policy {
    (ProvidedBuf) => {
        RecordPolicy::NewProvidedBuffer
    };
    (AcceptedSocket) => {
        RecordPolicy::NewAcceptedSocket
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
/// descriptor row 中的 completion mapper 把 CQE 的结果变成这个操作的 `Completion`。它
/// 拿不到 payload——记录 payload 里没有提交时的信息，这正是两者分家的意思。
macro_rules! impl_uring_record_payload_op {
    ($OpType:ty, $record_variant:ident, $record:ty, $completion:ty) => {
        impl IntoPlatformOp<UringSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as UringOperationDescriptor>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let kernel_payload = (descriptor.new_kernel)(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.encode_submit)(payload)
            }

            fn try_record_from_erased(
                erased: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.try_record)(erased)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                OpCompletion::new((descriptor.map_completion)(res), payload)
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

            const PAYLOAD_KIND: OpKind = <$OpType as UringOperationDescriptor>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (UringKernelOp, Self::SubmitPayload) {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let kernel_payload = (descriptor.new_kernel)(&self);
                let op = UringKernelOp::new::<$OpType>(kernel_payload);
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> UringUserPayload {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.encode_submit)(payload)
            }

            fn try_record_from_erased(
                payload: UringUserPayload,
            ) -> UringResult<Self::RecordPayload> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                (descriptor.try_record)(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: UringResult<usize>,
            ) -> OpCompletion<Self::Output, UringError, Self::Completion> {
                let descriptor = <$OpType as UringOperationDescriptor>::descriptor();
                let completion = (descriptor.map_completion)(res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
}

macro_rules! impl_uring_single_shot_marker {
    (single, $OpType:ty) => {
        impl SingleShotOp<UringSlotSpec> for $OpType {}
    };
    (multi, $OpType:ty) => {};
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OperationCoverage {
    name: &'static str,
    descriptor_name: &'static str,
    user_payload: &'static str,
    kernel_payload: &'static str,
    kind: OpKind,
    record_payload: &'static str,
    record_policy: RecordPolicy,
    strategy: SubmissionStrategy,
    cardinality: CompletionCardinality,
}

#[cfg(test)]
macro_rules! operation_coverage_entries {
    (@collect [] [$($entries:tt)*]) => {
        &[$($entries)*]
    };
    (
        @collect [
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
        ] [$($entries:tt)*]
    ) => {
        operation_coverage_entries!(@collect [$($rest)*] [
            $($entries)*
            OperationCoverage {
            name: stringify!($OpType),
            descriptor_name: <$OpType as UringOperationDescriptor>::NAME,
            user_payload: stringify!($user_variant),
            kernel_payload: stringify!($kernel_variant),
            kind: $kind,
            record_payload: "submit",
            record_policy: RecordPolicy::UseSubmitPayload,
            strategy: $strategy,
            cardinality: descriptor_cardinality!($cardinality),
            },
        ])
    };
    (
        @collect [
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
        ] [$($entries:tt)*]
    ) => {
        operation_coverage_entries!(@collect [$($rest)*] [
            $($entries)*
            OperationCoverage {
            name: stringify!($OpType),
            descriptor_name: <$OpType as UringOperationDescriptor>::NAME,
            user_payload: stringify!($user_variant),
            kernel_payload: stringify!($kernel_variant),
            kind: $kind,
            record_payload: stringify!($record_variant),
            record_policy: descriptor_record_policy!($record_variant),
            strategy: $strategy,
            cardinality: descriptor_cardinality!($cardinality),
            },
        ])
    };
}

/// Stable, allocation-free identity used by projection diagnostics.
///
/// The operation rows provide the names for each storage arm.  The domain is part of the id so
/// that a user payload and a kernel payload with the same Rust variant name remain distinct.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PayloadId {
    User(&'static str),
    Kernel(&'static str),
    Record(&'static str),
}

impl PayloadId {
    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::User(name) | Self::Kernel(name) | Self::Record(name) => name,
        }
    }
}

macro_rules! generated_user_storage {
    (@collect [$provided:ident $accepted:ident] [$($output:tt)*]) => {
        pub(crate) enum UringUserPayloadStorage {
            $($output)*
        }
    };
    (
        @collect [$provided:ident $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType), ProvidedBuf($record),]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [provided accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [provided $accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident no_accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided accepted]
            [$($output)* $user_variant($OpType), AcceptedSocket($record),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident accepted] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided accepted]
            [$($output)* $user_variant($OpType),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident $accepted:ident] [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_storage!(@collect
            [$provided $accepted]
            [$($output)* $user_variant($OpType), $record_variant($record),]
            $($tail)*
        );
    };
}

macro_rules! generated_kernel_storage {
    (@collect [$($output:tt)*]) => {
        pub(crate) enum UringKernelPayloadStorage {
            $($output)*
        }
    };
    (
        @collect [$($output:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_kernel_storage!(@collect
            [$($output)* $kernel_variant($kernel_type),]
            $($tail)*
        );
    };
}

macro_rules! generated_user_ids {
    (@collect [$provided:ident $accepted:ident] [$($arms:tt)*]) => {
        impl UringUserPayloadStorage {
            pub(super) const fn payload_id(&self) -> PayloadId {
                match self {
                    $($arms)*
                }
            }
        }
    };
    (
        @collect [$provided:ident $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::ProvidedBuf(_) => PayloadId::Record("ProvidedBuf"),
            ]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident no_accepted] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::AcceptedSocket(_) => PayloadId::Record("AcceptedSocket"),
            ]
            $($tail)*
        );
    };
    (
        @collect [$provided:ident accepted] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: AcceptedSocket($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: ProvidedBuf($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [provided $accepted]
            [$($arms)* Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),]
            $($tail)*
        );
    };
    (
        @collect [no_provided $accepted:ident] [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_user_ids!(@collect
            [$provided $accepted]
            [$($arms)*
                Self::$user_variant(_) => PayloadId::User(stringify!($user_variant)),
                Self::$record_variant(_) => PayloadId::Record(stringify!($record_variant)),
            ]
            $($tail)*
        );
    };
}

macro_rules! generated_kernel_ids {
    (@collect [$($arms:tt)*]) => {
        impl UringKernelPayloadStorage {
            pub(super) const fn payload_id(&self) -> PayloadId {
                match self {
                    $($arms)*
                }
            }
        }
    };
    (
        @collect [$($arms:tt)*]
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            $($rest:tt)*
        };
        $($tail:tt)*
    ) => {
        generated_kernel_ids!(@collect
            [$($arms)* Self::$kernel_variant(_) => PayloadId::Kernel(stringify!($kernel_variant)),]
            $($tail)*
        );
    };
}

macro_rules! declare_payload_storage {
    ($($operations:tt)*) => {
        generated_user_storage!(@collect [no_provided no_accepted] [] $($operations)*);
        generated_kernel_storage!(@collect [] $($operations)*);
        generated_user_ids!(@collect [no_provided no_accepted] [] $($operations)*);
        generated_kernel_ids!(@collect [] $($operations)*);

        impl UringUserPayload {
            pub(super) fn from_storage(storage: UringUserPayloadStorage) -> Self {
                Self { storage }
            }

            pub(super) fn into_storage(self) -> UringUserPayloadStorage {
                self.storage
            }

            pub(super) const fn payload_id(&self) -> PayloadId {
                self.storage.payload_id()
            }
        }
    };
}

macro_rules! declare_uring_operations {
    ($($operations:tt)*) => {
        declare_payload_storage!($($operations)*);
        #[cfg(test)]
        const OPERATION_COVERAGE: &[OperationCoverage] =
            operation_coverage_entries!(@collect [$($operations)*] []);
        declare_uring_operations_impl!($($operations)*);
    };
}

macro_rules! declare_uring_operations_impl {
    () => {};
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: submit,
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_spec!(
            $OpType,
            $kernel_type,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory
        );
        impl_uring_single_shot_op!($OpType, $completion);
        impl_uring_operation_descriptor! {
            $OpType {
                user: $user_variant,
                kernel: $kernel_variant,
                kernel_type: $kernel_type,
                kind: $kind,
                completion: $completion,
                record: submit,
                strategy: $strategy,
                cardinality: $cardinality,
                new_kernel: $new_kernel,
                make_sqe: $make_sqe,
                resolve_chunks: $resolve_chunks,
                on_complete: $on_complete,
                get_timeout: $get_timeout,
                record_factory: $record_factory,
                cleanup: $cleanup,
                map: $map,
            };
        }
        declare_uring_operations_impl!($($rest)*);
    };
    (
        $OpType:ty {
            user: $user_variant:ident,
            kernel: $kernel_variant:ident,
            kernel_type: $kernel_type:ty,
            kind: $kind:path,
            completion: $completion:ty,
            record: $record_variant:ident($record:ty),
            strategy: $strategy:path,
            cardinality: $cardinality:ident,
            new_kernel: $new_kernel:path,
            make_sqe: $make_sqe:path,
            resolve_chunks: $resolve_chunks:ident,
            on_complete: $on_complete:ident,
            get_timeout: $get_timeout:ident,
            record_factory: $record_factory:ident,
            cleanup: $cleanup:ident,
            map: $map:path,
        };
        $($rest:tt)*
    ) => {
        impl_uring_op_spec!(
            $OpType,
            $kernel_type,
            $make_sqe,
            $resolve_chunks,
            $on_complete,
            $get_timeout,
            $record_factory
        );
        impl_uring_record_payload_op!($OpType, $record_variant, $record, $completion);
        impl_uring_single_shot_marker!($cardinality, $OpType);
        impl_uring_operation_descriptor! {
            $OpType {
                user: $user_variant,
                kernel: $kernel_variant,
                kernel_type: $kernel_type,
                kind: $kind,
                completion: $completion,
                record: $record_variant($record),
                strategy: $strategy,
                cardinality: $cardinality,
                new_kernel: $new_kernel,
                make_sqe: $make_sqe,
                resolve_chunks: $resolve_chunks,
                on_complete: $on_complete,
                get_timeout: $get_timeout,
                record_factory: $record_factory,
                cleanup: $cleanup,
                map: $map,
            };
        }
        declare_uring_operations_impl!($($rest)*);
    };
}

declare_uring_operations! {
    ReadFixed {
        user: ReadFixed,
        kernel: Read,
        kernel_type: payload::KernelRef<ReadFixed>,
        kind: OpKind::ReadFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_read_fixed,
        resolve_chunks: resolve_chunks_read_fixed_descriptor,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    ReadRaw {
        user: ReadRaw,
        kernel: ReadRaw,
        kernel_type: payload::KernelRef<ReadRaw>,
        kind: OpKind::ReadFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_read_raw,
        resolve_chunks: resolve_chunks_read_raw_descriptor,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    WriteFixed {
        user: WriteFixed,
        kernel: Write,
        kernel_type: payload::KernelRef<WriteFixed>,
        kind: OpKind::WriteFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_write_fixed,
        resolve_chunks: resolve_chunks_write_fixed_descriptor,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    WriteRaw {
        user: WriteRaw,
        kernel: WriteRaw,
        kernel_type: payload::KernelRef<WriteRaw>,
        kind: OpKind::WriteFixed,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_write_raw,
        resolve_chunks: resolve_chunks_write_raw_descriptor,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Recv {
        user: Recv,
        kernel: Recv,
        kernel_type: payload::KernelRef<Recv>,
        kind: OpKind::Recv,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_recv,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    RecvProvided {
        user: RecvProvided,
        kernel: RecvProvided,
        kernel_type: payload::KernelRef<RecvProvided>,
        kind: OpKind::RecvProvided,
        completion: usize,
        record: ProvidedBuf(ProvidedBuf),
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_recv_provided,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: provided_buffer,
        cleanup: none,
        map: identity,
    };
    RecvMulti {
        user: RecvMulti,
        kernel: RecvMulti,
        kernel_type: payload::KernelRef<RecvMulti>,
        kind: OpKind::RecvMulti,
        completion: usize,
        record: ProvidedBuf(ProvidedBuf),
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: multi,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_recv_multi,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: provided_buffer,
        cleanup: none,
        map: identity,
    };
    OpSend {
        user: OpSend,
        kernel: Send,
        kernel_type: payload::KernelRef<OpSend>,
        kind: OpKind::Send,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_send,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    UdpRecv {
        user: UdpRecv,
        kernel: UdpRecv,
        kernel_type: payload::KernelRef<UdpRecv>,
        kind: OpKind::UdpRecv,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_udp_recv,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    UdpSend {
        user: UdpSend,
        kernel: UdpSend,
        kernel_type: payload::KernelRef<UdpSend>,
        kind: OpKind::UdpSend,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_udp_send,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Connect {
        user: Connect,
        kernel: Connect,
        kernel_type: payload::KernelRef<Connect>,
        kind: OpKind::Connect,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_connect,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    UdpConnect {
        user: UdpConnect,
        kernel: UdpConnect,
        kernel_type: payload::KernelRef<UdpConnect>,
        kind: OpKind::UdpConnect,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_udp_connect,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Close {
        user: Close,
        kernel: Close,
        kernel_type: payload::KernelRef<Close>,
        kind: OpKind::Close,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_close,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Fsync {
        user: Fsync,
        kernel: Fsync,
        kernel_type: payload::KernelRef<Fsync>,
        kind: OpKind::Fsync,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_fsync,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    FsyncRaw {
        user: FsyncRaw,
        kernel: FsyncRaw,
        kernel_type: payload::KernelRef<FsyncRaw>,
        kind: OpKind::Fsync,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_fsync_raw,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    SyncFileRange {
        user: SyncFileRange,
        kernel: SyncRange,
        kernel_type: payload::KernelRef<SyncFileRange>,
        kind: OpKind::SyncFileRange,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_sync_range,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    SyncFileRangeRaw {
        user: SyncFileRangeRaw,
        kernel: SyncRangeRaw,
        kernel_type: payload::KernelRef<SyncFileRangeRaw>,
        kind: OpKind::SyncFileRange,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_sync_range_raw,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Fallocate {
        user: Fallocate,
        kernel: Fallocate,
        kernel_type: payload::KernelRef<Fallocate>,
        kind: OpKind::Fallocate,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_fallocate,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    FallocateRaw {
        user: FallocateRaw,
        kernel: FallocateRaw,
        kernel_type: payload::KernelRef<FallocateRaw>,
        kind: OpKind::Fallocate,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_fallocate_raw,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Accept {
        user: Accept,
        kernel: Accept,
        kernel_type: payload::AcceptPayload,
        kind: OpKind::Accept,
        completion: OwnedRawHandle,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: new_accept_kernel,
        make_sqe: submit::make_sqe_accept,
        resolve_chunks: none,
        on_complete: on_complete_accept_descriptor,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: cleanup_close_raw_fd,
        map: submit::accepted_handle_from_res,
    };
    AcceptMulti {
        user: AcceptMulti,
        kernel: AcceptMulti,
        kernel_type: payload::KernelRef<AcceptMulti>,
        kind: OpKind::AcceptMulti,
        completion: OwnedRawHandle,
        record: AcceptedSocket(AcceptedSocket),
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: multi,
        new_kernel: payload::kernel_ref,
        make_sqe: submit::make_sqe_accept_multi,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: accepted_socket,
        cleanup: cleanup_close_raw_fd,
        map: submit::accepted_handle_from_res,
    };
    SendTo {
        user: SendTo,
        kernel: SendTo,
        kernel_type: payload::SendToPayload,
        kind: OpKind::SendTo,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: new_send_to_kernel,
        make_sqe: submit::make_sqe_send_to,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    UdpRecvFrom {
        user: UdpRecvFrom,
        kernel: UdpRecvFrom,
        kernel_type: payload::UdpRecvFromPayload,
        kind: OpKind::UdpRecvFrom,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: new_udp_recv_from_kernel,
        make_sqe: submit::make_sqe_udp_recv_from,
        resolve_chunks: none,
        on_complete: on_complete_udp_recv_from_descriptor,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Open {
        user: Open,
        kernel: Open,
        kernel_type: payload::OpenPayload,
        kind: OpKind::Open,
        completion: OwnedRawHandle,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: new_open_kernel,
        make_sqe: submit::make_sqe_open,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: cleanup_close_raw_fd,
        map: submit::opened_handle_from_res,
    };
    Wakeup {
        user: Wakeup,
        kernel: Wakeup,
        kernel_type: payload::WakeupPayload,
        kind: OpKind::Wakeup,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: single,
        new_kernel: new_wakeup_kernel,
        make_sqe: submit::make_sqe_wakeup,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    Timeout {
        user: Timeout,
        kernel: Timeout,
        kernel_type: payload::TimeoutPayload,
        kind: OpKind::Timeout,
        completion: usize,
        record: submit,
        strategy: SubmissionStrategy::SoftwareTimer,
        cardinality: single,
        new_kernel: new_timeout_kernel,
        make_sqe: submit::make_sqe_timeout,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: descriptor_get_timeout_timeout,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{FileTableExhaustion, IoFd, SockAddrStorage, UringRawHandle},
        diagnostics::UringCompletionDiagnostics,
        driver::{
            env::{CqeEnv, SqeEnv},
            lifecycle::UringOpState,
            registration::file_table::FileTable,
        },
        net::socket_addr_to_storage,
        op::{UringOp, UringOpRegistry, UringOperationDescriptor},
        test_alloc::{AllocationCounts, measure},
    };
    use veloq_buf::{FixedBuf, NoopRegistrar, heap::ChunkId};
    use veloq_driver_core::{
        driver::{OpToken, SubmitTokenContext, registry::OpEntry},
        op::IntoPlatformOp,
        slot::{
            CheckedSlotView, Reserved, Slot, SlotAccess, SlotAccessOutcome, SlotRegistryExt,
            SlotView,
        },
    };
    use veloq_io_uring::squeue;
    use veloq_std::{
        mem::{align_of, size_of},
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

    fn test_sqe_env<'a>(file_table: &'a FileTable, registrar: &'a NoopRegistrar) -> SqeEnv<'a> {
        SqeEnv::for_test(file_table, registrar)
    }

    fn test_provided_sqe_env<'a>(
        file_table: &'a FileTable,
        registrar: &'a NoopRegistrar,
    ) -> SqeEnv<'a> {
        SqeEnv::for_test_with_provided(file_table, registrar, 7, 16)
    }

    fn with_test_slot_pair<F, R>(op: UringOp, payload: UringUserPayload, f: F) -> R
    where
        F: FnOnce(OpToken, &mut Slot<'_, Reserved, UringSlotSpec>) -> R,
    {
        let mut registry = UringOpRegistry::new(1);
        let handle = registry
            .insert(OpEntry::new(UringOpState::new()))
            .unwrap_or_else(|_| panic!("test registry should have capacity"));
        let token = OpToken::from_registry_parts(handle.index, handle.generation)
            .expect("test token should be encodable");
        registry
            .with_slot_storage_mut(token, |_result, slot_payload, _sidecar| {
                *slot_payload = Some(payload);
            })
            .expect("test slot storage should exist");
        let reserved = match registry
            .checked_slot_view(token)
            .expect("test slot lookup should succeed")
        {
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot,
            _ => panic!("test slot should be reserved"),
        };
        let mut reserved = reserved
            .init_op_with(op, |_| {})
            .expect("test reserved slot should accept operation");
        f(token, &mut reserved)
    }

    fn with_test_slot<S, F, R>(operation: S, f: F) -> R
    where
        S: UringOperationDescriptor + IntoPlatformOp<UringSlotSpec>,
        F: FnOnce(OpToken, &mut Slot<'_, Reserved, UringSlotSpec>) -> R,
    {
        let (op, payload) =
            <S as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(operation);
        let payload = <S as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload);
        with_test_slot_pair(op, payload, f)
    }

    fn with_test_access<F, R>(
        slot: &mut Slot<'_, Reserved, UringSlotSpec>,
        f: F,
    ) -> SlotAccessOutcome<R>
    where
        F: FnOnce(&mut SlotAccess<'_, UringSlotSpec>) -> R,
    {
        slot.with_access_mut(f)
    }

    fn assert_no_fast_path_allocations(name: &str, counts: AllocationCounts) {
        assert_eq!(
            counts.dynamic_allocations(),
            0,
            "{name}: operation dispatch allocated: {counts:?}"
        );
        assert_eq!(
            counts.deallocations, 0,
            "{name}: operation dispatch deallocated: {counts:?}"
        );
    }

    fn measure_sqe_and_completion<S>(
        operation: S,
        env: &SqeEnv<'_>,
        completion_result: i32,
    ) -> AllocationCounts
    where
        S: UringOperationDescriptor + IntoPlatformOp<UringSlotSpec>,
    {
        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        with_test_slot(operation, |token, slot| {
            let (_, counts) = measure(|| {
                with_test_access(slot, |access| {
                    let descriptor = access.operation().get_ref().descriptor();
                    let _entry = unsafe {
                        (descriptor.make_sqe)(access, env, SubmitTokenContext::user(token))
                    }
                    .expect("baseline SQE dispatch should succeed");
                    let completion =
                        unsafe { (descriptor.on_complete)(access, token, completion_result) }
                            .expect("baseline completion dispatch should succeed");
                    assert_eq!(completion, completion_result as usize);
                    let _record = unsafe {
                        (descriptor.record_item)(access, token, completion_result, 0, &mut cqe_env)
                    }
                    .expect("baseline record dispatch should succeed");
                    let _cleanup = (descriptor.completion_cleanup)(completion_result);
                })
                .expect("test operation access should succeed");
            });
            counts
        })
    }

    fn measure_udp_recv_from_dispatch(
        operation: UdpRecvFrom,
        env: &SqeEnv<'_>,
    ) -> AllocationCounts {
        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        let (storage, storage_len) = storage_addr();
        with_test_slot(operation, |token, slot| {
            let (_, counts) = measure(|| {
                with_test_access(slot, |access| {
                    let descriptor = access.operation().get_ref().descriptor();
                    let _entry = unsafe {
                        (descriptor.make_sqe)(access, env, SubmitTokenContext::user(token))
                    }
                    .expect("baseline UDP recv-from SQE dispatch should succeed");
                    unsafe {
                        <UdpRecvFrom as UringOperationDescriptor>::with_projected_access(
                            access,
                            Some(token),
                            "uring.op.spec.test.measure_udp_recv_from",
                            |kernel, _| {
                                kernel
                                    .get_unchecked_mut()
                                    .test_set_received_address(storage.0, storage_len as usize);
                            },
                        )
                    }
                    .expect("UDP kernel payload projection should succeed");
                    let completion =
                        unsafe { (descriptor.on_complete)(access, token, storage_len as i32) }
                            .expect("baseline UDP recv-from completion dispatch should succeed");
                    assert_eq!(completion, storage_len as usize);
                    let _record = unsafe {
                        (descriptor.record_item)(access, token, storage_len as i32, 0, &mut cqe_env)
                    }
                    .expect("baseline UDP recv-from record dispatch should succeed");
                })
                .expect("test operation access should succeed");
            });
            counts
        })
    }

    fn measure_timer_dispatch(operation: Timeout) -> AllocationCounts {
        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        with_test_slot(operation, |token, slot| {
            let (_, counts) = measure(|| {
                with_test_access(slot, |access| {
                    let descriptor = access.operation().get_ref().descriptor();
                    let duration = unsafe { (descriptor.get_timeout)(access, token) }
                        .expect("baseline timer dispatch should succeed")
                        .expect("baseline timer should expose a duration");
                    assert_eq!(duration, Duration::from_secs(1));
                    let completion = unsafe { (descriptor.on_complete)(access, token, 0) }
                        .expect("baseline timer completion dispatch should succeed");
                    assert_eq!(completion, 0);
                    let _record =
                        unsafe { (descriptor.record_item)(access, token, 0, 0, &mut cqe_env) }
                            .expect("baseline timer record dispatch should succeed");
                })
                .expect("test operation access should succeed");
            });
            counts
        })
    }

    #[test]
    fn operation_descriptor_coverage_has_all_26_rows() {
        const EXPECTED_NAMES: [&str; 26] = [
            "ReadFixed",
            "ReadRaw",
            "WriteFixed",
            "WriteRaw",
            "Recv",
            "RecvProvided",
            "RecvMulti",
            "OpSend",
            "UdpRecv",
            "UdpSend",
            "Connect",
            "UdpConnect",
            "Close",
            "Fsync",
            "FsyncRaw",
            "SyncFileRange",
            "SyncFileRangeRaw",
            "Fallocate",
            "FallocateRaw",
            "Accept",
            "AcceptMulti",
            "SendTo",
            "UdpRecvFrom",
            "Open",
            "Wakeup",
            "Timeout",
        ];

        assert_eq!(OPERATION_COVERAGE.len(), EXPECTED_NAMES.len());
        for (coverage, expected_name) in OPERATION_COVERAGE.iter().zip(EXPECTED_NAMES) {
            assert_eq!(coverage.name, expected_name);
            assert_eq!(coverage.descriptor_name, expected_name);
            assert_eq!(coverage.user_payload, expected_name);
        }

        assert_eq!(OPERATION_COVERAGE[0].kernel_payload, "Read");
        assert_eq!(OPERATION_COVERAGE[1].kind, OpKind::ReadFixed);
        assert_eq!(OPERATION_COVERAGE[2].kernel_payload, "Write");
        assert_eq!(OPERATION_COVERAGE[3].kind, OpKind::WriteFixed);
        assert_eq!(OPERATION_COVERAGE[6].record_payload, "ProvidedBuf");
        assert_eq!(
            OPERATION_COVERAGE[5].record_policy,
            RecordPolicy::NewProvidedBuffer
        );
        assert_eq!(
            OPERATION_COVERAGE[6].record_policy,
            RecordPolicy::NewProvidedBuffer
        );
        assert_eq!(
            OPERATION_COVERAGE[6].cardinality,
            CompletionCardinality::Multi
        );
        assert_eq!(OPERATION_COVERAGE[20].record_payload, "AcceptedSocket");
        assert_eq!(
            OPERATION_COVERAGE[20].record_policy,
            RecordPolicy::NewAcceptedSocket
        );
        assert_eq!(
            OPERATION_COVERAGE[20].cardinality,
            CompletionCardinality::Multi
        );
        assert_eq!(
            OPERATION_COVERAGE[25].strategy,
            SubmissionStrategy::SoftwareTimer
        );
    }

    #[test]
    fn operation_payload_layout_baseline() {
        let layout = (
            size_of::<UringOp>(),
            align_of::<UringOp>(),
            size_of::<UringUserPayload>(),
            align_of::<UringUserPayload>(),
            size_of::<UringKernelPayloadStorage>(),
            align_of::<UringKernelPayloadStorage>(),
        );
        assert_eq!(layout, (224, 8, 192, 32, 216, 8));
    }

    #[test]
    fn operation_dispatch_allocation_baseline() {
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let sqe_env = test_sqe_env(&file_table, &registrar);
        let provided_sqe_env = test_provided_sqe_env(&file_table, &registrar);

        let read_counts = measure_sqe_and_completion(
            ReadRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            &sqe_env,
            4,
        );
        let write_counts = measure_sqe_and_completion(
            WriteRaw {
                fd: UringRawHandle::for_file(-1),
                buf: buffer(4),
                offset: 0,
                buf_offset: 0,
            },
            &sqe_env,
            4,
        );
        let provided_counts =
            measure_sqe_and_completion(RecvProvided { fd: socket_fd() }, &provided_sqe_env, 0);
        let multishot_counts =
            measure_sqe_and_completion(RecvMulti { fd: socket_fd() }, &provided_sqe_env, 0);
        let send_to_counts = measure_sqe_and_completion(
            SendTo {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
                addr: socket_addr(),
            },
            &sqe_env,
            4,
        );
        let udp_recv_from_counts = measure_udp_recv_from_dispatch(
            UdpRecvFrom {
                fd: socket_fd(),
                buf: buffer(4),
                buf_offset: 0,
                addr: None,
            },
            &sqe_env,
        );
        let timer_counts = measure_timer_dispatch(Timeout {
            duration: Duration::from_secs(1),
        });

        for (name, counts) in [
            ("ReadRaw", read_counts),
            ("WriteRaw", write_counts),
            ("RecvProvided", provided_counts),
            ("RecvMulti", multishot_counts),
            ("SendTo", send_to_counts),
            ("UdpRecvFrom", udp_recv_from_counts),
            ("Timeout", timer_counts),
        ] {
            assert_no_fast_path_allocations(name, counts);
        }
    }

    fn assert_internal_error<T>(result: UringResult<T>) {
        let Err(report) = result else {
            panic!("projection mismatch should return an internal error");
        };
        assert_eq!(*report.inner(), UringError::Internal);
    }

    fn assert_descriptor_mapping<S, K, U>(
        name: &str,
        operation: S,
        expected_kind: OpKind,
        expected_kernel_variant: K,
        expected_user_variant: U,
    ) where
        S: UringOperationDescriptor + IntoPlatformOp<UringSlotSpec>,
        K: Fn(&UringKernelPayloadStorage) -> bool,
        U: Fn(&UringUserPayload) -> bool,
    {
        let (op, payload) =
            <S as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(operation);
        let payload = <S as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload);
        let descriptor = <S as UringOperationDescriptor>::descriptor();

        assert_eq!(
            <S as UringOperationDescriptor>::PAYLOAD_KIND,
            expected_kind,
            "{name}: spec kind"
        );
        assert_eq!(
            <S as IntoPlatformOp<UringSlotSpec>>::PAYLOAD_KIND,
            expected_kind,
            "{name}: platform kind"
        );
        assert_eq!(descriptor.erased.name, name, "{name}: descriptor name");
        assert!(
            core::ptr::eq(op.descriptor(), &descriptor.erased),
            "{name}: descriptor erased view"
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
            op.descriptor().strategy,
            <S as UringOperationDescriptor>::descriptor()
                .erased
                .strategy,
            "{name}: submission strategy"
        );
    }

    macro_rules! assert_mapping {
        ($name:literal, $ty:ty, $operation:expr, $kind:expr, $kernel:ident, $user:ident) => {
            assert_descriptor_mapping::<$ty, _, _>(
                $name,
                $operation,
                $kind,
                |payload| <$ty as UringOperationDescriptor>::kernel_payload_ref(payload).is_some(),
                |payload| <$ty as UringOperationDescriptor>::user_payload_ref(payload).is_some(),
            );
        };
    }

    #[test]
    fn all_operation_descriptor_mappings_match_the_declaration() {
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
                UringUserPayload::from_storage(UringUserPayloadStorage::AcceptedSocket(
                    AcceptedSocket
                ))
            ),
            Ok(AcceptedSocket)
        ));
        assert!(matches!(
            <RecvProvided as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(
                UringUserPayload::from_storage(UringUserPayloadStorage::ProvidedBuf(ProvidedBuf {
                    buf: None
                }))
            ),
            Ok(ProvidedBuf { buf: None })
        ));
        assert!(matches!(
            <RecvMulti as IntoPlatformOp<UringSlotSpec>>::try_record_from_erased(
                UringUserPayload::from_storage(UringUserPayloadStorage::ProvidedBuf(ProvidedBuf {
                    buf: None
                }))
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
        op.with_descriptor_for_test(&<ReadRaw as UringOperationDescriptor>::descriptor().erased)
    }

    #[test]
    fn descriptor_dispatch_returns_explicit_internal_errors() {
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);

        // SQE construction and completion report the same internal mismatch class.
        let (read_op, _) = read_raw_parts();
        let result = with_test_slot_pair(read_op, write_raw_payload(), |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::make_sqe_dispatch(
                    access,
                    &env,
                    SubmitTokenContext::user(token),
                )
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let (valid_op, _) = read_raw_parts();
        let result = with_test_slot_pair(valid_op, write_raw_payload(), |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::on_complete_dispatch(access, token, 0)
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        // Cleanup is result-only and deliberately does not project either payload.
        let wrong_op = wrong_kernel_op();
        let _cleanup = (wrong_op.descriptor().completion_cleanup)(0);

        let wrong_op = wrong_kernel_op();
        let _cleanup = (wrong_op.descriptor().orphan_cleanup)(0);

        let wrong_op = wrong_kernel_op();
        let (_, valid_user) = read_raw_parts();
        let result = with_test_slot_pair(wrong_op, valid_user, |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::get_timeout_dispatch(access, token)
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let (valid_op, _) = read_raw_parts();
        let result = with_test_slot_pair(valid_op, write_raw_payload(), |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::get_timeout_dispatch(access, token)
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let mut chunks = [ChunkId::ZERO; 1];
        let wrong_op = wrong_kernel_op();
        let (_, valid_user) = read_raw_parts();
        let result = with_test_slot_pair(wrong_op, valid_user, |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::resolve_chunks_dispatch(
                    access,
                    token,
                    &mut chunks,
                )
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let (valid_op, _) = read_raw_parts();
        let result = with_test_slot_pair(valid_op, write_raw_payload(), |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::resolve_chunks_dispatch(
                    access,
                    token,
                    &mut chunks,
                )
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        let wrong_op = wrong_kernel_op();
        let (_, valid_user) = read_raw_parts();
        let result = with_test_slot_pair(wrong_op, valid_user, |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::record_item_dispatch(
                    access,
                    token,
                    0,
                    0,
                    &mut cqe_env,
                )
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);

        let (valid_op, _) = read_raw_parts();
        let result = with_test_slot_pair(valid_op, write_raw_payload(), |token, slot| {
            with_test_access(slot, |access| unsafe {
                <ReadRaw as UringOperationDescriptor>::record_item_dispatch(
                    access,
                    token,
                    0,
                    0,
                    &mut cqe_env,
                )
            })
        })
        .expect("test operation access should succeed");
        assert_internal_error(result);
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
        // Moving this operation through the descriptor layer is valid before make_sqe establishes
        // any self-reference.
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);
        with_test_slot(user, |token, slot| {
            let entry = with_test_access(slot, |access| {
                let descriptor = access.operation().get_ref().descriptor();
                unsafe { (descriptor.make_sqe)(access, &env, SubmitTokenContext::user(token)) }
            })
            .expect("test operation access should succeed")
            .expect("send_to SQE should be built with a direct socket descriptor");

            let (msg_name, iovec, msghdr) = with_test_access(slot, |access| unsafe {
                <SendTo as UringOperationDescriptor>::with_projected_access(
                    access,
                    Some(token),
                    "uring.op.spec.test.send_to_pointers",
                    |kernel, _| kernel.get_unchecked_mut().test_pointers(),
                )
            })
            .expect("test operation access should succeed")
            .expect("SendTo kernel payload projection should succeed");
            assert_eq!(unsafe { (*msghdr).msg_name }, msg_name);
            assert_eq!(unsafe { (*msghdr).msg_iov }, iovec);
            assert_eq!(sqe_addr(&entry), msghdr as usize);
        });
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
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let registrar = NoopRegistrar;
        let env = test_sqe_env(&file_table, &registrar);
        let expected_addr = socket_addr();
        let (storage, len) = storage_addr();
        with_test_slot(user, |token, slot| {
            let entry = with_test_access(slot, |access| {
                let descriptor = access.operation().get_ref().descriptor();
                unsafe { (descriptor.make_sqe)(access, &env, SubmitTokenContext::user(token)) }
            })
            .expect("test operation access should succeed")
            .expect("udp_recv_from SQE should be built with a direct socket descriptor");

            let (msg_name, iovec, msghdr) = with_test_access(slot, |access| unsafe {
                <UdpRecvFrom as UringOperationDescriptor>::with_projected_access(
                    access,
                    Some(token),
                    "uring.op.spec.test.udp_recv_from_pointers",
                    |kernel, _| kernel.get_unchecked_mut().test_pointers(),
                )
            })
            .expect("test operation access should succeed")
            .expect("UdpRecvFrom kernel payload projection should succeed");
            assert_eq!(unsafe { (*msghdr).msg_name }, msg_name);
            assert_eq!(unsafe { (*msghdr).msg_iov }, iovec);
            assert_eq!(sqe_addr(&entry), msghdr as usize);

            // Writing the storage field in place does not move the payload; it models the
            // kernel's address write before the existing completion callback reads it.
            with_test_access(slot, |access| unsafe {
                <UdpRecvFrom as UringOperationDescriptor>::with_projected_access(
                    access,
                    Some(token),
                    "uring.op.spec.test.udp_recv_from_address",
                    |kernel, _| {
                        kernel
                            .get_unchecked_mut()
                            .test_set_received_address(storage.0, len as usize);
                    },
                )
            })
            .expect("test operation access should succeed")
            .expect("UdpRecvFrom kernel payload projection should succeed");
            let result = with_test_access(slot, |access| unsafe {
                let descriptor = access.operation().get_ref().descriptor();
                (descriptor.on_complete)(access, token, 4)
            })
            .expect("test operation access should succeed");
            assert_eq!(result.expect("valid UDP completion"), 4);
            let actual_addr = with_test_access(slot, |access| {
                let (_, payload) = access
                    .operation_and_payload_mut()
                    .expect("test payload should remain bound");
                <UdpRecvFrom as UringOperationDescriptor>::user_payload_ref(payload)
                    .map(|user| user.addr)
            })
            .expect("test operation access should succeed")
            .expect("test payload should contain UdpRecvFrom");
            assert_eq!(actual_addr, Some(expected_addr));
        });
    }

    #[test]
    fn udp_recv_from_rejects_an_address_length_beyond_storage() {
        let user = UdpRecvFrom {
            fd: socket_fd(),
            buf: buffer(4),
            buf_offset: 0,
            addr: None,
        };
        let (storage, _) = storage_addr();
        with_test_slot(user, |token, slot| {
            with_test_access(slot, |access| unsafe {
                <UdpRecvFrom as UringOperationDescriptor>::with_projected_access(
                    access,
                    Some(token),
                    "uring.op.spec.test.udp_recv_from_oversized_address",
                    |kernel, _| {
                        kernel.get_unchecked_mut().test_set_received_address(
                            storage.0,
                            size_of::<libc::sockaddr_storage>() + 1,
                        );
                    },
                )
            })
            .expect("test operation access should succeed")
            .expect("UdpRecvFrom kernel payload projection should succeed");

            let result = with_test_access(slot, |access| unsafe {
                let descriptor = access.operation().get_ref().descriptor();
                (descriptor.on_complete)(access, token, 4)
            })
            .expect("test operation access should succeed");
            let Err(report) = result else {
                panic!("an oversized sockaddr length must be rejected");
            };
            assert_eq!(*report.inner(), UringError::InvalidState);
        });
    }
}
