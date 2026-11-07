mod callbacks;
#[macro_use]
mod descriptor_macros;
#[macro_use]
mod storage_macros;

use crate::{
    OwnedRawHandle,
    diagnostics::UringCompletionDiagnostics,
    driver::{CqeEnv, SqeEnv, UringOpState},
    error::{UringError, UringResult},
    op::{
        Accept, AcceptMulti, AcceptedSocket, Close, CompletionCardinality, Connect,
        ErasedOperationDescriptor, Fallocate, FallocateRaw, Fsync, FsyncRaw, OpSend, Open,
        OperationDescriptor, ProvidedBuf, ReadFixed, ReadRaw, RecordPolicy, Recv, RecvMulti,
        RecvProvided, SendTo, SubmissionStrategy, SyncFileRange, SyncFileRangeRaw, Timeout,
        UdpConnect, UdpRecvMulti, UdpRecvPacket, UdpSend, UringKernelOp, UringOp, UringRecordItem,
        UringUserPayload, Wakeup, WriteFixed, WriteRaw,
    },
};
use diagweave::prelude::*;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    IntoPlatformOp, LostReason, OpCompletion, OpError, OpKind, OpResult, OpToken,
    SingleShotOperation, SlotAccess, SlotAccessError, SlotSpec as CoreSlotSpec, SubmitTokenContext,
};
use veloq_io_uring::squeue;
use veloq_std::{convert::identity, format, pin::Pin, time::Duration};

use callbacks::{DescriptorCallbacks, RecordItemContext};

pub enum UringSlotSpec {}

impl CoreSlotSpec for UringSlotSpec {
    type Op = UringOp;
    type UserPayload = UringUserPayload;
    type PlatformData = UringOpState;
    type Sidecar = ();
    type Error = UringError;
    type Completion = usize;
    type CompletionDiagnostics = UringCompletionDiagnostics;
}

mod payload {
    pub(super) use crate::op::{
        AcceptPayload, KernelRef, OpenPayload, SendToPayload, TimeoutPayload, UdpRecvMultiPayload,
        WakeupPayload,
    };
}

mod submit {
    pub(super) use crate::op::{FileSubmission, NetSubmission, SubmissionSupport};
}

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
    /// Creates a record payload per completion. Most operations reuse their submission payload; multishot and provided-buffer operations need a distinct payload lifetime.
    fn record_item(
        _kernel: Pin<&mut Self::KernelPayload>,
        _payload: &mut Self,
        _context: RecordItemContext<'_, '_, '_>,
    ) -> UringResult<UringRecordItem> {
        Ok(UringRecordItem::use_submit_payload())
    }

    fn resume_item(
        _kernel: Pin<&mut Self::KernelPayload>,
        _payload: &mut Self,
        _token: OpToken,
        _logical_receiver_generation: u32,
        _env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem> {
        Err(UringError::InvalidState.report(
            "uring.op.spec.resume_item",
            "operation does not support backend resume ingress",
        ))
    }

    fn receive_generation(_payload: &Self) -> Option<u32> {
        None
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

    unsafe fn resume_item_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
        token: OpToken,
        logical_receiver_generation: u32,
        env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem>;

    unsafe fn receive_generation_dispatch(
        access: &mut SlotAccess<'_, UringSlotSpec>,
    ) -> Option<u32>;
}

struct ProjectionMismatch {
    scope: &'static str,
    operation: &'static str,
    token: Option<OpToken>,
    expected_kernel: Option<PayloadId>,
    actual_kernel: Option<PayloadId>,
    expected_user: Option<PayloadId>,
    actual_user: Option<PayloadId>,
}

fn projection_mismatch_report(mismatch: ProjectionMismatch) -> Report<UringError> {
    let ProjectionMismatch {
        scope,
        operation,
        token,
        expected_kernel,
        actual_kernel,
        expected_user,
        actual_user,
    } = mismatch;
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
        .with_ctx("slot_index", error.snapshot().index())
        .with_ctx("slot_generation", error.snapshot().generation())
        .with_ctx("slot_status", format!("{:?}", error.snapshot().status()))
        .with_ctx("slot_has_op", error.snapshot().has_op())
        .with_ctx("slot_has_payload", error.snapshot().has_payload())
        .with_ctx("slot_access_action", format!("{:?}", error.action()))
        .with_ctx("slot_access_reason", format!("{:?}", error.reason()));
    if let Some(token) = token {
        report = report
            .with_ctx("token_index", token.index())
            .with_ctx("token_generation", token.generation());
    }
    report
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_read_fixed,
        resolve_chunks: read_fixed,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_read_raw,
        resolve_chunks: read_raw,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_write_fixed,
        resolve_chunks: write_fixed,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_write_raw,
        resolve_chunks: write_raw,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_recv,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_recv_provided,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_recv_multi,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_send,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_udp_send,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_connect,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_udp_connect,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_close,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_fsync,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_fsync_raw,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_sync_range,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_sync_range_raw,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_fallocate,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::FileSubmission::make_sqe_fallocate_raw,
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
        new_kernel: DescriptorCallbacks::new_accept_kernel,
        make_sqe: submit::NetSubmission::make_sqe_accept,
        resolve_chunks: none,
        on_complete: accept,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: close_raw_fd,
        map: submit::NetSubmission::accepted_handle_from_res,
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
        new_kernel: payload::KernelRef::from_user,
        make_sqe: submit::NetSubmission::make_sqe_accept_multi,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: accepted_socket,
        cleanup: close_raw_fd,
        map: submit::NetSubmission::accepted_handle_from_res,
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
        new_kernel: DescriptorCallbacks::new_send_to_kernel,
        make_sqe: submit::NetSubmission::make_sqe_send_to,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
    UdpRecvMulti {
        user: UdpRecvMulti,
        kernel: UdpRecvMulti,
        kernel_type: payload::UdpRecvMultiPayload,
        kind: OpKind::UdpRecvMulti,
        completion: usize,
        record: UdpRecvPacket(UdpRecvPacket),
        strategy: SubmissionStrategy::SubmitSqe,
        cardinality: multi,
        new_kernel: DescriptorCallbacks::new_udp_recv_multi_kernel,
        make_sqe: submit::NetSubmission::make_sqe_udp_recv_multi,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: udp_packet,
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
        new_kernel: DescriptorCallbacks::new_open_kernel,
        make_sqe: submit::FileSubmission::make_sqe_open,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: default,
        record_factory: use_submit,
        cleanup: close_raw_fd,
        map: submit::FileSubmission::opened_handle_from_res,
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
        new_kernel: DescriptorCallbacks::new_wakeup_kernel,
        make_sqe: submit::SubmissionSupport::make_sqe_wakeup,
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
        new_kernel: DescriptorCallbacks::new_timeout_kernel,
        make_sqe: submit::SubmissionSupport::make_sqe_timeout,
        resolve_chunks: none,
        on_complete: default,
        get_timeout: timeout,
        record_factory: use_submit,
        cleanup: none,
        map: identity,
    };
}

#[cfg(test)]
mod tests {
    use super::{OPERATION_COVERAGE, UringKernelPayloadStorage, UringUserPayloadStorage};
    use crate::{
        config::{FileTableExhaustion, IoFd, SockAddrStorage, UringRawHandle},
        diagnostics::UringCompletionDiagnostics,
        driver::{CqeEnv, FileTable, SqeEnv, UringOpState},
        error::{UringError, UringResult},
        op::{
            Accept, AcceptMulti, AcceptedSocket, Close, CompletionCardinality, Connect, Fallocate,
            FallocateRaw, Fsync, FsyncRaw, OpSend, Open, ProvidedBuf, ReadFixed, ReadRaw,
            RecordPolicy, Recv, RecvMulti, RecvProvided, SendTo, SubmissionStrategy, SyncFileRange,
            SyncFileRangeRaw, Timeout, UdpConnect, UdpRecvMulti, UdpSend, UringOp, UringOpRegistry,
            UringOperationDescriptor, UringSlotSpec, UringUserPayload, Wakeup, WriteFixed,
            WriteRaw,
        },
        socket_addr_to_storage,
        test_alloc::AllocationCounts,
    };
    use core::ptr::{eq, read_unaligned};
    use veloq_buf::{FixedBuf, heap::ChunkId};
    use veloq_driver_core::{
        CheckedSlotView, IntoPlatformOp, OpEntry, OpKind, OpToken, ReceivePumpConfig,
        ReceivePumpState, ReceiveSlot, Reserved, Slot, SlotAccess, SlotAccessOutcome,
        SlotRegistryExt, SlotView, SubmitTokenContext,
    };
    use veloq_io_uring::squeue;
    use veloq_std::{
        mem::{align_of, size_of},
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        num::NonZeroUsize,
        time::Duration,
        vec,
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

    fn udp_pump() -> ReceivePumpState {
        let config = ReceivePumpConfig::new(
            NonZeroUsize::new(1).expect("depth is non-zero"),
            NonZeroUsize::new(1).expect("queue is non-zero"),
            NonZeroUsize::new(16).expect("datagram capacity is non-zero"),
            Duration::from_secs(1),
        );
        let slots = vec![ReceiveSlot::new(0, buffer(4))].into_boxed_slice();
        ReceivePumpState::try_new(config, slots).expect("test UDP pump should be valid")
    }

    fn storage_addr() -> (SockAddrStorage, u32) {
        socket_addr_to_storage(socket_addr())
    }

    fn test_sqe_env<'a>(file_table: &'a FileTable) -> SqeEnv<'a> {
        SqeEnv::for_test(file_table)
    }

    fn test_provided_sqe_env<'a>(file_table: &'a FileTable) -> SqeEnv<'a> {
        SqeEnv::for_test_with_provided(file_table, 7, 16)
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
            counts.deallocations(),
            0,
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
            let (_, counts) = AllocationCounts::measure(|| {
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

    fn measure_timer_dispatch(operation: Timeout) -> AllocationCounts {
        let diagnostics = UringCompletionDiagnostics::default();
        let mut cqe_env = CqeEnv::new(None, &diagnostics);
        with_test_slot(operation, |token, slot| {
            let (_, counts) = AllocationCounts::measure(|| {
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
    fn operation_descriptor_coverage_has_all_25_rows() {
        const EXPECTED_NAMES: [&str; 25] = [
            "ReadFixed",
            "ReadRaw",
            "WriteFixed",
            "WriteRaw",
            "Recv",
            "RecvProvided",
            "RecvMulti",
            "OpSend",
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
            "UdpRecvMulti",
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
        assert_eq!(OPERATION_COVERAGE[19].record_payload, "AcceptedSocket");
        assert_eq!(
            OPERATION_COVERAGE[19].record_policy,
            RecordPolicy::NewAcceptedSocket
        );
        assert_eq!(
            OPERATION_COVERAGE[19].cardinality,
            CompletionCardinality::Multi
        );
        assert_eq!(OPERATION_COVERAGE[21].record_payload, "UdpRecvPacket");
        assert_eq!(
            OPERATION_COVERAGE[21].record_policy,
            RecordPolicy::UdpMultishot
        );
        assert_eq!(
            OPERATION_COVERAGE[21].cardinality,
            CompletionCardinality::Multi
        );
        assert_eq!(
            OPERATION_COVERAGE[24].strategy,
            SubmissionStrategy::SoftwareTimer
        );
    }

    #[derive(Debug, PartialEq, Eq)]
    struct PayloadLayout {
        kernel_op_size: usize,
        kernel_op_alignment: usize,
        user_payload_size: usize,
        user_payload_alignment: usize,
        kernel_storage_size: usize,
        kernel_storage_alignment: usize,
    }

    #[test]
    fn operation_payload_layout_baseline() {
        let layout = PayloadLayout {
            kernel_op_size: size_of::<UringOp>(),
            kernel_op_alignment: align_of::<UringOp>(),
            user_payload_size: size_of::<UringUserPayload>(),
            user_payload_alignment: align_of::<UringUserPayload>(),
            kernel_storage_size: size_of::<UringKernelPayloadStorage>(),
            kernel_storage_alignment: align_of::<UringKernelPayloadStorage>(),
        };
        assert_eq!(
            layout,
            PayloadLayout {
                kernel_op_size: 224,
                kernel_op_alignment: 8,
                user_payload_size: 384,
                user_payload_alignment: 32,
                kernel_storage_size: 216,
                kernel_storage_alignment: 8,
            }
        );
    }

    #[test]
    fn operation_dispatch_allocation_baseline() {
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let sqe_env = test_sqe_env(&file_table);
        let provided_sqe_env = test_provided_sqe_env(&file_table);

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
        let timer_counts = measure_timer_dispatch(Timeout {
            duration: Duration::from_secs(1),
        });

        for (name, counts) in [
            ("ReadRaw", read_counts),
            ("WriteRaw", write_counts),
            ("RecvProvided", provided_counts),
            ("RecvMulti", multishot_counts),
            ("SendTo", send_to_counts),
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
            eq(op.descriptor(), &descriptor.erased),
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
            "UdpRecvMulti",
            UdpRecvMulti,
            UdpRecvMulti::from_backend(socket_fd(), udp_pump()),
            OpKind::UdpRecvMulti,
            UdpRecvMulti,
            UdpRecvMulti
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
        let env = test_sqe_env(&file_table);

        // SQE construction and completion report the same internal mismatch class. SQE 构造与完成路径报告相同的内部不匹配类别。
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

        // Cleanup is result-only and deliberately does not project either payload. cleanup 只依赖结果值，不投影任一 payload。
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
    /// 读取 `io_uring_sqe.addr`（C ABI SQE 中第二个 64 位 union 字段）；偏移和大小断言可发现依赖布局变化。
    fn sqe_addr(entry: &squeue::Entry) -> usize {
        const ADDR_OFFSET: usize = 16;
        assert_eq!(size_of::<squeue::Entry>(), 64);
        // SAFETY: `Entry` is `#[repr(C)]` around the dependency's 64-byte `io_uring_sqe`, and
        // the binding places its `addr` union at byte offset 16. `read_unaligned` permits the
        // byte-level inspection without imposing an alignment assumption on the test mirror.
        // 安全性：`Entry` 以 `repr(C)` 包装 64 字节 SQE，绑定将 `addr` 放在偏移 16；`read_unaligned` 不假设测试镜像对齐。
        unsafe {
            read_unaligned(
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
        // 在 `make_sqe` 建立自引用之前，通过 descriptor 层移动该 operation 是安全的。
        let file_table = FileTable::new(0, FileTableExhaustion::Fallback);
        let env = test_sqe_env(&file_table);
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
}
