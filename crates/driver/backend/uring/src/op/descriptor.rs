use crate::{
    driver::{CqeEnv, SqeEnv},
    error::UringResult,
};
use io_uring::squeue;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::{
    driver::{CompletionCleanupGuard, OpToken, SubmitTokenContext},
    slot::PinnedSlotParts,
};
use veloq_std::time::Duration;

use super::{
    UringKernelPayloadStorage, UringSlotSpec, UringUserPayload,
    spec::{UringOpSpec, UringOperationDescriptor},
};

pub(crate) enum UringRecordItem {
    UseSubmitPayload,
    New(UringUserPayload),
}

/// 描述一次完成是单发还是会继续产生完成。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CompletionCardinality {
    Single,
    Multi,
}

/// 描述一条完成记录如何取得自己的 payload。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RecordPolicy {
    UseSubmitPayload,
    NewProvidedBuffer,
    NewAcceptedSocket,
}

pub(crate) type MakeSqeFn = unsafe fn(
    parts: &mut PinnedSlotParts<'_, UringSlotSpec>,
    env: &SqeEnv<'_>,
    token: SubmitTokenContext,
) -> UringResult<squeue::Entry>;
pub(crate) type OnCompleteFn = unsafe fn(
    parts: &mut PinnedSlotParts<'_, UringSlotSpec>,
    token: OpToken,
    result: i32,
) -> UringResult<usize>;
pub(crate) type CompletionCleanupFn = fn(result: i32) -> CompletionCleanupGuard;
pub(crate) type CompletionCleanupHintFn = fn(result: i32) -> CompletionCleanupGuard;
pub(crate) type GetTimeoutFn = unsafe fn(
    parts: &mut PinnedSlotParts<'_, UringSlotSpec>,
    token: OpToken,
) -> UringResult<Option<Duration>>;
pub(crate) type ResolveChunksFn = unsafe fn(
    parts: &mut PinnedSlotParts<'_, UringSlotSpec>,
    token: OpToken,
    chunks: &mut [ChunkId],
) -> UringResult<usize>;
pub(crate) type RecordItemFn = unsafe fn(
    parts: &mut PinnedSlotParts<'_, UringSlotSpec>,
    token: OpToken,
    result: i32,
    flags: u32,
    env: &mut CqeEnv<'_>,
) -> UringResult<UringRecordItem>;

/// slot runtime 使用的类型擦除视图。
///
/// 该结构嵌在 [`OperationDescriptor`] 中。`UringKernelOp` 只保存这个静态视图的指针，
/// 所有运行时 dispatch 都从同一个 descriptor 入口读取。
pub(crate) struct ErasedOperationDescriptor {
    pub(crate) name: &'static str,
    pub(crate) strategy: super::SubmissionStrategy,
    pub(crate) cardinality: CompletionCardinality,
    pub(crate) record_policy: RecordPolicy,
    pub(crate) make_sqe: MakeSqeFn,
    pub(crate) on_complete: OnCompleteFn,
    pub(crate) completion_cleanup: CompletionCleanupFn,
    pub(crate) completion_cleanup_hint: Option<CompletionCleanupHintFn>,
    pub(crate) orphan_cleanup: CompletionCleanupFn,
    pub(crate) get_timeout: GetTimeoutFn,
    pub(crate) resolve_chunks: ResolveChunksFn,
    pub(crate) record_item: RecordItemFn,
}

pub(crate) type NewKernelFn<S> = fn(&S) -> <S as UringOpSpec>::KernelPayload;
/// 一个 operation 的完整静态描述。
///
/// 所有字段均由 `declare_uring_operations!` 的单一 row 生成。typed 字段保留 Rust 关联
/// 类型，`erased` 字段则是 slot runtime 所需的函数指针投影；两者共享同一个静态实例，
/// 不需要运行时 registry、字符串查找或额外分配。
pub(crate) struct OperationDescriptor<S>
where
    S: UringOperationDescriptor,
{
    pub(crate) new_kernel: NewKernelFn<S>,
    pub(crate) encode_kernel: fn(<S as UringOpSpec>::KernelPayload) -> UringKernelPayloadStorage,
    pub(crate) encode_submit: fn(S) -> UringUserPayload,
    pub(crate) try_record: fn(UringUserPayload) -> UringResult<S::RecordPayload>,
    pub(crate) map_completion: fn(UringResult<usize>) -> UringResult<S::Completion>,
    pub(crate) erased: ErasedOperationDescriptor,
}
