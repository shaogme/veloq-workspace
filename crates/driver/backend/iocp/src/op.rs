//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, SubmitPayload)`

mod file;
mod net;
mod payload;
mod spec;
mod state;
mod submit;

pub use payload::IocpUserPayload;
pub(crate) use payload::{
    ACCEPT_EX_ADDR_SECTION_LEN, ACCEPT_EX_OUTPUT_BUFFER_LEN, AcceptPayload, IocpOpPayload,
    KernelRef, OpenPayload, PayloadRef, SendToPayload, UdpRecvFromPayload, kernel_ref,
};
use spec::{IocpOpErasure, IocpOpSpec};
pub(crate) use state::{BlockingCompletion, BlockingSuccessCleanup, IocpOpRegistry, Slot};
pub use state::{IocpOpState, IocpSlotSpec, OverlappedEntry};
pub(crate) use submit::{SubmissionResult, locate_registered_slot, resolve_fd_handle};

use veloq_std::sync::Arc;

use diagweave::{prelude::*, report::Report};

use crate::{
    config::{IoFd, IocpHandle, OwnedRawHandle, RegisteredSlot},
    error::{IocpError, IocpResult},
    ext::Extensions,
    net::addr::SockAddrStorage,
    rio::RioState,
};

use veloq_driver_core::{
    driver::{CompletionCleanupGuard, CompletionToken, OpToken, PlatformOp},
    op::{
        IntoPlatformOp, LostReason, OpCompletion, OpError, OpResult, SingleShotOp,
        payload_projection_mismatch_report,
        types::{
            Accept as AcceptBase, AcceptMulti as AcceptMultiBase, AcceptedSocket,
            Close as CloseBase, Connect as ConnectBase, Fallocate as FallocateBase,
            FallocateRaw as FallocateRawBase, Fsync as FsyncBase, FsyncRaw as FsyncRawBase, OpKind,
            Open as OpenBase, ProvidedBuf, ReadFixed as ReadFixedBase, ReadRaw as ReadRawBase,
            Recv as RecvBase, RecvMulti as RecvMultiBase, RecvProvided as RecvProvidedBase,
            Send as OpSendBase, SendTo as SendToBase, SyncFileRange as SyncFileRangeBase,
            SyncFileRangeRaw as SyncFileRangeRawBase, Timeout as TimeoutBase,
            UdpConnect as UdpConnectBase, UdpRecv as UdpRecvBase, UdpRecvFrom as UdpRecvFromBase,
            UdpSend as UdpSendBase, Wakeup as WakeupBase, WriteFixed as WriteFixedBase,
            WriteRaw as WriteRawBase,
        },
    },
    slot::Generation,
};

// ============================================================================
// Type Aliases for Core Ops
// ============================================================================

pub(crate) type ReadFixed = ReadFixedBase<IocpHandle>;
pub(crate) type ReadRaw = ReadRawBase<IocpHandle>;
pub(crate) type WriteFixed = WriteFixedBase<IocpHandle>;
pub(crate) type WriteRaw = WriteRawBase<IocpHandle>;
pub(crate) type Recv = RecvBase<IocpHandle>;
pub(crate) type OpSend = OpSendBase<IocpHandle>;
pub(crate) type UdpRecv = UdpRecvBase<IocpHandle>;
pub(crate) type UdpSend = UdpSendBase<IocpHandle>;
pub(crate) type Close = CloseBase<IocpHandle>;
pub(crate) type Fsync = FsyncBase<IocpHandle>;
pub(crate) type FsyncRaw = FsyncRawBase<IocpHandle>;
pub(crate) type Connect = ConnectBase<IocpHandle, SockAddrStorage>;
pub(crate) type UdpConnect = UdpConnectBase<IocpHandle, SockAddrStorage>;
pub(crate) type Accept = AcceptBase<IocpHandle, SockAddrStorage>;
pub(crate) type SendTo = SendToBase<IocpHandle>;
pub(crate) type SyncFileRange = SyncFileRangeBase<IocpHandle>;
pub(crate) type SyncFileRangeRaw = SyncFileRangeRawBase<IocpHandle>;
pub(crate) type Fallocate = FallocateBase<IocpHandle>;
pub(crate) type FallocateRaw = FallocateRawBase<IocpHandle>;
pub(crate) type UdpRecvFrom = UdpRecvFromBase<IocpHandle>;
pub(crate) type Open = OpenBase;
pub(crate) type Timeout = TimeoutBase;
pub(crate) type Wakeup = WakeupBase<IocpHandle>;

// 下面三个 IOCP 提交不了，见 [`impl_iocp_unsupported_op!`]。
pub(crate) type AcceptMulti = AcceptMultiBase<IocpHandle>;
pub(crate) type RecvProvided = RecvProvidedBase<IocpHandle>;
pub(crate) type RecvMulti = RecvMultiBase<IocpHandle>;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: Arc<crate::win32::IoCompletionPort>,
    pub(crate) overlapped: *mut crate::win32::Overlapped,
    pub(crate) op_token: OpToken,
    pub(crate) completion_token: CompletionToken,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_slots: &'a mut [RegisteredSlot],
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,

    // RIO Support
    pub(crate) rio: &'a mut RioState,
}

// ============================================================================
// Type-Erased VTable
// ============================================================================

pub(crate) struct OpVTable {
    pub(crate) submit: fn(&mut IocpKernelOp, &mut SubmitContext) -> IocpResult<SubmissionResult>,
    pub(crate) on_complete:
        unsafe fn(&mut IocpKernelOp, result: usize, ext: &Extensions) -> IocpResult<usize>,
    pub(crate) completion_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) orphan_cleanup:
        unsafe fn(&mut IocpKernelOp, result: &IocpResult<usize>) -> CompletionCleanupGuard,
    pub(crate) get_fd: unsafe fn(&IocpKernelOp) -> Option<IoFd>,
    pub(crate) bind_user_payload: fn(&mut IocpKernelOp, &mut IocpUserPayload) -> IocpResult<()>,
    pub(crate) unbind_user_payload: fn(&mut IocpKernelOp),
}

pub struct IocpKernelOp {
    pub(crate) vtable: &'static OpVTable,
    pub(crate) header: OverlappedEntry,
    pub(crate) payload: IocpOpPayload,
}

impl PlatformOp for IocpKernelOp {
    type CleanupContext<'a> = &'a IocpResult<usize>;

    #[inline]
    fn completion_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.completion_cleanup)(self, result) }
    }

    #[inline]
    fn orphan_cleanup(&mut self, result: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        unsafe { (self.vtable.orphan_cleanup)(self, result) }
    }
}

impl IocpKernelOp {
    pub(crate) fn bind_user_payload(&mut self, erased: &mut IocpUserPayload) -> IocpResult<()> {
        (self.vtable.bind_user_payload)(self, erased)
    }

    pub(crate) fn unbind_user_payload(&mut self) {
        (self.vtable.unbind_user_payload)(self);
    }

    pub(crate) fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.get_fd)(self) }
    }

    pub(crate) fn submit(&mut self, ctx: &mut SubmitContext) -> IocpResult<SubmissionResult> {
        (self.vtable.submit)(self, ctx)
    }

    pub(crate) fn on_complete(&mut self, result: usize, ext: &Extensions) -> IocpResult<usize> {
        unsafe { (self.vtable.on_complete)(self, result, ext) }
    }

    pub(crate) fn completion_cleanup(
        &mut self,
        result: &IocpResult<usize>,
    ) -> CompletionCleanupGuard {
        PlatformOp::completion_cleanup(self, result)
    }

    pub(crate) fn orphan_cleanup(&mut self, result: &IocpResult<usize>) -> CompletionCleanupGuard {
        PlatformOp::orphan_cleanup(self, result)
    }
}

macro_rules! impl_iocp_op_erasure {
    ($OpType:ty, $user_variant:ident, $kernel_variant:ident, $completion:ty) => {
        impl IocpOpErasure for $OpType {
            fn erase_kernel_payload(payload: Self::KernelPayload) -> IocpOpPayload {
                IocpOpPayload::$kernel_variant(payload)
            }

            fn kernel_payload_ref(payload: &IocpOpPayload) -> Option<&Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn kernel_payload_mut(payload: &mut IocpOpPayload) -> Option<&mut Self::KernelPayload> {
                match payload {
                    IocpOpPayload::$kernel_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn erase_user_payload(payload: Self) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            fn try_user_payload(payload: IocpUserPayload) -> IocpResult<Self> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Ok(payload),
                    _ => Err(veloq_driver_core::op::payload_projection_mismatch_report::<
                        IocpError,
                    >(stringify!($OpType), "IocpUserPayload")),
                }
            }

            fn user_payload_mut(payload: &mut IocpUserPayload) -> Option<&mut Self> {
                match payload {
                    IocpUserPayload::$user_variant(payload) => Some(payload),
                    _ => None,
                }
            }

            fn vtable() -> &'static OpVTable {
                static TABLE: OpVTable = OpVTable {
                    submit: spec::submit_shim::<$OpType>,
                    on_complete: spec::on_complete_shim::<$OpType>,
                    completion_cleanup: spec::completion_cleanup_shim::<$OpType>,
                    orphan_cleanup: spec::orphan_cleanup_shim::<$OpType>,
                    get_fd: spec::get_fd_shim::<$OpType>,
                    bind_user_payload: spec::bind_user_payload_shim::<$OpType>,
                    unbind_user_payload: spec::unbind_user_payload_shim::<$OpType>,
                };
                &TABLE
            }
        }

        /// IOCP 后端一个 multishot 操作都没有（`capabilities()` 恒为全 `false`），所以
        /// 这里每个 op 的提交 payload 与记录 payload 都是它自己。
        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $OpType;
            type Output = $OpType;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = <$OpType as IocpOpSpec>::PAYLOAD_KIND;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let kernel_payload = <$OpType as IocpOpSpec>::new_kernel_payload(&self);
                let op = IocpKernelOp {
                    vtable: <$OpType as IocpOpErasure>::vtable(),
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: <$OpType as IocpOpErasure>::erase_kernel_payload(kernel_payload),
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                <$OpType as IocpOpErasure>::erase_user_payload(payload)
            }

            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                <$OpType as IocpOpErasure>::try_user_payload(payload)
            }

            fn complete(
                payload: Self::RecordPayload,
                res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                let completion = <$OpType as IocpOpSpec>::map_completion(&payload, res);
                OpCompletion::new(completion, payload)
            }
        }

        impl SingleShotOp<IocpSlotSpec> for $OpType {}
    };
}

/// 「这个操作 IOCP 做不了」。
///
/// 带上 `ERROR_NOT_SUPPORTED` 的 errno，好让上层想解读的时候解读得了——虽然门面层判断能力
/// 靠的是 [`DriverCapabilities`](veloq_driver_core::driver::DriverCapabilities)，不是 errno。
fn unsupported_op_report(op_type: &'static str) -> Report<IocpError> {
    IocpError::Unsupported
        .to_report()
        .push_ctx("scope", "iocp/op/unsupported")
        .with_ctx("op_type", op_type)
        .attach_note("this operation has no IOCP equivalent")
}

/// 三个不支持的操作共用的 vtable。
///
/// `submit` 同步失败，于是 driver 侧回 [`SubmitStatus::Void`]，core 侧走
/// [`IntoPlatformOp::submit_failed`]——一条完成都不会产生，其余钩子因此都到不了。它们仍然
/// 是安全的空实现而不是 `unreachable!`：vtable 是运行期分发，用崩溃去表达一个不变式不值得。
///
/// [`SubmitStatus::Void`]: veloq_driver_core::driver::SubmitStatus
static UNSUPPORTED_VTABLE: OpVTable = OpVTable {
    submit: unsupported_submit,
    on_complete: unsupported_on_complete,
    completion_cleanup: unsupported_cleanup,
    orphan_cleanup: unsupported_cleanup,
    get_fd: unsupported_get_fd,
    bind_user_payload: unsupported_bind,
    unbind_user_payload: unsupported_unbind,
};

fn unsupported_submit(
    _op: &mut IocpKernelOp,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    Err(unsupported_op_report("unsupported"))
}

unsafe fn unsupported_on_complete(
    _op: &mut IocpKernelOp,
    _result: usize,
    _ext: &Extensions,
) -> IocpResult<usize> {
    Err(unsupported_op_report("unsupported"))
}

unsafe fn unsupported_cleanup(
    _op: &mut IocpKernelOp,
    _result: &IocpResult<usize>,
) -> CompletionCleanupGuard {
    CompletionCleanupGuard::default()
}

unsafe fn unsupported_get_fd(_op: &IocpKernelOp) -> Option<IoFd> {
    None
}

/// 没有内核 payload 可以绑，也不需要绑：`submit` 之前唯一会碰它的就是这里。
fn unsupported_bind(_op: &mut IocpKernelOp, _erased: &mut IocpUserPayload) -> IocpResult<()> {
    Ok(())
}

fn unsupported_unbind(_op: &mut IocpKernelOp) {}

/// IOCP 没有的操作的 [`IntoPlatformOp`]：类型在，提交不了。
///
/// 存在的理由是**分层**，不是补齐功能。这三个操作的可用性是运行期事实（
/// `capabilities().accept_multi` / `.recv_multi` / `.provided_buffers` 在 IOCP 上恒为
/// `false`），门面层照着能力位选路径就够了。如果这里不给出实现，那个运行期事实就会变成
/// 一个编译期事实——`S::Stream<AcceptMulti>` 的 bound 在 Windows 上不成立，于是
/// `AcceptStream` / `RecvStream` 里每一个碰到它的地方都得写 `#[cfg]`，平台差异从 driver
/// 层一路渗到用户 API 的实现里。
///
/// 代价是 Windows 上会编译出一段永远走不到的 `Native` 分支。那段代码在 Linux 上是真跑
/// 的，不存在「只在一个平台上腐烂」的风险，而它换掉的是门面层四十处 `#[cfg]`。
///
/// 每个操作走完的路是：提交 payload 擦除进 slot → [`unsupported_submit`] 同步失败 →
/// `SubmitStatus::Void` → [`IntoPlatformOp::submit_failed`] 把 payload 丢掉并交出
/// `ResourceLost`。`try_record_from_erased` / `complete` 因此都到不了。
macro_rules! impl_iocp_unsupported_op {
    ($OpType:ty, $user_variant:ident, $record:ty, $completion:ty, $kind:ident) => {
        impl IntoPlatformOp<IocpSlotSpec> for $OpType {
            type SubmitPayload = $OpType;
            type RecordPayload = $record;
            type Output = $record;
            type Completion = $completion;

            const PAYLOAD_KIND: OpKind = OpKind::$kind;

            fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::SubmitPayload) {
                let op = IocpKernelOp {
                    vtable: &UNSUPPORTED_VTABLE,
                    header: OverlappedEntry::new(
                        OpToken::from_registry_parts(0, Generation::ZERO)
                            .expect("zero token should be encodable"),
                    ),
                    payload: IocpOpPayload::Unsupported,
                };
                (op, self)
            }

            fn payload_into_erased(payload: Self::SubmitPayload) -> IocpUserPayload {
                IocpUserPayload::$user_variant(payload)
            }

            /// 记录 payload 在 IOCP 上不存在：这个操作一条完成都产生不了，所以也就没有
            /// 「每条完成的产物」可以从 slot 里投影出来。
            fn try_record_from_erased(payload: IocpUserPayload) -> IocpResult<Self::RecordPayload> {
                drop(payload);
                Err(payload_projection_mismatch_report::<IocpError>(
                    stringify!($record),
                    "IocpUserPayload",
                ))
            }

            fn complete(
                payload: Self::RecordPayload,
                _res: IocpResult<usize>,
            ) -> OpCompletion<Self::Output, IocpError, Self::Completion> {
                OpCompletion::new(Err(unsupported_op_report(stringify!($OpType))), payload)
            }

            /// slot 里躺着的是**提交** payload，不是任何一条完成的产物——没有 item 可以交
            /// 给用户，也没有用户交出来的资源要还。与 uring 侧
            /// `impl_uring_record_payload_op!` 同理：默认实现会把它当记录 payload 去投影，
            /// 那必然失败并给出一个含义完全错误的 `PayloadTypeMismatch`。
            fn submit_failed(
                erased: IocpUserPayload,
                report: Report<IocpError>,
            ) -> OpResult<Self::Output, IocpError, Self::Completion> {
                drop(erased);
                OpResult::ResourceLost(OpError::new(LostReason::Other, report))
            }
        }
    };
}

/// Alias for the platform-specific IOCP kernel operation.
pub type IocpOp = IocpKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

impl IocpOpSpec for Timeout {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Timeout;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_timeout(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl IocpOpSpec for Wakeup {
    type KernelPayload = KernelRef<Self>;
    type Completion = usize;

    const PAYLOAD_KIND: OpKind = OpKind::Wakeup;

    fn new_kernel_payload(user: &Self) -> Self::KernelPayload {
        kernel_ref(user)
    }

    fn submit(
        header: &mut OverlappedEntry,
        payload: &mut Self::KernelPayload,
        ctx: &mut SubmitContext,
    ) -> IocpResult<SubmissionResult> {
        submit::submit_wakeup(header, payload, ctx)
    }

    fn map_completion(_payload: &Self, res: IocpResult<usize>) -> IocpResult<Self::Completion> {
        res
    }
}

impl_iocp_op_erasure!(ReadFixed, ReadFixed, Read, usize);
impl_iocp_op_erasure!(ReadRaw, ReadRaw, ReadRaw, usize);
impl_iocp_op_erasure!(WriteFixed, WriteFixed, Write, usize);
impl_iocp_op_erasure!(WriteRaw, WriteRaw, WriteRaw, usize);
impl_iocp_op_erasure!(Recv, Recv, Recv, usize);
impl_iocp_op_erasure!(OpSend, OpSend, Send, usize);
impl_iocp_op_erasure!(UdpRecv, UdpRecv, UdpRecv, usize);
impl_iocp_op_erasure!(UdpSend, UdpSend, UdpSend, usize);
impl_iocp_op_erasure!(Close, Close, Close, usize);
impl_iocp_op_erasure!(Fsync, Fsync, Fsync, usize);
impl_iocp_op_erasure!(FsyncRaw, FsyncRaw, FsyncRaw, usize);
impl_iocp_op_erasure!(SyncFileRange, SyncFileRange, SyncRange, usize);
impl_iocp_op_erasure!(SyncFileRangeRaw, SyncFileRangeRaw, SyncRangeRaw, usize);
impl_iocp_op_erasure!(Fallocate, Fallocate, Fallocate, usize);
impl_iocp_op_erasure!(FallocateRaw, FallocateRaw, FallocateRaw, usize);
impl_iocp_op_erasure!(Timeout, Timeout, Timeout, usize);
impl_iocp_op_erasure!(Connect, Connect, Connect, usize);
impl_iocp_op_erasure!(UdpConnect, UdpConnect, UdpConnect, usize);
impl_iocp_op_erasure!(Accept, Accept, Accept, OwnedRawHandle);
impl_iocp_op_erasure!(SendTo, SendTo, SendTo, usize);
impl_iocp_op_erasure!(UdpRecvFrom, UdpRecvFrom, UdpRecvFrom, usize);
impl_iocp_op_erasure!(Open, Open, Open, OwnedRawHandle);
impl_iocp_op_erasure!(Wakeup, Wakeup, Wakeup, usize);

// multishot accept 要 io_uring 5.19，IOCP 一个对应物都没有。`AcceptStream` 在这里恒走
// `Emulated`（每取一条重新提交一次单发 `Accept`），所以这个 op 永远不会被提交。
//
// 与 uring 侧一样不实现 `SingleShotOp`：`await` 一个 multishot 操作等于「取第一条完成然后
// 取消」，那在哪个平台上都是陷阱。
impl_iocp_unsupported_op!(
    AcceptMulti,
    AcceptMulti,
    AcceptedSocket,
    OwnedRawHandle,
    AcceptMulti
);

// provided buffer 环是 `IORING_REGISTER_PBUF_RING`（5.19），IOCP 没有等价物：Winsock 收
// 数据时 buffer 必须由调用方在提交时交出来，而这个操作的全部意义就是不交。
//
// 它**是** `SingleShotOp`——一次提交一条完成，只是那一条在 IOCP 上永远不会来。
impl_iocp_unsupported_op!(RecvProvided, RecvProvided, ProvidedBuf, usize, RecvProvided);

impl SingleShotOp<IocpSlotSpec> for RecvProvided {}

// 上面两个的交集：既要 multishot 又要 provided buffer。
impl_iocp_unsupported_op!(RecvMulti, RecvMulti, ProvidedBuf, usize, RecvMulti);
