use crate::{
    BorrowedRawHandle, DriverReport, DriverResult, IoFd, OwnedRawHandle, RawHandleMeta,
    slot::{self, SlotError, SlotOp, SlotPayload, SlotSpec as CoreSlotSpec},
};
use veloq_buf::{AnyBufPool, heap::ChunkId};
use veloq_std::{
    error::Error,
    marker::PhantomData,
    sync::{Arc, mpsc},
    task::{Poll, Waker},
    time::Duration,
    vec::Vec,
};

mod completion;
pub mod registry;

pub use completion::*;

pub trait PlatformOp {
    type CleanupContext<'a>
    where
        Self: 'a;

    fn completion_cleanup(&mut self, _context: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }

    fn orphan_cleanup(&mut self, context: Self::CleanupContext<'_>) -> CompletionCleanupGuard {
        self.completion_cleanup(context)
    }
}

pub enum RegisterFd<'a, H: RawHandleMeta> {
    Borrowed(BorrowedRawHandle<'a, H>),
    Owned(OwnedRawHandle<H>),
}

pub type SharedSlotTable<Spec> = Arc<slot::SlotTable<Spec>>;
pub type SharedDriverSlotTable<D> = SharedSlotTable<<D as DriverRaw>::SlotSpec>;
pub type RemoteCancelSender = mpsc::Sender<CancelRequest>;

#[must_use]
pub enum DriverSubmitResult<E> {
    Submitted(Poll<()>),
    Failed {
        report: DriverReport<E>,
        status: SubmitStatus,
    },
}

impl<E> DriverSubmitResult<E> {
    pub fn submitted(poll: Poll<()>) -> Self {
        Self::Submitted(poll)
    }

    pub fn failed(report: DriverReport<E>, status: SubmitStatus) -> Self {
        Self::Failed { report, status }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmittedOpSlot {
    token: OpToken,
}

impl SubmittedOpSlot {
    pub fn token(self) -> OpToken {
        self.token
    }

    pub fn completion_token(self) -> CompletionToken {
        CompletionToken::user(self.token)
    }
}

pub struct ReservedOpSlot<'a, D: Driver + ?Sized> {
    driver: &'a mut D,
    token: OpToken,
    release_on_drop: bool,
}

impl<'a, D: Driver + ?Sized> ReservedOpSlot<'a, D> {
    fn new(driver: &'a mut D, token: OpToken) -> Self {
        Self {
            driver,
            token,
            release_on_drop: true,
        }
    }

    pub fn token(&self) -> OpToken {
        self.token
    }

    pub fn completion_token(&self) -> CompletionToken {
        CompletionToken::user(self.token)
    }

    pub fn completion_table(&self) -> SharedCompletionTable<<D as DriverRaw>::SlotSpec> {
        self.driver.completion_table()
    }

    pub fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.driver.remote_cancel_sender()
    }

    pub fn create_waker(&self) -> Arc<dyn RemoteWaker<SlotError<<D as DriverRaw>::SlotSpec>>> {
        self.driver.create_waker()
    }

    /// 将用户 payload 的所有权交给预留 slot。
    ///
    /// 必须在 [`Self::submit`] 前调用一次。提交成功或进入途中的失败都由 slot 持有
    /// payload，直到完成记录被消费；同步失败则由 [`Self::recover_payload`] 取回。
    pub fn set_payload(&mut self, payload: SlotPayload<<D as DriverRaw>::SlotSpec>) {
        self.driver.slot_set_payload_raw(self.token, payload);
    }

    /// 提交预留 slot 中的操作。
    ///
    /// `DriverRaw::submit_op_raw` 返回 [`DriverSubmitResult::Submitted`] 或带有
    /// [`SubmitStatus::InFlight`] 的失败时，slot 已经进入在途生命周期，本包装会自动关闭
    /// drop 回收路径。调用者仍可调用 [`Self::persist`] 取出提交 token，但不再需要依靠它
    /// 才能避免错误地释放在途 slot。
    ///
    /// 返回带有 [`SubmitStatus::Void`] 的失败时，不会产生完成记录，slot 仍由本包装持有；
    /// 调用者应丢弃 `op_in` 中被回填的操作，并调用 [`Self::recover_payload`] 取回 payload。
    pub fn submit(
        &mut self,
        op_in: &mut Option<SlotOp<<D as DriverRaw>::SlotSpec>>,
    ) -> DriverSubmitResult<SlotError<<D as DriverRaw>::SlotSpec>> {
        let result = self.driver.submit_op_raw(self.token, op_in);
        if matches!(
            &result,
            DriverSubmitResult::Submitted(_)
                | DriverSubmitResult::Failed {
                    status: SubmitStatus::InFlight,
                    ..
                }
        ) {
            self.release_on_drop = false;
        }
        result
    }

    /// 标记在途 slot 不再由此包装释放，并返回其 token。
    ///
    /// [`Self::submit`] 在确认操作进入途中的结果后已经自动完成标记；此方法保留用于显式
    /// 取出 token 的调用点，并使生命周期意图清晰。
    pub fn persist(mut self) -> SubmittedOpSlot {
        self.release_on_drop = false;
        SubmittedOpSlot { token: self.token }
    }

    /// 取回同步失败操作的 payload，并释放预留 slot。
    ///
    /// 只允许在提交结果为 [`SubmitStatus::Void`] 时调用。释放操作必须保留 slot 信箱里已经
    /// 发布的完成记录；同步失败按契约不会发布完成，因此 payload 是唯一需要取回的所有权。
    pub fn recover_payload(mut self) -> Option<SlotPayload<<D as DriverRaw>::SlotSpec>> {
        let payload = self.driver.slot_take_payload_raw(self.token);
        self.driver.release_op_slot_raw(self.token);
        self.release_on_drop = false;
        payload
    }
}

impl<D: Driver + ?Sized> Drop for ReservedOpSlot<'_, D> {
    fn drop(&mut self) {
        if self.release_on_drop {
            self.driver.release_op_slot_raw(self.token);
        }
    }
}

#[doc(hidden)]
pub mod sealed {
    pub trait Sealed {}
}

/// 后端实现必须满足的低层 driver 接口。
///
/// 这个 trait 承载 slot 所有权与后端提交状态机的实现细节。普通使用者应当约束
/// [`Driver`]，而不是直接调用这里的方法；[`Driver`] 的 blanket implementation 会提供
/// 面向使用者的安全包装。
pub trait DriverRaw: sealed::Sealed {
    type SlotSpec: CoreSlotSpec;
    type Raw: RawHandleMeta;

    /// 预留一个新的操作 slot，并返回带有当前 generation 的 token。
    ///
    /// 成功时 slot 必须进入 `Reserved` 生命周期，且只由随后创建的 [`ReservedOpSlot`]
    /// 所有。此时 slot 中不得有旧的操作、payload 或完成记录；失败时不得留下可见的活跃
    /// slot。调用者可以在尚未成功提交前直接丢弃包装，包装的 drop 路径会调用
    /// [`Self::release_op_slot_raw`]。
    fn reserve_op_raw(&mut self) -> DriverResult<OpToken, SlotError<Self::SlotSpec>>;

    /// 返回一个远端线程投递的取消请求，且不得阻塞。
    ///
    /// 队列为空时必须返回 `None`；返回 `Some` 只表示请求已经从接收队列取出，真正的取消
    /// 仍由 [`Driver::drain_cancel_requests`] 通过 [`Driver::cancel_op`] 执行。
    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest>;

    /// 把 payload 的所有权写入当前 token 对应的预留 slot。
    ///
    /// 调用者保证 token 来自当前尚未提交的 [`Self::reserve_op_raw`] 成功结果，并且在一次
    /// 提交中最多设置一次。实现必须让 payload 在提交成功后一直留在 slot 中，直到完成记录
    ///消费或同步失败后的 [`Self::slot_take_payload_raw`]；不得在此方法中复制、借出或丢弃
    ///它。对失效 token 的调用不得触碰其它 slot。
    fn slot_set_payload_raw(&mut self, token: OpToken, payload: SlotPayload<Self::SlotSpec>);

    /// 从当前 token 对应的 slot 中取出 payload 的所有权。
    ///
    /// 此方法只用于同步失败（[`SubmitStatus::Void`]）路径，并且必须在
    /// [`Self::release_op_slot_raw`] 前调用。当前 token 仍有效且 slot 持有 payload 时返回
    /// `Some` 并清空 slot payload；token 失效、payload 已被取走或 slot 状态不匹配时返回
    /// `None`。它不得消费完成信箱里的记录。
    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<SlotPayload<Self::SlotSpec>>;

    /// 释放一个未成功提交的预留 slot。
    ///
    /// 这个方法由 [`ReservedOpSlot`] 的 drop 路径调用，也由同步失败路径在取回 payload 后
    /// 调用。实现必须令 slot 的生命周期归还 free list、减少活跃计数，并对 stale/重复
    /// token 幂等。实现必须使用等价于 [`crate::driver::registry::OpRegistry::remove`] 的
    /// 释放语义：保留该 slot 信箱里已经发布的完成和 generation，不得使用会清空 ready
    /// 信箱或强制推进 generation 的 `recycle` 语义。这样即使释放与完成发布竞速，detached
    /// future 仍能消费已经到达的完成。
    fn release_op_slot_raw(&mut self, token: OpToken);

    /// 将操作提交给后端，并报告提交后的生命周期。
    ///
    /// 调用者传入的 `op_in` 是操作所有权的唯一入口。返回 [`DriverSubmitResult::Submitted`]
    /// 或 [`SubmitStatus::InFlight`] 时，操作已经被 slot/后端接管，`op_in` 必须为 `None`，
    /// 并且后端最终必须向完成表发布一条或多条完成记录。返回 [`SubmitStatus::Void`] 时，
    /// 不得发布完成，slot 必须仍可由 [`Self::slot_take_payload_raw`] 和
    /// [`Self::release_op_slot_raw`] 清理；如果输入的 `op_in` 曾经是 `Some`，未提交的操作
    /// 必须在返回时放回 `op_in`，不得静默丢弃。输入本来就是 `None` 时可直接返回
    /// `Void`，无需伪造操作。
    ///
    /// `Submitted` 的 `Poll` 只描述当前提交动作是否立即推进，不能改变 slot 的在途语义。
    /// `Failed { status: InFlight, .. }` 虽然带有报告，仍然必须产生最终完成记录；只有
    /// `Void` 才表示调用者可以回收预留 slot。
    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<SlotOp<Self::SlotSpec>>,
    ) -> DriverSubmitResult<SlotError<Self::SlotSpec>>;

    /// 返回此 driver 使用的共享 slot 表。
    ///
    /// 返回值必须与 [`Self::completion_table_raw`] 指向同一套 slot 生命周期和完成信箱，
    /// 并且在 driver 存活期间保持可用于跨线程完成访问。
    fn slot_table_raw(&self) -> SharedSlotTable<Self::SlotSpec>;

    /// 返回用于向本 driver 投递远端取消请求的 sender。
    ///
    /// 返回的 sender 必须可以安全克隆并跨线程保存；请求最终由 driver 线程通过
    /// [`Self::try_recv_remote_cancel_request`] 取走。
    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender;

    /// 推进后端一次，并返回是否仍有需要处理的进展以及下次超时提示。
    ///
    /// `Poll` 不得阻塞；`Wait` 可以等待内核或计时器。实现必须在返回前处理本 driver
    /// 能观察到的完成和远端取消请求，但不得为了报告空闲而回收仍在途的 slot。
    fn drive_raw(
        &mut self,
        mode: DriveMode,
    ) -> DriverResult<DriveOutcome, SlotError<Self::SlotSpec>>;

    /// 返回用于发布和消费完成记录的共享完成表。
    ///
    /// 它必须与 [`Self::slot_table_raw`] 使用相同的 slot 表；完成发布方和 detached/local
    /// 操作句柄都依赖 generation、ready 信箱和 waker 的一致性。
    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec>;

    /// 接受一个取消请求。
    ///
    /// 请求目标已经消失时应返回对应的 `TargetGone`/等价结果，而不是破坏其它 slot。对仍
    /// 在途的操作，后端必须继续完成其收尾路径；对可本地完成的操作，可以直接向完成表
    /// 发布取消结果。
    fn cancel_op_raw(
        &mut self,
        request: CancelRequest,
    ) -> DriverResult<CancelSubmitOutcome, SlotError<Self::SlotSpec>>;

    /// 注册一段可供后端定位的 buffer chunk。
    ///
    /// 成功后后端必须能够按 `id` 解析这段仍由调用者保持有效的内存；失败不得留下半个
    /// 注册项。
    fn register_chunk_raw(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>>;

    /// 注册文件或 socket，并返回带 generation 的后端 descriptor。
    ///
    /// `Borrowed` 不得转移所有权，`Owned` 必须由后端接管并在注销或 driver 销毁时关闭。
    fn register_files_raw<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd<Self::Raw>>, SlotError<Self::SlotSpec>>;

    /// 注销此前返回的 descriptor，并释放其对应的后端资源。
    ///
    /// 注销必须推进 registered descriptor 的 generation，使旧 descriptor 后续提交失败，
    /// 且不能误伤同一注册表中的其它 descriptor。
    fn unregister_files_raw(
        &mut self,
        files: Vec<IoFd<Self::Raw>>,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>>;

    /// 创建一个可跨线程唤醒 driver 的句柄。
    ///
    /// `wake` 成功后，下一次驱动循环必须有机会观察远端取消或其它待处理工作；唤醒句柄
    /// 的生命周期不得借用 driver 内部短生命周期数据。
    fn create_waker_raw(&self) -> Arc<dyn RemoteWaker<SlotError<Self::SlotSpec>>>;

    /// 把本 worker 的缓冲池交给 driver，供它自己需要 buffer 的机制使用。
    ///
    /// 必须在池建好之后单独调用，而不能作为构造参数：池是从 driver 建起来的，所以
    /// driver 构造时它还不存在。默认什么都不做；没有这类机制的后端不必实现。
    fn attach_buffer_pool_raw(
        &mut self,
        _pool: AnyBufPool,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>> {
        Ok(())
    }

    /// 返回本 driver 当前可用的可选能力。后端不支持可保持默认的全否结果。
    fn capabilities_raw(&self) -> DriverCapabilities {
        DriverCapabilities::default()
    }

    /// 记录一个能力在运行期被内核拒绝，后续不再尝试。
    fn note_capability_rejected_raw(&mut self, _capability: DriverCapability) {}
}

/// 使用者面向的 driver 接口。
///
/// 所有实现均由 [`DriverRaw`] 的 blanket implementation 派生。slot 的预留、payload
/// 所有权和提交状态由 [`ReservedOpSlot`] 统一管理，调用者不需要实现或直接调用 raw
/// 生命周期方法。
pub trait Driver: DriverRaw {
    fn reserve_op(
        &mut self,
    ) -> DriverResult<ReservedOpSlot<'_, Self>, SlotError<<Self as DriverRaw>::SlotSpec>>
    where
        Self: Sized,
    {
        let token = self.reserve_op_raw()?;
        Ok(ReservedOpSlot::new(self, token))
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.slot_table_raw()
    }

    fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.remote_cancel_sender_raw()
    }

    fn drive(
        &mut self,
        mode: DriveMode,
    ) -> DriverResult<DriveOutcome, SlotError<<Self as DriverRaw>::SlotSpec>> {
        self.drive_raw(mode)
    }

    fn completion_table(&self) -> SharedCompletionTable<<Self as DriverRaw>::SlotSpec> {
        self.completion_table_raw()
    }

    fn register_completion_waker(
        &mut self,
        token: OpToken,
        waker: &Waker,
    ) -> CompletionMutationOutcome {
        self.completion_table().register_waker(token, waker)
    }

    fn cancel_op(
        &mut self,
        request: CancelRequest,
    ) -> DriverResult<CancelSubmitOutcome, SlotError<<Self as DriverRaw>::SlotSpec>> {
        self.cancel_op_raw(request)
    }

    fn register_chunk(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), SlotError<<Self as DriverRaw>::SlotSpec>> {
        self.register_chunk_raw(id, ptr, len)
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, <Self as DriverRaw>::Raw>>,
    ) -> DriverResult<Vec<IoFd<<Self as DriverRaw>::Raw>>, SlotError<<Self as DriverRaw>::SlotSpec>>
    {
        self.register_files_raw(files)
    }

    fn unregister_files(
        &mut self,
        files: Vec<IoFd<<Self as DriverRaw>::Raw>>,
    ) -> DriverResult<(), SlotError<<Self as DriverRaw>::SlotSpec>> {
        self.unregister_files_raw(files)
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<SlotError<<Self as DriverRaw>::SlotSpec>>> {
        self.create_waker_raw()
    }

    fn attach_buffer_pool(
        &mut self,
        pool: AnyBufPool,
    ) -> DriverResult<(), SlotError<<Self as DriverRaw>::SlotSpec>> {
        self.attach_buffer_pool_raw(pool)
    }

    fn capabilities(&self) -> DriverCapabilities {
        self.capabilities_raw()
    }

    fn note_capability_rejected(&mut self, capability: DriverCapability) {
        self.note_capability_rejected_raw(capability)
    }

    fn drain_cancel_requests(
        &mut self,
    ) -> DriverResult<CancelDrainOutcome, SlotError<<Self as DriverRaw>::SlotSpec>> {
        let mut outcome = CancelDrainOutcome::default();
        while let Some(request) = self.try_recv_remote_cancel_request() {
            let submit_outcome = self.cancel_op(request)?;
            outcome.record(submit_outcome);
        }
        Ok(outcome)
    }
}

impl<D: DriverRaw + ?Sized> Driver for D {}

pub trait ContextDriverProvider<D: Driver + ?Sized> {
    fn with_driver_mut<R>(&self, f: impl FnOnce(&mut D) -> R) -> R;
    fn with_driver_ref<R>(&self, f: impl FnOnce(&D) -> R) -> R;
}

pub struct RuntimeContextDriver<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> {
    provider: &'a P,
    _phantom: PhantomData<fn() -> D>,
}

impl<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> RuntimeContextDriver<'a, D, P> {
    pub fn new(provider: &'a P) -> Self {
        Self {
            provider,
            _phantom: PhantomData,
        }
    }
}

impl<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> sealed::Sealed
    for RuntimeContextDriver<'a, D, P>
{
}

impl<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> DriverRaw
    for RuntimeContextDriver<'a, D, P>
{
    type SlotSpec = <D as DriverRaw>::SlotSpec;
    type Raw = <D as DriverRaw>::Raw;

    fn reserve_op_raw(&mut self) -> DriverResult<OpToken, SlotError<Self::SlotSpec>> {
        self.provider.with_driver_mut(|d| d.reserve_op_raw())
    }

    fn slot_table_raw(&self) -> SharedSlotTable<Self::SlotSpec> {
        self.provider.with_driver_ref(|d| d.slot_table())
    }

    fn remote_cancel_sender_raw(&self) -> RemoteCancelSender {
        self.provider.with_driver_ref(|d| d.remote_cancel_sender())
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.provider
            .with_driver_mut(|d| d.try_recv_remote_cancel_request())
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: SlotPayload<Self::SlotSpec>) {
        self.provider
            .with_driver_mut(|d| d.slot_set_payload_raw(token, payload))
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<SlotPayload<Self::SlotSpec>> {
        self.provider
            .with_driver_mut(|d| d.slot_take_payload_raw(token))
    }

    fn release_op_slot_raw(&mut self, token: OpToken) {
        self.provider
            .with_driver_mut(|d| d.release_op_slot_raw(token))
    }

    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<SlotOp<Self::SlotSpec>>,
    ) -> DriverSubmitResult<SlotError<Self::SlotSpec>> {
        self.provider
            .with_driver_mut(|d| d.submit_op_raw(token, op_in))
    }

    fn drive_raw(
        &mut self,
        mode: DriveMode,
    ) -> DriverResult<DriveOutcome, SlotError<Self::SlotSpec>> {
        self.provider.with_driver_mut(|d| d.drive(mode))
    }

    fn completion_table_raw(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.provider.with_driver_ref(|d| d.completion_table())
    }

    fn cancel_op_raw(
        &mut self,
        request: CancelRequest,
    ) -> DriverResult<CancelSubmitOutcome, SlotError<Self::SlotSpec>> {
        self.provider.with_driver_mut(|d| d.cancel_op(request))
    }

    fn register_chunk_raw(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>> {
        self.provider
            .with_driver_mut(|d| d.register_chunk(id, ptr, len))
    }

    fn register_files_raw<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd<Self::Raw>>, SlotError<Self::SlotSpec>> {
        self.provider.with_driver_mut(|d| d.register_files(files))
    }

    fn unregister_files_raw(
        &mut self,
        files: Vec<IoFd<Self::Raw>>,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>> {
        self.provider.with_driver_mut(|d| d.unregister_files(files))
    }

    fn create_waker_raw(&self) -> Arc<dyn RemoteWaker<SlotError<Self::SlotSpec>>> {
        self.provider.with_driver_ref(|d| d.create_waker())
    }

    fn attach_buffer_pool_raw(
        &mut self,
        pool: AnyBufPool,
    ) -> DriverResult<(), SlotError<Self::SlotSpec>> {
        self.provider
            .with_driver_mut(|d| d.attach_buffer_pool(pool))
    }

    fn capabilities_raw(&self) -> DriverCapabilities {
        self.provider.with_driver_ref(|d| d.capabilities())
    }

    fn note_capability_rejected_raw(&mut self, capability: DriverCapability) {
        self.provider
            .with_driver_mut(|d| d.note_capability_rejected(capability))
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct CancelDrainOutcome {
    pub requests: u64,
    pub submitted: u64,
    pub queued: u64,
    pub completed_locally: u64,
    pub target_missing: u64,
    pub target_stale: u64,
    pub target_corrupt: u64,
    pub no_backend_handle: u64,
}

impl CancelDrainOutcome {
    fn record(&mut self, outcome: CancelSubmitOutcome) {
        self.requests = self.requests.saturating_add(1);
        match outcome {
            CancelSubmitOutcome::Submitted => {
                self.submitted = self.submitted.saturating_add(1);
            }
            CancelSubmitOutcome::Queued => {
                self.queued = self.queued.saturating_add(1);
            }
            CancelSubmitOutcome::CompletedLocally => {
                self.completed_locally = self.completed_locally.saturating_add(1);
            }
            CancelSubmitOutcome::TargetGone { reason } => match reason {
                CancelTargetGoneReason::Missing => {
                    self.target_missing = self.target_missing.saturating_add(1);
                }
                CancelTargetGoneReason::Stale => {
                    self.target_stale = self.target_stale.saturating_add(1);
                }
                CancelTargetGoneReason::Corrupt => {
                    self.target_corrupt = self.target_corrupt.saturating_add(1);
                }
            },
            CancelSubmitOutcome::NoBackendHandle => {
                self.no_backend_handle = self.no_backend_handle.saturating_add(1);
            }
        }
    }
}

/// 后端可选能力的集合。
///
/// 全 `false` 是**合法且必须能工作**的配置：IOCP 一个都没有，Linux 5.6–5.18 也没有
/// （multishot accept / buf ring 要 5.19，multishot recv 要 6.0，而仓库声明的最低内核是
/// 5.6）。所以「不支持时怎么办」是一条会被真实测试覆盖的主路径，不是兼容层。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriverCapabilities {
    pub accept_multi: bool,
    pub recv_multi: bool,
    pub provided_buffers: bool,
}

/// [`DriverCapabilities`] 里的单个条目，供运行期降级使用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverCapability {
    AcceptMulti,
    RecvMulti,
    ProvidedBuffers,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriveMode {
    Poll,
    Wait,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DriveOutcome {
    pub next_timeout_hint: Option<Duration>,
    pub pending_progress: bool,
}

pub trait RemoteWaker<E>: Send + Sync
where
    E: Error + Send + Sync + 'static,
{
    fn wake(&self) -> DriverResult<(), E>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitStatus {
    /// Operation successfully submitted or queued. It *will* eventually produce
    /// a completion result in the `CompletionTable`.
    InFlight,
    /// Operation failed synchronously and no completion result will be produced.
    Void,
}

#[cfg(feature = "test-hooks")]
pub mod test_hooks {
    pub trait DriverTestHooks {
        fn debug_chunk_register_attempts(&self) -> u64;
    }
}

#[cfg(feature = "test-hooks")]
use test_hooks::DriverTestHooks;

#[cfg(feature = "test-hooks")]
impl<'a, D: Driver + ?Sized + DriverTestHooks, P: ContextDriverProvider<D> + ?Sized> DriverTestHooks
    for RuntimeContextDriver<'a, D, P>
{
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.provider
            .with_driver_ref(|d| d.debug_chunk_register_attempts())
    }
}
