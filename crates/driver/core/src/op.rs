mod future;
pub mod types;

pub use future::*;
pub use types::OpKind;

use tracing::trace;
use veloq_std::marker::Send;

use crate::{
    DriverCoreError, DriverError, DriverReport, DriverResult,
    driver::{Driver, DriverRaw, DriverSubmitResult, SubmitStatus},
    slot::{SlotCompletion, SlotError, SlotOp, SlotPayload, SlotSpec},
};
use diagweave::prelude::*;

pub trait DriverProvider: Clone + Unpin {
    type SlotSpec: SlotSpec;
    type Driver<'a>: Driver + DriverRaw<SlotSpec = Self::SlotSpec>
    where
        Self: 'a;

    fn with_driver<'a, R>(&'a self, f: impl FnOnce(Self::Driver<'a>) -> R) -> R;
}

/// Trait to convert a user-facing operation to a platform-specific driver operation.
///
/// 一个操作提交一次，产出 **1..N** 条完成。两个 payload 关联类型对应这句话里的两侧：
///
/// - [`SubmitPayload`](Self::SubmitPayload) 在提交时进入 slot，并在整个操作期间留在
///   那里（内核可能仍持有指向它的指针），取消与 orphan cleanup 都要用它；
/// - [`RecordPayload`](Self::RecordPayload) 是**每条完成记录**里携带的东西。
///
/// **单发操作里两者是同一个类型**：记录被取走时把提交 payload 一并搬出来，所以后端只需
/// 把两个关联类型都写成自己。只有 multishot 操作（`AcceptMulti` 之类）会让它们分开——
/// 提交 payload 是监听 socket，每条完成的产物是一个新连接。
///
/// 早期设计只有一个 `UserPayload`，于是 multishot 要么塞一个「空壳 item」（此后每一处
/// payload 访问都要处理一个永远不该出现的 `None`），要么另开一个平行的 trait。拆成这两个
/// 名字之后，两条路径都没有空状态，也不需要第二个 trait。
pub trait IntoPlatformOp<Spec: SlotSpec>: Sized + Send {
    /// 提交进 slot 的 payload。
    type SubmitPayload: Send;
    /// 每条完成记录里携带的 payload。单发操作里它就是 [`Self::SubmitPayload`]。
    type RecordPayload;
    type Output: Send;
    type Completion: Send;
    const PAYLOAD_KIND: types::OpKind;

    fn into_kernel_and_payload(self) -> (SlotOp<Spec>, Self::SubmitPayload);

    fn payload_into_erased(payload: Self::SubmitPayload) -> SlotPayload<Spec>;

    fn try_record_from_erased(
        erased: SlotPayload<Spec>,
    ) -> DriverResult<Self::RecordPayload, SlotError<Spec>>;

    fn complete(
        payload: Self::RecordPayload,
        res: DriverResult<SlotCompletion<Spec>, SlotError<Spec>>,
    ) -> OpCompletion<Self::Output, SlotError<Spec>, Self::Completion>;

    /// 提交同步失败，slot 里的 payload 被原样取回：这个操作一条完成都不会产生。
    ///
    /// 默认实现把 payload 还给用户——单发操作的提交 payload 就是它的记录 payload，而且
    /// 通常还带着用户交出去的 buffer，必须还回去。multishot 覆盖它：那里的提交 payload
    /// 不是任何一条完成的产物，没有 item 可交付。
    fn submit_failed(
        erased: SlotPayload<Spec>,
        report: DriverReport<SlotError<Spec>>,
    ) -> OpResult<Self::Output, SlotError<Spec>, Self::Completion> {
        match Self::try_record_from_erased(erased) {
            Ok(payload) => {
                let completion = Self::complete(payload, Err(report));
                OpResult::Completed(completion.result, completion.output)
            }
            // 投影都失败了说明 slot 里根本不是这个操作的 payload，那比「提交失败」严重得
            // 多，报它而不是报 `report`。
            Err(mismatch) => OpResult::ResourceLost(OpError::payload_projection(mismatch)),
        }
    }
}

/// 只产出一条完成的操作。
///
/// 这是 [`LocalOp`] / [`DetachedOp`] 的 [`Future`](veloq_std::future::Future) 实现的边界：两者
/// 对**任何**操作都是完成流（[`futures_core::Stream`]），但只有单发操作能被 `await`——
/// `await` 一个 multishot 操作等于「取第一条完成然后取消」，那是个陷阱而不是特性，所以让
/// 它在编译期就不成立。
///
/// 后端为每个单发 op 实现它（宏里一行）；multishot op 不实现。
pub trait SingleShotOp<Spec: SlotSpec>: IntoPlatformOp<Spec> {}

#[inline]
pub fn payload_projection_mismatch_report<E>(
    expected_payload: &'static str,
    erased_payload: &'static str,
) -> DriverReport<E>
where
    E: DriverError,
{
    E::from_core_report(
        DriverCoreError::Internal
            .to_report()
            .push_ctx("scope", "driver-core/op/payload_projection")
            .with_ctx("expected_payload", expected_payload)
            .with_ctx("erased_payload", erased_payload)
            .attach_note("operation payload variant mismatch"),
    )
}

/// A generic wrapper for IO operation data.
pub struct Op<T> {
    pub data: T,
}

impl<T> Op<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }

    /// 提交一个操作，得到一个不借用驱动的句柄。
    ///
    /// 句柄既是单发操作的 [`Future`](veloq_std::future::Future)，也是任意操作的完成流
    /// （[`futures_core::Stream`]）——「一条还是多条」由 slot 层的
    /// [`CompletionContinuation`](crate::driver::CompletionContinuation) 决定，提交路径
    /// 对两者完全相同。
    pub fn submit_detached<D>(self, driver: &mut D) -> DetachedOp<T, <D as DriverRaw>::SlotSpec>
    where
        T: IntoPlatformOp<<D as DriverRaw>::SlotSpec> + Send,
        D: Driver,
    {
        let data = self.data;
        trace!("Submitting detached op");

        let mut slot = match driver.reserve_op() {
            Ok(slot) => slot,
            Err(report) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                drop(kernel_op);
                let erased = T::payload_into_erased(payload);
                return DetachedOp::settled(T::submit_failed(erased, report));
            }
        };

        let (kernel_op, payload) = data.into_kernel_and_payload();
        let mut op_platform = Some(kernel_op);
        let completion_table = slot.completion_table();
        let cancel_sender = slot.remote_cancel_sender();
        let cancel_waker = slot.create_waker();
        slot.set_payload(T::payload_into_erased(payload));

        match slot.submit(&mut op_platform) {
            DriverSubmitResult::Submitted(_)
            | DriverSubmitResult::Failed {
                status: SubmitStatus::InFlight,
                ..
            } => {
                let token = slot.persist().token();
                completion_table.mark_waiting(token);
                DetachedOp::armed(completion_table, cancel_sender, cancel_waker, token)
            }
            DriverSubmitResult::Failed {
                report,
                status: SubmitStatus::Void,
            } => {
                trace!("Submit failed synchronously: {report}");
                drop(op_platform.take());
                let Some(erased) = slot.recover_payload() else {
                    return DetachedOp::settled(OpResult::ResourceLost(OpError::payload_missing()));
                };
                DetachedOp::settled(T::submit_failed(erased, report))
            }
        }
    }

    pub fn submit_local<'a, P: DriverProvider>(self, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<P::SlotSpec>,
    {
        LocalOp::new(self.data, provider)
    }
}
