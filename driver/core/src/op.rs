use tracing::trace;

use crate::driver::{Driver, DriverSubmitResult, PlatformOp, SubmitStatus};
use crate::{DriverCoreError, DriverError, DriverReport, DriverResult};
use diagweave::prelude::*;

pub trait DriverProvider: Clone + Unpin {
    type Op: PlatformOp;
    type UP: std::marker::Send;
    type Completion: crate::driver::CompletionValue;
    type Error: DriverError;
    type SlotSpec: crate::slot::SlotSpec<
            Op = Self::Op,
            UserPayload = Self::UP,
            Completion = Self::Completion,
            Error = Self::Error,
        >;
    type Driver<'a>: crate::driver::Driver<
            Op = Self::Op,
            UP = Self::UP,
            Completion = Self::Completion,
            Error = Self::Error,
            SlotSpec = Self::SlotSpec,
        >
    where
        Self: 'a;

    fn with_driver<'a, R>(&'a self, f: impl FnOnce(Self::Driver<'a>) -> R) -> R;
}

mod buffer_range;
mod future;
mod types;

pub use buffer_range::*;
pub use future::*;
pub use types::*;

/// Trait for managing the lifecycle of an operation.
pub trait OpLifecycle: Sized {
    type PreAlloc;
    type Output;
    type Raw: crate::RawHandleMeta;
    type CompletionValue;
    type Error: DriverError;

    fn pre_alloc(fd: Self::Raw) -> DriverResult<Self::PreAlloc, Self::Error>;

    fn into_op(fd: Self::Raw, pre: Self::PreAlloc) -> Self;

    fn into_output(
        self,
        res: DriverResult<Self::CompletionValue, Self::Error>,
    ) -> DriverResult<Self::Output, Self::Error>;

    fn prepare_op(fd: Self::Raw) -> DriverResult<Self, Self::Error> {
        let pre = Self::pre_alloc(fd)?;
        Ok(Self::into_op(fd, pre))
    }
}

/// Trait to convert a user-facing operation to a platform-specific driver operation.
pub trait IntoPlatformOp<O: PlatformOp>: Sized + std::marker::Send {
    type UserPayload: std::marker::Send;
    type ErasedPayload: std::marker::Send;
    type Output;
    type Completion;
    type DriverCompletion: crate::driver::CompletionValue;
    type Error: DriverError;
    const PAYLOAD_KIND: OpKind;

    fn into_kernel_and_payload(self) -> (O, Self::UserPayload);

    fn payload_into_erased(payload: Self::UserPayload) -> Self::ErasedPayload;

    fn try_payload_from_erased(
        erased: Self::ErasedPayload,
    ) -> DriverResult<Self::UserPayload, Self::Error>;

    fn complete(
        payload: Self::UserPayload,
        res: DriverResult<Self::DriverCompletion, <Self as IntoPlatformOp<O>>::Error>,
    ) -> OpCompletion<Self::Output, Self::Error, Self::Completion>;
}

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

    pub fn submit_detached<D>(self, driver: &mut D) -> DetachedOp<T, D::SlotSpec>
    where
        T: IntoPlatformOp<
                D::Op,
                DriverCompletion = D::Completion,
                ErasedPayload = D::UP,
                Error = D::Error,
            > + std::marker::Send,
        D: Driver,
    {
        let data = self.data;
        trace!("Submitting detached op");

        match driver.reserve_op() {
            Ok(mut slot) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                let mut op_platform = Some(kernel_op);
                let completion_table = slot.completion_table();
                let cancel_sender = slot.remote_cancel_sender();
                let cancel_waker = slot.create_waker();
                slot.set_payload(T::payload_into_erased(payload));

                match slot.submit(&mut op_platform) {
                    DriverSubmitResult::Submitted(_) => {
                        let token = slot.persist().token();
                        completion_table.mark_waiting(token);
                        DetachedOp {
                            completion_table: Some(completion_table),
                            cancel_sender: Some(cancel_sender),
                            cancel_waker: Some(cancel_waker),
                            token: Some(token),
                            immediate_failure: None,
                            immediate_resource_lost: None,
                            _phantom: std::marker::PhantomData,
                        }
                    }
                    DriverSubmitResult::Failed { report, status } => {
                        trace!(
                            "Submit failed synchronously: {} (status={:?})",
                            report, status
                        );
                        match status {
                            SubmitStatus::Void => {
                                let Some(payload_erased) = slot.recover_payload() else {
                                    if let Some(op) = op_platform.take() {
                                        drop(op);
                                    }
                                    return DetachedOp {
                                        completion_table: None,
                                        cancel_sender: None,
                                        cancel_waker: None,
                                        token: None,
                                        immediate_failure: None,
                                        immediate_resource_lost: Some(
                                            future::payload_missing_error(),
                                        ),
                                        _phantom: std::marker::PhantomData,
                                    };
                                };

                                let payload = match T::try_payload_from_erased(payload_erased) {
                                    Ok(payload) => payload,
                                    Err(report) => {
                                        if let Some(op) = op_platform.take() {
                                            drop(op);
                                        }
                                        return DetachedOp {
                                            completion_table: None,
                                            cancel_sender: None,
                                            cancel_waker: None,
                                            token: None,
                                            immediate_failure: None,
                                            immediate_resource_lost: Some(
                                                future::payload_projection_error(report),
                                            ),
                                            _phantom: std::marker::PhantomData,
                                        };
                                    }
                                };
                                if let Some(op) = op_platform.take() {
                                    drop(op);
                                }
                                DetachedOp {
                                    completion_table: None,
                                    cancel_sender: None,
                                    cancel_waker: None,
                                    token: None,
                                    immediate_failure: Some((report, payload)),
                                    immediate_resource_lost: None,
                                    _phantom: std::marker::PhantomData,
                                }
                            }
                            SubmitStatus::InFlight => {
                                let token = slot.persist().token();
                                completion_table.mark_waiting(token);
                                DetachedOp {
                                    completion_table: Some(completion_table),
                                    cancel_sender: Some(cancel_sender),
                                    cancel_waker: Some(cancel_waker),
                                    token: Some(token),
                                    immediate_failure: None,
                                    immediate_resource_lost: None,
                                    _phantom: std::marker::PhantomData,
                                }
                            }
                        }
                    }
                }
            }
            Err(e) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                drop(kernel_op);
                DetachedOp {
                    completion_table: None,
                    cancel_sender: None,
                    cancel_waker: None,
                    token: None,
                    immediate_failure: Some((e, payload)),
                    immediate_resource_lost: None,
                    _phantom: std::marker::PhantomData,
                }
            }
        }
    }

    pub fn submit_local<'a, P: DriverProvider>(self, provider: P) -> LocalOp<'a, T, P>
    where
        T: IntoPlatformOp<
                P::Op,
                DriverCompletion = P::Completion,
                ErasedPayload = P::UP,
                Error = P::Error,
            >,
    {
        LocalOp::new(self.data, provider)
    }
}
