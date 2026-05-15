use tracing::trace;

use crate::DriverResult;
use crate::driver::{Driver, PlatformOp, SubmitBinder, SubmitStatus, encode_completion_token};

pub trait DriverProvider: Clone + Unpin {
    type Driver: crate::driver::Driver;
    fn with_driver<R>(&self, f: impl FnOnce(&mut Self::Driver) -> R) -> R;
}

mod future;
mod types;

pub use future::*;
pub use types::*;

/// Trait for managing the lifecycle of an operation.
pub trait OpLifecycle: Sized {
    type PreAlloc;
    type Output;
    type Raw: crate::RawHandleMeta;
    type CompletionValue;

    fn pre_alloc(fd: Self::Raw) -> DriverResult<Self::PreAlloc>;

    fn into_op(fd: Self::Raw, pre: Self::PreAlloc) -> Self;

    fn into_output(self, res: DriverResult<Self::CompletionValue>) -> DriverResult<Self::Output>;

    fn prepare_op(fd: Self::Raw) -> DriverResult<Self> {
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
    const PAYLOAD_KIND: OpKind;

    fn into_kernel_and_payload(self) -> (O, Self::UserPayload);

    fn payload_into_erased(payload: Self::UserPayload) -> Self::ErasedPayload;

    fn payload_from_erased(erased: Self::ErasedPayload) -> Self::UserPayload;

    fn complete(
        payload: Self::UserPayload,
        res: DriverResult<Self::DriverCompletion>,
    ) -> OpCompletion<Self::Output, Self::Completion>;
}

/// A generic wrapper for IO operation data.
pub struct Op<T> {
    pub data: T,
}

impl<T> Op<T> {
    pub fn new(data: T) -> Self {
        Self { data }
    }

    pub fn submit_detached<D>(self, driver: &mut D) -> DetachedOp<T, D::Op, D::Completion>
    where
        T: IntoPlatformOp<D::Op, DriverCompletion = D::Completion, ErasedPayload = D::UP>
            + std::marker::Send,
        D: Driver,
    {
        let data = self.data;
        trace!("Submitting detached op");

        match driver.reserve_op() {
            Ok((user_data, generation)) => {
                let (kernel_op, payload) = data.into_kernel_and_payload();
                let mut op_platform = Some(kernel_op);
                let token = encode_completion_token(user_data, generation);
                let completion_table = driver.completion_table();
                let cancel_signal = driver.detached_cancel_table();
                let cancel_waker = driver.create_waker();
                driver.slot_set_payload(user_data, T::payload_into_erased(payload));

                let result = driver
                    .submit(user_data, &mut op_platform, SubmitBinder::new())
                    .into_inner();

                match result {
                    Ok(_) => {
                        completion_table.mark_waiting(token);
                        DetachedOp {
                            completion_table: Some(completion_table),
                            cancel_signal: Some(cancel_signal),
                            cancel_waker: Some(cancel_waker),
                            token,
                            immediate_failure: None,
                            _phantom: std::marker::PhantomData,
                        }
                    }
                    Err((e, status)) => {
                        trace!("Submit failed synchronously: {} (status={:?})", e, status);
                        if status == SubmitStatus::Void {
                            let payload_erased = driver.slot_take_payload(user_data).unwrap_or_else(|| {
                                panic!(
                                    "Payload missing while recovering submit failure: user_data={}, status={:?}, error={}",
                                    user_data, status, e
                                )
                            });

                            let payload = T::payload_from_erased(payload_erased);
                            if let Some(op) = op_platform.take() {
                                drop(op);
                            }
                            DetachedOp {
                                completion_table: None,
                                cancel_signal: None,
                                cancel_waker: None,
                                token: 0,
                                immediate_failure: Some((e, payload)),
                                _phantom: std::marker::PhantomData,
                            }
                        } else {
                            completion_table.mark_waiting(token);
                            DetachedOp {
                                completion_table: Some(completion_table),
                                cancel_signal: Some(cancel_signal),
                                cancel_waker: Some(cancel_waker),
                                token,
                                immediate_failure: None,
                                _phantom: std::marker::PhantomData,
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
                    cancel_signal: None,
                    cancel_waker: None,
                    token: 0,
                    immediate_failure: Some((e, payload)),
                    _phantom: std::marker::PhantomData,
                }
            }
        }
    }

    pub fn submit_local<P: DriverProvider>(self, provider: P) -> LocalOp<T, P>
    where
        T: IntoPlatformOp<
                <P::Driver as Driver>::Op,
                DriverCompletion = <P::Driver as Driver>::Completion,
                ErasedPayload = <P::Driver as Driver>::UP,
            >,
    {
        LocalOp::new(self.data, provider)
    }
}
