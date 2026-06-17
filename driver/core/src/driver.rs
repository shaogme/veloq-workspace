use crate::{
    BorrowedRawHandle, DriverError, DriverReport, DriverResult, IoFd, OwnedRawHandle,
    RawHandleMeta, SlotSidecar,
    slot::{self, SlotSpec as CoreSlotSpec},
};
use std::{
    error::Error,
    marker::PhantomData,
    sync::{Arc, mpsc},
    task::{Poll, Waker},
    time::Duration,
};
use veloq_buf::heap::ChunkId;

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
pub type SharedDriverSlotTable<D> = SharedSlotTable<<D as Driver>::SlotSpec>;
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

    pub fn completion_table(&self) -> SharedCompletionTable<D::SlotSpec> {
        self.driver.completion_table()
    }

    pub fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.driver.remote_cancel_sender()
    }

    pub fn create_waker(&self) -> Arc<dyn RemoteWaker<D::Error>> {
        self.driver.create_waker()
    }

    pub fn set_payload(&mut self, payload: D::UP) {
        self.driver.slot_set_payload_raw(self.token, payload);
    }

    pub fn submit(&mut self, op_in: &mut Option<D::Op>) -> DriverSubmitResult<D::Error> {
        self.driver.submit_op_raw(self.token, op_in)
    }

    pub fn persist(mut self) -> SubmittedOpSlot {
        self.release_on_drop = false;
        SubmittedOpSlot { token: self.token }
    }

    pub fn recover_payload(mut self) -> Option<D::UP> {
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

pub trait Driver {
    type Op: PlatformOp;
    type UP: Send;
    type Raw: RawHandleMeta;
    type Sidecar: SlotSidecar;
    type Completion: CompletionValue;
    type Error: DriverError;
    type SlotSpec: CoreSlotSpec<
            Op = Self::Op,
            UserPayload = Self::UP,
            Sidecar = Self::Sidecar,
            Error = Self::Error,
            Completion = Self::Completion,
        >;

    #[doc(hidden)]
    fn reserve_op_raw(&mut self) -> DriverResult<OpToken, Self::Error>;

    fn reserve_op(&mut self) -> DriverResult<ReservedOpSlot<'_, Self>, Self::Error>
    where
        Self: Sized,
    {
        let token = self.reserve_op_raw()?;
        Ok(ReservedOpSlot::new(self, token))
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self>;

    fn remote_cancel_sender(&self) -> RemoteCancelSender;

    #[doc(hidden)]
    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest>;

    #[doc(hidden)]
    fn slot_set_payload_raw(&mut self, token: OpToken, payload: Self::UP);

    #[doc(hidden)]
    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<Self::UP>;

    #[doc(hidden)]
    fn release_op_slot_raw(&mut self, token: OpToken);

    #[doc(hidden)]
    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error>;

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome, Self::Error>;

    fn completion_table(&self) -> SharedCompletionTable<Self::SlotSpec>;

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
    ) -> DriverResult<CancelSubmitOutcome, Self::Error>;

    fn register_chunk(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), Self::Error>;

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>, Self::Error>;

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<(), Self::Error>;

    fn create_waker(&self) -> Arc<dyn RemoteWaker<Self::Error>>;
}

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

impl<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> Driver
    for RuntimeContextDriver<'a, D, P>
{
    type Op = D::Op;
    type UP = D::UP;
    type Raw = D::Raw;
    type Sidecar = D::Sidecar;
    type Completion = D::Completion;
    type Error = D::Error;
    type SlotSpec = D::SlotSpec;

    fn reserve_op_raw(&mut self) -> DriverResult<OpToken, Self::Error> {
        self.provider.with_driver_mut(|d| d.reserve_op_raw())
    }

    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.provider.with_driver_ref(|d| d.slot_table())
    }

    fn remote_cancel_sender(&self) -> RemoteCancelSender {
        self.provider.with_driver_ref(|d| d.remote_cancel_sender())
    }

    fn try_recv_remote_cancel_request(&mut self) -> Option<CancelRequest> {
        self.provider
            .with_driver_mut(|d| d.try_recv_remote_cancel_request())
    }

    fn slot_set_payload_raw(&mut self, token: OpToken, payload: Self::UP) {
        self.provider
            .with_driver_mut(|d| d.slot_set_payload_raw(token, payload))
    }

    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<Self::UP> {
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
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        self.provider
            .with_driver_mut(|d| d.submit_op_raw(token, op_in))
    }

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome, Self::Error> {
        self.provider.with_driver_mut(|d| d.drive(mode))
    }

    fn completion_table(&self) -> SharedCompletionTable<Self::SlotSpec> {
        self.provider.with_driver_ref(|d| d.completion_table())
    }

    fn cancel_op(
        &mut self,
        request: CancelRequest,
    ) -> DriverResult<CancelSubmitOutcome, Self::Error> {
        self.provider.with_driver_mut(|d| d.cancel_op(request))
    }

    fn register_chunk(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), Self::Error> {
        self.provider
            .with_driver_mut(|d| d.register_chunk(id, ptr, len))
    }

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>, Self::Error> {
        self.provider.with_driver_mut(|d| d.register_files(files))
    }

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<(), Self::Error> {
        self.provider.with_driver_mut(|d| d.unregister_files(files))
    }

    fn create_waker(&self) -> Arc<dyn RemoteWaker<Self::Error>> {
        self.provider.with_driver_ref(|d| d.create_waker())
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
    pub diagnostic_only: u64,
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
            CancelSubmitOutcome::DiagnosticOnly { kind: _ } => {
                self.diagnostic_only = self.diagnostic_only.saturating_add(1);
            }
            CancelSubmitOutcome::NoBackendHandle => {
                self.no_backend_handle = self.no_backend_handle.saturating_add(1);
            }
        }
    }
}

#[inline]
pub fn drain_cancel_requests<D: Driver>(
    driver: &mut D,
) -> DriverResult<CancelDrainOutcome, D::Error> {
    let mut outcome = CancelDrainOutcome::default();
    while let Some(request) = driver.try_recv_remote_cancel_request() {
        let submit_outcome = driver.cancel_op(request)?;
        outcome.record(submit_outcome);
    }
    Ok(outcome)
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
