use crate::slot;
use crate::slot::SlotSpec as CoreSlotSpec;
use crate::slot::is_runnable_state;
use crate::{BorrowedRawHandle, IoFd, OwnedRawHandle, RawHandleMeta, SlotSidecar};
use crate::{DriverError, DriverReport, DriverResult};

use std::sync::Arc;
use std::task::Poll;
use std::task::Waker;
use std::time::Duration;

pub mod completion;
pub mod registry;

pub use completion::*;

pub trait PlatformOp {}

pub enum RegisterFd<'a, H: RawHandleMeta> {
    Borrowed(BorrowedRawHandle<'a, H>),
    Owned(OwnedRawHandle<H>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DriverControlCommand {
    UnregisterFiles(Vec<IoFd>),
}

pub type SharedSlotTable<Spec> = Arc<slot::SlotTable<Spec>>;
pub type SharedDriverSlotTable<D> = SharedSlotTable<<D as Driver>::SlotSpec>;

#[must_use]
pub enum DriverSubmitResult<E> {
    Submitted(Poll<()>),
    Failed {
        report: DriverReport<E>,
        status: SubmitStatus,
    },
}

impl<E> DriverSubmitResult<E> {
    #[inline]
    pub fn submitted(poll: Poll<()>) -> Self {
        Self::Submitted(poll)
    }

    #[inline]
    pub fn failed(report: DriverReport<E>, status: SubmitStatus) -> Self {
        Self::Failed { report, status }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubmittedOpSlot {
    token: OpToken,
}

impl SubmittedOpSlot {
    #[inline]
    pub fn token(self) -> OpToken {
        self.token
    }

    #[inline]
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
    #[inline]
    fn new(driver: &'a mut D, token: OpToken) -> Self {
        Self {
            driver,
            token,
            release_on_drop: true,
        }
    }

    #[inline]
    pub fn token(&self) -> OpToken {
        self.token
    }

    #[inline]
    pub fn completion_token(&self) -> CompletionToken {
        CompletionToken::user(self.token)
    }

    #[inline]
    pub fn completion_table(&self) -> SharedCompletionTable<D::UP, D::Error, D::Completion> {
        self.driver.completion_table()
    }

    #[inline]
    pub fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable> {
        self.driver.detached_cancel_table()
    }

    #[inline]
    pub fn create_waker(&self) -> Arc<dyn RemoteWaker<D::Error>> {
        self.driver.create_waker()
    }

    #[inline]
    pub fn set_payload(&mut self, payload: D::UP) {
        self.driver.slot_set_payload_raw(self.token, payload);
    }

    #[inline]
    pub fn submit(&mut self, op_in: &mut Option<D::Op>) -> DriverSubmitResult<D::Error> {
        self.driver.submit_op_raw(self.token, op_in)
    }

    #[inline]
    pub fn persist(mut self) -> SubmittedOpSlot {
        self.release_on_drop = false;
        SubmittedOpSlot { token: self.token }
    }

    #[inline]
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

    fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable>;

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

    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Error, Self::Completion>;

    fn register_completion_waker(&mut self, token: CompletionToken, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    fn cancel_op(&mut self, request: CancelRequest);

    fn register_chunk(
        &mut self,
        id: veloq_buf::heap::ChunkId,
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
    _phantom: std::marker::PhantomData<fn() -> D>,
}

impl<'a, D: Driver + ?Sized, P: ContextDriverProvider<D> + ?Sized> RuntimeContextDriver<'a, D, P> {
    pub fn new(provider: &'a P) -> Self {
        Self {
            provider,
            _phantom: std::marker::PhantomData,
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

    #[inline]
    fn reserve_op_raw(&mut self) -> DriverResult<OpToken, Self::Error> {
        self.provider.with_driver_mut(|d| d.reserve_op_raw())
    }

    #[inline]
    fn slot_table(&self) -> SharedDriverSlotTable<Self> {
        self.provider.with_driver_ref(|d| d.slot_table())
    }

    #[inline]
    fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable> {
        self.provider.with_driver_ref(|d| d.detached_cancel_table())
    }

    #[inline]
    fn slot_set_payload_raw(&mut self, token: OpToken, payload: Self::UP) {
        self.provider
            .with_driver_mut(|d| d.slot_set_payload_raw(token, payload))
    }

    #[inline]
    fn slot_take_payload_raw(&mut self, token: OpToken) -> Option<Self::UP> {
        self.provider
            .with_driver_mut(|d| d.slot_take_payload_raw(token))
    }

    #[inline]
    fn release_op_slot_raw(&mut self, token: OpToken) {
        self.provider
            .with_driver_mut(|d| d.release_op_slot_raw(token))
    }

    #[inline]
    fn submit_op_raw(
        &mut self,
        token: OpToken,
        op_in: &mut Option<Self::Op>,
    ) -> DriverSubmitResult<Self::Error> {
        self.provider
            .with_driver_mut(|d| d.submit_op_raw(token, op_in))
    }

    #[inline]
    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome, Self::Error> {
        self.provider.with_driver_mut(|d| d.drive(mode))
    }

    #[inline]
    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Error, Self::Completion> {
        self.provider.with_driver_ref(|d| d.completion_table())
    }

    #[inline]
    fn cancel_op(&mut self, request: CancelRequest) {
        self.provider.with_driver_mut(|d| d.cancel_op(request))
    }

    #[inline]
    fn register_chunk(
        &mut self,
        id: veloq_buf::heap::ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> DriverResult<(), Self::Error> {
        self.provider
            .with_driver_mut(|d| d.register_chunk(id, ptr, len))
    }

    #[inline]
    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>, Self::Error> {
        self.provider.with_driver_mut(|d| d.register_files(files))
    }

    #[inline]
    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<(), Self::Error> {
        self.provider.with_driver_mut(|d| d.unregister_files(files))
    }

    #[inline]
    fn create_waker(&self) -> Arc<dyn RemoteWaker<Self::Error>> {
        self.provider.with_driver_ref(|d| d.create_waker())
    }
}

#[inline]
pub fn drain_cancel_requests<D: Driver>(driver: &mut D) {
    let shared = driver.slot_table();
    let cancel_table = driver.detached_cancel_table();
    let word_count = cancel_table.cancel_word_count();
    for word_idx in 0..word_count {
        let mut bits = cancel_table.take_cancel_word(word_idx);
        while bits != 0 {
            let bit_idx = bits.trailing_zeros() as usize;
            bits &= bits - 1;

            let user_data = word_idx * 64 + bit_idx;
            let Some((generation, state)) = shared.slot_snapshot(user_data) else {
                continue;
            };
            if cancel_table.cancel_generation(user_data) == generation as u64
                && is_runnable_state(state)
            {
                driver.cancel_op(CancelRequest::abandon(OpToken::new(user_data, generation)));
            }
        }
    }
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
    E: std::error::Error + Send + Sync + 'static,
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
impl<'a, D: Driver + ?Sized + test_hooks::DriverTestHooks, P: ContextDriverProvider<D> + ?Sized>
    test_hooks::DriverTestHooks for RuntimeContextDriver<'a, D, P>
{
    #[inline]
    fn debug_chunk_register_attempts(&self) -> u64 {
        self.provider
            .with_driver_ref(|d| d.debug_chunk_register_attempts())
    }
}
