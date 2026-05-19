use crate::slot;
use crate::slot::is_runnable_state;
use crate::{BorrowedRawHandle, IoFd, OwnedRawHandle, RawHandleMeta, SlotSidecar};
use crate::{DriverErrorReport, DriverResult};

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

pub type SharedSlotTable<Op, UP, S, C> = Arc<slot::SlotTable<Op, UP, S, C>>;

pub trait Driver {
    type Op: PlatformOp;
    type UP: Send;
    type Raw: RawHandleMeta;
    type Sidecar: SlotSidecar;
    type Completion: CompletionValue;

    fn reserve_op(&mut self) -> DriverResult<(usize, u32)>;

    fn slot_table(&self) -> SharedSlotTable<Self::Op, Self::UP, Self::Sidecar, Self::Completion>;

    fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable>;

    fn slot_set_payload(&mut self, user_data: usize, payload: Self::UP);

    fn slot_take_payload(&mut self, user_data: usize) -> Option<Self::UP>;

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>>;

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome>;

    fn completion_queue(&self) -> SharedCompletionQueue;

    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Completion>;

    fn try_pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completion_queue().pop()
    }

    fn register_completion_waker(&mut self, token: u64, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    fn cancel_op(&mut self, user_data: usize);

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()>;

    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>>;

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()>;

    fn create_waker(&self) -> Arc<dyn RemoteWaker>;
}

impl<D: Driver + ?Sized> Driver for &mut D {
    type Op = D::Op;
    type UP = D::UP;
    type Raw = D::Raw;
    type Sidecar = D::Sidecar;
    type Completion = D::Completion;

    #[inline]
    fn reserve_op(&mut self) -> DriverResult<(usize, u32)> {
        (**self).reserve_op()
    }

    #[inline]
    fn slot_table(&self) -> SharedSlotTable<Self::Op, Self::UP, Self::Sidecar, Self::Completion> {
        (**self).slot_table()
    }

    #[inline]
    fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable> {
        (**self).detached_cancel_table()
    }

    #[inline]
    fn slot_set_payload(&mut self, user_data: usize, payload: Self::UP) {
        (**self).slot_set_payload(user_data, payload)
    }

    #[inline]
    fn slot_take_payload(&mut self, user_data: usize) -> Option<Self::UP> {
        (**self).slot_take_payload(user_data)
    }

    #[inline]
    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        (**self).submit(user_data, op_in, binder)
    }

    #[inline]
    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome> {
        (**self).drive(mode)
    }

    #[inline]
    fn completion_queue(&self) -> SharedCompletionQueue {
        (**self).completion_queue()
    }

    #[inline]
    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Completion> {
        (**self).completion_table()
    }

    #[inline]
    fn cancel_op(&mut self, user_data: usize) {
        (**self).cancel_op(user_data)
    }

    #[inline]
    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        (**self).register_chunk(id, ptr, len)
    }

    #[inline]
    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>> {
        (**self).register_files(files)
    }

    #[inline]
    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        (**self).unregister_files(files)
    }

    #[inline]
    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
        (**self).create_waker()
    }
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

    #[inline]
    fn reserve_op(&mut self) -> DriverResult<(usize, u32)> {
        self.provider.with_driver_mut(|d| d.reserve_op())
    }

    #[inline]
    fn slot_table(&self) -> SharedSlotTable<Self::Op, Self::UP, Self::Sidecar, Self::Completion> {
        self.provider.with_driver_ref(|d| d.slot_table())
    }

    #[inline]
    fn detached_cancel_table(&self) -> Arc<slot::DetachedCancelTable> {
        self.provider.with_driver_ref(|d| d.detached_cancel_table())
    }

    #[inline]
    fn slot_set_payload(&mut self, user_data: usize, payload: Self::UP) {
        self.provider
            .with_driver_mut(|d| d.slot_set_payload(user_data, payload))
    }

    #[inline]
    fn slot_take_payload(&mut self, user_data: usize) -> Option<Self::UP> {
        self.provider
            .with_driver_mut(|d| d.slot_take_payload(user_data))
    }

    #[inline]
    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        self.provider
            .with_driver_mut(|d| d.submit(user_data, op_in, binder))
    }

    #[inline]
    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome> {
        self.provider.with_driver_mut(|d| d.drive(mode))
    }

    #[inline]
    fn completion_queue(&self) -> SharedCompletionQueue {
        self.provider.with_driver_ref(|d| d.completion_queue())
    }

    #[inline]
    fn completion_table(&self) -> SharedCompletionTable<Self::UP, Self::Completion> {
        self.provider.with_driver_ref(|d| d.completion_table())
    }

    #[inline]
    fn cancel_op(&mut self, user_data: usize) {
        self.provider.with_driver_mut(|d| d.cancel_op(user_data))
    }

    #[inline]
    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()> {
        self.provider
            .with_driver_mut(|d| d.register_chunk(id, ptr, len))
    }

    #[inline]
    fn register_files<'f>(
        &mut self,
        files: Vec<RegisterFd<'f, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>> {
        self.provider.with_driver_mut(|d| d.register_files(files))
    }

    #[inline]
    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()> {
        self.provider.with_driver_mut(|d| d.unregister_files(files))
    }

    #[inline]
    fn create_waker(&self) -> Arc<dyn RemoteWaker> {
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
                driver.cancel_op(user_data);
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

pub trait RemoteWaker: Send + Sync {
    fn wake(&self) -> DriverResult<()>;
}

#[must_use]
pub struct Outcome<T>(T);

impl<T> Outcome<T> {
    #[inline]
    pub fn into_inner(self) -> T {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubmitStatus {
    /// Operation successfully submitted or queued. It *will* eventually produce
    /// a completion result in the `CompletionTable`.
    InFlight,
    /// Operation failed synchronously and no completion result will be produced.
    Void,
}

#[derive(Default)]
pub struct SubmitBinder;

impl SubmitBinder {
    #[inline]
    pub fn new() -> Self {
        Self
    }

    #[inline]
    pub fn ok(
        self,
        poll: Poll<()>,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        Outcome(Ok(poll))
    }

    #[inline]
    pub fn err(
        self,
        err: DriverErrorReport,
        status: SubmitStatus,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>> {
        Outcome(Err((err, status)))
    }
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
