use crate::slot;
use crate::slot::is_runnable_state;
use crate::{BorrowedRawHandle, IoFd, OwnedRawHandle, RawHandleMeta, SlotSidecar};
use crate::{DriverErrorReport, DriverResult};

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

pub trait Driver {
    type Op: PlatformOp;
    type Raw: RawHandleMeta;
    type Sidecar: SlotSidecar;
    type Completion: CompletionValue;

    fn reserve_op(&mut self) -> DriverResult<(usize, u32)>;

    fn slot_table(
        &self,
    ) -> std::sync::Arc<slot::SlotTable<Self::Op, Self::Sidecar, Self::Completion>>;

    fn detached_cancel_table(&self) -> std::sync::Arc<slot::DetachedCancelTable>;

    fn slot_set_payload(&mut self, user_data: usize, payload: slot::ErasedPayload);

    fn slot_take_payload(&mut self, user_data: usize) -> Option<slot::ErasedPayload>;

    fn submit(
        &mut self,
        user_data: usize,
        op_in: &mut Option<Self::Op>,
        binder: SubmitBinder,
    ) -> Outcome<Result<Poll<()>, (DriverErrorReport, SubmitStatus)>>;

    fn drive(&mut self, mode: DriveMode) -> DriverResult<DriveOutcome>;

    fn completion_queue(&self) -> SharedCompletionQueue;

    fn completion_table(&self) -> SharedCompletionTable<Self::Completion>;

    fn try_pop_completion(&mut self) -> Option<CompletionEvent> {
        self.completion_queue().pop()
    }

    fn register_completion_waker(&mut self, token: u64, waker: &Waker) {
        self.completion_table().register_waker(token, waker);
    }

    fn cancel_op(&mut self, user_data: usize);

    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> DriverResult<()>;

    fn register_files<'a>(
        &mut self,
        files: Vec<RegisterFd<'a, Self::Raw>>,
    ) -> DriverResult<Vec<IoFd>>;

    fn unregister_files(&mut self, files: Vec<IoFd>) -> DriverResult<()>;

    fn warmup_udp_socket(
        &mut self,
        fd: IoFd,
        buf_capacity: std::num::NonZeroUsize,
        credits: usize,
    ) -> DriverResult<()>;

    fn create_waker(&self) -> std::sync::Arc<dyn RemoteWaker>;

    fn set_registrar(&mut self, registrar: Box<dyn veloq_buf::BufferRegistrar>);
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
