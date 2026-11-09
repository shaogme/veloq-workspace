use core::{mem::transmute, num::NonZeroUsize};

use crate::{
    boxed::Box,
    error::Error,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    panic::{AssertUnwindSafe, Panic},
    string::String,
    sync::{Arc, atomic::Ordering},
    thread::{
        AbortedError, LoomThread, Thread, ThreadErrorKind, ThreadId,
        shared::{
            CURRENT_THREAD, CURRENT_THREAD_STATUS, STATE_ABORTED, STATE_FINISHED, STATE_INCOMPLETE,
            STATE_PANICKED, SendSyncPanicPayload, ThreadPanicPayload, ThreadResultReceiver,
            ThreadSharedState, ThreadSharedStateTrait,
        },
        traits::{RawJoinHandleTrait, RawThreadErrorTrait, SystemImpl},
    },
    time::Duration,
};
use loom::thread::{
    JoinHandle as LoomJoinHandle, spawn as spawn_loom_thread, yield_now as yield_now_loom,
};

/// Loom 模拟平台下的原始加入句柄，包裹 loom::thread::JoinHandle
pub struct RawJoinHandle<'a, T> {
    inner: Option<LoomJoinHandle<()>>,
    result: Option<ThreadResultReceiver<'a, T>>,
    _marker: PhantomData<&'a ()>,
    thread: Thread,
}

unsafe impl<T: Send> Send for RawJoinHandle<'_, T> {}
unsafe impl<T: Send> Sync for RawJoinHandle<'_, T> {}

#[derive(Debug)]
pub enum RawThreadError {
    AlreadyJoined,
    ResultAlreadyTaken,
    ResultMissing,
    Panicked(Option<SendSyncPanicPayload>),
    Aborted,
}

impl Display for RawThreadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            RawThreadError::AlreadyJoined => write!(f, "thread already joined"),
            RawThreadError::ResultAlreadyTaken => write!(f, "thread result already taken"),
            RawThreadError::ResultMissing => write!(f, "thread result missing"),
            RawThreadError::Panicked(_) => write!(f, "thread panicked during execution"),
            RawThreadError::Aborted => write!(f, "thread execution was aborted"),
        }
    }
}

impl Error for RawThreadError {}

impl RawThreadErrorTrait for RawThreadError {
    fn kind(&self) -> ThreadErrorKind {
        match self {
            RawThreadError::AlreadyJoined => ThreadErrorKind::AlreadyJoined,
            RawThreadError::ResultAlreadyTaken => ThreadErrorKind::ResultAlreadyTaken,
            RawThreadError::ResultMissing => ThreadErrorKind::ResultMissing,
            RawThreadError::Panicked(_) => ThreadErrorKind::Panicked,
            RawThreadError::Aborted => ThreadErrorKind::Aborted,
        }
    }

    #[inline]
    fn from_panic(payload: ThreadPanicPayload) -> Self {
        RawThreadError::Panicked(SendSyncPanicPayload::from_payload(payload))
    }

    #[inline]
    fn take_panic(&mut self) -> ThreadPanicPayload {
        if let RawThreadError::Panicked(payload) = self {
            SendSyncPanicPayload::take_payload(payload)
        } else {
            Default::default()
        }
    }
}

impl<'a, T: Send> RawJoinHandle<'a, T> {
    pub(crate) fn thread(&self) -> &Thread {
        &self.thread
    }

    pub(crate) fn thread(&self) -> &Thread {
        &self.thread
    }

    pub fn join(mut self) -> Result<T, RawThreadError> {
        let handle = self.inner.take().ok_or(RawThreadError::AlreadyJoined)?;

        // 等待 loom 线程结束
        if handle.join().is_err() {
            return Err(RawThreadError::Panicked(None));
        }

        let receiver = self
            .result
            .take()
            .ok_or(RawThreadError::ResultAlreadyTaken)?;
        let state = receiver.clone_state();

        match unsafe { receiver.receive() } {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Err(RawThreadError::ResultMissing),
            Err(STATE_PANICKED) => {
                let payload = unsafe { state.take_panic() };
                Err(RawThreadError::from_panic(payload))
            }
            Err(STATE_ABORTED) => Err(RawThreadError::Aborted),
            Err(_) => Err(RawThreadError::Aborted),
        }
    }

    pub fn abort(&self) -> Result<(), RawThreadError> {
        if let Some(ref receiver) = self.result {
            receiver.set_aborted();
        }
        Ok(())
    }
}

impl<'a, T: Send> RawJoinHandleTrait<T> for RawJoinHandle<'a, T> {
    type Error = RawThreadError;

    fn join(self) -> Result<T, Self::Error> {
        Self::join(self)
    }

    fn abort(&self) -> Result<(), Self::Error> {
        Self::abort(self)
    }
}

pub struct System;

impl SystemImpl for System {
    type Error = RawThreadError;
    type RawJoinHandle<'a, T: Send>
        = RawJoinHandle<'a, T>
    where
        T: 'a;

    fn spawn<'a, F, T>(
        name: Option<String>,
        _stack_size: Option<usize>,
        f: F,
    ) -> Result<Self::RawJoinHandle<'a, T>, Self::Error>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        type BoxF<'a, T> = Box<dyn FnOnce() -> T + Send + 'a>;

        let thread = Thread::new(name.clone());

        let state = Arc::new(ThreadSharedState::new(
            Box::new(f) as BoxF<'a, T>,
            name,
            thread.clone(),
        ));

        let receiver = ThreadResultReceiver::new(state.clone());

        // 擦除生命周期，包括将 T 转换为具有 'static 生命周期限制的等价类型，以安全地传递给具有 'static 要求的 loom::thread::spawn
        // 在 spawn 结束前，闭包执行完毕并将结果写回，整个过程中 Arc 控制了生命周期。
        trait ErasedSharedState: Send + Sync {
            fn run(&self);
        }

        impl<F, T> ErasedSharedState for ThreadSharedState<F, T>
        where
            F: FnOnce() -> T + Send,
            T: Send,
        {
            fn run(&self) {
                let _ = CURRENT_THREAD.set_owned(self.thread().clone());
                CURRENT_THREAD_STATUS.with_or_default(|cell| {
                    cell.set(Some(self.status_ptr()));
                });

                struct ThreadStatusGuard;
                impl Drop for ThreadStatusGuard {
                    fn drop(&mut self) {
                        CURRENT_THREAD_STATUS.with_or_default(|cell| {
                            cell.set(None);
                        });
                    }
                }
                let _status_guard = ThreadStatusGuard;

                let mut sentinel = self.sentinel();

                if let Some(f) = unsafe { self.take_closure() } {
                    let res = Panic::catch_unwind(AssertUnwindSafe::new(f));
                    match res {
                        Ok(r) => {
                            unsafe { self.store_result(r) };
                            sentinel.mark_successful();
                            self.mark_finished();
                        }
                        Err(err) => unsafe { self.store_panic_payload(err) },
                    }
                }
            }
        }

        let ptr = Arc::into_raw(state.clone());
        let trait_ptr: *const (dyn ErasedSharedState + 'a) =
            ptr as *const (dyn ErasedSharedState + 'a);
        let trait_ptr_static: *const (dyn ErasedSharedState + 'static) =
            unsafe { transmute(trait_ptr) };
        let erased_state_static: Arc<dyn ErasedSharedState + 'static> =
            unsafe { Arc::from_raw(trait_ptr_static) };

        let handle = spawn_loom_thread(move || {
            erased_state_static.run();
        });

        Ok(RawJoinHandle {
            inner: Some(handle),
            result: Some(receiver),
            _marker: PhantomData,
            thread,
        })
    }

    fn yield_now() -> Result<bool, AbortedError> {
        let aborted = CURRENT_THREAD_STATUS.with_or_default(|cell| {
            if let Some(status_ptr) = cell.get() {
                let status = unsafe { &*status_ptr };
                status.load(Ordering::Acquire) == STATE_ABORTED
            } else {
                false
            }
        });
        if aborted {
            return Err(AbortedError);
        }
        yield_now_loom();
        Ok(true)
    }

    fn sleep(_dur: Duration) -> Result<(), AbortedError> {
        let aborted = || {
            CURRENT_THREAD_STATUS.with_or_default(|cell| {
                if let Some(status_ptr) = cell.get() {
                    let status = unsafe { &*status_ptr };
                    status.load(Ordering::Acquire) == STATE_ABORTED
                } else {
                    false
                }
            })
        };

        if aborted() {
            return Err(AbortedError);
        }

        yield_now_loom();

        if aborted() {
            return Err(AbortedError);
        }

        Ok(())
    }

    fn current_id() -> ThreadId {
        LoomThread::current_id()
    }

    fn available_parallelism() -> Result<NonZeroUsize, Self::Error> {
        Ok(NonZeroUsize::new(1).unwrap())
    }
}
