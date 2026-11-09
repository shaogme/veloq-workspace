use core::{cmp::max, num::NonZeroUsize};

use crate::{
    error::Error,
    ffi::{c_int, c_void},
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::zeroed,
    panic::{AssertUnwindSafe, Panic},
    ptr::{null, null_mut},
    string::String,
    sync::{Arc, atomic::Ordering},
    thread::{
        AbortedError, NativeThread, Thread, ThreadErrorKind, ThreadId,
        impls::shared::{
            CURRENT_THREAD, CURRENT_THREAD_STATUS, STATE_ABORTED, STATE_PANICKED,
            SendSyncPanicPayload, ThreadPanicPayload, ThreadResultReceiver, ThreadSharedState,
            ThreadSharedStateTrait,
        },
        traits::{RawJoinHandleTrait, RawThreadErrorTrait, SystemImpl},
    },
    time::Duration,
};
use libc::{
    _SC_NPROCESSORS_ONLN, nanosleep, pthread_attr_destroy, pthread_attr_init,
    pthread_attr_setstacksize, pthread_attr_t, pthread_create, pthread_detach, pthread_join,
    pthread_self, pthread_setname_np, pthread_t, sched_yield, sysconf, timespec,
};

/// Linux 平台下的原始线程加入句柄
pub struct RawJoinHandle<'a, T> {
    thread_id: Option<pthread_t>,
    result: Option<ThreadResultReceiver<'a, T>>,
    _marker: PhantomData<&'a ()>,
    thread: Thread,
}

unsafe impl<T: Send> Send for RawJoinHandle<'_, T> {}

unsafe impl<T: Send> Sync for RawJoinHandle<'_, T> {}

#[derive(Debug)]
pub enum RawThreadError {
    CreationFailed(c_int),
    JoinFailed(c_int),
    AbortFailed(c_int),
    AlreadyJoined,
    ResultAlreadyTaken,
    ResultMissing,
    Panicked(Option<SendSyncPanicPayload>),
    Aborted,
}

impl Display for RawThreadError {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        match self {
            RawThreadError::CreationFailed(err) => write!(f, "thread creation failed: {}", err),
            RawThreadError::JoinFailed(err) => write!(f, "thread join failed: {}", err),
            RawThreadError::AbortFailed(err) => write!(f, "thread abort failed: {}", err),
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
            RawThreadError::CreationFailed(_) => ThreadErrorKind::CreationFailed,
            RawThreadError::JoinFailed(_) => ThreadErrorKind::JoinFailed,
            RawThreadError::AbortFailed(_) => ThreadErrorKind::AbortFailed,
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

extern "C" fn thread_entry_posix<F, T>(param: *mut c_void) -> *mut c_void
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    let state = unsafe { Arc::from_raw(param as *const ThreadSharedState<F, T>) };

    let _ = CURRENT_THREAD.set_owned(state.thread().clone());

    CURRENT_THREAD_STATUS.with_or_default(|cell| {
        cell.set(Some(state.status_ptr()));
    });

    if let Some(name) = state.name() {
        let mut name_bytes = name.as_bytes().to_vec();
        if name_bytes.len() > 15 {
            name_bytes.truncate(15);
        }
        name_bytes.push(0);
        unsafe {
            let current_thread = pthread_self();
            let _ = pthread_setname_np(current_thread, name_bytes.as_ptr() as *const _);
        }
    }

    struct ThreadStatusGuard;
    impl Drop for ThreadStatusGuard {
        fn drop(&mut self) {
            CURRENT_THREAD_STATUS.with_or_default(|cell| {
                cell.set(None);
            });
        }
    }
    let _status_guard = ThreadStatusGuard;

    let mut sentinel = state.sentinel();

    if let Some(f) = unsafe { state.take_closure() } {
        let res = Panic::catch_unwind(AssertUnwindSafe::new(f));
        match res {
            Ok(r) => {
                unsafe { state.store_result(r) };
                sentinel.mark_successful();
                state.mark_finished();
            }
            Err(err) => unsafe { state.store_panic_payload(err) },
        }
    }

    null_mut()
}

fn spawn<'a, F, T>(
    name: Option<String>,
    stack_size: Option<usize>,
    f: F,
) -> Result<RawJoinHandle<'a, T>, RawThreadError>
where
    F: FnOnce() -> T + Send + 'a,
    T: Send + 'a,
{
    let thread = Thread::new(name.clone());

    let state = Arc::new(ThreadSharedState::new(f, name, thread.clone()));

    let receiver = ThreadResultReceiver::new(unsafe {
        Arc::cast_unsized(state.clone(), |p| {
            p as *const (dyn ThreadSharedStateTrait<T> + 'a)
        })
    });

    let param = Arc::into_raw(state) as *mut c_void;
    let mut thread_id: pthread_t = unsafe { zeroed() };

    unsafe {
        let entry = thread_entry_posix::<F, T>;

        let mut attr: pthread_attr_t = zeroed();
        let mut attr_ptr = null();
        if let Some(size) = stack_size
            && pthread_attr_init(&mut attr) == 0
        {
            // Ensure stack size is at least 16KB to prevent failure
            let target_size = max(size, 16384);
            if pthread_attr_setstacksize(&mut attr, target_size as _) == 0 {
                attr_ptr = &attr;
            }
        }

        let res = pthread_create(&mut thread_id, attr_ptr, entry, param);

        if !attr_ptr.is_null() {
            let _ = pthread_attr_destroy(&mut attr);
        }

        if res != 0 {
            let _ = Arc::from_raw(param as *const ThreadSharedState<F, T>);
            return Err(RawThreadError::CreationFailed(res));
        }

        Ok(RawJoinHandle {
            thread_id: Some(thread_id),
            result: Some(receiver),
            _marker: PhantomData,
            thread,
        })
    }
}

impl<'a, T: Send> RawJoinHandle<'a, T> {
    pub(crate) fn thread(&self) -> &Thread {
        &self.thread
    }

    pub fn join(mut self) -> Result<T, RawThreadError> {
        let thread_id = self.thread_id.take().ok_or(RawThreadError::AlreadyJoined)?;
        unsafe {
            let res = pthread_join(thread_id, null_mut());
            if res != 0 {
                return Err(RawThreadError::JoinFailed(res));
            }
            let receiver = self
                .result
                .take()
                .ok_or(RawThreadError::ResultAlreadyTaken)?;
            let state = receiver.clone_state();
            match receiver.receive() {
                Ok(Some(val)) => Ok(val),
                Ok(None) => Err(RawThreadError::ResultMissing),
                Err(STATE_PANICKED) => {
                    let payload = state.take_panic();
                    Err(RawThreadError::from_panic(payload))
                }
                Err(STATE_ABORTED) => Err(RawThreadError::Aborted),
                Err(_) => Err(RawThreadError::Aborted),
            }
        }
    }

    pub fn abort(&self) -> Result<(), RawThreadError> {
        if let Some(ref receiver) = self.result {
            receiver.set_aborted();
        }
        Ok(())
    }

    pub fn yield_now() -> Result<bool, AbortedError> {
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
        unsafe { Ok(sched_yield() == 0) }
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

/// Linux 平台下的平台实现结构体
pub struct System;

impl SystemImpl for System {
    type Error = RawThreadError;
    type RawJoinHandle<'a, T: Send>
        = RawJoinHandle<'a, T>
    where
        T: 'a;

    fn spawn<'a, F, T>(
        name: Option<String>,
        stack_size: Option<usize>,
        f: F,
    ) -> Result<Self::RawJoinHandle<'a, T>, Self::Error>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        spawn(name, stack_size, f)
    }

    fn yield_now() -> Result<bool, AbortedError> {
        RawJoinHandle::<()>::yield_now()
    }

    fn sleep(dur: Duration) -> Result<(), AbortedError> {
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

        let mut req = timespec {
            tv_sec: dur.as_secs() as _,
            tv_nsec: dur.subsec_nanos() as _,
        };

        unsafe {
            loop {
                let mut rem = zeroed();
                let res = nanosleep(&req, &mut rem);
                if res == 0 {
                    break;
                }

                if aborted() {
                    return Err(AbortedError);
                }

                req = rem;
            }
        }

        if aborted() {
            return Err(AbortedError);
        }

        Ok(())
    }

    fn current_id() -> ThreadId {
        NativeThread::current_id()
    }

    fn available_parallelism() -> Result<NonZeroUsize, Self::Error> {
        let val = unsafe { sysconf(_SC_NPROCESSORS_ONLN) };
        if let Some(n) = NonZeroUsize::new(val as usize) {
            return Ok(n);
        }
        Ok(NonZeroUsize::new(1).unwrap())
    }
}

impl<'a, T> Drop for RawJoinHandle<'a, T> {
    fn drop(&mut self) {
        if let Some(thread_id) = self.thread_id.take() {
            unsafe {
                let _ = pthread_detach(thread_id);
            }
        }
    }
}
