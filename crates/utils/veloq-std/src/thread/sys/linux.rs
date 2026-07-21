use core::num::NonZeroUsize;

use super::{SafeUnsafeCell, Sentinel, ThreadResultReceiver, ThreadSharedState};

use crate::{
    cell::UnsafeCell,
    error::Error,
    ffi::{c_int, c_void},
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    mem::zeroed,
    ptr::{null, null_mut},
    string::String,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread::{
        AbortedError, ThreadErrorKind, ThreadId,
        traits::{RawJoinHandleTrait, RawThreadErrorTrait, SystermImpl},
    },
    time::Duration,
};
use libc::{
    nanosleep, pthread_create, pthread_detach, pthread_join, pthread_t, sched_yield, timespec,
};

use super::SendSyncPanicPayload;

/// Linux 平台下的原始线程加入句柄
pub struct RawJoinHandle<'a, T> {
    thread_id: Option<pthread_t>,
    result: Option<ThreadResultReceiver<'a, T>>,
    _marker: PhantomData<&'a ()>,
    pub(crate) thread: crate::thread::Thread,
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
    fn from_panic(payload: super::ThreadPanicPayload) -> Self {
        RawThreadError::Panicked(super::from_panic_payload(payload))
    }

    #[inline]
    fn take_panic(&mut self) -> super::ThreadPanicPayload {
        if let RawThreadError::Panicked(payload) = self {
            super::take_panic_payload(payload)
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

    let _ = super::CURRENT_THREAD.set_owned(state.thread.clone());

    super::CURRENT_THREAD_STATUS.with_or_default(|cell| {
        cell.set(Some(&state.status as *const AtomicU8));
    });

    if let Some(ref name) = state.name {
        let mut name_bytes = name.as_bytes().to_vec();
        if name_bytes.len() > 15 {
            name_bytes.truncate(15);
        }
        name_bytes.push(0);
        unsafe {
            let current_thread = libc::pthread_self();
            let _ = libc::pthread_setname_np(current_thread, name_bytes.as_ptr() as *const _);
        }
    }

    struct ThreadStatusGuard;
    impl Drop for ThreadStatusGuard {
        fn drop(&mut self) {
            super::CURRENT_THREAD_STATUS.with_or_default(|cell| {
                cell.set(None);
            });
        }
    }
    let _status_guard = ThreadStatusGuard;

    let mut sentinel = Sentinel {
        status: &state.status,
        panicked: true,
    };

    if let Some(f) = unsafe { state.closure.with_mut(|x| x.take()) } {
        let res = crate::panic::catch_unwind_safe(f);
        match res {
            Ok(r) => {
                unsafe {
                    state.result.with_mut(|opt| *opt = Some(r));
                }
                sentinel.panicked = false;
                let _ = state.status.compare_exchange(
                    super::STATE_INCOMPLETE,
                    super::STATE_FINISHED,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                );
            }
            Err(err) => unsafe {
                state.panic_payload.with_mut(|opt| *opt = err);
            },
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
    let thread = crate::thread::Thread::new(name.clone());

    let state = Arc::new(ThreadSharedState {
        closure: UnsafeCell::new(Some(f)),
        status: AtomicU8::new(super::STATE_INCOMPLETE),
        result: SafeUnsafeCell::new(None),
        panic_payload: SafeUnsafeCell::new(None),
        name,
        thread: thread.clone(),
    });

    let receiver = ThreadResultReceiver {
        state: state.clone(),
    };

    let param = Arc::into_raw(state) as *mut c_void;
    let mut thread_id: pthread_t = unsafe { zeroed() };

    unsafe {
        let entry = thread_entry_posix::<F, T>;

        let mut attr: libc::pthread_attr_t = zeroed();
        let mut attr_ptr = null();
        if let Some(size) = stack_size
            && libc::pthread_attr_init(&mut attr) == 0
        {
            // Ensure stack size is at least 16KB to prevent failure
            let target_size = core::cmp::max(size, 16384);
            if libc::pthread_attr_setstacksize(&mut attr, target_size as _) == 0 {
                attr_ptr = &attr;
            }
        }

        let res = pthread_create(&mut thread_id, attr_ptr, entry, param);

        if !attr_ptr.is_null() {
            let _ = libc::pthread_attr_destroy(&mut attr);
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
            let state = receiver.state.clone();
            match receiver.receive() {
                Ok(Some(val)) => Ok(val),
                Ok(None) => Err(RawThreadError::ResultMissing),
                Err(super::STATE_PANICKED) => {
                    let payload = state.take_panic();
                    Err(RawThreadError::from_panic(payload))
                }
                Err(super::STATE_ABORTED) => Err(RawThreadError::Aborted),
                Err(_) => Err(RawThreadError::Aborted),
            }
        }
    }

    pub fn abort(&self) -> Result<(), RawThreadError> {
        if let Some(ref receiver) = self.result {
            receiver.state.set_aborted();
        }
        Ok(())
    }

    pub fn yield_now() -> Result<bool, AbortedError> {
        let aborted = super::CURRENT_THREAD_STATUS.with_or_default(|cell| {
            if let Some(status_ptr) = cell.get() {
                let status = unsafe { &*status_ptr };
                status.load(Ordering::Acquire) == super::STATE_ABORTED
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
pub struct Systerm;

impl SystermImpl for Systerm {
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
            super::CURRENT_THREAD_STATUS.with_or_default(|cell| {
                if let Some(status_ptr) = cell.get() {
                    let status = unsafe { &*status_ptr };
                    status.load(Ordering::Acquire) == super::STATE_ABORTED
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
        crate::thread::current().id()
    }

    fn available_parallelism() -> Result<NonZeroUsize, Self::Error> {
        let val = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
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
