use core::num::NonZeroUsize;

use crate::{
    error::Error,
    ffi::c_void,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
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
    vec::Vec,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, GetLastError, HANDLE, WAIT_OBJECT_0},
    System::Threading::{
        ALL_PROCESSOR_GROUPS, CreateThread, GetActiveProcessorCount, GetCurrentThread, INFINITE,
        SetThreadDescription, Sleep, SwitchToThread, WaitForSingleObject,
    },
};

/// Windows 平台下的原始线程加入句柄
pub struct RawJoinHandle<'a, T> {
    handle: Option<HANDLE>,
    result: Option<ThreadResultReceiver<'a, T>>,
    _marker: PhantomData<&'a ()>,
    thread: Thread,
}

unsafe impl<T: Send> Send for RawJoinHandle<'_, T> {}

unsafe impl<T: Send> Sync for RawJoinHandle<'_, T> {}

#[derive(Debug)]
pub enum RawThreadError {
    CreationFailed(u32),
    JoinFailed(u32),
    AbortFailed(u32),
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

unsafe extern "system" fn thread_entry_win<F, T>(param: *mut c_void) -> u32
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
        let mut name_u16: Vec<u16> = name.encode_utf16().collect();
        name_u16.push(0);
        unsafe {
            let current_thread = GetCurrentThread();
            let _ = SetThreadDescription(current_thread, name_u16.as_ptr());
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

    0
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

    unsafe {
        let entry = thread_entry_win::<F, T>;

        let handle = CreateThread(
            null(),
            stack_size.unwrap_or(0),
            Some(entry),
            param,
            0,
            null_mut(),
        );

        if handle.is_null() {
            let err = GetLastError();
            let _ = Arc::from_raw(param as *const ThreadSharedState<F, T>);
            return Err(RawThreadError::CreationFailed(err));
        }

        Ok(RawJoinHandle {
            handle: Some(handle),
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
        let handle = self.handle.take().ok_or(RawThreadError::AlreadyJoined)?;
        unsafe {
            let res = WaitForSingleObject(handle, INFINITE);
            let _ = CloseHandle(handle);
            if res != WAIT_OBJECT_0 {
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
        unsafe { Ok(SwitchToThread() != 0) }
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

/// Windows 平台下的平台实现结构体
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

        let ms = if dur.as_millis() > u32::MAX as u128 {
            u32::MAX
        } else {
            dur.as_millis() as u32
        };

        unsafe {
            Sleep(ms);
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
        let count = unsafe { GetActiveProcessorCount(ALL_PROCESSOR_GROUPS) };
        if let Some(n) = NonZeroUsize::new(count as usize) {
            return Ok(n);
        }
        Ok(NonZeroUsize::new(1).unwrap())
    }
}

impl<'a, T> Drop for RawJoinHandle<'a, T> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            unsafe {
                let _ = CloseHandle(handle);
            }
        }
    }
}
