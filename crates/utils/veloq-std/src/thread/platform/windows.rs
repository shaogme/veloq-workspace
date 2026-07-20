use core::num::NonZeroUsize;

use super::{SafeUnsafeCell, Sentinel, ThreadResultReceiver, ThreadSharedState};
use crate::{
    cell::UnsafeCell,
    error::Error,
    ffi::c_void,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    ptr::{null, null_mut},
    string::String,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread::{
        AbortedError, ThreadErrorKind, ThreadId,
        traits::{PlatformImpl, RawJoinHandleTrait, RawThreadErrorTrait},
    },
    time::Duration,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, GetLastError, HANDLE, WAIT_OBJECT_0},
    System::Threading::{
        ALL_PROCESSOR_GROUPS, CreateThread, GetActiveProcessorCount, GetCurrentThread,
        GetCurrentThreadId, INFINITE, SetThreadDescription, Sleep, SwitchToThread,
        WaitForSingleObject,
    },
};

use super::SendSyncPanicPayload;

/// Windows 平台下的原始线程加入句柄
pub struct RawJoinHandle<'a, T> {
    handle: Option<HANDLE>,
    result: Option<ThreadResultReceiver<'a, T>>,
    _marker: PhantomData<&'a ()>,
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

unsafe extern "system" fn thread_entry_win<F, T>(param: *mut c_void) -> u32
where
    F: FnOnce() -> T + Send,
    T: Send,
{
    let state = unsafe { Arc::from_raw(param as *const ThreadSharedState<F, T>) };

    super::CURRENT_THREAD_STATUS.with_or_default(|cell| {
        cell.set(Some(&state.status as *const AtomicU8));
    });

    if let Some(ref name) = state.name {
        let mut name_u16: crate::vec::Vec<u16> = name.encode_utf16().collect();
        name_u16.push(0);
        unsafe {
            let current_thread = GetCurrentThread();
            let _ = SetThreadDescription(current_thread, name_u16.as_ptr());
        }
        let _ = super::CURRENT_THREAD_NAME.set_owned(name.clone());
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
    #[cfg(feature = "loom")]
    type BoxF<'a, T> = crate::boxed::Box<dyn FnOnce() -> T + Send + 'a>;

    #[cfg(not(feature = "loom"))]
    let state = Arc::new(ThreadSharedState {
        closure: UnsafeCell::new(Some(f)),
        status: AtomicU8::new(super::STATE_INCOMPLETE),
        result: SafeUnsafeCell::new(None),
        panic_payload: SafeUnsafeCell::new(None),
        name,
    });

    #[cfg(feature = "loom")]
    let state = Arc::new(ThreadSharedState {
        closure: UnsafeCell::new(Some(crate::boxed::Box::new(f) as BoxF<'a, T>)),
        status: AtomicU8::new(super::STATE_INCOMPLETE),
        result: SafeUnsafeCell::new(None),
        panic_payload: SafeUnsafeCell::new(None),
        name,
    });

    let receiver = ThreadResultReceiver {
        state: state.clone(),
    };

    let param = Arc::into_raw(state) as *mut c_void;

    unsafe {
        #[cfg(not(feature = "loom"))]
        let entry = thread_entry_win::<F, T>;
        #[cfg(feature = "loom")]
        let entry = thread_entry_win::<BoxF<'a, T>, T>;

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
            #[cfg(not(feature = "loom"))]
            let _ = Arc::from_raw(param as *const ThreadSharedState<F, T>);
            #[cfg(feature = "loom")]
            let _ = Arc::from_raw(param as *const ThreadSharedState<BoxF<'a, T>, T>);
            return Err(RawThreadError::CreationFailed(err));
        }

        Ok(RawJoinHandle {
            handle: Some(handle),
            result: Some(receiver),
            _marker: PhantomData,
        })
    }
}

impl<'a, T: Send> RawJoinHandle<'a, T> {
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
pub struct Platform;

impl PlatformImpl for Platform {
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
        let id = unsafe { GetCurrentThreadId() };
        ThreadId(id as u64)
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
