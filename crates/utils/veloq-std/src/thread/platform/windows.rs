use super::{SafeUnsafeCell, Sentinel, ThreadResultReceiver, ThreadSharedState};
use crate::{
    cell::UnsafeCell,
    error::Error,
    ffi::c_void,
    fmt::{Display, Formatter, Result as FmtResult},
    marker::PhantomData,
    ptr::{null, null_mut},
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicU32, Ordering},
    },
    thread::{
        AbortedError, ThreadErrorKind,
        traits::{PlatformImpl, RawJoinHandleTrait, RawThreadErrorTrait},
    },
    time::Duration,
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, GetLastError, HANDLE, WAIT_OBJECT_0},
    System::Threading::{
        CreateThread, INFINITE, Sleep, SwitchToThread, WaitForSingleObject, WaitOnAddress,
        WakeByAddressAll, WakeByAddressSingle,
    },
};

#[cfg(feature = "std")]
use super::SendSyncPanicPayload;

#[cfg(feature = "std")]
use crate::{any::Any, boxed::Box};

#[cfg(feature = "std")]
use std::panic::{AssertUnwindSafe, catch_unwind};

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
    #[cfg(feature = "std")]
    Panicked(Option<SendSyncPanicPayload>),
    #[cfg(not(feature = "std"))]
    Panicked,
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
            #[cfg(feature = "std")]
            RawThreadError::Panicked(_) => write!(f, "thread panicked during execution"),
            #[cfg(not(feature = "std"))]
            RawThreadError::Panicked => write!(f, "thread panicked during execution"),
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
            #[cfg(feature = "std")]
            RawThreadError::Panicked(_) => ThreadErrorKind::Panicked,
            #[cfg(not(feature = "std"))]
            RawThreadError::Panicked => ThreadErrorKind::Panicked,
            RawThreadError::Aborted => ThreadErrorKind::Aborted,
        }
    }

    #[cfg(feature = "std")]
    fn from_panic(payload: Box<dyn Any + Send + 'static>) -> Self {
        RawThreadError::Panicked(Some(SendSyncPanicPayload(payload)))
    }

    #[cfg(feature = "std")]
    fn take_panic(&mut self) -> Option<Box<dyn Any + Send + 'static>> {
        if let RawThreadError::Panicked(payload) = self {
            payload.take().map(|p| p.0)
        } else {
            None
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
        #[cfg(feature = "std")]
        {
            let res = catch_unwind(AssertUnwindSafe(f));
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
                    state.panic_payload.with_mut(|opt| *opt = Some(err));
                },
            }
        }
        #[cfg(not(feature = "std"))]
        {
            let r = f();
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
    }

    0
}

fn spawn<'a, F, T>(f: F) -> Result<RawJoinHandle<'a, T>, RawThreadError>
where
    F: FnOnce() -> T + Send + 'a,
    T: Send + 'a,
{
    let state = Arc::new(ThreadSharedState {
        closure: UnsafeCell::new(Some(f)),
        status: AtomicU8::new(super::STATE_INCOMPLETE),
        result: SafeUnsafeCell::new(None),
        #[cfg(feature = "std")]
        panic_payload: SafeUnsafeCell::new(None),
    });

    let receiver = ThreadResultReceiver {
        state: state.clone(),
    };

    let param = Arc::into_raw(state) as *mut c_void;

    unsafe {
        let handle = CreateThread(
            null(),
            0,
            Some(thread_entry_win::<F, T>),
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
        })
    }
}

impl<'a, T> RawJoinHandle<'a, T> {
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
            #[cfg(feature = "std")]
            let state = receiver.state.clone();
            match receiver.receive() {
                Ok(Some(val)) => Ok(val),
                Ok(None) => Err(RawThreadError::ResultMissing),
                Err(super::STATE_PANICKED) => {
                    #[cfg(feature = "std")]
                    {
                        let payload = state.take_panic();
                        Err(RawThreadError::Panicked(payload.map(SendSyncPanicPayload)))
                    }
                    #[cfg(not(feature = "std"))]
                    Err(RawThreadError::Panicked)
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

    fn spawn<'a, F, T>(f: F) -> Result<Self::RawJoinHandle<'a, T>, Self::Error>
    where
        F: FnOnce() -> T + Send + 'a,
        T: Send + 'a,
    {
        spawn(f)
    }

    fn yield_now() -> Result<bool, AbortedError> {
        RawJoinHandle::<()>::yield_now()
    }

    fn wait_on_address(address: &AtomicU32, expected: u32) {
        Self::wait_on_address_timeout(address, expected, None);
    }

    fn wait_on_address_timeout(
        address: &AtomicU32,
        expected: u32,
        timeout: Option<Duration>,
    ) -> bool {
        let ms = match timeout {
            Some(dur) => {
                if dur.as_millis() > INFINITE as u128 {
                    INFINITE
                } else {
                    dur.as_millis() as u32
                }
            }
            None => INFINITE,
        };
        unsafe {
            let res = WaitOnAddress(
                address as *const AtomicU32 as *const c_void as *mut c_void,
                &expected as *const u32 as *const c_void,
                4,
                ms,
            );
            if res == 0 {
                let err = GetLastError();
                err == 1460 // ERROR_TIMEOUT
            } else {
                false
            }
        }
    }

    fn wake_by_address(address: &AtomicU32) {
        unsafe {
            WakeByAddressSingle(address as *const AtomicU32 as *const c_void);
        }
    }

    fn wake_all_by_address(address: &AtomicU32) {
        unsafe {
            WakeByAddressAll(address as *const AtomicU32 as *const c_void);
        }
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
