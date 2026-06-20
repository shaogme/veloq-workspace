use crate::{PlatformImpl, ThreadParkerTrait};
use alloc::boxed::Box;
use core::{
    error::Error,
    ffi::c_void,
    fmt::Display,
    marker::PhantomData,
    ptr::{null, null_mut},
    sync::atomic::{AtomicU8, Ordering},
};
use windows_sys::Win32::{
    Foundation::{CloseHandle, GetLastError, HANDLE, WAIT_OBJECT_0},
    System::Threading::{
        CreateThread, INFINITE, SwitchToThread, TerminateThread, WaitForSingleObject,
        WaitOnAddress, WakeByAddressSingle,
    },
};

/// Windows 平台下的线程实现
pub struct Thread<'a> {
    handle: Option<HANDLE>,
    _marker: PhantomData<&'a ()>,
}

#[derive(Debug)]
pub enum ThreadError {
    CreationFailed(u32),
    JoinFailed(u32),
    AbortFailed(u32),
}

impl Display for ThreadError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ThreadError::CreationFailed(err) => write!(f, "thread creation failed: {}", err),
            ThreadError::JoinFailed(err) => write!(f, "thread join failed: {}", err),
            ThreadError::AbortFailed(err) => write!(f, "thread abort failed: {}", err),
        }
    }
}

impl Error for ThreadError {}

unsafe extern "system" fn thread_entry_win(param: *mut c_void) -> u32 {
    // 恢复并安全释放 Box 内存
    let f = unsafe { Box::from_raw(param as *mut Box<dyn FnOnce()>) };
    f();
    0
}

impl<'a> Thread<'a> {
    pub fn spawn<F>(f: F) -> Result<Self, ThreadError>
    where
        F: FnOnce() + Send + 'a,
    {
        let main_box: Box<Box<dyn FnOnce() + 'a>> = Box::new(Box::new(f));
        let param = Box::into_raw(main_box) as *mut c_void;

        unsafe {
            let handle = CreateThread(null(), 0, Some(thread_entry_win), param, 0, null_mut());

            if handle.is_null() {
                let err = GetLastError();
                let _ = Box::from_raw(param as *mut Box<dyn FnOnce() + 'a>);
                return Err(ThreadError::CreationFailed(err));
            }

            Ok(Self {
                handle: Some(handle),
                _marker: PhantomData,
            })
        }
    }

    pub fn join(mut self) -> Result<(), ThreadError> {
        let handle = self.handle.take().ok_or(ThreadError::JoinFailed(0))?;
        unsafe {
            let res = WaitForSingleObject(handle, INFINITE);
            let _ = CloseHandle(handle);
            if res != WAIT_OBJECT_0 {
                return Err(ThreadError::JoinFailed(res));
            }
            Ok(())
        }
    }

    pub fn abort(&self) -> Result<(), ThreadError> {
        if let Some(handle) = self.handle {
            unsafe {
                let res = TerminateThread(handle, 1);
                if res == 0 {
                    let err = GetLastError();
                    return Err(ThreadError::AbortFailed(err));
                }
            }
        }
        Ok(())
    }

    pub fn yield_now() -> bool {
        unsafe { SwitchToThread() != 0 }
    }
}

/// Windows 平台下的平台实现结构体
pub struct Platform;

impl PlatformImpl for Platform {
    type Error = ThreadError;
    type Parker = ThreadParker;
    type Thread<'a> = Thread<'a>;

    fn spawn<'a, F>(f: F) -> Result<Self::Thread<'a>, Self::Error>
    where
        F: FnOnce() + Send + 'a,
    {
        Thread::spawn(f)
    }

    fn join<'a>(thread: Self::Thread<'a>) -> Result<(), Self::Error> {
        thread.join()
    }

    fn abort<'a>(thread: &Self::Thread<'a>) -> Result<(), Self::Error> {
        thread.abort()
    }

    fn yield_now() -> bool {
        Thread::yield_now()
    }
}

impl<'a> Drop for Thread<'a> {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            unsafe {
                let _ = CloseHandle(handle);
            }
        }
    }
}

pub struct ThreadParker {
    state: AtomicU8,
}

impl ThreadParkerTrait for ThreadParker {
    fn new() -> Self {
        Self::new()
    }

    fn park(&self) {
        self.park();
    }

    fn unpark(&self) {
        self.unpark();
    }
}

impl ThreadParker {
    pub const fn new() -> Self {
        Self {
            state: AtomicU8::new(0),
        }
    }

    pub fn park(&self) {
        if self
            .state
            .compare_exchange(1, 0, Ordering::Acquire, Ordering::Acquire)
            .is_ok()
        {
            return;
        }

        self.state.store(2, Ordering::Release);

        while self.state.load(Ordering::Acquire) == 2 {
            unsafe {
                let expected = 2u8;
                let _ = WaitOnAddress(
                    self.state.as_ptr() as *mut c_void,
                    &expected as *const u8 as *const c_void,
                    1,
                    INFINITE,
                );
            }
        }

        self.state.swap(0, Ordering::Acquire);
    }

    pub fn unpark(&self) {
        let old = self.state.swap(1, Ordering::AcqRel);
        if old == 2 {
            unsafe {
                WakeByAddressSingle(self.state.as_ptr() as *const c_void);
            }
        }
    }
}
