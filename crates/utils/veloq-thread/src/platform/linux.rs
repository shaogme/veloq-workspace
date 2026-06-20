use crate::{PlatformImpl, ThreadParkerTrait};
use alloc::boxed::Box;
use core::{
    error::Error,
    ffi::{c_int, c_void},
    fmt::Display,
    marker::PhantomData,
    mem::zeroed,
    ptr::{null, null_mut},
    sync::atomic::{AtomicI32, Ordering},
};
use libc::{
    FUTEX_PRIVATE_FLAG, FUTEX_WAIT, FUTEX_WAKE, SYS_futex, pthread_cancel, pthread_create,
    pthread_detach, pthread_join, pthread_t, sched_yield, syscall, timespec,
};

/// Linux 平台下的线程实现
pub struct Thread<'a> {
    thread_id: Option<pthread_t>,
    _marker: PhantomData<&'a ()>,
}

#[derive(Debug)]
pub enum ThreadError {
    CreationFailed(c_int),
    JoinFailed(c_int),
    AbortFailed(c_int),
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

extern "C" fn thread_entry_posix(param: *mut c_void) -> *mut c_void {
    let f = unsafe { Box::from_raw(param as *mut Box<dyn FnOnce()>) };
    f();
    null_mut()
}

impl<'a> Thread<'a> {
    pub fn spawn<F>(f: F) -> Result<Self, ThreadError>
    where
        F: FnOnce() + Send + 'a,
    {
        let main_box: Box<Box<dyn FnOnce() + 'a>> = Box::new(Box::new(f));
        let param = Box::into_raw(main_box) as *mut c_void;
        let mut thread_id: pthread_t = unsafe { zeroed() };

        unsafe {
            let res = pthread_create(&mut thread_id, null(), thread_entry_posix, param);

            if res != 0 {
                let _ = Box::from_raw(param as *mut Box<dyn FnOnce() + 'a>);
                return Err(ThreadError::CreationFailed(res));
            }

            Ok(Self {
                thread_id: Some(thread_id),
                _marker: PhantomData,
            })
        }
    }

    pub fn join(mut self) -> Result<(), ThreadError> {
        let thread_id = self.thread_id.take().ok_or(ThreadError::JoinFailed(0))?;
        unsafe {
            let res = pthread_join(thread_id, null_mut());
            if res != 0 {
                return Err(ThreadError::JoinFailed(res));
            }
            Ok(())
        }
    }

    pub fn abort(&self) -> Result<(), ThreadError> {
        if let Some(thread_id) = self.thread_id {
            unsafe {
                let res = pthread_cancel(thread_id);
                if res != 0 {
                    return Err(ThreadError::AbortFailed(res));
                }
            }
        }
        Ok(())
    }

    pub fn yield_now() -> bool {
        unsafe { sched_yield() == 0 }
    }
}

/// Linux 平台下的平台实现结构体
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
        if let Some(thread_id) = self.thread_id.take() {
            unsafe {
                let _ = pthread_detach(thread_id);
            }
        }
    }
}

pub struct ThreadParker {
    state: AtomicI32,
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
            state: AtomicI32::new(0),
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
                let _ = syscall(
                    SYS_futex,
                    self.state.as_ptr() as *mut c_void,
                    FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
                    2,
                    null::<timespec>(),
                    null_mut::<c_void>(),
                    0,
                );
            }
        }

        self.state.swap(0, Ordering::Acquire);
    }

    pub fn unpark(&self) {
        let old = self.state.swap(1, Ordering::AcqRel);
        if old == 2 {
            unsafe {
                let _ = syscall(
                    SYS_futex,
                    self.state.as_ptr() as *mut c_void,
                    FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
                    1,
                );
            }
        }
    }
}
