#[cfg(target_os = "windows")]
mod windows;
#[cfg(target_os = "windows")]
pub use windows::{Platform, RawJoinHandle, RawThreadError};

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(any(target_os = "linux", target_os = "android"))]
pub use linux::{Platform, RawJoinHandle, RawThreadError};

use crate::{
    boxed::Box,
    cell::{Cell, UnsafeCell},
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

#[cfg(feature = "std")]
pub(crate) type ThreadPanicPayload = Option<Box<dyn crate::any::Any + Send + 'static>>;

#[cfg(feature = "std")]
pub struct SendSyncPanicPayload(pub Box<dyn crate::any::Any + Send + 'static>);

#[cfg(feature = "std")]
unsafe impl Send for SendSyncPanicPayload {}
#[cfg(feature = "std")]
unsafe impl Sync for SendSyncPanicPayload {}

#[cfg(feature = "std")]
impl crate::fmt::Debug for SendSyncPanicPayload {
    fn fmt(&self, f: &mut crate::fmt::Formatter<'_>) -> crate::fmt::Result {
        f.write_str("SendSyncPanicPayload")
    }
}

#[cfg(not(feature = "std"))]
pub(crate) type ThreadPanicPayload = ();

#[cfg(not(feature = "std"))]
pub type SendSyncPanicPayload = ();

#[inline]
pub(crate) fn from_panic_payload(payload: ThreadPanicPayload) -> Option<SendSyncPanicPayload> {
    #[cfg(feature = "std")]
    {
        payload.map(SendSyncPanicPayload)
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = payload;
        Some(())
    }
}

#[inline]
pub(crate) fn take_panic_payload(opt: &mut Option<SendSyncPanicPayload>) -> ThreadPanicPayload {
    #[cfg(feature = "std")]
    {
        opt.take().map(|p| p.0)
    }
    #[cfg(not(feature = "std"))]
    {
        let _ = opt.take();
        ()
    }
}

pub(crate) const STATE_INCOMPLETE: u8 = 0;
pub(crate) const STATE_FINISHED: u8 = 1;
pub(crate) const STATE_PANICKED: u8 = 2;
pub(crate) const STATE_ABORTED: u8 = 3;

pub(crate) static CURRENT_THREAD_STATUS: veloq_tls::Tls<Cell<Option<*const AtomicU8>>> =
    veloq_tls::Tls::new();

/// 包装以在线程间安全共享的 UnsafeCell
pub(crate) struct SafeUnsafeCell<T>(UnsafeCell<T>);
unsafe impl<T: Send> Send for SafeUnsafeCell<T> {}
unsafe impl<T: Send> Sync for SafeUnsafeCell<T> {}

impl<T> SafeUnsafeCell<T> {
    /// 创建一个新的 SafeUnsafeCell
    #[cfg(not(feature = "loom"))]
    pub(crate) const fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    #[cfg(feature = "loom")]
    pub(crate) fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    pub unsafe fn with_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut T) -> R,
    {
        unsafe { self.0.with_mut(f) }
    }
}

pub(crate) trait ThreadSharedStateTrait<T>: Send + Sync {
    unsafe fn receive(&self) -> Option<T>;
    fn status(&self) -> u8;
    fn set_aborted(&self);
    unsafe fn take_panic(&self) -> ThreadPanicPayload;
}

pub(crate) struct ThreadSharedState<F, T> {
    pub(crate) closure: UnsafeCell<Option<F>>,
    pub(crate) status: AtomicU8,
    pub(crate) result: SafeUnsafeCell<Option<T>>,
    pub(crate) panic_payload: SafeUnsafeCell<ThreadPanicPayload>,
}

unsafe impl<F: Send, T: Send> Send for ThreadSharedState<F, T> {}
unsafe impl<F: Send, T: Send> Sync for ThreadSharedState<F, T> {}

impl<F, T> ThreadSharedStateTrait<T> for ThreadSharedState<F, T>
where
    F: Send,
    T: Send,
{
    unsafe fn receive(&self) -> Option<T> {
        unsafe { self.result.with_mut(|x| x.take()) }
    }

    fn status(&self) -> u8 {
        self.status.load(Ordering::Acquire)
    }

    fn set_aborted(&self) {
        self.status.store(STATE_ABORTED, Ordering::Release);
    }

    unsafe fn take_panic(&self) -> ThreadPanicPayload {
        unsafe { self.panic_payload.with_mut(|x| core::mem::take(x)) }
    }
}

pub(crate) struct ThreadResultReceiver<'a, T> {
    #[cfg(not(feature = "loom"))]
    pub(crate) state: Arc<dyn ThreadSharedStateTrait<T> + 'a>,
    #[cfg(feature = "loom")]
    pub(crate) state: Arc<ThreadSharedState<Box<dyn FnOnce() -> T + Send + 'a>, T>>,
}

unsafe impl<T: Send> Send for ThreadResultReceiver<'_, T> {}
unsafe impl<T: Send> Sync for ThreadResultReceiver<'_, T> {}

impl<'a, T: Send> ThreadResultReceiver<'a, T> {
    pub(crate) unsafe fn receive(self) -> Result<Option<T>, u8> {
        let status = ThreadSharedStateTrait::status(&*self.state);
        if status == STATE_FINISHED {
            unsafe { Ok(ThreadSharedStateTrait::receive(&*self.state)) }
        } else {
            Err(status)
        }
    }
}

pub(crate) struct Sentinel<'a> {
    pub(crate) status: &'a AtomicU8,
    pub(crate) panicked: bool,
}

impl Drop for Sentinel<'_> {
    fn drop(&mut self) {
        if self.panicked {
            let _ = self.status.compare_exchange(
                STATE_INCOMPLETE,
                STATE_PANICKED,
                Ordering::AcqRel,
                Ordering::Acquire,
            );
        }
    }
}
