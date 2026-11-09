use core::mem::take;

use crate::{
    cell::{Cell, UnsafeCell},
    fmt::{self, Formatter, Result as FmtResult},
    panic::PanicPayload,
    string::String,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    thread::Thread,
};

#[cfg(feature = "loom")]
use crate::boxed::Box;

pub(crate) type ThreadPanicPayload = Option<PanicPayload>;

pub struct SendSyncPanicPayload(PanicPayload);

unsafe impl Send for SendSyncPanicPayload {}

unsafe impl Sync for SendSyncPanicPayload {}

impl fmt::Debug for SendSyncPanicPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.write_str("SendSyncPanicPayload")
    }
}

impl SendSyncPanicPayload {
    pub(crate) fn from_payload(payload: ThreadPanicPayload) -> Option<Self> {
        payload.map(Self)
    }

    pub(crate) fn take_payload(opt: &mut Option<Self>) -> ThreadPanicPayload {
        opt.take().map(|payload| payload.0)
    }
}

pub(crate) const STATE_INCOMPLETE: u8 = 0;
pub(crate) const STATE_FINISHED: u8 = 1;
pub(crate) const STATE_PANICKED: u8 = 2;
pub(crate) const STATE_ABORTED: u8 = 3;

pub(crate) static CURRENT_THREAD_STATUS: veloq_tls::Tls<Cell<Option<*const AtomicU8>>> =
    veloq_tls::Tls::new();

pub(crate) static CURRENT_THREAD: veloq_tls::Tls<Thread> = veloq_tls::Tls::new();

pub(crate) struct SafeUnsafeCell<T>(UnsafeCell<T>);

unsafe impl<T: Send> Send for SafeUnsafeCell<T> {}

unsafe impl<T: Send> Sync for SafeUnsafeCell<T> {}

impl<T> SafeUnsafeCell<T> {
    #[cfg(not(feature = "loom"))]
    pub(crate) const fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    #[cfg(feature = "loom")]
    pub(crate) fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    pub(crate) unsafe fn with_mut<F, R>(&self, f: F) -> R
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

pub(crate) struct Sentinel<'a> {
    status: &'a AtomicU8,
    panicked: bool,
}

impl<'a> Sentinel<'a> {
    pub(crate) fn new(status: &'a AtomicU8) -> Self {
        Self {
            status,
            panicked: true,
        }
    }

    pub(crate) fn mark_successful(&mut self) {
        self.panicked = false;
    }
}

pub(crate) struct ThreadSharedState<F, T> {
    closure: UnsafeCell<Option<F>>,
    status: AtomicU8,
    result: SafeUnsafeCell<Option<T>>,
    panic_payload: SafeUnsafeCell<ThreadPanicPayload>,
    name: Option<String>,
    thread: Thread,
}

unsafe impl<F: Send, T: Send> Send for ThreadSharedState<F, T> {}

unsafe impl<F: Send, T: Send> Sync for ThreadSharedState<F, T> {}

impl<F, T> ThreadSharedState<F, T> {
    pub(crate) fn new(closure: F, name: Option<String>, thread: Thread) -> Self {
        Self {
            closure: UnsafeCell::new(Some(closure)),
            status: AtomicU8::new(STATE_INCOMPLETE),
            result: SafeUnsafeCell::new(None),
            panic_payload: SafeUnsafeCell::new(None),
            name,
            thread,
        }
    }

    pub(crate) fn thread(&self) -> &Thread {
        &self.thread
    }

    pub(crate) fn status_ptr(&self) -> *const AtomicU8 {
        &self.status
    }

    pub(crate) fn name(&self) -> Option<&String> {
        self.name.as_ref()
    }

    pub(crate) unsafe fn take_closure(&self) -> Option<F> {
        unsafe { self.closure.with_mut(Option::take) }
    }

    pub(crate) unsafe fn store_result(&self, result: T) {
        unsafe { self.result.with_mut(|slot| *slot = Some(result)) };
    }

    pub(crate) unsafe fn store_panic_payload(&self, payload: PanicPayload) {
        unsafe { self.panic_payload.with_mut(|slot| *slot = Some(payload)) };
    }

    pub(crate) fn mark_finished(&self) {
        let _ = self.status.compare_exchange(
            STATE_INCOMPLETE,
            STATE_FINISHED,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub(crate) fn sentinel(&self) -> Sentinel<'_> {
        Sentinel::new(&self.status)
    }
}

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
        unsafe { self.panic_payload.with_mut(take) }
    }
}

pub(crate) struct ThreadResultReceiver<'a, T> {
    #[cfg(not(feature = "loom"))]
    state: Arc<dyn ThreadSharedStateTrait<T> + 'a>,
    #[cfg(feature = "loom")]
    state: Arc<ThreadSharedState<Box<dyn FnOnce() -> T + Send + 'a>, T>>,
}

unsafe impl<T: Send> Send for ThreadResultReceiver<'_, T> {}

unsafe impl<T: Send> Sync for ThreadResultReceiver<'_, T> {}

impl<'a, T: Send> ThreadResultReceiver<'a, T> {
    #[cfg(not(feature = "loom"))]
    pub(crate) fn new(state: Arc<dyn ThreadSharedStateTrait<T> + 'a>) -> Self {
        Self { state }
    }

    #[cfg(feature = "loom")]
    pub(crate) fn new(
        state: Arc<ThreadSharedState<Box<dyn FnOnce() -> T + Send + 'a>, T>>,
    ) -> Self {
        Self { state }
    }

    pub(crate) fn clone_state(&self) -> Arc<dyn ThreadSharedStateTrait<T> + 'a> {
        Arc::clone(&self.state)
    }

    pub(crate) fn set_aborted(&self) {
        self.state.set_aborted();
    }

    pub(crate) unsafe fn receive(self) -> Result<Option<T>, u8> {
        let status = ThreadSharedStateTrait::status(&*self.state);
        if status == STATE_FINISHED {
            unsafe { Ok(ThreadSharedStateTrait::receive(&*self.state)) }
        } else {
            Err(status)
        }
    }
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
