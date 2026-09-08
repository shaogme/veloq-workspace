use crate::{
    config::{IoFd, OwnedRawHandle, RawHandle, UringRawHandle},
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use veloq_driver_core::driver::RemoteWaker;
use veloq_std::{
    boxed::Box,
    io, mem,
    string::ToString,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU8, Ordering},
    },
};

pub(crate) const WAKER_IDLE: u8 = 0;
pub(crate) const WAKER_NOTIFIED: u8 = 1;
pub(crate) const WAKER_PROCESSING: u8 = 2;
pub(crate) const WAKER_REARM: u8 = 3;
pub(crate) const WAKER_RENOTIFIED: u8 = 4;

pub(crate) struct EventFd {
    pub(crate) fd: OwnedRawHandle,
}

pub(crate) struct WakerFdState {
    fd: Mutex<Arc<EventFd>>,
}

impl WakerFdState {
    #[inline]
    pub(crate) fn new(fd: Arc<EventFd>) -> Self {
        Self { fd: Mutex::new(fd) }
    }

    #[inline]
    fn lock_fd(&self) -> MutexGuard<'_, Arc<EventFd>> {
        self.fd.lock()
    }

    #[inline]
    pub(crate) fn current(&self) -> Arc<EventFd> {
        self.lock_fd().clone()
    }

    #[inline]
    pub(crate) fn replace(&self, fd: Arc<EventFd>) -> Arc<EventFd> {
        mem::replace(&mut *self.lock_fd(), fd)
    }
}

pub(crate) struct UringWaker {
    pub(crate) state: Arc<WakerFdState>,
    pub(crate) notification_state: Arc<AtomicU8>,
}

impl RemoteWaker<UringError> for UringWaker {
    fn wake(&self) -> UringResult<()> {
        loop {
            let state = self.notification_state.load(Ordering::Acquire);
            match state {
                WAKER_RENOTIFIED => return Ok(()),
                WAKER_NOTIFIED => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_NOTIFIED,
                            WAKER_RENOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }
                    self.notify_event_fd()?;
                    return Ok(());
                }
                WAKER_IDLE | WAKER_PROCESSING | WAKER_REARM => {
                    if self
                        .notification_state
                        .compare_exchange(
                            state,
                            WAKER_NOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_err()
                    {
                        continue;
                    }

                    self.notify_event_fd()?;
                    return Ok(());
                }
                _ => unreachable!("invalid io_uring waker state"),
            }
        }
    }
}

impl UringWaker {
    fn notify_event_fd(&self) -> UringResult<()> {
        let buf = 1u64.to_ne_bytes();
        let fd = self.state.current();
        let ret = unsafe { libc::write(fd.fd.raw().as_fd(), buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            self.notification_state
                .compare_exchange(
                    WAKER_NOTIFIED,
                    WAKER_IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .ok();
            return Err(UringError::Internal
                .to_report()
                .push_ctx("scope", "uring.driver.waker.wake")
                .set_error_code(err.raw_os_error().unwrap_or(libc::EIO))
                .attach_note(err.to_string()));
        }
        if ret != 8 {
            self.notification_state
                .compare_exchange(
                    WAKER_NOTIFIED,
                    WAKER_IDLE,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                )
                .ok();
            return Err(UringError::Internal
                .to_report()
                .push_ctx("scope", "uring.driver.waker.wake")
                .with_ctx("bytes_written", ret)
                .attach_note("eventfd write returned an unexpected byte count"));
        }
        Ok(())
    }
}

pub(crate) struct WakerHooksView<'a> {
    pub(crate) buf_len: usize,
    pub(crate) armed: &'a mut bool,
    pub(crate) notification_state: &'a AtomicU8,
}

pub(crate) struct UringWakerManager {
    state: Arc<WakerFdState>,
    registered_fd: Option<IoFd>,
    armed: bool,
    buf: Box<[u8; 8]>,
    notification_state: Arc<AtomicU8>,
}

impl UringWakerManager {
    pub(crate) fn new() -> UringResult<Self> {
        let waker_fd = Self::create_event_fd("driver.new.eventfd")?;
        Ok(Self {
            state: Arc::new(WakerFdState::new(waker_fd)),
            registered_fd: None,
            armed: false,
            buf: Box::new([0; 8]),
            notification_state: Arc::new(AtomicU8::new(WAKER_IDLE)),
        })
    }

    pub(crate) fn create_event_fd(scope: &'static str) -> UringResult<Arc<EventFd>> {
        let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
        if fd < 0 {
            return Err(UringError::DriverInit.io_report(scope, io::Error::last_os_error()));
        }
        Ok(Arc::new(EventFd {
            // SAFETY: `eventfd` returns a freshly created fd owned by this driver.
            fd: unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(UringRawHandle::for_file(fd)))
            },
        }))
    }

    #[inline]
    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<UringError>> {
        Arc::new(UringWaker {
            state: self.state.clone(),
            notification_state: self.notification_state.clone(),
        })
    }

    #[inline]
    pub(crate) fn is_armed(&self) -> bool {
        self.armed
    }

    #[inline]
    pub(crate) fn set_armed(&mut self, armed: bool) {
        self.armed = armed;
    }

    #[inline]
    pub(crate) fn hooks_view(&mut self) -> WakerHooksView<'_> {
        WakerHooksView {
            buf_len: self.buf.len(),
            armed: &mut self.armed,
            notification_state: &self.notification_state,
        }
    }

    pub(crate) fn begin_processing(&self) {
        begin_processing(&self.notification_state);
    }

    pub(crate) fn finish_rearm(&self) {
        self.notification_state
            .compare_exchange(WAKER_REARM, WAKER_IDLE, Ordering::AcqRel, Ordering::Acquire)
            .ok();
    }

    #[inline]
    pub(crate) fn registered_fd(&self) -> Option<IoFd> {
        self.registered_fd
    }

    #[inline]
    pub(crate) fn set_registered_fd(&mut self, fd: Option<IoFd>) {
        self.registered_fd = fd;
    }

    #[inline]
    pub(crate) fn state(&self) -> Arc<WakerFdState> {
        self.state.clone()
    }

    #[inline]
    pub(crate) fn buf_mut_ptr(&mut self) -> *mut u8 {
        self.buf.as_mut_ptr()
    }

    #[inline]
    pub(crate) fn buf_len(&self) -> usize {
        self.buf.len()
    }

    pub(crate) fn replace_state_fd(&mut self, new_fd: Arc<EventFd>) -> Arc<EventFd> {
        self.state.replace(new_fd)
    }
}

fn begin_processing(state: &AtomicU8) {
    loop {
        let current = state.load(Ordering::Acquire);
        match current {
            WAKER_IDLE | WAKER_NOTIFIED | WAKER_RENOTIFIED => {
                if state
                    .compare_exchange(
                        current,
                        WAKER_PROCESSING,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    )
                    .is_ok()
                {
                    return;
                }
            }
            WAKER_PROCESSING | WAKER_REARM => return,
            _ => unreachable!("invalid io_uring waker state"),
        }
    }
}
