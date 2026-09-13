use crate::{
    config::{IoFd, OwnedRawHandle, RawHandle, UringRawHandle},
    error::{UringError, UringResult},
};
use diagweave::prelude::*;
use veloq_driver_core::driver::RemoteWaker;
use veloq_std::{
    boxed::Box,
    io,
    string::ToString,
    sync::{
        Arc, UnpoisonedMutex, UnpoisonedMutexGuard,
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
    fd: UnpoisonedMutex<Arc<EventFd>>,
}

impl WakerFdState {
    #[inline]
    pub(crate) fn new(fd: Arc<EventFd>) -> Self {
        Self {
            fd: UnpoisonedMutex::new(fd),
        }
    }

    #[inline]
    fn lock_fd(&self) -> UnpoisonedMutexGuard<'_, Arc<EventFd>> {
        self.fd.lock()
    }

    #[inline]
    pub(crate) fn current(&self) -> Arc<EventFd> {
        self.lock_fd().clone()
    }

    pub(crate) fn with_lock<F, T>(&self, f: F) -> T
    where
        F: FnOnce(&mut Arc<EventFd>) -> T,
    {
        let mut fd = self.lock_fd();
        f(&mut fd)
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
        let result = self.state.with_lock(|fd| Self::write_event_fd(fd));
        if result.is_ok() {
            return Ok(());
        }

        self.rollback_notification();
        result
    }

    pub(crate) fn write_event_fd(fd: &EventFd) -> UringResult<()> {
        let buf = 1u64.to_ne_bytes();
        let ret = unsafe { libc::write(fd.fd.raw().as_fd(), buf.as_ptr() as *const _, 8) };
        if ret < 0 {
            let err = io::Error::last_os_error();
            if err.raw_os_error() == Some(libc::EAGAIN) {
                return Ok(());
            }
            return Err(UringError::Internal
                .to_report()
                .push_ctx("scope", "uring.driver.waker.wake")
                .set_error_code(err.raw_os_error().unwrap_or(libc::EIO))
                .attach_note(err.to_string()));
        }
        if ret != 8 {
            return Err(UringError::Internal
                .to_report()
                .push_ctx("scope", "uring.driver.waker.wake")
                .with_ctx("bytes_written", ret)
                .attach_note("eventfd write returned an unexpected byte count"));
        }
        Ok(())
    }

    fn rollback_notification(&self) {
        loop {
            match self.notification_state.load(Ordering::Acquire) {
                WAKER_NOTIFIED => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_NOTIFIED,
                            WAKER_IDLE,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                WAKER_RENOTIFIED => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_RENOTIFIED,
                            WAKER_NOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return;
                    }
                }
                _ => return,
            }
        }
    }
}

pub(crate) struct WakerHooksView {
    pub(crate) buf_len: usize,
    pub(crate) generation: u64,
}

pub(crate) struct UringWakerManager {
    state: Arc<WakerFdState>,
    registered_fd: Option<IoFd>,
    armed: bool,
    buf: Box<[u8; 8]>,
    notification_state: Arc<AtomicU8>,
    next_generation: u64,
    armed_generation: u64,
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
            next_generation: 0,
            armed_generation: 0,
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
    pub(crate) fn write_event_fd(fd: &EventFd) -> UringResult<()> {
        UringWaker::write_event_fd(fd)
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
    pub(crate) fn hooks_view(&self) -> WakerHooksView {
        WakerHooksView {
            buf_len: self.buf.len(),
            generation: self.armed_generation,
        }
    }

    #[inline]
    pub(crate) fn arm(&mut self) -> u64 {
        self.next_generation = self.next_generation.wrapping_add(1);
        if self.next_generation == 0 {
            self.next_generation = 1;
        }
        self.armed_generation = self.next_generation;
        self.armed = true;
        self.armed_generation
    }

    #[inline]
    pub(crate) fn armed_generation(&self) -> u64 {
        self.armed_generation
    }

    #[inline]
    pub(crate) fn prepare_rearm(&mut self, generation: u64) -> bool {
        if !self.armed || self.armed_generation != generation {
            return false;
        }
        self.armed = false;
        true
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
    pub(crate) fn has_pending_notification(&self) -> bool {
        matches!(
            self.notification_state.load(Ordering::Acquire),
            WAKER_NOTIFIED | WAKER_RENOTIFIED
        )
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

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_std::mem;

    fn read_event_fd(fd: &EventFd) -> u64 {
        let mut value = 0u64;
        let ret = unsafe {
            libc::read(
                fd.fd.raw().as_fd(),
                (&mut value as *mut u64).cast(),
                mem::size_of::<u64>(),
            )
        };
        assert_eq!(ret, mem::size_of::<u64>() as isize);
        value
    }

    #[test]
    fn pending_notification_is_migrated_to_the_replaced_eventfd() {
        let manager = UringWakerManager::new().expect("eventfd should be created");
        let remote_waker = manager.create_waker();
        let old_fd = manager.state.current();
        let new_fd =
            UringWakerManager::create_event_fd("uring.test.pending_notification.new_eventfd")
                .expect("replacement eventfd should be created");

        manager.begin_processing();
        remote_waker
            .wake()
            .expect("initial notification should succeed");
        assert!(manager.has_pending_notification());

        manager.state.with_lock(|current_fd| {
            UringWakerManager::write_event_fd(&new_fd)
                .expect("pending notification should be copied");
            *current_fd = new_fd.clone();
        });

        remote_waker
            .wake()
            .expect("notification after replacement should succeed");
        assert_eq!(read_event_fd(&new_fd), 2);
        assert_eq!(read_event_fd(&old_fd), 1);
        assert_eq!(manager.state.current().fd.raw(), new_fd.fd.raw());
        assert_ne!(old_fd.fd.raw(), new_fd.fd.raw());
    }

    #[test]
    fn waker_rearm_is_bound_to_one_arm_generation() {
        let mut manager = UringWakerManager::new().expect("eventfd should be created");
        let first = manager.arm();
        assert_eq!(manager.hooks_view().generation, first);
        assert!(manager.prepare_rearm(first));
        assert!(!manager.prepare_rearm(first));

        let second = manager.arm();
        assert_ne!(first, second);
        assert!(!manager.prepare_rearm(first));
        assert!(manager.prepare_rearm(second));
    }
}
