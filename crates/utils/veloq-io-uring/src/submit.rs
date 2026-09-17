//! Submission, enter, and resource registration APIs for an io_uring instance.

use core::{
    mem, ptr,
    sync::atomic::{self, AtomicU32, Ordering},
};

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::{AsRawFd, OwnedFd, RawFd},
};

use crate::{
    register::{self, Probe},
    ring::Parameters,
    sys,
    types::SubmitArgs,
};

/// Flags accepted by `io_uring_enter`.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EnterFlags(u32);

impl EnterFlags {
    /// Wait for at least `min_complete` completion events.
    pub const GETEVENTS: Self = Self(sys::IORING_ENTER_GETEVENTS);
    /// Wake the SQPOLL kernel thread when it is sleeping.
    pub const SQ_WAKEUP: Self = Self(sys::IORING_ENTER_SQ_WAKEUP);
    /// Wait for a free SQE before returning.
    pub const SQ_WAIT: Self = Self(sys::IORING_ENTER_SQ_WAIT);
    /// Interpret the argument as an extended enter argument.
    pub const EXT_ARG: Self = Self(sys::IORING_ENTER_EXT_ARG);
    /// Submit through a previously registered ring fd.
    pub const REGISTERED_RING: Self = Self(sys::IORING_ENTER_REGISTERED_RING);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub fn insert(&mut self, other: Self) {
        self.0 |= other.0;
    }
}

/// Interface for submitting SQEs and registering ring resources.
pub struct Submitter<'ring> {
    fd: &'ring OwnedFd,
    params: &'ring Parameters,
    sq_head: *const AtomicU32,
    sq_tail: *const AtomicU32,
    sq_flags: *const AtomicU32,
}

impl<'ring> Submitter<'ring> {
    #[inline]
    pub(crate) const fn new(
        fd: &'ring OwnedFd,
        params: &'ring Parameters,
        sq_head: *const AtomicU32,
        sq_tail: *const AtomicU32,
        sq_flags: *const AtomicU32,
    ) -> Self {
        Self {
            fd,
            params,
            sq_head,
            sq_tail,
            sq_flags,
        }
    }

    #[inline]
    fn sq_len(&self) -> usize {
        unsafe {
            let head = (*self.sq_head).load(Ordering::Acquire);
            let tail = (*self.sq_tail).load(Ordering::Acquire);
            tail.wrapping_sub(head) as usize
        }
    }

    #[inline]
    fn sq_need_wakeup(&self) -> bool {
        unsafe { (*self.sq_flags).load(Ordering::Relaxed) & sys::IORING_SQ_NEED_WAKEUP != 0 }
    }

    #[inline]
    fn sq_cq_overflow(&self) -> bool {
        unsafe { (*self.sq_flags).load(Ordering::Relaxed) & sys::IORING_SQ_CQ_OVERFLOW != 0 }
    }

    /// Enter the kernel with a raw `io_uring_enter` argument.
    ///
    /// # Safety
    ///
    /// `flags`, `arg`, and `T` must describe the ABI expected by the kernel.
    /// If `arg` is `Some`, its pointed-to value must remain valid for the
    /// duration of the syscall and must have the layout selected by `flags`.
    pub unsafe fn enter<T: Sized>(
        &self,
        to_submit: u32,
        min_complete: u32,
        flags: u32,
        arg: Option<&T>,
    ) -> Result<usize> {
        let arg = arg.map_or(ptr::null(), |value| ptr::from_ref(value).cast());
        let result = unsafe {
            sys::io_uring_enter(
                self.fd.as_raw_fd(),
                to_submit,
                min_complete,
                flags,
                arg,
                mem::size_of::<T>(),
            )
        }?;
        Ok(result as usize)
    }

    /// Submit all currently published SQEs.
    #[inline]
    pub fn submit(&self) -> Result<usize> {
        self.submit_and_wait(0)
    }

    /// Submit all currently published SQEs and wait for completions.
    pub fn submit_and_wait(&self, want: usize) -> Result<usize> {
        let to_submit = checked_u32(self.sq_len())?;
        let min_complete = checked_u32(want)?;
        let sq_cq_overflow = self.sq_cq_overflow();
        let need_syscall_for_overflow = sq_cq_overflow && self.params.is_feature_nodrop();
        let mut flags = EnterFlags::empty();

        if want > 0 || self.params.is_setup_iopoll() || sq_cq_overflow {
            flags.insert(EnterFlags::GETEVENTS);
        }

        if self.params.is_setup_sqpoll() {
            // The SeqCst fence is required before interpreting NEED_WAKEUP.
            atomic::fence(Ordering::SeqCst);
            if self.sq_need_wakeup() {
                flags.insert(EnterFlags::SQ_WAKEUP);
            } else if want == 0 && !need_syscall_for_overflow {
                return Ok(to_submit as usize);
            }
        }

        unsafe { self.enter::<libc::sigset_t>(to_submit, min_complete, flags.bits(), None) }
    }

    /// Submit SQEs and wait for completions using an extended enter argument.
    pub fn submit_with_args(&self, want: usize, args: &SubmitArgs<'_, '_>) -> Result<usize> {
        let to_submit = checked_u32(self.sq_len())?;
        let min_complete = checked_u32(want)?;
        let sq_cq_overflow = self.sq_cq_overflow();
        let need_syscall_for_overflow = sq_cq_overflow && self.params.is_feature_nodrop();
        let mut flags = EnterFlags::EXT_ARG;

        if want > 0 || self.params.is_setup_iopoll() || sq_cq_overflow {
            flags.insert(EnterFlags::GETEVENTS);
        }

        if self.params.is_setup_sqpoll() {
            atomic::fence(Ordering::SeqCst);
            if self.sq_need_wakeup() {
                flags.insert(EnterFlags::SQ_WAKEUP);
            } else if want == 0 && !need_syscall_for_overflow {
                return Ok(to_submit as usize);
            }
        }

        unsafe { self.enter(to_submit, min_complete, flags.bits(), Some(args)) }
    }

    /// Wait for the submission queue to have free entries.
    pub fn squeue_wait(&self) -> Result<usize> {
        unsafe { self.enter::<libc::sigset_t>(0, 0, EnterFlags::SQ_WAIT.bits(), None) }
    }

    /// Register fixed buffers.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers(&self, bufs: &[libc::iovec]) -> Result<()> {
        let nr_args = checked_u32(bufs.len())?;
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_BUFFERS,
            bufs.as_ptr().cast(),
            nr_args,
        )
        .map(drop)
    }

    /// Update fixed buffers beginning at `offset`.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers_update(
        &self,
        offset: u32,
        bufs: &[libc::iovec],
        tags: Option<&[u64]>,
    ) -> Result<()> {
        let nr = tags.map_or(bufs.len(), |values| bufs.len().min(values.len()));
        let nr = checked_u32(nr)?;
        let update = sys::IoUringRsrcUpdate2 {
            offset,
            data: bufs.as_ptr() as u64,
            tags: tags.map_or(0, |values| values.as_ptr() as u64),
            nr,
            ..Default::default()
        };

        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_BUFFERS_UPDATE,
            ptr::from_ref(&update).cast(),
            mem::size_of::<sys::IoUringRsrcUpdate2>() as u32,
        )
        .map(drop)
    }

    /// Register a fixed file table.
    pub fn register_files(&self, files: &[RawFd]) -> Result<()> {
        let nr_args = checked_u32(files.len())?;
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES,
            files.as_ptr().cast(),
            nr_args,
        )
        .map(drop)
    }

    /// Update fixed files and return the number of entries accepted by the kernel.
    pub fn register_files_update(&self, offset: u32, files: &[RawFd]) -> Result<usize> {
        let nr_args = checked_u32(files.len())?;
        let update = sys::IoUringFilesUpdate {
            offset,
            fds: files.as_ptr() as u64,
            ..Default::default()
        };
        let result = register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES_UPDATE,
            ptr::from_ref(&update).cast(),
            nr_args,
        )?;
        Ok(result as usize)
    }

    /// Fill a probe with the opcodes supported by the running kernel.
    pub fn register_probe(&self, probe: &mut Probe) -> Result<()> {
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_PROBE,
            probe.as_mut_ptr().cast(),
            Probe::COUNT as u32,
        )
        .map(drop)
    }

    /// Register a provided-buffer ring.
    ///
    /// # Safety
    ///
    /// The mapping at `ring_addr` must contain `ring_entries` entries and
    /// remain valid until the group is unregistered or the ring is destroyed.
    pub unsafe fn register_buf_ring_with_flags(
        &self,
        ring_addr: u64,
        ring_entries: u16,
        bgid: u16,
        flags: u16,
    ) -> Result<()> {
        let register = sys::IoUringBufReg {
            ring_addr,
            ring_entries: ring_entries as u32,
            bgid,
            flags,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_PBUF_RING,
            ptr::from_ref(&register).cast(),
            1,
        )
        .map(drop)
    }

    /// Unregister a provided-buffer ring.
    pub fn unregister_buf_ring(&self, bgid: u16) -> Result<()> {
        let unregister = sys::IoUringBufReg {
            bgid,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_UNREGISTER_PBUF_RING,
            ptr::from_ref(&unregister).cast(),
            1,
        )
        .map(drop)
    }
}

fn checked_u32(value: usize) -> Result<u32> {
    value
        .try_into()
        .map_err(|_| Error::from_raw_os_error(libc::EOVERFLOW))
}
