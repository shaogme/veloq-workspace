//! Ring construction and ownership.

use core::{
    cmp,
    mem::{ManuallyDrop, size_of},
};

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use crate::{
    cqueue::{CompletionQueue, Entry as CompletionEntry, Inner as CompletionInner},
    mmap::Mmap,
    squeue::{Entry as SubmissionEntry, Inner as SubmissionInner, SubmissionQueue},
    submit::Submitter,
    sys,
};

/// Parameters returned by `io_uring_setup`.
#[derive(Clone)]
#[repr(transparent)]
pub struct Parameters(pub(crate) sys::IoUringParams);

impl Parameters {
    #[inline]
    pub fn is_setup_sqpoll(&self) -> bool {
        self.0.flags & sys::IORING_SETUP_SQPOLL != 0
    }

    #[inline]
    pub fn is_setup_iopoll(&self) -> bool {
        self.0.flags & sys::IORING_SETUP_IOPOLL != 0
    }

    #[inline]
    pub fn is_feature_nodrop(&self) -> bool {
        self.0.features & sys::IORING_FEAT_NODROP != 0
    }

    #[inline]
    pub fn is_feature_single_mmap(&self) -> bool {
        self.0.features & sys::IORING_FEAT_SINGLE_MMAP != 0
    }

    #[inline]
    pub fn sq_entries(&self) -> u32 {
        self.0.sq_entries
    }

    #[inline]
    pub fn cq_entries(&self) -> u32 {
        self.0.cq_entries
    }
}

/// Configure an io_uring instance before creating it.
#[derive(Clone, Default)]
pub struct Builder {
    params: sys::IoUringParams,
}

impl Builder {
    #[inline]
    pub fn setup_iopoll(&mut self) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_IOPOLL;
        self
    }

    #[inline]
    pub fn setup_coop_taskrun(&mut self) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_COOP_TASKRUN;
        self
    }

    #[inline]
    pub fn setup_single_issuer(&mut self) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_SINGLE_ISSUER;
        self
    }

    #[inline]
    pub fn setup_defer_taskrun(&mut self) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_DEFER_TASKRUN;
        self
    }

    #[inline]
    pub fn setup_sqpoll(&mut self, idle_ms: u32) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_SQPOLL;
        self.params.sq_thread_idle = idle_ms;
        self
    }

    #[inline]
    pub fn setup_cqsize(&mut self, entries: u32) -> &mut Self {
        self.params.flags |= sys::IORING_SETUP_CQSIZE;
        self.params.cq_entries = entries;
        self
    }

    /// Create a ring with `entries` submission queue slots.
    pub fn build(self, entries: u32) -> Result<IoUring> {
        if entries == 0 {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let mut params = self.params;
        let raw_fd = unsafe { sys::io_uring_setup(entries, &mut params) }?;
        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        unsafe { IoUring::from_fd_and_params(fd, params) }
    }
}

/// An io_uring instance and its mapped submission/completion rings.
pub struct IoUring {
    sq: SubmissionInner,
    cq: CompletionInner,
    _fd: OwnedFd,
    params: Parameters,
    // This field is deliberately manually dropped before `fd`; the kernel may
    // still access the mappings while the ring descriptor is open.
    memory: ManuallyDrop<MemoryMap>,
}

struct MemoryMap {
    _sq_mmap: Mmap,
    _cq_mmap: Option<Mmap>,
    _sqe_mmap: Mmap,
}

impl IoUring {
    #[inline]
    pub fn new(entries: u32) -> Result<Self> {
        Self::builder().build(entries)
    }

    #[must_use]
    #[inline]
    pub fn builder() -> Builder {
        Builder::default()
    }

    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    #[inline]
    pub fn submission(&mut self) -> SubmissionQueue<'_> {
        unsafe { self.sq.borrow() }
    }

    #[inline]
    pub fn completion(&mut self) -> CompletionQueue<'_> {
        unsafe { self.cq.borrow() }
    }

    /// Return a handle for submitting entries and registering ring resources.
    #[inline]
    pub fn submitter(&self) -> Submitter<'_> {
        let (sq_head, sq_tail, sq_flags) = self.sq.submitter_parts();
        Submitter::new(&self._fd, &self.params, sq_head, sq_tail, sq_flags)
    }

    /// Submit all currently published SQEs.
    #[inline]
    pub fn submit(&self) -> Result<usize> {
        self.submitter().submit()
    }

    /// Split the ring into independently usable submission and completion views.
    #[inline]
    pub fn split(&mut self) -> (Submitter<'_>, SubmissionQueue<'_>, CompletionQueue<'_>) {
        let (sq_head, sq_tail, sq_flags) = self.sq.submitter_parts();
        let submitter = Submitter::new(&self._fd, &self.params, sq_head, sq_tail, sq_flags);
        let submission = unsafe { self.sq.borrow() };
        let completion = unsafe { self.cq.borrow() };
        (submitter, submission, completion)
    }

    unsafe fn from_fd_and_params(fd: OwnedFd, params: sys::IoUringParams) -> Result<Self> {
        let sq_len = checked_map_len(params.sq_off.array, params.sq_entries, size_of::<u32>())?;
        let cq_len = checked_map_len(
            params.cq_off.cqes,
            params.cq_entries,
            size_of::<CompletionEntry>(),
        )?;
        let sqe_len = checked_map_len(0, params.sq_entries, size_of::<SubmissionEntry>())?;

        let sqe_mmap = Mmap::new(fd.as_raw_fd(), sys::IORING_OFF_SQES, sqe_len)?;
        let (sq_mmap, cq_mmap) = if params.features & sys::IORING_FEAT_SINGLE_MMAP != 0 {
            (
                Mmap::new(
                    fd.as_raw_fd(),
                    sys::IORING_OFF_SQ_RING,
                    cmp::max(sq_len, cq_len),
                )?,
                None,
            )
        } else {
            (
                Mmap::new(fd.as_raw_fd(), sys::IORING_OFF_SQ_RING, sq_len)?,
                Some(Mmap::new(fd.as_raw_fd(), sys::IORING_OFF_CQ_RING, cq_len)?),
            )
        };

        let sq = unsafe { SubmissionInner::new(&sq_mmap, &sqe_mmap, &params) };
        let cq_map = cq_mmap.as_ref().unwrap_or(&sq_mmap);
        let cq = unsafe { CompletionInner::new(cq_map, &params) };
        let memory = MemoryMap {
            _sq_mmap: sq_mmap,
            _cq_mmap: cq_mmap,
            _sqe_mmap: sqe_mmap,
        };

        Ok(Self {
            sq,
            cq,
            _fd: fd,
            params: Parameters(params),
            memory: ManuallyDrop::new(memory),
        })
    }
}

// Queue views borrow the mapped memory through `&mut self`, while the kernel
// may access the same mappings asynchronously through the owned ring fd.
unsafe impl Send for IoUring {}
unsafe impl Sync for IoUring {}

impl Drop for IoUring {
    fn drop(&mut self) {
        unsafe {
            ManuallyDrop::drop(&mut self.memory);
        }
    }
}

fn checked_map_len(offset: u32, entries: u32, element_size: usize) -> Result<usize> {
    let bytes = (entries as usize)
        .checked_mul(element_size)
        .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
    (offset as usize)
        .checked_add(bytes)
        .filter(|length| *length != 0)
        .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))
}
