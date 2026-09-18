//! Submission, enter, and resource registration APIs for an io_uring instance.

use core::{
    convert::TryFrom,
    marker::PhantomData,
    mem, ptr,
    sync::atomic::{self, Ordering},
};

use veloq_std::{
    io::{Error, ErrorKind, Result},
    os::unix::fd::{AsRawFd, OwnedFd, RawFd},
};

use crate::{
    register::{
        self, Probe, ResourceKind, ResourceLayout, ResourceRegistration, ResourceRegistrationState,
    },
    ring::Parameters,
    squeue::SubmissionState,
    sys,
    types::SubmitArgs,
};

/// The immutable subset of ring capabilities needed by the submission hot path.
///
/// `Parameters` is an initialization result and must not be reinterpreted on every
/// `io_uring_enter` call. This copy is created once when a [`Submitter`] is built;
/// feature negotiation and diagnostics keep using the full capability snapshot.
#[derive(Clone, Copy)]
struct SubmitterCapabilities {
    sq_entries: u32,
    setup_flags: u32,
    features: u32,
}

impl SubmitterCapabilities {
    #[inline]
    fn from_parameters(parameters: &Parameters) -> Self {
        Self {
            sq_entries: parameters.sq_entries(),
            setup_flags: parameters.setup_flags(),
            features: parameters.features(),
        }
    }

    #[inline]
    const fn is_setup_sqpoll(self) -> bool {
        self.setup_flags & sys::IORING_SETUP_SQPOLL != 0
    }

    #[inline]
    const fn is_setup_iopoll(self) -> bool {
        self.setup_flags & sys::IORING_SETUP_IOPOLL != 0
    }

    #[inline]
    const fn is_feature_nodrop(self) -> bool {
        self.features & sys::IORING_FEAT_NODROP != 0
    }
}

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
    /// Interpret the extended timeout as an absolute time.
    pub const ABS_TIMER: Self = Self(sys::IORING_ENTER_ABS_TIMER);
    /// Use an offset into previously registered wait regions.
    pub const EXT_ARG_REG: Self = Self(sys::IORING_ENTER_EXT_ARG_REG);
    /// Do not account a waiting task as being in iowait.
    pub const NO_IOWAIT: Self = Self(sys::IORING_ENTER_NO_IOWAIT);

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

const KNOWN_ENTER_FLAGS: u32 = sys::IORING_ENTER_GETEVENTS
    | sys::IORING_ENTER_SQ_WAKEUP
    | sys::IORING_ENTER_SQ_WAIT
    | sys::IORING_ENTER_EXT_ARG
    | sys::IORING_ENTER_REGISTERED_RING
    | sys::IORING_ENTER_ABS_TIMER
    | sys::IORING_ENTER_EXT_ARG_REG
    | sys::IORING_ENTER_NO_IOWAIT;

#[derive(Clone, Copy, Debug)]
enum EnterArgument<'a> {
    None,
    SignalMask(&'a libc::sigset_t),
    Extended(SubmitArgs<'a, 'a>),
}

/// Typed arguments for one `io_uring_enter` call.
///
/// The argument kind and the `EXT_ARG` flag are validated together before the
/// syscall. Callers cannot provide an arbitrary pointer or an unrelated
/// argument size through this API.
#[derive(Clone, Copy, Debug)]
pub struct EnterArgs<'a> {
    to_submit: u32,
    min_complete: u32,
    flags: EnterFlags,
    argument: EnterArgument<'a>,
}

impl EnterArgs<'static> {
    /// Create a legacy enter request without an argument pointer.
    #[inline]
    #[must_use]
    pub const fn new(to_submit: u32, min_complete: u32) -> Self {
        Self {
            to_submit,
            min_complete,
            flags: EnterFlags::empty(),
            argument: EnterArgument::None,
        }
    }
}

impl<'a> EnterArgs<'a> {
    /// Set the flags for this enter request.
    #[inline]
    #[must_use]
    pub const fn flags(mut self, flags: EnterFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Use a legacy signal mask argument.
    #[inline]
    #[must_use]
    pub fn sigmask<'next>(self, sigmask: &'next libc::sigset_t) -> EnterArgs<'next> {
        EnterArgs {
            to_submit: self.to_submit,
            min_complete: self.min_complete,
            flags: self.flags,
            argument: EnterArgument::SignalMask(sigmask),
        }
    }

    /// Use a validated extended enter argument.
    #[inline]
    #[must_use]
    pub fn extended<'next>(self, args: SubmitArgs<'next, 'next>) -> EnterArgs<'next> {
        EnterArgs {
            to_submit: self.to_submit,
            min_complete: self.min_complete,
            flags: self.flags,
            argument: EnterArgument::Extended(args),
        }
    }

    #[inline]
    pub const fn to_submit(self) -> u32 {
        self.to_submit
    }

    #[inline]
    pub const fn min_complete(self) -> u32 {
        self.min_complete
    }

    #[inline]
    pub const fn enter_flags(self) -> EnterFlags {
        self.flags
    }

    fn validate(&self, capabilities: SubmitterCapabilities) -> Result<()> {
        if self.flags.bits() & !KNOWN_ENTER_FLAGS != 0 {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.to_submit > capabilities.sq_entries {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.min_complete != 0 && !self.flags.contains(EnterFlags::GETEVENTS) {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.flags.contains(EnterFlags::SQ_WAKEUP) && !capabilities.is_setup_sqpoll() {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.flags.contains(EnterFlags::SQ_WAIT) && !capabilities.is_setup_sqpoll() {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        if self.flags.contains(EnterFlags::REGISTERED_RING) {
            return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        if self.flags.contains(EnterFlags::EXT_ARG_REG) {
            return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        if self.flags.contains(EnterFlags::ABS_TIMER) && !self.flags.contains(EnterFlags::EXT_ARG) {
            return Err(Error::from(ErrorKind::InvalidInput));
        }

        match self.argument {
            EnterArgument::None => {
                if self.flags.contains(EnterFlags::EXT_ARG) {
                    return Err(Error::from(ErrorKind::InvalidInput));
                }
            }
            EnterArgument::SignalMask(_) => {
                if self.flags.contains(EnterFlags::EXT_ARG) {
                    return Err(Error::from(ErrorKind::InvalidInput));
                }
            }
            EnterArgument::Extended(args) => {
                if !self.flags.contains(EnterFlags::EXT_ARG) {
                    return Err(Error::from(ErrorKind::InvalidInput));
                }
                args.validate()?;
            }
        }
        Ok(())
    }

    #[inline]
    fn raw_argument(self) -> (*const libc::c_void, usize) {
        match self.argument {
            EnterArgument::None => (ptr::null(), 0),
            EnterArgument::SignalMask(sigmask) => (
                ptr::from_ref(sigmask).cast(),
                mem::size_of::<libc::sigset_t>(),
            ),
            EnterArgument::Extended(args) => (
                ptr::from_ref(&args).cast(),
                mem::size_of::<sys::IoUringGeteventsArg>(),
            ),
        }
    }
}

/// The typed result of an enter operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitReceipt {
    /// No syscall was needed because there was no work to publish.
    NoEntries { cq_overflow: bool },
    /// SQEs were published to a live SQPOLL thread without a syscall.
    PublishedToSqpoll { published: u32, cq_overflow: bool },
    /// The syscall returned a confirmed number of consumed SQEs.
    Submitted {
        requested: u32,
        submitted: u32,
        woke_sqpoll: bool,
        cq_overflow: bool,
    },
}

/// A submission error with an explicit kernel-consumption boundary.
#[derive(Debug)]
pub enum SubmitError {
    /// The request was rejected before any SQE could be consumed.
    Rejected {
        error: Error,
        requested: u32,
        woke_sqpoll: bool,
        cq_overflow: bool,
    },
    /// The syscall did not provide enough evidence to retry the SQEs safely.
    Unknown {
        error: Error,
        requested: u32,
        woke_sqpoll: bool,
        cq_overflow: bool,
    },
}

impl SubmitError {
    #[inline]
    pub fn error(&self) -> &Error {
        match self {
            Self::Rejected { error, .. } | Self::Unknown { error, .. } => error,
        }
    }

    #[inline]
    pub fn into_error(self) -> Error {
        match self {
            Self::Rejected { error, .. } | Self::Unknown { error, .. } => error,
        }
    }

    #[inline]
    pub const fn requested(&self) -> u32 {
        match self {
            Self::Rejected { requested, .. } | Self::Unknown { requested, .. } => *requested,
        }
    }

    #[inline]
    pub const fn woke_sqpoll(&self) -> bool {
        match self {
            Self::Rejected { woke_sqpoll, .. } | Self::Unknown { woke_sqpoll, .. } => *woke_sqpoll,
        }
    }

    #[inline]
    pub const fn cq_overflow(&self) -> bool {
        match self {
            Self::Rejected { cq_overflow, .. } | Self::Unknown { cq_overflow, .. } => *cq_overflow,
        }
    }

    #[inline]
    pub const fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }
}

/// The result returned by submission APIs.
pub type SubmitResult = core::result::Result<SubmitReceipt, SubmitError>;

/// Interface for submitting SQEs and registering ring resources.
pub struct Submitter<'ring> {
    fd: &'ring OwnedFd,
    capabilities: SubmitterCapabilities,
    sq: SubmissionState,
    _owner: PhantomData<&'ring mut Parameters>,
}

impl<'ring> Submitter<'ring> {
    #[inline]
    pub(crate) fn new(fd: &'ring OwnedFd, params: &'ring Parameters, sq: SubmissionState) -> Self {
        Self {
            fd,
            capabilities: SubmitterCapabilities::from_parameters(params),
            sq,
            _owner: PhantomData,
        }
    }

    #[inline]
    fn sq_len(&self) -> usize {
        self.sq.len()
    }

    #[inline]
    fn sq_need_wakeup(&self) -> bool {
        self.sq.flags(Ordering::Relaxed) & sys::IORING_SQ_NEED_WAKEUP != 0
    }

    #[inline]
    fn sq_cq_overflow(&self) -> bool {
        self.sq.flags(Ordering::Relaxed) & sys::IORING_SQ_CQ_OVERFLOW != 0
    }

    /// Enter the kernel with validated, typed arguments.
    pub fn enter(&self, args: EnterArgs<'_>) -> SubmitResult {
        let requested = args.to_submit();
        let woke_sqpoll = args.enter_flags().contains(EnterFlags::SQ_WAKEUP);
        let cq_overflow = self.sq_cq_overflow();
        if let Err(error) = args.validate(self.capabilities) {
            return Err(SubmitError::Rejected {
                error,
                requested,
                woke_sqpoll,
                cq_overflow,
            });
        }

        let (arg, arg_size) = args.raw_argument();
        match unsafe {
            sys::io_uring_enter(
                self.fd.as_raw_fd(),
                requested,
                args.min_complete(),
                args.enter_flags().bits(),
                arg,
                arg_size,
            )
        } {
            Ok(value) => match u32::try_from(value) {
                Ok(submitted) if submitted <= requested => Ok(SubmitReceipt::Submitted {
                    requested,
                    submitted,
                    woke_sqpoll,
                    cq_overflow,
                }),
                Ok(_) => Err(SubmitError::Unknown {
                    error: Error::from_raw_os_error(libc::EPROTO),
                    requested,
                    woke_sqpoll,
                    cq_overflow,
                }),
                Err(_) => Err(SubmitError::Unknown {
                    error: Error::from_raw_os_error(libc::EOVERFLOW),
                    requested,
                    woke_sqpoll,
                    cq_overflow,
                }),
            },
            Err(error) => Err(classify_enter_error(
                error,
                requested,
                woke_sqpoll,
                cq_overflow,
            )),
        }
    }

    /// Submit all currently published SQEs.
    #[inline]
    pub fn submit(&self) -> SubmitResult {
        self.submit_and_wait(0)
    }

    /// Submit all currently published SQEs and wait for completions.
    pub fn submit_and_wait(&self, want: usize) -> SubmitResult {
        let to_submit = match checked_u32(self.sq_len()) {
            Ok(value) => value,
            Err(error) => return Err(rejected_input(error)),
        };
        let min_complete = match checked_u32(want) {
            Ok(value) => value,
            Err(error) => return Err(rejected_input(error)),
        };
        let sq_cq_overflow = self.sq_cq_overflow();
        let need_syscall_for_overflow = sq_cq_overflow && self.capabilities.is_feature_nodrop();
        let mut flags = EnterFlags::empty();

        if want > 0 || self.capabilities.is_setup_iopoll() || sq_cq_overflow {
            flags.insert(EnterFlags::GETEVENTS);
        }

        if self.capabilities.is_setup_sqpoll() {
            // The SeqCst fence is required before interpreting NEED_WAKEUP.
            atomic::fence(Ordering::SeqCst);
            if self.sq_need_wakeup() {
                flags.insert(EnterFlags::SQ_WAKEUP);
            } else if want == 0 && !need_syscall_for_overflow {
                return Ok(if to_submit == 0 {
                    SubmitReceipt::NoEntries {
                        cq_overflow: sq_cq_overflow,
                    }
                } else {
                    SubmitReceipt::PublishedToSqpoll {
                        published: to_submit,
                        cq_overflow: sq_cq_overflow,
                    }
                });
            }
        }

        self.enter(EnterArgs::new(to_submit, min_complete).flags(flags))
    }

    /// Submit SQEs and wait for completions using an extended enter argument.
    pub fn submit_with_args(&self, want: usize, args: &SubmitArgs<'_, '_>) -> SubmitResult {
        let to_submit = match checked_u32(self.sq_len()) {
            Ok(value) => value,
            Err(error) => return Err(rejected_input(error)),
        };
        let min_complete = match checked_u32(want) {
            Ok(value) => value,
            Err(error) => return Err(rejected_input(error)),
        };
        let sq_cq_overflow = self.sq_cq_overflow();
        let need_syscall_for_overflow = sq_cq_overflow && self.capabilities.is_feature_nodrop();
        let mut flags = EnterFlags::EXT_ARG;

        if want > 0 || self.capabilities.is_setup_iopoll() || sq_cq_overflow {
            flags.insert(EnterFlags::GETEVENTS);
        }

        if self.capabilities.is_setup_sqpoll() {
            atomic::fence(Ordering::SeqCst);
            if self.sq_need_wakeup() {
                flags.insert(EnterFlags::SQ_WAKEUP);
            } else if want == 0 && !need_syscall_for_overflow {
                return Ok(if to_submit == 0 {
                    SubmitReceipt::NoEntries {
                        cq_overflow: sq_cq_overflow,
                    }
                } else {
                    SubmitReceipt::PublishedToSqpoll {
                        published: to_submit,
                        cq_overflow: sq_cq_overflow,
                    }
                });
            }
        }

        self.enter(
            EnterArgs::new(to_submit, min_complete)
                .flags(flags)
                .extended(args.reborrow()),
        )
    }

    /// Wait for the submission queue to have free entries.
    pub fn squeue_wait(&self) -> SubmitResult {
        self.enter(EnterArgs::new(0, 0).flags(EnterFlags::SQ_WAIT))
    }

    /// Register a contiguous fixed-buffer table.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers(&self, bufs: &[libc::iovec]) -> Result<ResourceRegistration> {
        let nr_args = checked_nonempty_u32(bufs.len())?;
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_BUFFERS,
            bufs.as_ptr().cast(),
            nr_args,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Buffers,
                ResourceLayout::Contiguous,
                nr_args,
                false,
            )
        })
    }

    /// Register a sparse fixed-buffer table using `IORING_REGISTER_BUFFERS2`.
    ///
    /// Unlike [`Self::register_buffers`], this does not pass an iovec array. The kernel creates
    /// `count` empty slots and the slots are populated later through the resource update ABI.
    pub fn register_buffers_sparse(&self, count: usize) -> Result<ResourceRegistration> {
        let nr = checked_nonempty_u32(count)?;
        let register = sys::IoUringRsrcRegister {
            nr,
            flags: sys::IORING_RSRC_REGISTER_SPARSE,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_BUFFERS2,
            ptr::from_ref(&register).cast(),
            mem::size_of::<sys::IoUringRsrcRegister>() as u32,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Buffers,
                ResourceLayout::Sparse,
                nr,
                false,
            )
        })
    }

    /// Register a contiguous fixed-buffer table with one tag per slot.
    ///
    /// The tagged registration ABI is available through `REGISTER_BUFFERS2`; unlike the legacy
    /// contiguous API it also records the tag pointer in the kernel resource table.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers_tagged(
        &self,
        bufs: &[libc::iovec],
        tags: &[u64],
    ) -> Result<ResourceRegistration> {
        let nr = checked_nonempty_u32(bufs.len())?;
        validate_tags(bufs.len(), tags)?;
        let register = sys::IoUringRsrcRegister {
            nr,
            data: bufs.as_ptr() as u64,
            tags: tags.as_ptr() as u64,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_BUFFERS2,
            ptr::from_ref(&register).cast(),
            mem::size_of::<sys::IoUringRsrcRegister>() as u32,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Buffers,
                ResourceLayout::Contiguous,
                nr,
                true,
            )
        })
    }

    /// Update an untagged fixed-buffer table beginning at `offset`.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers_update(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        bufs: &[libc::iovec],
    ) -> Result<usize> {
        unsafe { self.register_buffers_update_inner(registration, offset, bufs, None) }
    }

    /// Update a tagged fixed-buffer table beginning at `offset`.
    ///
    /// # Safety
    ///
    /// Every iovec and the memory it references must remain valid until the
    /// buffers are unregistered or the ring is destroyed.
    pub unsafe fn register_buffers_update_tagged(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        bufs: &[libc::iovec],
        tags: &[u64],
    ) -> Result<usize> {
        validate_tags(bufs.len(), tags)?;
        unsafe { self.register_buffers_update_inner(registration, offset, bufs, Some(tags)) }
    }

    unsafe fn register_buffers_update_inner(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        bufs: &[libc::iovec],
        tags: Option<&[u64]>,
    ) -> Result<usize> {
        let nr = checked_nonempty_u32(bufs.len())?;
        let tagged = tags.is_some();
        registration.validate_update(
            self.fd.as_raw_fd(),
            ResourceKind::Buffers,
            offset,
            nr,
            tagged,
        )?;
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
        .and_then(|result| to_updated_count(result, nr))
    }

    /// Register a contiguous fixed-file table.
    pub fn register_files(&self, files: &[RawFd]) -> Result<ResourceRegistration> {
        let nr_args = checked_nonempty_u32(files.len())?;
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES,
            files.as_ptr().cast(),
            nr_args,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Files,
                ResourceLayout::Contiguous,
                nr_args,
                false,
            )
        })
    }

    /// Register a contiguous fixed-file table with one tag per slot.
    pub fn register_files_tagged(
        &self,
        files: &[RawFd],
        tags: &[u64],
    ) -> Result<ResourceRegistration> {
        let nr = checked_nonempty_u32(files.len())?;
        validate_tags(files.len(), tags)?;
        let register = sys::IoUringRsrcRegister {
            nr,
            data: files.as_ptr() as u64,
            tags: tags.as_ptr() as u64,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES2,
            ptr::from_ref(&register).cast(),
            mem::size_of::<sys::IoUringRsrcRegister>() as u32,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Files,
                ResourceLayout::Contiguous,
                nr,
                true,
            )
        })
    }

    /// Update an untagged fixed-file table and return the number of entries accepted by the
    /// kernel.
    pub fn register_files_update(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        files: &[RawFd],
    ) -> Result<usize> {
        let nr_args = checked_nonempty_u32(files.len())?;
        registration.validate_update(
            self.fd.as_raw_fd(),
            ResourceKind::Files,
            offset,
            nr_args,
            false,
        )?;
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
        to_updated_count(result, nr_args)
    }

    /// Update a tagged fixed-file table and return the number of entries accepted by the kernel.
    pub fn register_files_update_tagged(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        files: &[RawFd],
        tags: &[u64],
    ) -> Result<usize> {
        let nr = checked_nonempty_u32(files.len())?;
        validate_tags(files.len(), tags)?;
        registration.validate_update(self.fd.as_raw_fd(), ResourceKind::Files, offset, nr, true)?;
        let update = sys::IoUringRsrcUpdate2 {
            offset,
            data: files.as_ptr() as u64,
            tags: tags.as_ptr() as u64,
            nr,
            ..Default::default()
        };
        let result = register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES_UPDATE2,
            ptr::from_ref(&update).cast(),
            mem::size_of::<sys::IoUringRsrcUpdate2>() as u32,
        )?;
        to_updated_count(result, nr)
    }

    /// Register a sparse fixed-file table using `IORING_REGISTER_FILES2`.
    pub fn register_files_sparse(&self, count: usize) -> Result<ResourceRegistration> {
        let nr = checked_nonempty_u32(count)?;
        let register = sys::IoUringRsrcRegister {
            nr,
            flags: sys::IORING_RSRC_REGISTER_SPARSE,
            ..Default::default()
        };
        register::execute(
            self.fd.as_raw_fd(),
            sys::IORING_REGISTER_FILES2,
            ptr::from_ref(&register).cast(),
            mem::size_of::<sys::IoUringRsrcRegister>() as u32,
        )
        .and_then(expect_zero)
        .map(|_| {
            ResourceRegistration::new(
                self.fd.as_raw_fd(),
                ResourceKind::Files,
                ResourceLayout::Sparse,
                nr,
                false,
            )
        })
    }

    /// Unregister a fixed-buffer table.
    pub fn unregister_buffers(&self, registration: &mut ResourceRegistration) -> Result<()> {
        unregister_resource(
            self.fd.as_raw_fd(),
            registration,
            ResourceKind::Buffers,
            sys::IORING_UNREGISTER_BUFFERS,
        )
    }

    /// Unregister a fixed-file table.
    pub fn unregister_files(&self, registration: &mut ResourceRegistration) -> Result<()> {
        unregister_resource(
            self.fd.as_raw_fd(),
            registration,
            ResourceKind::Files,
            sys::IORING_UNREGISTER_FILES,
        )
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

fn checked_nonempty_u32(value: usize) -> Result<u32> {
    if value == 0 {
        return Err(Error::from(ErrorKind::InvalidInput));
    }
    checked_u32(value)
}

fn expect_zero(result: i64) -> Result<()> {
    if result == 0 {
        Ok(())
    } else {
        Err(Error::from_raw_os_error(libc::EPROTO))
    }
}

fn to_updated_count(result: i64, requested: u32) -> Result<usize> {
    let updated = usize::try_from(result).map_err(|_| Error::from_raw_os_error(libc::EOVERFLOW))?;
    if updated > requested as usize {
        return Err(Error::from_raw_os_error(libc::EPROTO));
    }
    Ok(updated)
}

fn validate_tags(resource_count: usize, tags: &[u64]) -> Result<()> {
    if tags.len() != resource_count {
        return Err(Error::from(ErrorKind::InvalidInput));
    }
    Ok(())
}

fn checked_u32(value: usize) -> Result<u32> {
    value
        .try_into()
        .map_err(|_| Error::from(ErrorKind::InvalidInput))
}

fn rejected_input(error: Error) -> SubmitError {
    SubmitError::Rejected {
        error,
        requested: 0,
        woke_sqpoll: false,
        cq_overflow: false,
    }
}

fn classify_enter_error(
    error: Error,
    requested: u32,
    woke_sqpoll: bool,
    cq_overflow: bool,
) -> SubmitError {
    match error.raw_os_error() {
        Some(libc::EBADF | libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP | libc::EPERM) => {
            SubmitError::Rejected {
                error,
                requested,
                woke_sqpoll,
                cq_overflow,
            }
        }
        Some(libc::EAGAIN | libc::EBUSY | libc::EINTR) | None => SubmitError::Unknown {
            error,
            requested,
            woke_sqpoll,
            cq_overflow,
        },
        Some(_) => SubmitError::Unknown {
            error,
            requested,
            woke_sqpoll,
            cq_overflow,
        },
    }
}

fn unregister_resource(
    fd: RawFd,
    registration: &mut ResourceRegistration,
    kind: ResourceKind,
    opcode: u32,
) -> Result<()> {
    if !registration.owner_matches(fd)
        || registration.kind() != kind
        || registration.state() != ResourceRegistrationState::Registered
    {
        return Err(Error::from(ErrorKind::InvalidInput));
    }

    match register::execute(fd, opcode, ptr::null(), 0).and_then(expect_zero) {
        Ok(()) => {
            registration.mark_unregistered();
            Ok(())
        }
        Err(error) => {
            registration.mark_unregister_unknown(error.raw_os_error());
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Timespec;

    use super::*;

    fn parameters(flags: u32, features: u32) -> Parameters {
        Parameters(sys::IoUringParams {
            flags,
            features,
            sq_entries: 4,
            ..Default::default()
        })
    }

    #[test]
    fn enter_args_reject_mismatched_argument_kind_and_flags() {
        let params = parameters(0, 0);
        let capabilities = SubmitterCapabilities::from_parameters(&params);
        let extended = EnterArgs::new(0, 0).flags(EnterFlags::EXT_ARG);
        assert_eq!(
            extended.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );

        let args = SubmitArgs::new();
        let legacy = EnterArgs::new(0, 0).extended(args);
        assert_eq!(
            legacy.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
    }

    #[test]
    fn submitter_capabilities_are_an_initialization_snapshot() {
        let mut params = parameters(sys::IORING_SETUP_SQPOLL, sys::IORING_FEAT_NODROP);
        let capabilities = SubmitterCapabilities::from_parameters(&params);
        params.0.flags = 0;
        params.0.features = 0;

        assert_eq!(params.setup_flags(), 0);
        assert_eq!(params.features(), 0);
        assert!(capabilities.is_setup_sqpoll());
        assert!(capabilities.is_feature_nodrop());
    }

    #[test]
    fn enter_args_reject_invalid_flags_and_incompatible_wait_flags() {
        let params = parameters(0, 0);
        let capabilities = SubmitterCapabilities::from_parameters(&params);
        let too_many = EnterArgs::new(5, 0);
        assert_eq!(
            too_many.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );

        let unknown = EnterArgs::new(0, 0).flags(EnterFlags::from_bits_retain(1 << 31));
        assert_eq!(
            unknown.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );

        let min_without_getevents = EnterArgs::new(0, 1);
        assert_eq!(
            min_without_getevents
                .validate(capabilities)
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );

        let wakeup = EnterArgs::new(0, 0).flags(EnterFlags::SQ_WAKEUP);
        assert_eq!(
            wakeup.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );

        let sq_wait = EnterArgs::new(0, 0).flags(EnterFlags::SQ_WAIT);
        assert_eq!(
            sq_wait.validate(capabilities).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );

        let registered_ring = EnterArgs::new(0, 0).flags(EnterFlags::REGISTERED_RING);
        assert_eq!(
            registered_ring
                .validate(capabilities)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EOPNOTSUPP)
        );
    }

    #[test]
    fn extended_arguments_validate_pointer_fields_and_abi_size() {
        let params = parameters(0, 0);
        let timespec = Timespec::new();
        let submit_args = SubmitArgs::new().timespec(&timespec);
        let enter_args = EnterArgs::new(0, 0)
            .flags(EnterFlags::EXT_ARG)
            .extended(submit_args.reborrow());
        enter_args
            .validate(SubmitterCapabilities::from_parameters(&params))
            .expect("valid ext args");

        let (_, arg_size) = enter_args.raw_argument();
        assert_eq!(arg_size, mem::size_of::<sys::IoUringGeteventsArg>());
    }

    #[test]
    fn enter_error_classification_preserves_retry_boundary() {
        let rejected =
            classify_enter_error(Error::from_raw_os_error(libc::EINVAL), 3, false, false);
        assert!(!rejected.is_unknown());
        assert_eq!(rejected.requested(), 3);

        let unknown = classify_enter_error(Error::from_raw_os_error(libc::EINTR), 3, true, true);
        assert!(unknown.is_unknown());
        assert_eq!(unknown.requested(), 3);
        assert!(unknown.woke_sqpoll());
        assert!(unknown.cq_overflow());
    }

    #[test]
    fn receipt_records_sqpoll_publication_and_syscall_consumption_separately() {
        let published = SubmitReceipt::PublishedToSqpoll {
            published: 2,
            cq_overflow: false,
        };
        let consumed = SubmitReceipt::Submitted {
            requested: 2,
            submitted: 1,
            woke_sqpoll: true,
            cq_overflow: false,
        };
        assert!(matches!(published, SubmitReceipt::PublishedToSqpoll { .. }));
        assert!(matches!(
            consumed,
            SubmitReceipt::Submitted { submitted: 1, .. }
        ));
    }

    #[test]
    fn registration_helpers_reject_short_tags_and_invalid_update_counts() {
        assert_eq!(
            validate_tags(2, &[1]).unwrap_err().kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            to_updated_count(-1, 1).unwrap_err().raw_os_error(),
            Some(libc::EOVERFLOW)
        );
        assert_eq!(
            to_updated_count(2, 1).unwrap_err().raw_os_error(),
            Some(libc::EPROTO)
        );
        assert_eq!(to_updated_count(1, 1).unwrap(), 1);
    }

    #[test]
    fn unregister_failure_keeps_the_resource_in_an_unknown_state() {
        let mut registration =
            ResourceRegistration::new(-1, ResourceKind::Buffers, ResourceLayout::Sparse, 4, false);
        let error = unregister_resource(
            -1,
            &mut registration,
            ResourceKind::Buffers,
            sys::IORING_UNREGISTER_BUFFERS,
        )
        .expect_err("invalid ring fd must fail");
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(
            registration.state(),
            ResourceRegistrationState::UnregisterUnknown {
                errno: Some(libc::EINVAL)
            }
        );
    }

    #[test]
    fn unregister_rejects_a_registration_owned_by_another_ring() {
        let mut registration =
            ResourceRegistration::new(7, ResourceKind::Buffers, ResourceLayout::Sparse, 4, false);
        let error = unregister_resource(
            8,
            &mut registration,
            ResourceKind::Buffers,
            sys::IORING_UNREGISTER_BUFFERS,
        )
        .expect_err("a registration must not be used with another ring");
        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(registration.state(), ResourceRegistrationState::Registered);
    }
}
