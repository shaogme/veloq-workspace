//! Submission, enter, and resource registration APIs for an io_uring instance.

mod enter;
mod result;

pub use enter::{EnterArgs, EnterFlags};
pub use result::{SubmitError, SubmitReceipt};

use enter::SubmitterCapabilities;

use core::{
    convert::TryFrom,
    marker::PhantomData,
    mem, ptr,
    result::Result as CoreResult,
    sync::atomic::{self, Ordering},
};
use libc::{
    EAGAIN, EBADF, EBUSY, EINTR, EINVAL, ENOSYS, EOPNOTSUPP, EOVERFLOW, EPERM, EPROTO, iovec,
};

use veloq_std::{
    io::{Error, ErrorKind, Result},
    os::unix::fd::{AsRawFd, OwnedFd, RawFd},
};

use crate::{
    internal::{
        register::{
            Probe, RegistrationUpdate, ResourceKind, ResourceLayout, ResourceRegistration,
            ResourceRegistrationState,
        },
        ring::Parameters,
        squeue::SubmissionState,
        syscalls::{EnterSyscallArgs, Syscalls},
        types::SubmitArgs,
    },
    sys,
};

#[cfg(test)]
use crate::internal::types::Timespec;

/// The result returned by submission APIs.
pub type SubmitResult = CoreResult<SubmitReceipt, SubmitError>;

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
            Syscalls::enter(EnterSyscallArgs {
                fd: self.fd.as_raw_fd(),
                to_submit: requested,
                min_complete: args.min_complete(),
                flags: args.enter_flags().bits(),
                arg,
                arg_size,
            })
        } {
            Ok(value) => match u32::try_from(value) {
                Ok(submitted) if submitted <= requested => Ok(SubmitReceipt::Submitted {
                    requested,
                    submitted,
                    woke_sqpoll,
                    cq_overflow,
                }),
                Ok(_) => Err(SubmitError::Unknown {
                    error: Error::from_raw_os_error(EPROTO),
                    requested,
                    woke_sqpoll,
                    cq_overflow,
                }),
                Err(_) => Err(SubmitError::Unknown {
                    error: Error::from_raw_os_error(EOVERFLOW),
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
    pub unsafe fn register_buffers(&self, bufs: &[iovec]) -> Result<ResourceRegistration> {
        let nr_args = checked_nonempty_u32(bufs.len())?;
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_BUFFERS,
                bufs.as_ptr().cast(),
                nr_args,
            )
        }
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_BUFFERS2,
                ptr::from_ref(&register).cast(),
                mem::size_of::<sys::IoUringRsrcRegister>() as u32,
            )
        }
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
        bufs: &[iovec],
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_BUFFERS2,
                ptr::from_ref(&register).cast(),
                mem::size_of::<sys::IoUringRsrcRegister>() as u32,
            )
        }
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
        bufs: &[iovec],
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
        bufs: &[iovec],
        tags: &[u64],
    ) -> Result<usize> {
        validate_tags(bufs.len(), tags)?;
        unsafe { self.register_buffers_update_inner(registration, offset, bufs, Some(tags)) }
    }

    unsafe fn register_buffers_update_inner(
        &self,
        registration: &ResourceRegistration,
        offset: u32,
        bufs: &[iovec],
        tags: Option<&[u64]>,
    ) -> Result<usize> {
        let nr = checked_nonempty_u32(bufs.len())?;
        let tagged = tags.is_some();
        registration.validate_update(RegistrationUpdate {
            owner_fd: self.fd.as_raw_fd(),
            kind: ResourceKind::Buffers,
            offset,
            count: nr,
            tagged,
        })?;
        let update = sys::IoUringRsrcUpdate2 {
            offset,
            data: bufs.as_ptr() as u64,
            tags: tags.map_or(0, |values| values.as_ptr() as u64),
            nr,
            ..Default::default()
        };

        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_BUFFERS_UPDATE,
                ptr::from_ref(&update).cast(),
                mem::size_of::<sys::IoUringRsrcUpdate2>() as u32,
            )
        }
        .and_then(|result| to_updated_count(result, nr))
    }

    /// Register a contiguous fixed-file table.
    pub fn register_files(&self, files: &[RawFd]) -> Result<ResourceRegistration> {
        let nr_args = checked_nonempty_u32(files.len())?;
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_FILES,
                files.as_ptr().cast(),
                nr_args,
            )
        }
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_FILES2,
                ptr::from_ref(&register).cast(),
                mem::size_of::<sys::IoUringRsrcRegister>() as u32,
            )
        }
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
        registration.validate_update(RegistrationUpdate {
            owner_fd: self.fd.as_raw_fd(),
            kind: ResourceKind::Files,
            offset,
            count: nr_args,
            tagged: false,
        })?;
        let update = sys::IoUringFilesUpdate {
            offset,
            fds: files.as_ptr() as u64,
            ..Default::default()
        };
        let result = unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_FILES_UPDATE,
                ptr::from_ref(&update).cast(),
                nr_args,
            )
        }?;
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
        registration.validate_update(RegistrationUpdate {
            owner_fd: self.fd.as_raw_fd(),
            kind: ResourceKind::Files,
            offset,
            count: nr,
            tagged: true,
        })?;
        let update = sys::IoUringRsrcUpdate2 {
            offset,
            data: files.as_ptr() as u64,
            tags: tags.as_ptr() as u64,
            nr,
            ..Default::default()
        };
        let result = unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_FILES_UPDATE2,
                ptr::from_ref(&update).cast(),
                mem::size_of::<sys::IoUringRsrcUpdate2>() as u32,
            )
        }?;
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_FILES2,
                ptr::from_ref(&register).cast(),
                mem::size_of::<sys::IoUringRsrcRegister>() as u32,
            )
        }
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_PROBE,
                probe.as_mut_ptr().cast(),
                Probe::COUNT as u32,
            )
        }
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
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_REGISTER_PBUF_RING,
                ptr::from_ref(&register).cast(),
                1,
            )
        }
        .map(drop)
    }

    /// Unregister a provided-buffer ring.
    pub fn unregister_buf_ring(&self, bgid: u16) -> Result<()> {
        let unregister = sys::IoUringBufReg {
            bgid,
            ..Default::default()
        };
        unsafe {
            Syscalls::register(
                self.fd.as_raw_fd(),
                sys::IORING_UNREGISTER_PBUF_RING,
                ptr::from_ref(&unregister).cast(),
                1,
            )
        }
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
        Err(Error::from_raw_os_error(EPROTO))
    }
}

fn to_updated_count(result: i64, requested: u32) -> Result<usize> {
    let updated = usize::try_from(result).map_err(|_| Error::from_raw_os_error(EOVERFLOW))?;
    if updated > requested as usize {
        return Err(Error::from_raw_os_error(EPROTO));
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
        Some(EBADF | EINVAL | ENOSYS | EOPNOTSUPP | EPERM) => SubmitError::Rejected {
            error,
            requested,
            woke_sqpoll,
            cq_overflow,
        },
        Some(EAGAIN | EBUSY | EINTR) | None => SubmitError::Unknown {
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

    match unsafe { Syscalls::register(fd, opcode, ptr::null(), 0) }.and_then(expect_zero) {
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
fn parameters(flags: u32, features: u32) -> Parameters {
    Parameters::from_raw(sys::IoUringParams {
        flags,
        features,
        sq_entries: 4,
        ..Default::default()
    })
}

#[cfg(test)]
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

#[cfg(test)]
#[test]
fn submitter_capabilities_are_an_initialization_snapshot() {
    let mut params = parameters(sys::IORING_SETUP_SQPOLL, sys::IORING_FEAT_NODROP);
    let capabilities = SubmitterCapabilities::from_parameters(&params);
    params.as_raw_mut().flags = 0;
    params.as_raw_mut().features = 0;

    assert_eq!(params.setup_flags(), 0);
    assert_eq!(params.features(), 0);
    assert!(capabilities.is_setup_sqpoll());
    assert!(capabilities.is_feature_nodrop());
}

#[cfg(test)]
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
        Some(EOPNOTSUPP)
    );
}

#[cfg(test)]
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

#[cfg(test)]
#[test]
fn enter_error_classification_preserves_retry_boundary() {
    let rejected = classify_enter_error(Error::from_raw_os_error(EINVAL), 3, false, false);
    assert!(!rejected.is_unknown());
    assert_eq!(rejected.requested(), 3);

    let unknown = classify_enter_error(Error::from_raw_os_error(EINTR), 3, true, true);
    assert!(unknown.is_unknown());
    assert_eq!(unknown.requested(), 3);
    assert!(unknown.woke_sqpoll());
    assert!(unknown.cq_overflow());
}

#[cfg(test)]
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

#[cfg(test)]
#[test]
fn registration_helpers_reject_short_tags_and_invalid_update_counts() {
    assert_eq!(
        validate_tags(2, &[1]).unwrap_err().kind(),
        ErrorKind::InvalidInput
    );
    assert_eq!(
        to_updated_count(-1, 1).unwrap_err().raw_os_error(),
        Some(EOVERFLOW)
    );
    assert_eq!(
        to_updated_count(2, 1).unwrap_err().raw_os_error(),
        Some(EPROTO)
    );
    assert_eq!(to_updated_count(1, 1).unwrap(), 1);
}

#[cfg(test)]
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
    assert_eq!(error.raw_os_error(), Some(EINVAL));
    assert_eq!(
        registration.state(),
        ResourceRegistrationState::UnregisterUnknown {
            errno: Some(EINVAL)
        }
    );
}

#[cfg(test)]
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
