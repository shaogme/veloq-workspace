//! Ring construction and ownership.

use core::{
    cmp,
    mem::{ManuallyDrop, align_of, size_of},
};

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::{AsRawFd, FromRawFd, OwnedFd},
};

use crate::{
    cqueue::{CompletionQueue, Entry as CompletionEntry, Inner as CompletionInner},
    mmap::Mmap,
    register::KernelCapabilities,
    squeue::{Entry as SubmissionEntry, Inner as SubmissionInner, SubmissionQueue},
    submit::{SubmitResult, Submitter},
    sys,
};

/// Setup flags understood by the checked-in io_uring ABI.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SetupFlags(u32);

impl SetupFlags {
    pub const EMPTY: Self = Self(0);
    pub const IOPOLL: Self = Self(sys::IORING_SETUP_IOPOLL);
    pub const SQPOLL: Self = Self(sys::IORING_SETUP_SQPOLL);
    pub const SQ_AFF: Self = Self(sys::IORING_SETUP_SQ_AFF);
    pub const CQSIZE: Self = Self(sys::IORING_SETUP_CQSIZE);
    pub const CLAMP: Self = Self(sys::IORING_SETUP_CLAMP);
    pub const ATTACH_WQ: Self = Self(sys::IORING_SETUP_ATTACH_WQ);
    pub const R_DISABLED: Self = Self(sys::IORING_SETUP_R_DISABLED);
    pub const SUBMIT_ALL: Self = Self(sys::IORING_SETUP_SUBMIT_ALL);
    pub const COOP_TASKRUN: Self = Self(sys::IORING_SETUP_COOP_TASKRUN);
    pub const TASKRUN_FLAG: Self = Self(sys::IORING_SETUP_TASKRUN_FLAG);
    pub const SQE128: Self = Self(sys::IORING_SETUP_SQE128);
    pub const CQE32: Self = Self(sys::IORING_SETUP_CQE32);
    pub const SINGLE_ISSUER: Self = Self(sys::IORING_SETUP_SINGLE_ISSUER);
    pub const DEFER_TASKRUN: Self = Self(sys::IORING_SETUP_DEFER_TASKRUN);
    pub const NO_MMAP: Self = Self(sys::IORING_SETUP_NO_MMAP);
    pub const REGISTERED_FD_ONLY: Self = Self(sys::IORING_SETUP_REGISTERED_FD_ONLY);
    pub const NO_SQARRAY: Self = Self(sys::IORING_SETUP_NO_SQARRAY);
    pub const HYBRID_IOPOLL: Self = Self(sys::IORING_SETUP_HYBRID_IOPOLL);

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
    pub const fn intersects(self, other: Self) -> bool {
        self.0 & other.0 != 0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }

    #[inline]
    pub const fn difference(self, other: Self) -> Self {
        Self(self.0 & !other.0)
    }

    #[inline]
    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }
}

const KNOWN_SETUP_FLAGS: SetupFlags = SetupFlags(
    sys::IORING_SETUP_IOPOLL
        | sys::IORING_SETUP_SQPOLL
        | sys::IORING_SETUP_SQ_AFF
        | sys::IORING_SETUP_CQSIZE
        | sys::IORING_SETUP_CLAMP
        | sys::IORING_SETUP_ATTACH_WQ
        | sys::IORING_SETUP_R_DISABLED
        | sys::IORING_SETUP_SUBMIT_ALL
        | sys::IORING_SETUP_COOP_TASKRUN
        | sys::IORING_SETUP_TASKRUN_FLAG
        | sys::IORING_SETUP_SQE128
        | sys::IORING_SETUP_CQE32
        | sys::IORING_SETUP_SINGLE_ISSUER
        | sys::IORING_SETUP_DEFER_TASKRUN
        | sys::IORING_SETUP_NO_MMAP
        | sys::IORING_SETUP_REGISTERED_FD_ONLY
        | sys::IORING_SETUP_NO_SQARRAY
        | sys::IORING_SETUP_HYBRID_IOPOLL,
);

const DEFAULT_SETUP_FLAGS: SetupFlags = SetupFlags(
    sys::IORING_SETUP_COOP_TASKRUN
        | sys::IORING_SETUP_SINGLE_ISSUER
        | sys::IORING_SETUP_DEFER_TASKRUN,
);

/// Policy for setup flags that cannot be enabled after `io_uring_setup`.
///
/// Required flags make ring construction fail when the kernel rejects them. Disabled flags are
/// never passed to the kernel; overlapping sets are rejected instead of silently changing
/// semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SetupPolicy {
    required: SetupFlags,
    disabled: SetupFlags,
}

impl SetupPolicy {
    /// Create a policy from disjoint required and disabled sets.
    #[inline]
    pub const fn new(required: SetupFlags, disabled: SetupFlags) -> Self {
        Self { required, disabled }
    }

    /// Create a policy that explicitly disables the default task-run flags.
    #[inline]
    pub const fn basic() -> Self {
        Self::new(SetupFlags::EMPTY, DEFAULT_SETUP_FLAGS)
    }

    #[inline]
    pub const fn required(self) -> SetupFlags {
        self.required
    }

    #[inline]
    pub const fn disabled(self) -> SetupFlags {
        self.disabled
    }

    #[inline]
    pub const fn with_required(self, flags: SetupFlags) -> Self {
        Self {
            required: self.required.union(flags),
            disabled: self.disabled.difference(flags),
        }
    }

    #[inline]
    pub const fn with_disabled(self, flags: SetupFlags) -> Self {
        Self {
            required: self.required.difference(flags),
            disabled: self.disabled.union(flags),
        }
    }

    /// Validate that the policy cannot accidentally hide a requested setup flag.
    pub fn validate(self) -> Result<()> {
        if self.required.intersects(self.disabled)
            || self.required.difference(KNOWN_SETUP_FLAGS).bits() != 0
            || self.disabled.difference(KNOWN_SETUP_FLAGS).bits() != 0
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        Ok(())
    }
}

impl Default for SetupPolicy {
    #[inline]
    fn default() -> Self {
        Self::new(DEFAULT_SETUP_FLAGS, SetupFlags::EMPTY)
    }
}

/// Configuration for one `io_uring_setup` profile.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingConfig {
    pub entries: u32,
    pub setup_policy: SetupPolicy,
    pub sq_thread_idle: u32,
    pub cq_entries: u32,
    pub mmap_populate: bool,
}

impl RingConfig {
    #[inline]
    pub const fn new(entries: u32) -> Self {
        Self {
            entries,
            setup_policy: SetupPolicy::new(DEFAULT_SETUP_FLAGS, SetupFlags::EMPTY),
            sq_thread_idle: 0,
            cq_entries: 0,
            mmap_populate: true,
        }
    }

    #[inline]
    pub const fn with_setup_policy(mut self, setup_policy: SetupPolicy) -> Self {
        self.setup_policy = setup_policy;
        self
    }

    #[inline]
    pub const fn with_sq_thread_idle(mut self, idle_ms: u32) -> Self {
        self.sq_thread_idle = idle_ms;
        self
    }

    #[inline]
    pub const fn with_cq_entries(mut self, entries: u32) -> Self {
        self.cq_entries = entries;
        self
    }

    #[inline]
    pub const fn with_mmap_populate(mut self, enabled: bool) -> Self {
        self.mmap_populate = enabled;
        self
    }
}

/// Parameters returned by `io_uring_setup`.
#[derive(Clone)]
#[repr(transparent)]
pub struct Parameters(pub(crate) sys::IoUringParams);

impl Parameters {
    /// Setup flags accepted by the kernel for this ring.
    #[inline]
    pub fn setup_flags(&self) -> u32 {
        self.0.flags
    }

    /// Feature bits reported by the kernel for this ring.
    #[inline]
    pub fn features(&self) -> u32 {
        self.0.features
    }

    /// Build an immutable capability snapshot from the setup response.
    #[inline]
    pub fn capabilities(&self) -> KernelCapabilities {
        KernelCapabilities::from_setup(self.setup_flags(), self.features())
    }

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
#[derive(Clone)]
pub struct Builder {
    config: RingConfig,
}

impl Default for Builder {
    fn default() -> Self {
        Self {
            config: RingConfig::new(0),
        }
    }
}

impl Builder {
    #[inline]
    pub fn setup_iopoll(&mut self) -> &mut Self {
        self.config.setup_policy = self.config.setup_policy.with_required(SetupFlags::IOPOLL);
        self
    }

    #[inline]
    pub fn setup_coop_taskrun(&mut self) -> &mut Self {
        self.config.setup_policy = self
            .config
            .setup_policy
            .with_required(SetupFlags::COOP_TASKRUN);
        self
    }

    #[inline]
    pub fn setup_single_issuer(&mut self) -> &mut Self {
        self.config.setup_policy = self
            .config
            .setup_policy
            .with_required(SetupFlags::SINGLE_ISSUER);
        self
    }

    #[inline]
    pub fn setup_defer_taskrun(&mut self) -> &mut Self {
        self.config.setup_policy = self
            .config
            .setup_policy
            .with_required(SetupFlags::DEFER_TASKRUN);
        self
    }

    #[inline]
    pub fn setup_sqpoll(&mut self, idle_ms: u32) -> &mut Self {
        self.config.setup_policy = self.config.setup_policy.with_required(SetupFlags::SQPOLL);
        self.config.sq_thread_idle = idle_ms;
        self
    }

    #[inline]
    pub fn setup_cqsize(&mut self, entries: u32) -> &mut Self {
        self.config.setup_policy = self.config.setup_policy.with_required(SetupFlags::CQSIZE);
        self.config.cq_entries = entries;
        self
    }

    /// Replace the setup policy used by this builder.
    #[inline]
    pub fn setup_policy(&mut self, setup_policy: SetupPolicy) -> &mut Self {
        self.config.setup_policy = setup_policy;
        self
    }

    /// Create a builder from one explicit setup profile.
    #[inline]
    pub const fn from_config(config: RingConfig) -> Self {
        Self { config }
    }

    /// Build one ring from an explicit setup profile.
    #[inline]
    pub fn build_config(self) -> Result<IoUring> {
        IoUring::from_config(self.config)
    }

    /// Configure whether ring mappings request eager page population.
    ///
    /// `true` is the default and preserves the existing startup behavior. The
    /// setting is passed directly to `mmap`; a failed mapping is returned as an
    /// error and is never retried with a different flag implicitly.
    #[inline]
    pub fn mmap_populate(&mut self, enabled: bool) -> &mut Self {
        self.config.mmap_populate = enabled;
        self
    }

    /// Create a ring with `entries` submission queue slots.
    pub fn build(self, entries: u32) -> Result<IoUring> {
        IoUring::from_config(RingConfig {
            entries,
            ..self.config
        })
    }
}

/// The validated layout and addresses of the mappings returned by the kernel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RingLayout {
    sq_ring_address: usize,
    cq_ring_address: usize,
    sqe_address: usize,
    sq_ring_length: usize,
    cq_ring_length: usize,
    sqe_length: usize,
    sq_entries: u32,
    cq_entries: u32,
    sqe_entry_size: usize,
    cqe_entry_size: usize,
    single_mmap: bool,
}

impl RingLayout {
    /// Address at which the SQ ring mapping starts while the owning ring lives.
    #[inline]
    pub fn sq_ring_address(&self) -> usize {
        self.sq_ring_address
    }

    /// Address at which the CQ ring mapping starts while the owning ring lives.
    #[inline]
    pub fn cq_ring_address(&self) -> usize {
        self.cq_ring_address
    }

    /// Address at which the SQE mapping starts while the owning ring lives.
    #[inline]
    pub fn sqe_address(&self) -> usize {
        self.sqe_address
    }

    /// Length of the SQ ring object described by the returned offsets.
    #[inline]
    pub fn sq_ring_length(&self) -> usize {
        self.sq_ring_length
    }

    /// Length of the CQ ring object described by the returned offsets.
    #[inline]
    pub fn cq_ring_length(&self) -> usize {
        self.cq_ring_length
    }

    /// Length of the SQE mapping.
    #[inline]
    pub fn sqe_length(&self) -> usize {
        self.sqe_length
    }

    #[inline]
    pub fn sq_entries(&self) -> u32 {
        self.sq_entries
    }

    #[inline]
    pub fn cq_entries(&self) -> u32 {
        self.cq_entries
    }

    /// Size of one SQE in the active ABI layout.
    #[inline]
    pub fn sqe_entry_size(&self) -> usize {
        self.sqe_entry_size
    }

    /// Size of one CQE in the active ABI layout.
    #[inline]
    pub fn cqe_entry_size(&self) -> usize {
        self.cqe_entry_size
    }

    /// Whether SQ and CQ share one mapping.
    #[inline]
    pub fn is_single_mmap(&self) -> bool {
        self.single_mmap
    }
}

#[derive(Debug)]
struct LayoutSpec {
    sq_ring_length: usize,
    cq_ring_length: usize,
    sqe_length: usize,
    sq_entries: u32,
    cq_entries: u32,
    sqe_entry_size: usize,
    cqe_entry_size: usize,
    single_mmap: bool,
}

impl LayoutSpec {
    fn from_params(params: &sys::IoUringParams) -> Result<Self> {
        let unsupported =
            sys::IORING_SETUP_NO_MMAP | sys::IORING_SETUP_SQE128 | sys::IORING_SETUP_CQE32;
        if params.flags & unsupported != 0 {
            return Err(Error::from_raw_os_error(libc::EOPNOTSUPP));
        }
        if params.sq_entries == 0
            || params.cq_entries == 0
            || !params.sq_entries.is_power_of_two()
            || !params.cq_entries.is_power_of_two()
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let mut sq_ring_length = 0;
        for offset in [
            params.sq_off.head,
            params.sq_off.tail,
            params.sq_off.ring_mask,
            params.sq_off.ring_entries,
            params.sq_off.flags,
            params.sq_off.dropped,
        ] {
            validate_alignment(offset, align_of::<u32>())?;
            sq_ring_length = max_length(sq_ring_length, checked_field_end(offset)?);
        }
        if params.flags & sys::IORING_SETUP_NO_SQARRAY == 0 {
            validate_alignment(params.sq_off.array, align_of::<u32>())?;
            sq_ring_length = max_length(
                sq_ring_length,
                checked_region_end(params.sq_off.array, params.sq_entries, size_of::<u32>())?,
            );
        }

        let mut cq_ring_length = 0;
        for offset in [
            params.cq_off.head,
            params.cq_off.tail,
            params.cq_off.ring_mask,
            params.cq_off.ring_entries,
            params.cq_off.overflow,
            params.cq_off.flags,
        ] {
            validate_alignment(offset, align_of::<u32>())?;
            cq_ring_length = max_length(cq_ring_length, checked_field_end(offset)?);
        }
        validate_alignment(params.cq_off.cqes, align_of::<CompletionEntry>())?;
        cq_ring_length = max_length(
            cq_ring_length,
            checked_region_end(
                params.cq_off.cqes,
                params.cq_entries,
                size_of::<CompletionEntry>(),
            )?,
        );

        let sqe_entry_size = size_of::<SubmissionEntry>();
        let cqe_entry_size = size_of::<CompletionEntry>();
        let sqe_length = checked_count_bytes(params.sq_entries, sqe_entry_size)?;
        if sq_ring_length == 0 || cq_ring_length == 0 || sqe_length == 0 {
            return Err(Error::from_raw_os_error(libc::EOVERFLOW));
        }

        Ok(Self {
            sq_ring_length,
            cq_ring_length,
            sqe_length,
            sq_entries: params.sq_entries,
            cq_entries: params.cq_entries,
            sqe_entry_size,
            cqe_entry_size,
            single_mmap: params.features & sys::IORING_FEAT_SINGLE_MMAP != 0,
        })
    }

    fn materialize(self, sq_mmap: &Mmap, cq_mmap: &Mmap, sqe_mmap: &Mmap) -> Result<RingLayout> {
        Ok(RingLayout {
            sq_ring_address: mapping_address(sq_mmap)?,
            cq_ring_address: mapping_address(cq_mmap)?,
            sqe_address: mapping_address(sqe_mmap)?,
            sq_ring_length: self.sq_ring_length,
            cq_ring_length: self.cq_ring_length,
            sqe_length: self.sqe_length,
            sq_entries: self.sq_entries,
            cq_entries: self.cq_entries,
            sqe_entry_size: self.sqe_entry_size,
            cqe_entry_size: self.cqe_entry_size,
            single_mmap: self.single_mmap,
        })
    }
}

/// An io_uring instance and its mapped submission/completion rings.
pub struct IoUring {
    sq: SubmissionInner,
    cq: CompletionInner,
    _fd: OwnedFd,
    params: Parameters,
    layout: RingLayout,
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

    /// Build a ring from one explicit setup profile.
    pub fn from_config(config: RingConfig) -> Result<Self> {
        config.setup_policy.validate()?;
        if config.entries == 0 {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        if config
            .setup_policy
            .disabled()
            .intersects(SetupFlags::CQSIZE)
            && config.cq_entries != 0
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let mut params = sys::IoUringParams {
            flags: config.setup_policy.required().bits(),
            sq_thread_idle: config.sq_thread_idle,
            ..Default::default()
        };
        if config.cq_entries != 0 {
            params.flags |= sys::IORING_SETUP_CQSIZE;
            params.cq_entries = config.cq_entries;
        }

        let raw_fd = unsafe { sys::io_uring_setup(config.entries, &mut params) }?;
        let accepted = SetupFlags::from_bits_retain(params.flags);
        if !accepted.contains(config.setup_policy.required())
            || accepted.intersects(config.setup_policy.disabled())
        {
            unsafe {
                libc::close(raw_fd);
            }
            return Err(Error::from_raw_os_error(libc::EPROTO));
        }

        let fd = unsafe { OwnedFd::from_raw_fd(raw_fd) };
        unsafe { Self::from_fd_and_params(fd, params, config.mmap_populate) }
    }

    #[inline]
    pub fn params(&self) -> &Parameters {
        &self.params
    }

    /// Return the validated mapping layout captured during ring creation.
    #[inline]
    pub fn layout(&self) -> &RingLayout {
        &self.layout
    }

    #[inline]
    pub fn submission(&mut self) -> SubmissionQueue<'_> {
        self.sq.borrow()
    }

    #[inline]
    pub fn completion(&mut self) -> CompletionQueue<'_> {
        self.cq.borrow()
    }

    /// Return a handle for submitting entries and registering ring resources.
    #[inline]
    pub fn submitter(&mut self) -> Submitter<'_> {
        let sq = self.sq.submitter_parts();
        Submitter::new(&self._fd, &self.params, sq)
    }

    /// Submit all currently published SQEs.
    #[inline]
    pub fn submit(&mut self) -> SubmitResult {
        self.submitter().submit()
    }

    /// Split the ring into independently usable submission and completion views.
    #[inline]
    pub fn split(&mut self) -> (Submitter<'_>, SubmissionQueue<'_>, CompletionQueue<'_>) {
        let sq = self.sq.submitter_parts();
        let submitter = Submitter::new(&self._fd, &self.params, sq);
        let submission = self.sq.borrow();
        let completion = self.cq.borrow();
        (submitter, submission, completion)
    }

    unsafe fn from_fd_and_params(
        fd: OwnedFd,
        params: sys::IoUringParams,
        mmap_populate: bool,
    ) -> Result<Self> {
        let spec = LayoutSpec::from_params(&params)?;
        let sqe_mmap = Mmap::new(
            fd.as_raw_fd(),
            sys::IORING_OFF_SQES,
            spec.sqe_length,
            mmap_populate,
        )?;
        let (sq_mmap, cq_mmap) = if spec.single_mmap {
            (
                Mmap::new(
                    fd.as_raw_fd(),
                    sys::IORING_OFF_SQ_RING,
                    cmp::max(spec.sq_ring_length, spec.cq_ring_length),
                    mmap_populate,
                )?,
                None,
            )
        } else {
            (
                Mmap::new(
                    fd.as_raw_fd(),
                    sys::IORING_OFF_SQ_RING,
                    spec.sq_ring_length,
                    mmap_populate,
                )?,
                Some(Mmap::new(
                    fd.as_raw_fd(),
                    sys::IORING_OFF_CQ_RING,
                    spec.cq_ring_length,
                    mmap_populate,
                )?),
            )
        };

        let cq_map = cq_mmap.as_ref().unwrap_or(&sq_mmap);
        let layout = spec.materialize(&sq_mmap, cq_map, &sqe_mmap)?;
        let sq = unsafe { SubmissionInner::new(&sq_mmap, &sqe_mmap, &params) }?;
        let cq = unsafe { CompletionInner::new(cq_map, &params) }?;
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
            layout,
            memory: ManuallyDrop::new(memory),
        })
    }
}

// Queue views borrow the mapped memory through `&mut self`, while the kernel
// may access the same mappings asynchronously through the owned ring fd. A
// ring must have one owner because the SQ/CQ protocol is deliberately not a
// multi-producer or multi-consumer abstraction.
unsafe impl Send for IoUring {}

impl Drop for IoUring {
    fn drop(&mut self) {
        // The fd must remain open until every kernel-shared mapping is released.
        debug_assert!(self._fd.as_raw_fd() >= 0);
        unsafe {
            ManuallyDrop::drop(&mut self.memory);
        }
    }
}

fn checked_count_bytes(entries: u32, element_size: usize) -> Result<usize> {
    (entries as usize)
        .checked_mul(element_size)
        .filter(|length| *length != 0)
        .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))
}

fn checked_region_end(offset: u32, entries: u32, element_size: usize) -> Result<usize> {
    let bytes = checked_count_bytes(entries, element_size)?;
    (offset as usize)
        .checked_add(bytes)
        .filter(|length| *length != 0)
        .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))
}

fn checked_field_end(offset: u32) -> Result<usize> {
    (offset as usize)
        .checked_add(size_of::<u32>())
        .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))
}

fn max_length(current: usize, candidate: usize) -> usize {
    current.max(candidate)
}

fn validate_alignment(offset: u32, alignment: usize) -> Result<()> {
    if !(offset as usize).is_multiple_of(alignment) {
        return Err(Error::from_raw_os_error(libc::EINVAL));
    }
    Ok(())
}

fn mapping_address(mapping: &Mmap) -> Result<usize> {
    let address = mapping.as_mut_ptr() as usize;
    if address == 0 {
        return Err(Error::from_raw_os_error(libc::EFAULT));
    }
    Ok(address)
}

#[cfg(test)]
mod tests {
    use core::mem::size_of;

    use super::*;

    fn valid_params() -> sys::IoUringParams {
        let mut params = sys::IoUringParams {
            sq_entries: 8,
            cq_entries: 16,
            ..Default::default()
        };
        params.sq_off.head = 0;
        params.sq_off.tail = 4;
        params.sq_off.ring_mask = 8;
        params.sq_off.ring_entries = 12;
        params.sq_off.flags = 16;
        params.sq_off.dropped = 20;
        params.sq_off.array = 64;
        params.cq_off.head = 0;
        params.cq_off.tail = 4;
        params.cq_off.ring_mask = 8;
        params.cq_off.ring_entries = 12;
        params.cq_off.overflow = 16;
        params.cq_off.cqes = 64;
        params.cq_off.flags = 24;
        params
    }

    #[test]
    fn layout_covers_every_shared_object_and_uses_returned_counts() {
        let params = valid_params();
        let spec = LayoutSpec::from_params(&params).expect("fixture layout is valid");

        assert_eq!(spec.sq_ring_length, 96);
        assert_eq!(spec.cq_ring_length, 320);
        assert_eq!(spec.sqe_length, 8 * size_of::<SubmissionEntry>());
        assert_eq!(spec.sq_entries, 8);
        assert_eq!(spec.cq_entries, 16);
        assert!(!spec.single_mmap);
    }

    #[test]
    fn single_mmap_is_recorded_without_losing_cq_offsets() {
        let mut params = valid_params();
        params.features = sys::IORING_FEAT_SINGLE_MMAP;
        params.cq_off.cqes = 256;
        let spec = LayoutSpec::from_params(&params).expect("fixture layout is valid");

        assert!(spec.single_mmap);
        assert_eq!(spec.sq_ring_length, 96);
        assert_eq!(spec.cq_ring_length, 512);
        assert_eq!(cmp::max(spec.sq_ring_length, spec.cq_ring_length), 512);
    }

    #[test]
    fn unsupported_layout_variants_are_rejected_before_mapping() {
        for flag in [
            sys::IORING_SETUP_NO_MMAP,
            sys::IORING_SETUP_SQE128,
            sys::IORING_SETUP_CQE32,
        ] {
            let mut params = valid_params();
            params.flags = flag;
            assert_eq!(
                LayoutSpec::from_params(&params).unwrap_err().raw_os_error(),
                Some(libc::EOPNOTSUPP)
            );
        }
    }

    #[test]
    fn invalid_offsets_and_entry_counts_are_rejected() {
        let mut unaligned = valid_params();
        unaligned.sq_off.head = 2;
        assert_eq!(
            LayoutSpec::from_params(&unaligned)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EINVAL)
        );

        let mut non_power_of_two = valid_params();
        non_power_of_two.sq_entries = 3;
        assert_eq!(
            LayoutSpec::from_params(&non_power_of_two)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EINVAL)
        );
    }

    #[test]
    fn checked_layout_arithmetic_reports_overflow() {
        assert_eq!(
            checked_region_end(1, 1, usize::MAX)
                .unwrap_err()
                .raw_os_error(),
            Some(libc::EOVERFLOW)
        );
    }

    #[test]
    fn setup_policy_defaults_to_required_task_run_flags() {
        let policy = SetupPolicy::default();
        assert_eq!(
            policy.required().bits(),
            SetupFlags::COOP_TASKRUN.bits()
                | SetupFlags::SINGLE_ISSUER.bits()
                | SetupFlags::DEFER_TASKRUN.bits()
        );
        assert!(policy.validate().is_ok());

        let polling = policy.with_required(SetupFlags::SQPOLL);
        assert!(polling.required().contains(SetupFlags::SQPOLL));
        assert!(polling.validate().is_ok());

        let basic = SetupPolicy::basic();
        assert!(basic.required().is_empty());
        assert!(basic.disabled().contains(SetupFlags::DEFER_TASKRUN));
        assert!(basic.validate().is_ok());
    }

    #[test]
    fn setup_policy_rejects_overlapping_or_unknown_flags() {
        let overlapping = SetupPolicy::new(SetupFlags::SQPOLL, SetupFlags::SQPOLL);
        assert_eq!(
            overlapping.validate().unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );

        let unknown = SetupPolicy::new(SetupFlags::EMPTY, SetupFlags::from_bits_retain(1 << 31));
        assert_eq!(
            unknown.validate().unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );
    }
}
