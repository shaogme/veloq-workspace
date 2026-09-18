//! Public opcode probe and private register ABI helpers.

use core::fmt;

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::RawFd,
};

use crate::sys;

#[cfg(target_os = "android")]
const TARGET_OS: &str = "android";

#[cfg(target_os = "linux")]
const TARGET_OS: &str = "linux";

#[cfg(target_arch = "aarch64")]
const TARGET_ARCH: &str = "aarch64";

#[cfg(target_arch = "loongarch64")]
const TARGET_ARCH: &str = "loongarch64";

#[cfg(all(target_arch = "powerpc64", target_endian = "big"))]
const TARGET_ARCH: &str = "powerpc64";

#[cfg(all(target_arch = "powerpc64", target_endian = "little"))]
const TARGET_ARCH: &str = "powerpc64le";

#[cfg(target_arch = "riscv64")]
const TARGET_ARCH: &str = "riscv64";

#[cfg(target_arch = "x86_64")]
const TARGET_ARCH: &str = "x86_64";

/// Architectures for which the checked-in Linux `io_uring` ABI is validated.
pub const SUPPORTED_ABI_ARCHITECTURES: &[&str] = &[
    "x86_64",
    "aarch64",
    "riscv64",
    "loongarch64",
    "powerpc64",
    "powerpc64le",
];

/// The kind of kernel resource owned by a registration handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceKind {
    /// Entries in the registered buffer table.
    Buffers,
    /// Entries in the registered file table.
    Files,
}

/// The layout used when a resource table was created.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceLayout {
    /// A table initialized from one contiguous slice.
    Contiguous,
    /// A table created with empty slots and filled by updates.
    Sparse,
}

/// The lifecycle state of a resource registration handle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceRegistrationState {
    /// The kernel may use the registered resources.
    Registered,
    /// Unregistration failed and the kernel-side ownership is no longer certain.
    UnregisterUnknown { errno: Option<i32> },
    /// The kernel accepted unregistration.
    Unregistered,
}

/// Metadata and lifecycle token for one registered resource table.
///
/// The handle is intentionally separate from [`Submitter`](crate::Submitter). It carries the
/// table capacity into every update call, so an update cannot silently address a slot outside
/// the table that created it. The handle must be retained until the table is unregistered or the
/// owning ring is destroyed.
#[derive(Debug)]
pub struct ResourceRegistration {
    owner_fd: RawFd,
    kind: ResourceKind,
    layout: ResourceLayout,
    capacity: u32,
    tagged: bool,
    state: ResourceRegistrationState,
}

impl ResourceRegistration {
    pub(crate) const fn new(
        owner_fd: RawFd,
        kind: ResourceKind,
        layout: ResourceLayout,
        capacity: u32,
        tagged: bool,
    ) -> Self {
        Self {
            owner_fd,
            kind,
            layout,
            capacity,
            tagged,
            state: ResourceRegistrationState::Registered,
        }
    }

    /// Return the kind of resources in this table.
    #[inline]
    pub const fn kind(&self) -> ResourceKind {
        self.kind
    }

    /// Return the layout used to create this table.
    #[inline]
    pub const fn layout(&self) -> ResourceLayout {
        self.layout
    }

    /// Return the number of slots allocated by the kernel.
    #[inline]
    pub const fn capacity(&self) -> u32 {
        self.capacity
    }

    /// Return whether updates for this table carry resource tags.
    #[inline]
    pub const fn is_tagged(&self) -> bool {
        self.tagged
    }

    /// Return the userspace view of the resource lifecycle.
    #[inline]
    pub const fn state(&self) -> ResourceRegistrationState {
        self.state
    }

    /// Capture this handle as a capability observation.
    #[inline]
    pub(crate) const fn capability(&self) -> ResourceRegistrationCapability {
        match self.state {
            ResourceRegistrationState::Registered => ResourceRegistrationCapability::Registered {
                kind: self.kind,
                slots: self.capacity,
                layout: self.layout,
                tagged: self.tagged,
            },
            ResourceRegistrationState::UnregisterUnknown { errno } => {
                ResourceRegistrationCapability::UnregisterUnknown {
                    kind: self.kind,
                    slots: self.capacity,
                    layout: self.layout,
                    tagged: self.tagged,
                    errno,
                }
            }
            ResourceRegistrationState::Unregistered => {
                ResourceRegistrationCapability::Unregistered {
                    kind: self.kind,
                    slots: self.capacity,
                    layout: self.layout,
                    tagged: self.tagged,
                }
            }
        }
    }

    /// Mark the handle as unregistered after a successful unregister syscall.
    #[inline]
    pub(crate) fn mark_unregistered(&mut self) {
        self.state = ResourceRegistrationState::Unregistered;
    }

    /// Preserve the errno and stop callers from reusing an uncertain table.
    #[inline]
    pub(crate) fn mark_unregister_unknown(&mut self, errno: Option<i32>) {
        self.state = ResourceRegistrationState::UnregisterUnknown { errno };
    }

    pub(crate) fn validate_update(
        &self,
        owner_fd: RawFd,
        kind: ResourceKind,
        offset: u32,
        count: u32,
        tagged: bool,
    ) -> Result<()> {
        if self.owner_fd != owner_fd
            || self.kind != kind
            || self.state != ResourceRegistrationState::Registered
            || self.tagged != tagged
            || count == 0
            || offset
                .checked_add(count)
                .is_none_or(|end| end > self.capacity)
        {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        Ok(())
    }

    #[inline]
    pub(crate) const fn owner_matches(&self, owner_fd: RawFd) -> bool {
        self.owner_fd == owner_fd
    }
}

/// Registration facts included in [`KernelCapabilities`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceRegistrationCapability {
    /// No registration attempt has been made for this resource kind.
    NotAttempted,
    /// The kernel accepted a table with the requested slot count.
    Registered {
        kind: ResourceKind,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
    },
    /// A registration syscall failed; `slots` is the requested table size.
    Failed {
        kind: ResourceKind,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
        errno: Option<i32>,
    },
    /// Unregistration failed, so the table must not be reused.
    UnregisterUnknown {
        kind: ResourceKind,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
        errno: Option<i32>,
    },
    /// The kernel accepted unregistration.
    Unregistered {
        kind: ResourceKind,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
    },
}

impl ResourceRegistrationCapability {
    #[inline]
    pub const fn slots(self) -> Option<u32> {
        match self {
            Self::Registered { slots, .. }
            | Self::Failed { slots, .. }
            | Self::UnregisterUnknown { slots, .. }
            | Self::Unregistered { slots, .. } => Some(slots),
            Self::NotAttempted => None,
        }
    }

    #[inline]
    pub const fn is_registered(self) -> bool {
        matches!(self, Self::Registered { .. })
    }
}

#[inline]
pub(crate) fn execute(
    fd: RawFd,
    opcode: u32,
    arg: *const libc::c_void,
    nr_args: u32,
) -> Result<i64> {
    unsafe { sys::io_uring_register(fd, opcode, arg, nr_args) }
}

/// State of the optional opcode probe operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProbeStatus {
    /// No probe has been attempted for this ring.
    NotAttempted,
    /// The kernel accepted the probe registration.
    Succeeded,
    /// The registration syscall failed with the recorded errno.
    Failed { errno: i32 },
}

/// Information about the opcodes supported by the running kernel.
pub struct Probe(ProbeAndOps);

/// Fixed-size diagnostic snapshot of an opcode probe.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct OpcodeProbeSnapshot {
    /// Highest opcode index reported by the kernel.
    pub last_op: u8,
    /// Bitset of supported opcodes, indexed by opcode number.
    pub supported: [u64; 4],
}

/// Immutable capability facts observed for one ring.
///
/// This type deliberately contains observations only. It does not decide whether a backend
/// feature should be enabled; callers can make that policy decision without rereading shared
/// kernel parameters or interpreting a raw probe buffer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct KernelCapabilities {
    /// Operating-system target used to build the ABI.
    pub target_os: &'static str,
    /// CPU architecture used to build the ABI.
    pub target_arch: &'static str,
    /// Setup flags accepted by the kernel for this ring.
    pub setup_flags: u32,
    /// Feature bits returned by `io_uring_setup`.
    pub features: u32,
    /// Result of registering the opcode probe.
    pub probe_status: ProbeStatus,
    /// Fixed-size opcode support snapshot, if probing succeeded.
    pub opcode_probe: Option<OpcodeProbeSnapshot>,
    /// Observed registered-buffer resource state.
    pub buffer_registration: ResourceRegistrationCapability,
    /// Observed registered-file resource state.
    pub file_registration: ResourceRegistrationCapability,
}

impl KernelCapabilities {
    pub(crate) const fn from_setup(setup_flags: u32, features: u32) -> Self {
        Self {
            target_os: TARGET_OS,
            target_arch: TARGET_ARCH,
            setup_flags,
            features,
            probe_status: ProbeStatus::NotAttempted,
            opcode_probe: None,
            buffer_registration: ResourceRegistrationCapability::NotAttempted,
            file_registration: ResourceRegistrationCapability::NotAttempted,
        }
    }

    /// Return a copy containing a successful probe result.
    #[must_use]
    pub fn with_probe(self, probe: &Probe) -> Self {
        Self {
            probe_status: ProbeStatus::Succeeded,
            opcode_probe: Some(probe.snapshot()),
            ..self
        }
    }

    /// Return a copy containing a failed probe result.
    #[must_use]
    pub const fn with_probe_error(self, errno: i32) -> Self {
        Self {
            probe_status: ProbeStatus::Failed { errno },
            opcode_probe: None,
            ..self
        }
    }

    /// Return whether an accepted setup flag is present.
    #[inline]
    pub const fn has_setup_flag(self, flag: u32) -> bool {
        self.setup_flags & flag == flag
    }

    /// Return whether an accepted setup flag is present.
    #[inline]
    pub const fn supports_setup_flag(self, flag: u32) -> bool {
        self.has_setup_flag(flag)
    }

    /// Return whether a kernel feature bit is present.
    #[inline]
    pub const fn has_feature(self, feature: u32) -> bool {
        self.features & feature == feature
    }

    /// Return whether a kernel feature bit is present.
    #[inline]
    pub const fn supports_feature(self, feature: u32) -> bool {
        self.has_feature(feature)
    }

    /// Return whether the successful opcode probe reported support.
    #[inline]
    pub const fn supports_opcode(self, opcode: u8) -> bool {
        match self.opcode_probe {
            Some(probe) => probe.is_supported(opcode),
            None => false,
        }
    }

    /// Return a copy containing the result of a buffer-table registration.
    #[must_use]
    pub fn with_buffer_registration(self, registration: &ResourceRegistration) -> Self {
        Self {
            buffer_registration: registration.capability(),
            ..self
        }
    }

    /// Return a copy containing a failed buffer-table registration attempt.
    #[must_use]
    pub const fn with_buffer_registration_error(
        self,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
        errno: Option<i32>,
    ) -> Self {
        Self {
            buffer_registration: ResourceRegistrationCapability::Failed {
                kind: ResourceKind::Buffers,
                slots,
                layout,
                tagged,
                errno,
            },
            ..self
        }
    }

    /// Return a copy containing the result of a file-table registration.
    #[must_use]
    pub fn with_file_registration(self, registration: &ResourceRegistration) -> Self {
        Self {
            file_registration: registration.capability(),
            ..self
        }
    }

    /// Return a copy containing a failed file-table registration attempt.
    #[must_use]
    pub const fn with_file_registration_error(
        self,
        slots: u32,
        layout: ResourceLayout,
        tagged: bool,
        errno: Option<i32>,
    ) -> Self {
        Self {
            file_registration: ResourceRegistrationCapability::Failed {
                kind: ResourceKind::Files,
                slots,
                layout,
                tagged,
                errno,
            },
            ..self
        }
    }
}

impl OpcodeProbeSnapshot {
    /// Return whether the kernel probe reported `opcode` as supported.
    #[inline]
    pub const fn is_supported(self, opcode: u8) -> bool {
        opcode <= self.last_op
            && self.supported[opcode as usize / 64] & (1u64 << (opcode as usize % 64)) != 0
    }
}

#[repr(C)]
struct ProbeAndOps(sys::IoUringProbe, [sys::IoUringProbeOp; Probe::COUNT]);

impl Probe {
    pub(crate) const COUNT: usize = 256;

    /// Create an empty opcode probe.
    #[must_use]
    pub fn new() -> Self {
        Self(ProbeAndOps(
            sys::IoUringProbe::default(),
            [sys::IoUringProbeOp::default(); Self::COUNT],
        ))
    }

    #[inline]
    pub(crate) fn as_mut_ptr(&mut self) -> *mut sys::IoUringProbe {
        core::ptr::from_mut(&mut self.0.0)
    }

    /// Return whether `opcode` is supported by the kernel.
    #[inline]
    pub fn is_supported(&self, opcode: u8) -> bool {
        let probe = &self.0.0;
        if opcode > probe.last_op {
            return false;
        }

        self.0.1[opcode as usize].flags & sys::IO_URING_OP_SUPPORTED != 0
    }

    /// Capture the probe result in a fixed-size, copyable diagnostic value.
    #[must_use]
    pub fn snapshot(&self) -> OpcodeProbeSnapshot {
        let mut supported = [0; 4];
        for (opcode, operation) in self.0.1.iter().enumerate() {
            if operation.flags & sys::IO_URING_OP_SUPPORTED != 0 {
                supported[opcode / 64] |= 1u64 << (opcode % 64);
            }
        }

        OpcodeProbeSnapshot {
            last_op: self.0.0.last_op,
            supported,
        }
    }
}

impl Default for Probe {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Probe {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = (self.0.0.last_op as usize + 1).min(Self::COUNT);
        let supported = self.0.1[..count]
            .iter()
            .filter(|operation| operation.flags & sys::IO_URING_OP_SUPPORTED != 0)
            .map(|operation| operation.op);

        formatter.debug_set().entries(supported).finish()
    }
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn probe_storage_matches_kernel_probe_capacity() {
        assert_eq!(
            size_of::<ProbeAndOps>(),
            size_of::<sys::IoUringProbe>() + 256 * 8
        );
        assert_eq!(align_of::<ProbeAndOps>(), align_of::<sys::IoUringProbe>());
    }

    #[test]
    fn empty_probe_reports_no_supported_opcode() {
        let probe = Probe::new();
        assert!(!probe.is_supported(0));
        assert!(!probe.is_supported(u8::MAX));
    }

    #[test]
    fn empty_probe_snapshot_is_stable() {
        let snapshot = Probe::new().snapshot();
        assert_eq!(snapshot.last_op, 0);
        assert_eq!(snapshot.supported, [0; 4]);
        assert!(!snapshot.is_supported(0));
        assert!(!snapshot.is_supported(u8::MAX));
    }

    #[test]
    fn capabilities_distinguish_probe_failure_from_unsupported_opcode() {
        let capabilities = KernelCapabilities::from_setup(0x100, 0x200);
        assert_eq!(capabilities.probe_status, ProbeStatus::NotAttempted);
        assert!(!capabilities.supports_opcode(sys::IORING_OP_NOP));
        assert!(capabilities.supports_setup_flag(0x100));
        assert!(capabilities.supports_feature(0x200));

        let failed = capabilities.with_probe_error(libc::EINVAL);
        assert_eq!(
            failed.probe_status,
            ProbeStatus::Failed {
                errno: libc::EINVAL
            }
        );
        assert!(!failed.supports_opcode(sys::IORING_OP_NOP));
    }

    #[test]
    fn resource_registration_tracks_shape_capacity_and_lifecycle() {
        let mut registration =
            ResourceRegistration::new(17, ResourceKind::Buffers, ResourceLayout::Sparse, 8, false);
        assert_eq!(registration.kind(), ResourceKind::Buffers);
        assert_eq!(registration.layout(), ResourceLayout::Sparse);
        assert_eq!(registration.capacity(), 8);
        assert!(!registration.is_tagged());
        assert_eq!(registration.state(), ResourceRegistrationState::Registered);
        assert!(
            registration
                .validate_update(17, ResourceKind::Buffers, 7, 1, false)
                .is_ok()
        );
        assert!(
            registration
                .validate_update(17, ResourceKind::Buffers, 8, 1, false)
                .is_err()
        );
        assert!(
            registration
                .validate_update(17, ResourceKind::Files, 0, 1, false)
                .is_err()
        );

        registration.mark_unregister_unknown(Some(libc::EIO));
        assert_eq!(
            registration.state(),
            ResourceRegistrationState::UnregisterUnknown {
                errno: Some(libc::EIO)
            }
        );
        assert!(
            registration
                .validate_update(17, ResourceKind::Buffers, 0, 1, false)
                .is_err()
        );
        registration.mark_unregistered();
        assert_eq!(
            registration.capability(),
            ResourceRegistrationCapability::Unregistered {
                kind: ResourceKind::Buffers,
                slots: 8,
                layout: ResourceLayout::Sparse,
                tagged: false,
            }
        );
    }

    #[test]
    fn capability_records_registration_failures_without_fallback() {
        let capabilities = KernelCapabilities::from_setup(0, 0).with_buffer_registration_error(
            8,
            ResourceLayout::Sparse,
            false,
            Some(libc::EINVAL),
        );
        assert_eq!(
            capabilities.buffer_registration,
            ResourceRegistrationCapability::Failed {
                kind: ResourceKind::Buffers,
                slots: 8,
                layout: ResourceLayout::Sparse,
                tagged: false,
                errno: Some(libc::EINVAL),
            }
        );
        assert_eq!(capabilities.buffer_registration.slots(), Some(8));
        assert!(!capabilities.buffer_registration.is_registered());
    }
}
