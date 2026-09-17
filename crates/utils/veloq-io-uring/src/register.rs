//! Public opcode probe and private register ABI helpers.

use core::fmt;

use veloq_std::{io::Result, os::unix::fd::RawFd};

use crate::sys;

#[inline]
pub(crate) fn execute(
    fd: RawFd,
    opcode: u32,
    arg: *const libc::c_void,
    nr_args: u32,
) -> Result<i32> {
    unsafe { sys::io_uring_register(fd, opcode, arg, nr_args) }
}

/// Information about the opcodes supported by the running kernel.
pub struct Probe(ProbeAndOps);

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
}
