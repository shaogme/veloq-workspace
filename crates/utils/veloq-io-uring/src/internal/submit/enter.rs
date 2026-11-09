use core::{mem, ptr};
use libc::{EOPNOTSUPP, c_void, sigset_t};

use veloq_std::io::{Error, ErrorKind, Result};

use crate::{
    internal::{ring::Parameters, types::SubmitArgs},
    sys,
};

/// The immutable subset of ring capabilities needed by the submission hot path.
///
/// `Parameters` is an initialization result and must not be reinterpreted on every
/// `io_uring_enter` call. This copy is created once when its submitter is built;
/// submission validation and diagnostics keep using the full parameter snapshot.
#[derive(Clone, Copy)]
pub(super) struct SubmitterCapabilities {
    sq_entries: u32,
    setup_flags: u32,
    features: u32,
}

impl SubmitterCapabilities {
    #[inline]
    pub(super) fn from_parameters(parameters: &Parameters) -> Self {
        Self {
            sq_entries: parameters.sq_entries(),
            setup_flags: parameters.as_raw().flags,
            features: parameters.as_raw().features,
        }
    }

    #[inline]
    pub(super) const fn is_setup_sqpoll(self) -> bool {
        self.setup_flags & sys::IORING_SETUP_SQPOLL != 0
    }

    #[inline]
    pub(super) const fn is_setup_iopoll(self) -> bool {
        self.setup_flags & sys::IORING_SETUP_IOPOLL != 0
    }

    #[inline]
    pub(super) const fn is_feature_nodrop(self) -> bool {
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
    SignalMask(&'a sigset_t),
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
    pub fn sigmask<'next>(self, sigmask: &'next sigset_t) -> EnterArgs<'next> {
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

    pub(super) fn validate(&self, capabilities: SubmitterCapabilities) -> Result<()> {
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
            return Err(Error::from_raw_os_error(EOPNOTSUPP));
        }
        if self.flags.contains(EnterFlags::EXT_ARG_REG) {
            return Err(Error::from_raw_os_error(EOPNOTSUPP));
        }
        if self.flags.contains(EnterFlags::ABS_TIMER) && !self.flags.contains(EnterFlags::EXT_ARG) {
            return Err(Error::from(ErrorKind::InvalidInput));
        }

        match self.argument {
            EnterArgument::None | EnterArgument::SignalMask(_) => {
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
    pub(super) fn raw_argument(self) -> (*const c_void, usize) {
        match self.argument {
            EnterArgument::None => (ptr::null(), 0),
            EnterArgument::SignalMask(sigmask) => {
                (ptr::from_ref(sigmask).cast(), mem::size_of::<sigset_t>())
            }
            EnterArgument::Extended(args) => (
                ptr::from_ref(&args).cast(),
                mem::size_of::<sys::IoUringGeteventsArg>(),
            ),
        }
    }
}
