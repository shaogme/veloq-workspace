use veloq_std::io::Error;

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

impl SubmitReceipt {
    /// Return the number of SQEs the kernel confirmed it consumed.
    #[inline]
    pub const fn submitted_count(&self) -> Option<u32> {
        match self {
            Self::Submitted { submitted, .. } => Some(*submitted),
            Self::NoEntries { .. } | Self::PublishedToSqpoll { .. } => None,
        }
    }

    /// Whether the submission woke an SQPOLL thread.
    #[inline]
    pub const fn woke_sqpoll(&self) -> bool {
        match self {
            Self::Submitted { woke_sqpoll, .. } => *woke_sqpoll,
            Self::NoEntries { .. } | Self::PublishedToSqpoll { .. } => false,
        }
    }

    /// Whether the kernel reported a completion queue overflow.
    #[inline]
    pub const fn cq_overflow(&self) -> bool {
        match self {
            Self::NoEntries { cq_overflow }
            | Self::PublishedToSqpoll { cq_overflow, .. }
            | Self::Submitted { cq_overflow, .. } => *cq_overflow,
        }
    }
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
