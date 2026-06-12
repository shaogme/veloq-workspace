#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufIoRangeBound {
    Capacity,
    Length,
}

impl BufIoRangeBound {
    #[inline]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Capacity => "capacity",
            Self::Length => "length",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BufIoRangeErrorKind {
    OffsetOutOfBounds,
    LengthExceedsU32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufIoRangeError {
    kind: BufIoRangeErrorKind,
    buffer_offset: usize,
    buffer_length: usize,
    buffer_capacity: usize,
    buffer_bound: usize,
    buffer_bound_kind: BufIoRangeBound,
    submission_length: usize,
}

impl BufIoRangeError {
    #[inline]
    pub const fn new(
        kind: BufIoRangeErrorKind,
        buffer_offset: usize,
        buffer_length: usize,
        buffer_capacity: usize,
        buffer_bound: usize,
        buffer_bound_kind: BufIoRangeBound,
        submission_length: usize,
    ) -> Self {
        Self {
            kind,
            buffer_offset,
            buffer_length,
            buffer_capacity,
            buffer_bound,
            buffer_bound_kind,
            submission_length,
        }
    }

    #[inline]
    pub const fn kind(self) -> BufIoRangeErrorKind {
        self.kind
    }

    #[inline]
    pub const fn buffer_offset(self) -> usize {
        self.buffer_offset
    }

    #[inline]
    pub const fn buffer_length(self) -> usize {
        self.buffer_length
    }

    #[inline]
    pub const fn buffer_capacity(self) -> usize {
        self.buffer_capacity
    }

    #[inline]
    pub const fn buffer_bound(self) -> usize {
        self.buffer_bound
    }

    #[inline]
    pub const fn buffer_bound_kind(self) -> BufIoRangeBound {
        self.buffer_bound_kind
    }

    #[inline]
    pub const fn submission_length(self) -> usize {
        self.submission_length
    }

    #[inline]
    pub const fn note(self) -> &'static str {
        match self.kind {
            BufIoRangeErrorKind::OffsetOutOfBounds => match self.buffer_bound_kind {
                BufIoRangeBound::Capacity => "buffer offset exceeds buffer capacity",
                BufIoRangeBound::Length => "buffer offset exceeds buffer length",
            },
            BufIoRangeErrorKind::LengthExceedsU32 => "buffer I/O length exceeds u32",
        }
    }
}
