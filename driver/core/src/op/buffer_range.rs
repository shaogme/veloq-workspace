use veloq_buf::FixedBuf;

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
    const fn new(
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

#[inline]
fn checked_buf_io_len(
    buf: &FixedBuf,
    buf_offset: usize,
    bound_kind: BufIoRangeBound,
) -> Result<u32, BufIoRangeError> {
    checked_buf_io_len_parts(buf_offset, buf.len(), buf.capacity(), bound_kind)
}

#[inline]
fn checked_buf_io_len_parts(
    buf_offset: usize,
    buffer_length: usize,
    buffer_capacity: usize,
    bound_kind: BufIoRangeBound,
) -> Result<u32, BufIoRangeError> {
    let bound = match bound_kind {
        BufIoRangeBound::Capacity => buffer_capacity,
        BufIoRangeBound::Length => buffer_length,
    };

    if buf_offset > bound {
        return Err(BufIoRangeError::new(
            BufIoRangeErrorKind::OffsetOutOfBounds,
            buf_offset,
            buffer_length,
            buffer_capacity,
            bound,
            bound_kind,
            0,
        ));
    }

    let submission_length = bound - buf_offset;
    u32::try_from(submission_length).map_err(|_| {
        BufIoRangeError::new(
            BufIoRangeErrorKind::LengthExceedsU32,
            buf_offset,
            buffer_length,
            buffer_capacity,
            bound,
            bound_kind,
            submission_length,
        )
    })
}

#[inline]
pub fn checked_read_buf_range(
    buf: &mut FixedBuf,
    buf_offset: usize,
) -> Result<(*mut u8, u32), BufIoRangeError> {
    let len = checked_buf_io_len(buf, buf_offset, BufIoRangeBound::Capacity)?;
    // SAFETY: buf_offset is verified to be within 0..=capacity above.
    let ptr = unsafe { buf.as_mut_ptr().add(buf_offset) };
    Ok((ptr, len))
}

#[inline]
pub fn checked_write_buf_range(
    buf: &FixedBuf,
    buf_offset: usize,
) -> Result<(*const u8, u32), BufIoRangeError> {
    let len = checked_buf_io_len(buf, buf_offset, BufIoRangeBound::Length)?;
    // SAFETY: buf_offset is verified to be within 0..=len above, and len <= capacity.
    let ptr = unsafe { buf.as_ptr().add(buf_offset) };
    Ok((ptr, len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::num::NonZeroUsize;

    fn fixed_buf(capacity: usize, len: usize) -> FixedBuf {
        let mut buf = FixedBuf::alloc_heap(NonZeroUsize::new(capacity).expect("non-zero capacity"))
            .expect("heap buffer allocation failed");
        buf.set_len(len);
        buf
    }

    #[test]
    fn read_range_uses_capacity_bound() {
        let mut buf = fixed_buf(8, 4);

        let (ptr, len) =
            checked_read_buf_range(&mut buf, 4).expect("offset inside capacity should pass");

        assert_eq!(len, 4);
        assert_eq!(ptr, unsafe { buf.as_mut_ptr().add(4) });
    }

    #[test]
    fn write_range_uses_length_bound() {
        let buf = fixed_buf(8, 4);

        let err =
            checked_write_buf_range(&buf, 5).expect_err("write offset past length should fail");

        assert_eq!(err.kind(), BufIoRangeErrorKind::OffsetOutOfBounds);
        assert_eq!(err.buffer_bound_kind(), BufIoRangeBound::Length);
        assert_eq!(err.buffer_bound(), 4);
    }

    #[test]
    fn exact_boundaries_produce_zero_length_ranges() {
        let mut buf = fixed_buf(8, 4);

        let (_, read_len) =
            checked_read_buf_range(&mut buf, 8).expect("read offset at capacity should be allowed");
        let (_, write_len) =
            checked_write_buf_range(&buf, 4).expect("write offset at length should be allowed");

        assert_eq!(read_len, 0);
        assert_eq!(write_len, 0);
    }

    #[test]
    fn out_of_bounds_offsets_are_rejected_before_pointer_math() {
        let mut buf = fixed_buf(8, 4);

        let read_err =
            checked_read_buf_range(&mut buf, 9).expect_err("read offset past capacity should fail");
        let write_err =
            checked_write_buf_range(&buf, 5).expect_err("write offset past length should fail");

        assert_eq!(read_err.buffer_bound_kind(), BufIoRangeBound::Capacity);
        assert_eq!(write_err.buffer_bound_kind(), BufIoRangeBound::Length);
    }

    #[test]
    fn oversized_submission_lengths_are_rejected() {
        let err =
            checked_buf_io_len_parts(0, 0, (u32::MAX as usize) + 1, BufIoRangeBound::Capacity)
                .expect_err("submission length above u32 should fail");

        assert_eq!(err.kind(), BufIoRangeErrorKind::LengthExceedsU32);
        assert_eq!(err.submission_length(), (u32::MAX as usize) + 1);
    }
}
