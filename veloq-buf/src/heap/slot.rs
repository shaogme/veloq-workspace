//! Slot: Note that the minimum atomic unit of system memory management is (4KB)
//!
//! Slot is the basic implementation unit of the new architecture, replacing the heavy Block.
//! It is just a concept of the standard unit of memory, implemented physically as a 4KB standard page.

use std::fmt;

/// Standard Slot Size: 4KB
/// Aligned with the physical page size of most architectures (x86_64/AArch64)
pub const SLOT_SIZE: usize = 4096;

/// Calculate the number of slots required for a given size
#[inline]
pub const fn slots_needed(size: usize) -> usize {
    (size + SLOT_SIZE - 1) / SLOT_SIZE
}

/// Slot Index
///
/// Represents the index of a Slot in the global continuous memory area (Arena).
/// Range: [0, total_slots)
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct SlotIndex(pub usize);

impl fmt::Debug for SlotIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Slot(#{})", self.0)
    }
}

impl SlotIndex {
    /// Calculate the memory offset (byte offset) corresponding to the Slot
    #[inline(always)]
    pub fn offset(&self) -> usize {
        self.0 * SLOT_SIZE
    }

    /// Convert from byte offset to Slot Index
    #[inline(always)]
    pub fn from_offset(offset: usize) -> Self {
        Self(offset / SLOT_SIZE)
    }
}
