#![no_std]
use core::fmt;

extern crate alloc;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BitSetError {
    OutOfBounds { index: usize, size: usize },
}

impl fmt::Display for BitSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BitSetError::OutOfBounds { index, size } => {
                write!(f, "Index {} is out of bounds for size {}", index, size)
            }
        }
    }
}

impl core::error::Error for BitSetError {}

#[derive(Debug, Clone)]
pub struct BitSet {
    bits: alloc::vec::Vec<u64>,
    size: usize,
}

impl BitSet {
    /// Creates a new BitSet with enough capacity to hold `size` bits.
    /// All bits are initially 0 (false).
    pub fn new(capacity: usize) -> Self {
        let num_u64 = capacity.div_ceil(64);
        Self {
            bits: alloc::vec![0; num_u64],
            size: capacity,
        }
    }

    /// Sets the bit at `index` to 1 (true).
    /// Returns Error if `index` is out of bounds.
    #[inline]
    pub fn set(&mut self, index: usize) -> Result<(), BitSetError> {
        if index >= self.size {
            return Err(BitSetError::OutOfBounds {
                index,
                size: self.size,
            });
        }
        let word_idx = index / 64;
        let bit_idx = index % 64;
        self.bits[word_idx] |= 1 << bit_idx;
        Ok(())
    }

    /// Sets the bit at `index` to 0 (false).
    /// Returns Error if `index` is out of bounds.
    #[inline]
    pub fn clear(&mut self, index: usize) -> Result<(), BitSetError> {
        if index >= self.size {
            return Err(BitSetError::OutOfBounds {
                index,
                size: self.size,
            });
        }
        let word_idx = index / 64;
        let bit_idx = index % 64;
        self.bits[word_idx] &= !(1 << bit_idx);
        Ok(())
    }

    /// Returns the value of the bit at `index`.
    /// Returns Error if `index` is out of bounds.
    #[inline]
    pub fn get(&self, index: usize) -> Result<bool, BitSetError> {
        if index >= self.size {
            return Err(BitSetError::OutOfBounds {
                index,
                size: self.size,
            });
        }
        let word_idx = index / 64;
        let bit_idx = index % 64;
        Ok((self.bits[word_idx] & (1 << bit_idx)) != 0)
    }

    /// Returns the capacity (number of bits) of the BitSet.
    pub fn capacity(&self) -> usize {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bitset_basic() {
        let mut bs = BitSet::new(100);
        assert!(!bs.get(0).unwrap());
        assert!(!bs.get(99).unwrap());

        bs.set(10).unwrap();
        assert!(bs.get(10).unwrap());
        assert!(!bs.get(11).unwrap());

        bs.clear(10).unwrap();
        assert!(!bs.get(10).unwrap());
    }

    #[test]
    fn test_out_of_bounds() {
        let mut bs = BitSet::new(10);
        let err = bs.set(10).err().unwrap(); // Index 10 is the 11th bit, so it's out of bounds 0..9
        match err {
            BitSetError::OutOfBounds { index, size } => {
                assert_eq!(index, 10);
                assert_eq!(size, 10);
            }
        }

        assert!(bs.get(10).is_err());
        assert!(bs.clear(10).is_err());
    }
}
