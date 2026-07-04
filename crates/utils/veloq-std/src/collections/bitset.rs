use crate::{boxed::Box, error::Error, fmt, vec};

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

impl Error for BitSetError {}

#[derive(Debug, Clone)]
pub struct BitSet {
    bits: Box<[u8]>,
    size: usize,
}

impl BitSet {
    /// Creates a new BitSet with enough capacity to hold `size` bits.
    /// All bits are initially 0 (false).
    pub fn new(capacity: usize) -> Self {
        let num_u8 = capacity.div_ceil(8);
        Self {
            bits: vec![0; num_u8].into_boxed_slice(),
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
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.bits[byte_idx] |= 1 << bit_idx;
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
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        self.bits[byte_idx] &= !(1 << bit_idx);
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
        let byte_idx = index / 8;
        let bit_idx = index % 8;
        Ok((self.bits[byte_idx] & (1 << bit_idx)) != 0)
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
