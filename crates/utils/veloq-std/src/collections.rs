pub use alloc_crate::collections::*;

pub mod bitset;
pub use bitset::{BitSet, BitSetError};

use veloq_hash::{VeloqBuildHasher, VeloqFastBuildHasher};

/// `HashMap` using `VeloqBuildHasher` as the build hasher.
pub type HashMap<K, V> = hashbrown::HashMap<K, V, VeloqBuildHasher>;

/// `FastHashMap` using `VeloqFastBuildHasher` as the build hasher.
///
/// # Warning
///
/// This uses a non-cryptographically secure, collision-prone fast hasher.
/// It should only be used for trusted, internal data.
pub type FastHashMap<K, V> = hashbrown::HashMap<K, V, VeloqFastBuildHasher>;

pub mod hash_map {
    pub use hashbrown::hash_map::{
        Entry, IntoIter, IntoKeys, IntoValues, Iter, IterMut, Keys, OccupiedEntry, VacantEntry,
        Values, ValuesMut,
    };

    use crate::hash::Hasher;
    use veloq_hash::VeloqHasher;

    /// 默认的高性能非加密安全哈希状态处理器。
    #[derive(Clone, Debug)]
    pub struct DefaultHasher(VeloqHasher);

    impl DefaultHasher {
        /// 创建一个新的 `DefaultHasher`。
        #[inline]
        pub const fn new() -> Self {
            Self(VeloqHasher::new(0))
        }
    }

    impl Hasher for DefaultHasher {
        #[inline]
        fn finish(&self) -> u64 {
            self.0.finish()
        }

        #[inline]
        fn write(&mut self, bytes: &[u8]) {
            self.0.write(bytes);
        }

        #[inline]
        fn write_u8(&mut self, i: u8) {
            self.0.write_u8(i);
        }

        #[inline]
        fn write_u16(&mut self, i: u16) {
            self.0.write_u16(i);
        }

        #[inline]
        fn write_u32(&mut self, i: u32) {
            self.0.write_u32(i);
        }

        #[inline]
        fn write_u64(&mut self, i: u64) {
            self.0.write_u64(i);
        }

        #[inline]
        fn write_u128(&mut self, i: u128) {
            self.0.write_u128(i);
        }

        #[inline]
        fn write_usize(&mut self, i: usize) {
            self.0.write_usize(i);
        }
    }

    impl Default for DefaultHasher {
        #[inline]
        fn default() -> Self {
            Self::new()
        }
    }
}

/// `HashSet` using `VeloqBuildHasher` as the build hasher.
pub type HashSet<T> = hashbrown::HashSet<T, VeloqBuildHasher>;

/// `FastHashSet` using `VeloqFastBuildHasher` as the build hasher.
///
/// # Warning
///
/// This uses a non-cryptographically secure, collision-prone fast hasher.
/// It should only be used for trusted, internal data.
pub type FastHashSet<T> = hashbrown::HashSet<T, VeloqFastBuildHasher>;

pub mod hash_set {
    pub use hashbrown::hash_set::{
        Difference, Intersection, IntoIter, Iter, SymmetricDifference, Union,
    };

    pub use super::hash_map::DefaultHasher;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hashmap_basic() {
        let mut map = HashMap::default();
        map.insert(1, 2);
        assert_eq!(map.get(&1), Some(&2));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_hashset_basic() {
        let mut set = HashSet::default();
        set.insert(42);
        assert!(set.contains(&42));
    }

    #[test]
    fn test_fast_hashmap_basic() {
        let mut map = FastHashMap::default();
        map.insert(1, 2);
        assert_eq!(map.get(&1), Some(&2));
        assert_eq!(map.len(), 1);
    }

    #[test]
    fn test_fast_hashset_basic() {
        let mut set = FastHashSet::default();
        set.insert(42);
        assert!(set.contains(&42));
    }
}
