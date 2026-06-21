pub use alloc::collections::*;

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
