#![no_std]
#![deny(warnings)]

use core::hash::{BuildHasher, Hasher};

pub mod sys;

/// 高性能无密码学安全但防碰撞攻击的哈希器
#[derive(Debug, Clone)]
pub struct VeloqHasher {
    seed: u64,
}

impl VeloqHasher {
    /// 使用指定的种子创建一个新的 VeloqHasher
    #[inline]
    pub const fn new(seed: u64) -> Self {
        Self { seed }
    }
}

#[inline(always)]
const fn wy_mix(a: u64, b: u64) -> u64 {
    let r = (a as u128).wrapping_mul(b as u128);
    (r as u64) ^ ((r >> 64) as u64)
}

#[inline(always)]
fn wy_hash(bytes: &[u8], mut seed: u64) -> u64 {
    let len = bytes.len();
    if len == 0 {
        return seed;
    }

    // 对于 1-3 字节的小数据，直接位移读取，避免分支和拷贝
    if len <= 3 {
        let val =
            ((bytes[0] as u64) << 16) | ((bytes[len >> 1] as u64) << 8) | (bytes[len - 1] as u64);
        seed = wy_mix(seed ^ val, 0xe7037ed1a0b428dbu64);
        return wy_mix(seed ^ (len as u64), 0x8ebc6af09c88c6e3u64);
    }

    // 对于 4-8 字节的小数据
    if len <= 8 {
        let val = if len == 8 {
            u64::from_le(unsafe { (bytes.as_ptr() as *const u64).read_unaligned() })
        } else if len == 4 {
            u32::from_le(unsafe { (bytes.as_ptr() as *const u32).read_unaligned() }) as u64
        } else {
            let v_lo =
                u32::from_le(unsafe { (bytes.as_ptr() as *const u32).read_unaligned() }) as u64;
            let v_hi = u32::from_le(unsafe {
                (bytes.as_ptr().add(len - 4) as *const u32).read_unaligned()
            }) as u64;
            (v_lo << 32) | v_hi
        };
        seed = wy_mix(seed ^ val, 0xe7037ed1a0b428dbu64);
        return wy_mix(seed ^ (len as u64), 0x8ebc6af09c88c6e3u64);
    }

    let mut pos = 0;

    // 1. 32 字节循环展开，引入指令级并行性 (ILP)
    if pos + 32 <= len {
        let mut s0 = seed;
        let mut s1 = seed ^ 0xa0761d6478bd642fu64;
        let mut s2 = seed ^ 0xe7037ed1a0b428dbu64;
        let mut s3 = seed ^ 0x8ebc6af09c88c6e3u64;

        while pos + 32 <= len {
            unsafe {
                let v0 = u64::from_le((bytes.as_ptr().add(pos) as *const u64).read_unaligned());
                let v1 = u64::from_le((bytes.as_ptr().add(pos + 8) as *const u64).read_unaligned());
                let v2 =
                    u64::from_le((bytes.as_ptr().add(pos + 16) as *const u64).read_unaligned());
                let v3 =
                    u64::from_le((bytes.as_ptr().add(pos + 24) as *const u64).read_unaligned());

                s0 = wy_mix(s0 ^ v0, 0xa0761d6478bd642fu64);
                s1 = wy_mix(s1 ^ v1, 0xe7037ed1a0b428dbu64);
                s2 = wy_mix(s2 ^ v2, 0x8ebc6af09c88c6e3u64);
                s3 = wy_mix(s3 ^ v3, 0x5824c63f1262ee15u64);
            }
            pos += 32;
        }
        seed = s0 ^ s1 ^ s2 ^ s3;
    }

    // 2. 8 字节剩余处理
    while pos + 8 <= len {
        unsafe {
            let val = u64::from_le((bytes.as_ptr().add(pos) as *const u64).read_unaligned());
            seed = wy_mix(seed ^ val, 0xa0761d6478bd642fu64);
        }
        pos += 8;
    }

    // 3. 不足 8 字节的尾部处理（已知 len > 8，因此安全地读取最后 8 字节）
    if pos < len {
        unsafe {
            let val = u64::from_le((bytes.as_ptr().add(len - 8) as *const u64).read_unaligned());
            seed = wy_mix(seed ^ val, 0xe7037ed1a0b428dbu64);
        }
    }

    wy_mix(seed ^ (len as u64), 0x8ebc6af09c88c6e3u64)
}

impl Hasher for VeloqHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.seed
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.seed = wy_hash(bytes, self.seed);
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.seed = wy_mix(self.seed ^ (i as u64), 0xa0761d6478bd642fu64);
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.seed = wy_mix(self.seed ^ (i as u64), 0xa0761d6478bd642fu64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.seed = wy_mix(self.seed ^ (i as u64), 0xa0761d6478bd642fu64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.seed = wy_mix(self.seed ^ i, 0xa0761d6478bd642fu64);
    }

    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.write_u64(i as u64);
        self.write_u64((i >> 64) as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.write_u64(i as u64);
    }
}

/// 支持在 HashMap / HashSet 中直接使用的构造器
#[derive(Debug, Clone, Copy)]
pub struct VeloqBuildHasher {
    seed: u64,
}

impl VeloqBuildHasher {
    /// 自动从系统时间获取种子初始化一个新的 VeloqBuildHasher
    #[inline]
    pub fn new() -> Self {
        Self {
            seed: crate::sys::get_system_time_seed(),
        }
    }

    /// 使用给定的种子创建一个 VeloqBuildHasher
    #[inline]
    pub const fn with_seed(seed: u64) -> Self {
        Self { seed }
    }
}

impl Default for VeloqBuildHasher {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl BuildHasher for VeloqBuildHasher {
    type Hasher = VeloqHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        VeloqHasher::new(self.seed)
    }
}

const K: u64 = 0xf1357aea2e62a9c5;

#[inline]
fn multiply_mix(x: u64, y: u64) -> u64 {
    let full = (x as u128).wrapping_mul(y as u128);
    ((full >> 64) as u64) ^ (full as u64)
}

#[inline]
fn fast_hash_bytes(bytes: &[u8]) -> u64 {
    let len = bytes.len();
    let mut s0 = 0x243f6a8885a308d3u64;
    let mut s1 = 0x13198a2e03707344u64;

    if len <= 16 {
        if len >= 8 {
            s0 ^= u64::from_le(unsafe { (bytes.as_ptr() as *const u64).read_unaligned() });
            s1 ^= u64::from_le(unsafe {
                (bytes.as_ptr().add(len - 8) as *const u64).read_unaligned()
            });
        } else if len >= 4 {
            s0 ^= u32::from_le(unsafe { (bytes.as_ptr() as *const u32).read_unaligned() }) as u64;
            s1 ^= u32::from_le(unsafe {
                (bytes.as_ptr().add(len - 4) as *const u32).read_unaligned()
            }) as u64;
        } else if len > 0 {
            let lo = bytes[0];
            let mid = bytes[len / 2];
            let hi = bytes[len - 1];
            s0 ^= lo as u64;
            s1 ^= ((hi as u64) << 8) | mid as u64;
        }
    } else {
        let mut bulk = &bytes[..(len - 1)];
        while bulk.len() >= 16 {
            let chunk = &bulk[..16];
            let x = u64::from_le(unsafe { (chunk.as_ptr() as *const u64).read_unaligned() });
            let y = u64::from_le(unsafe { (chunk.as_ptr().add(8) as *const u64).read_unaligned() });

            let t = multiply_mix(s0 ^ x, 0xa4093822299f31d0u64 ^ y);
            s0 = s1;
            s1 = t;
            bulk = &bulk[16..];
        }

        let suffix = &bytes[len - 16..];
        s0 ^= u64::from_le(unsafe { (suffix.as_ptr() as *const u64).read_unaligned() });
        s1 ^= u64::from_le(unsafe { (suffix.as_ptr().add(8) as *const u64).read_unaligned() });
    }

    multiply_mix(s0, s1) ^ (len as u64)
}

/// 极速非安全哈希器，适用于完全受信任的内部数据
#[derive(Debug, Clone)]
pub struct VeloqFastHasher {
    hash: u64,
}

impl VeloqFastHasher {
    /// 使用指定的种子创建一个新的 VeloqFastHasher
    #[inline]
    pub const fn new(seed: u64) -> Self {
        Self { hash: seed }
    }
}

impl Default for VeloqFastHasher {
    #[inline]
    fn default() -> Self {
        Self::new(0)
    }
}

impl VeloqFastHasher {
    #[inline]
    fn add_to_hash(&mut self, i: u64) {
        self.hash = self.hash.wrapping_add(i).wrapping_mul(K);
    }
}

impl Hasher for VeloqFastHasher {
    #[inline]
    fn finish(&self) -> u64 {
        const ROTATE: u32 = 26;
        self.hash.rotate_left(ROTATE)
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.write_u64(fast_hash_bytes(bytes));
    }

    #[inline]
    fn write_u8(&mut self, i: u8) {
        self.add_to_hash(i as u64);
    }

    #[inline]
    fn write_u16(&mut self, i: u16) {
        self.add_to_hash(i as u64);
    }

    #[inline]
    fn write_u32(&mut self, i: u32) {
        self.add_to_hash(i as u64);
    }

    #[inline]
    fn write_u64(&mut self, i: u64) {
        self.add_to_hash(i);
    }

    #[inline]
    fn write_u128(&mut self, i: u128) {
        self.add_to_hash(i as u64);
        self.add_to_hash((i >> 64) as u64);
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        self.add_to_hash(i as u64);
    }
}

/// 支持在 HashMap / HashSet 中直接使用的极速非安全构造器
#[derive(Debug, Clone, Copy, Default)]
pub struct VeloqFastBuildHasher;

impl VeloqFastBuildHasher {
    /// 创建一个新的 VeloqFastBuildHasher
    #[inline]
    pub const fn new() -> Self {
        Self
    }
}

impl BuildHasher for VeloqFastBuildHasher {
    type Hasher = VeloqFastHasher;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        VeloqFastHasher::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hasher_basic() {
        let mut hasher = VeloqHasher::new(12345);
        hasher.write(b"hello world");
        let hash1 = hasher.finish();

        let mut hasher2 = VeloqHasher::new(12345);
        hasher2.write(b"hello world");
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hasher_different_seeds() {
        let mut hasher1 = VeloqHasher::new(1);
        hasher1.write(b"hello");
        let hash1 = hasher1.finish();

        let mut hasher2 = VeloqHasher::new(2);
        hasher2.write(b"hello");
        let hash2 = hasher2.finish();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_build_hasher() {
        let bh = VeloqBuildHasher::new();
        let mut h1 = bh.build_hasher();
        let mut h2 = bh.build_hasher();

        h1.write_u64(42);
        h2.write_u64(42);

        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_fast_hasher_basic() {
        let mut hasher = VeloqFastHasher::new(12345);
        hasher.write(b"hello world");
        let hash1 = hasher.finish();

        let mut hasher2 = VeloqFastHasher::new(12345);
        hasher2.write(b"hello world");
        let hash2 = hasher2.finish();

        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_fast_hasher_different_seeds() {
        let mut hasher1 = VeloqFastHasher::new(1);
        hasher1.write(b"hello");
        let hash1 = hasher1.finish();

        let mut hasher2 = VeloqFastHasher::new(2);
        hasher2.write(b"hello");
        let hash2 = hasher2.finish();

        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_fast_build_hasher() {
        let bh = VeloqFastBuildHasher::new();
        let mut h1 = bh.build_hasher();
        let mut h2 = bh.build_hasher();

        h1.write_u64(42);
        h2.write_u64(42);

        assert_eq!(h1.finish(), h2.finish());
    }
}
