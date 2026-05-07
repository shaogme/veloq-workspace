/// 一个简单的 Xorshift64 随机数生成器。
pub struct FastRand {
    state: u64,
}

impl FastRand {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed.wrapping_add(0x9E3779B97F4A7C15), // 避免种子为 0
        }
    }

    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    /// 返回 [0, max) 范围内的随机数。
    pub fn next_u32(&mut self, max: u32) -> u32 {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as u32) % max
    }
}
