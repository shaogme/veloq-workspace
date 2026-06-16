use std::cell::Cell;

/// 一个使用 WyRand 算法实现的快速伪随机数生成器。
pub struct FastRand {
    state: Cell<u64>,
}

impl FastRand {
    pub fn new(seed: u64) -> Self {
        Self {
            state: Cell::new(seed),
        }
    }

    pub fn next_u64(&self) -> u64 {
        let mut state = self.state.get();
        state = state.wrapping_add(0xa076_1d64_78bd_642f);
        self.state.set(state);
        let hash = (state as u128).wrapping_mul((state ^ 0xe703_7ed1_a0b4_28db) as u128);
        ((hash >> 64) as u64) ^ (hash as u64)
    }

    /// 返回 [0, max) 范围内的随机数。
    pub fn next_u32(&self, max: u32) -> u32 {
        if max == 0 {
            return 0;
        }
        (self.next_u64() as u32) % max
    }
}
