use diagweave::prelude::ContextValue;
use veloq_std::fmt;

/// slot 的 ABA 计数器：把"这个 index 上的第几次占用"与 index 本身分开表达。
///
/// 一轮完整的 alloc → 提交 → 完成 → 消费会让它推进 **2 代**，并在 `u32` 上自然回绕。
/// 回绕意味着裸 `<` / `>` 在跨过 `u32::MAX` 之后会把新旧关系整体判反，所以本类型
/// **刻意不实现** `PartialOrd` / `Ord`——新旧判定只能走 [`Self::is_older_than`] /
/// [`Self::is_newer_than`]，它们按差值的符号位比较，只要两者的真实距离不超过
/// `u32::MAX / 2` 结论就与"谁更新"一致（真实距离是个位数）。
///
/// **位宽约束**：`Generation` 必须与 `CompletionToken` 的 generation 位域等宽。内核只
/// 回传那一个 `u64`，解码出的 generation 一旦比这里窄，跨过窄宽度边界后完成就会被
/// `record_completion` 判成 Stale 而静默丢弃——slot 永久停留在 `InFlightWaiting`，其
/// 持有的 buffer 永不归还。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Generation(u32);

impl Generation {
    /// slot 尚未被分配过时的初始代号。
    pub const ZERO: Self = Self(0);

    #[inline]
    pub const fn new(raw: u32) -> Self {
        Self(raw)
    }

    /// 取出裸值。仅供位编码与诊断输出使用，**不要**拿它做新旧比较。
    #[inline]
    pub const fn get(self) -> u32 {
        self.0
    }

    /// 推进一代，在 `u32` 上回绕。
    #[inline]
    pub const fn next(self) -> Self {
        Self(self.0.wrapping_add(1))
    }

    /// `self` 是否比 `other` 旧（落后于 `other`）。相等时为 `false`。
    #[inline]
    pub const fn is_older_than(self, other: Self) -> bool {
        (self.0.wrapping_sub(other.0) as i32) < 0
    }

    /// `self` 是否比 `other` 新。相等时为 `false`。
    #[inline]
    pub const fn is_newer_than(self, other: Self) -> bool {
        other.is_older_than(self)
    }
}

impl fmt::Display for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for Generation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

/// 让 `Report::with_ctx("generation", generation)` 无需在每个诊断点手写 `.get()`。
impl From<Generation> for ContextValue {
    fn from(value: Generation) -> Self {
        Self::from(value.0)
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;

    #[test]
    fn ordering_survives_wraparound() {
        let zero = Generation::ZERO;
        let one = Generation::new(1);
        let max = Generation::new(u32::MAX);

        assert!(zero.is_older_than(one));
        assert!(one.is_newer_than(zero));
        assert!(!one.is_older_than(one));
        assert!(!one.is_newer_than(one));

        // 回绕点：`u32::MAX` 的下一代是 0 / 1，裸 `<` 会把它们判反。
        assert!(zero.is_newer_than(max));
        assert!(one.is_newer_than(Generation::new(u32::MAX - 1)));
        assert!(max.is_older_than(one));
        assert!(Generation::new(u32::MAX - 1).is_older_than(zero));
    }

    #[test]
    fn next_wraps_at_the_top_of_u32() {
        assert_eq!(Generation::ZERO.next(), Generation::new(1));
        assert_eq!(Generation::new(u32::MAX).next(), Generation::ZERO);
    }
}
