use crate::{
    fmt,
    ops::{Add, AddAssign, Sub, SubAssign},
    time::platform::{Platform, PlatformImpl},
};

pub use core::time::*;

mod platform;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant(<PlatformImpl as Platform>::RawInstant);

impl Instant {
    pub fn now() -> Self {
        Self(PlatformImpl::now())
    }

    pub fn duration_since(&self, earlier: Instant) -> Duration {
        PlatformImpl::duration_since(self.0, earlier.0)
    }

    pub fn checked_duration_since(&self, earlier: Instant) -> Option<Duration> {
        if self.0 >= earlier.0 {
            Some(self.duration_since(earlier))
        } else {
            None
        }
    }

    pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
        self.checked_duration_since(earlier)
            .unwrap_or(Duration::ZERO)
    }

    pub fn elapsed(&self) -> Duration {
        Self::now().duration_since(*self)
    }

    pub fn checked_add(&self, other: Duration) -> Option<Instant> {
        PlatformImpl::checked_add(self.0, other).map(Instant)
    }

    pub fn checked_sub(&self, other: Duration) -> Option<Instant> {
        PlatformImpl::checked_sub(self.0, other).map(Instant)
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    fn add(self, other: Duration) -> Instant {
        self.checked_add(other)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, other: Duration) {
        *self = *self + other;
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    fn sub(self, other: Duration) -> Instant {
        self.checked_sub(other)
            .expect("overflow when subtracting duration from instant")
    }
}

impl SubAssign<Duration> for Instant {
    fn sub_assign(&mut self, other: Duration) {
        *self = *self - other;
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

impl fmt::Debug for Instant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Instant").field(&self.0).finish()
    }
}

#[cfg(test)]
mod tests {
    use alloc_crate::format;

    use super::*;
    use crate::thread;

    #[test]
    fn test_instant_now_and_elapsed() {
        let now = Instant::now();
        thread::sleep(Duration::from_millis(5)).unwrap();
        let elapsed = now.elapsed();
        assert!(elapsed >= Duration::from_millis(5));
    }

    #[test]
    fn test_duration_since() {
        let t1 = Instant::now();
        thread::sleep(Duration::from_millis(5)).unwrap();
        let t2 = Instant::now();

        let diff = t2.duration_since(t1);
        assert!(diff >= Duration::from_millis(5));

        // 逆向情况应该返回 ZERO 长度的 Duration（依据 linux/windows.rs 底层实现）
        let diff_rev = t1.duration_since(t2);
        assert_eq!(diff_rev, Duration::ZERO);
    }

    #[test]
    fn test_checked_duration_since() {
        let t1 = Instant::now();
        thread::sleep(Duration::from_millis(5)).unwrap();
        let t2 = Instant::now();

        assert!(t2.checked_duration_since(t1).is_some());
        assert!(t2.checked_duration_since(t1).unwrap() >= Duration::from_millis(5));

        // t1 比 t2 早，所以 t1.checked_duration_since(t2) 应当返回 None
        assert!(t1.checked_duration_since(t2).is_none());
    }

    #[test]
    fn test_saturating_duration_since() {
        let t1 = Instant::now();
        thread::sleep(Duration::from_millis(5)).unwrap();
        let t2 = Instant::now();

        assert!(t2.saturating_duration_since(t1) >= Duration::from_millis(5));
        assert_eq!(t1.saturating_duration_since(t2), Duration::ZERO);
    }

    #[test]
    fn test_checked_add_sub() {
        let now = Instant::now();
        let dur = Duration::from_secs(10);

        let later = now.checked_add(dur).expect("checked_add failed");
        let earlier = later.checked_sub(dur).expect("checked_sub failed");

        // 验证加减后的时间回滚是一致的
        assert_eq!(earlier.duration_since(now), Duration::ZERO);
        assert_eq!(now.duration_since(earlier), Duration::ZERO);
    }

    #[test]
    fn test_operators() {
        let now = Instant::now();
        let dur = Duration::from_secs(5);

        // Add
        let later = now + dur;
        assert_eq!(later.duration_since(now), dur);

        // AddAssign
        let mut t = now;
        t += dur;
        assert_eq!(t, later);

        // Sub
        let earlier = later - dur;
        assert_eq!(earlier, now);

        // SubAssign
        let mut t2 = later;
        t2 -= dur;
        assert_eq!(t2, now);

        // Sub Instant
        let diff = later - now;
        assert_eq!(diff, dur);
    }

    #[test]
    #[should_panic(expected = "overflow when adding duration to instant")]
    fn test_add_overflow_panic() {
        let now = Instant::now();
        // 尝试加一个极大的 Duration 造成溢出 panic
        let _ = now + Duration::MAX;
    }

    #[test]
    #[should_panic(expected = "overflow when subtracting duration from instant")]
    fn test_sub_underflow_panic() {
        let now = Instant::now();
        // 尝试减去极大的 Duration 造成下溢 panic
        let _ = now - Duration::MAX;
    }

    #[test]
    fn test_debug_format() {
        let now = Instant::now();
        let debug_str = format!("{:?}", now);
        assert!(debug_str.contains("Instant("));
    }
}
