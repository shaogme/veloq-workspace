use crate::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, platform::Platform},
};

use windows_sys::Win32::System::Performance::{QueryPerformanceCounter, QueryPerformanceFrequency};

static FREQUENCY: AtomicU64 = AtomicU64::new(0);

fn get_frequency() -> u64 {
    let cached = FREQUENCY.load(Ordering::Relaxed);
    if cached != 0 {
        return cached;
    }
    let mut freq = 0i64;
    unsafe {
        QueryPerformanceFrequency(&mut freq);
    }
    let val = freq as u64;
    FREQUENCY.store(val, Ordering::Relaxed);
    val
}

pub struct PlatformImpl;

impl Platform for PlatformImpl {
    type RawInstant = u64;

    fn now() -> Self::RawInstant {
        let mut qpc = 0i64;
        unsafe {
            QueryPerformanceCounter(&mut qpc);
        }
        qpc as u64
    }

    fn duration_since(later: Self::RawInstant, earlier: Self::RawInstant) -> Duration {
        if later <= earlier {
            return Duration::ZERO;
        }
        let diff = later - earlier;
        let freq = get_frequency();
        if freq == 0 {
            return Duration::ZERO;
        }
        let secs = diff / freq;
        let rem = diff % freq;
        let nanos = (rem as u128 * 1_000_000_000) / freq as u128;
        Duration::new(secs, nanos as u32)
    }

    fn checked_add(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant> {
        let freq = get_frequency();
        if freq == 0 {
            return None;
        }
        let ticks_secs = duration.as_secs().checked_mul(freq)?;
        let ticks_nanos = (duration.subsec_nanos() as u128 * freq as u128 / 1_000_000_000) as u64;
        let total_ticks = ticks_secs.checked_add(ticks_nanos)?;
        instant.checked_add(total_ticks)
    }

    fn checked_sub(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant> {
        let freq = get_frequency();
        if freq == 0 {
            return None;
        }
        let ticks_secs = duration.as_secs().checked_mul(freq)?;
        let ticks_nanos = (duration.subsec_nanos() as u128 * freq as u128 / 1_000_000_000) as u64;
        let total_ticks = ticks_secs.checked_add(ticks_nanos)?;
        instant.checked_sub(total_ticks)
    }
}
