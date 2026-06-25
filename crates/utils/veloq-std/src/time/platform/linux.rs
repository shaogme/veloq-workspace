use crate::time::{Duration, platform::Platform};

use libc::{CLOCK_MONOTONIC, c_long, clock_gettime, time_t, timespec};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Timespec {
    pub tv_sec: time_t,
    pub tv_nsec: c_long,
}

pub struct PlatformImpl;

impl Platform for PlatformImpl {
    type RawInstant = Timespec;

    fn now() -> Self::RawInstant {
        let mut ts = timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe {
            clock_gettime(CLOCK_MONOTONIC, &mut ts);
        }
        Timespec {
            tv_sec: ts.tv_sec,
            tv_nsec: ts.tv_nsec,
        }
    }

    fn duration_since(later: Self::RawInstant, earlier: Self::RawInstant) -> Duration {
        if later <= earlier {
            return Duration::ZERO;
        }
        let mut secs = later.tv_sec - earlier.tv_sec;
        let mut nsecs = later.tv_nsec - earlier.tv_nsec;
        if nsecs < 0 {
            secs -= 1;
            nsecs += 1_000_000_000;
        }
        Duration::new(secs as u64, nsecs as u32)
    }

    fn checked_add(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant> {
        let secs_to_add: time_t = duration.as_secs().try_into().ok()?;
        let secs = instant.tv_sec.checked_add(secs_to_add)?;
        let nsecs = instant
            .tv_nsec
            .checked_add(duration.subsec_nanos() as c_long)?;

        let mut final_secs = secs;
        let mut final_nsecs = nsecs;
        if final_nsecs >= 1_000_000_000 {
            final_secs = final_secs.checked_add(1)?;
            final_nsecs -= 1_000_000_000;
        }
        Some(Timespec {
            tv_sec: final_secs,
            tv_nsec: final_nsecs,
        })
    }

    fn checked_sub(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant> {
        let secs_to_sub: time_t = duration.as_secs().try_into().ok()?;
        let secs = instant.tv_sec.checked_sub(secs_to_sub)?;
        let nsecs = instant
            .tv_nsec
            .checked_sub(duration.subsec_nanos() as c_long)?;

        let mut final_secs = secs;
        let mut final_nsecs = nsecs;
        if final_nsecs < 0 {
            final_secs = final_secs.checked_sub(1)?;
            final_nsecs += 1_000_000_000;
        }
        if final_secs < 0 {
            return None;
        }
        Some(Timespec {
            tv_sec: final_secs,
            tv_nsec: final_nsecs,
        })
    }
}
