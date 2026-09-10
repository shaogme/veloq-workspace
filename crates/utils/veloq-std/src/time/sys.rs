use crate::{fmt::Debug, hash::Hash, time::Duration};

#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub use linux::SystermImpl;

#[cfg(target_os = "windows")]
pub use windows::SystermImpl;

pub trait Systerm {
    type RawInstant: Copy + Clone + Ord + Eq + Hash + Debug + Send + Sync;

    fn now() -> Self::RawInstant;
    fn duration_since(later: Self::RawInstant, earlier: Self::RawInstant) -> Duration;
    fn checked_add(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant>;
    fn checked_sub(instant: Self::RawInstant, duration: Duration) -> Option<Self::RawInstant>;
}
