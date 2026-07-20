use crate::{fmt::Debug, hash::Hash, time::Duration};

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "windows")]
mod windows;

#[cfg(target_os = "linux")]
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
