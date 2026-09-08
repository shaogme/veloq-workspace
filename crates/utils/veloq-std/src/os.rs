//! Platform-specific operating-system handles and conversion traits.
//!
//! The modules in this facade intentionally mirror the stable standard-library
//! paths used by the driver crates. They are only populated when the `std`
//! feature is enabled; the empty module keeps the facade available to
//! `no_std` consumers without exposing standard-library types.

#[cfg(all(feature = "std", unix))]
mod unix;

#[cfg(all(feature = "std", unix))]
pub use unix::fd;

#[cfg(all(feature = "std", windows))]
pub mod windows;
