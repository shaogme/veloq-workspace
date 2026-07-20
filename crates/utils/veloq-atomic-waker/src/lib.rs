//! `futures::task::AtomicWaker` extracted into its own crate and adapted to support [Loom].
//!
//! [Loom]: https://crates.io/crates/loom
#![no_std]
#![doc(
    html_favicon_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]
#![doc(
    html_logo_url = "https://raw.githubusercontent.com/smol-rs/smol/master/assets/images/logo_fullsize_transparent.png"
)]

pub mod atomic;
pub mod mwsr;

pub use atomic::AtomicWaker;
pub use mwsr::MwsrWaker;
