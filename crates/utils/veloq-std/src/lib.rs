#![no_std]
#![deny(warnings)]

extern crate alloc;

pub mod cell;
pub mod collections;
pub mod sync;
pub mod thread;
pub mod time;

pub mod hint {
    #[cfg(not(feature = "loom"))]
    pub use core::hint::spin_loop;
    #[cfg(feature = "loom")]
    pub use loom::hint::spin_loop;
}
