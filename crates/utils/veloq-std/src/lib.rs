#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]

extern crate alloc;

pub mod cell;
pub mod collections;
pub mod sync;
pub mod sys;
pub mod thread;
pub mod time;

pub mod any {
    pub use core::any::*;
}

pub mod hint {
    #[cfg(not(feature = "loom"))]
    pub use core::hint::spin_loop;
    #[cfg(feature = "loom")]
    pub use loom::hint::spin_loop;
}

pub mod ptr {
    pub use core::ptr::*;
}

pub mod error {
    pub use core::error::*;
}

pub mod ffi {
    pub use core::ffi::*;
}

pub mod fmt {
    pub use core::fmt::*;
}

pub mod marker {
    pub use core::marker::*;
}

pub mod ops {
    pub use core::ops::*;
}

pub mod hash {
    pub use core::hash::*;
}

pub mod mem {
    pub use core::mem::*;
}

pub mod boxed {
    pub use alloc::boxed::*;
}

pub mod vec {
    pub use alloc::vec::*;
}

pub mod slice {
    pub use alloc::slice::*;
}

pub mod str {
    pub use alloc::str::*;
}

pub mod string {
    pub use alloc::string::*;
}
