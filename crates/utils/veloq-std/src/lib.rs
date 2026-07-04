#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]

#[doc(hidden)]
pub extern crate alloc as alloc_crate;

pub mod cell;
pub mod collections;
pub mod macros;
pub mod sync;
pub mod sys;
pub mod thread;
pub mod time;

pub mod alloc {
    pub use alloc_crate::alloc::*;
}

pub mod array {
    pub use core::array::*;
}

pub mod any {
    pub use core::any::*;
}

pub mod convert {
    pub use core::convert::*;
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

pub mod task {
    pub use core::task::*;
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

pub mod rc {
    pub use alloc_crate::rc::*;
}

pub mod pin {
    pub use core::pin::*;
}

pub mod hash {
    pub use core::hash::*;
}

pub mod mem {
    pub use core::mem::*;
}

pub mod num {
    pub use core::num::*;
}

pub mod boxed {
    pub use alloc_crate::boxed::*;
}

pub mod vec {
    pub use alloc_crate::vec::*;
}

pub mod slice {
    pub use alloc_crate::slice::*;
}

pub mod str {
    pub use alloc_crate::str::*;
}

pub mod string {
    pub use alloc_crate::string::*;
}

pub mod panic {
    pub use core::panic::*;
}
