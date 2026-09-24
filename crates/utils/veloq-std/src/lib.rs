#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]

#[doc(hidden)]
pub(crate) extern crate alloc as alloc_crate;

#[doc(hidden)]
pub mod __private {
    use crate::{string::String, vec::Vec};

    pub use core::{concat, format_args};

    pub fn format(args: core::fmt::Arguments<'_>) -> String {
        alloc_crate::fmt::format(args)
    }

    pub fn vec_from_array<T, const N: usize>(items: [T; N]) -> Vec<T> {
        items.into_iter().collect()
    }

    pub fn vec_repeat<T: Clone>(item: T, count: usize) -> Vec<T> {
        let mut items = Vec::with_capacity(count);
        for _ in 0..count {
            items.push(item.clone());
        }
        items
    }
}

pub mod cell;
pub mod collections;
pub mod env;
pub mod fs;
pub mod io;
pub mod macros;
pub mod path;
pub mod process;
pub mod sync;
pub mod thread;
pub mod time;

pub mod alloc {
    pub use alloc_crate::alloc::*;
}

pub mod array {
    pub use core::array::*;
}

pub mod borrow {
    pub use alloc_crate::borrow::*;
}

pub mod any {
    pub use core::any::*;
}

pub mod cmp {
    pub use core::cmp::*;
}

pub mod convert {
    pub use core::convert::*;
}

pub mod hint {
    #[cfg(not(feature = "loom"))]
    pub use core::hint::*;

    #[cfg(feature = "loom")]
    pub use core::hint::{
        assert_unchecked, black_box, cold_path, select_unpredictable, unreachable_unchecked,
    };

    #[cfg(feature = "loom")]
    pub use loom::hint::spin_loop;
}

pub mod ptr {
    pub use core::ptr::*;
}

pub mod result {
    pub use core::result::*;
}

pub mod task;

pub mod error {
    pub use core::error::*;
}

pub mod ffi;

pub mod fmt {
    pub use core::fmt::*;
}

pub mod future {
    pub use core::future::*;
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

pub mod net;

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

pub mod os;

pub mod panic;
