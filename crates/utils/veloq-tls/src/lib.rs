#![no_std]

extern crate alloc;

mod error;
mod sys;
mod tls;

use core::ptr::null_mut;

pub(crate) use sys::{AtomicKey, Key, SystemKey};

// Public exports
pub use error::{TlsError, TlsErrorKind};
pub use tls::Tls;

/// Checks whether a TLS pointer marks an in-progress initialization.
#[inline(always)]
pub(crate) fn is_sentinel<T>(ptr: *const T) -> bool {
    ptr as usize == 1
}

/// Creates the pointer used to mark an in-progress TLS initialization.
#[inline(always)]
pub(crate) fn sentinel_ptr<T>() -> *mut T {
    1 as *mut T
}

pub(crate) struct ResetGuard {
    key: Key,
    active: bool,
}

impl ResetGuard {
    #[inline(always)]
    pub(crate) fn new(key: Key) -> Self {
        Self { key, active: true }
    }

    #[inline(always)]
    pub(crate) fn cancel(mut self) {
        self.active = false;
    }
}

impl Drop for ResetGuard {
    #[inline(always)]
    fn drop(&mut self) {
        if self.active {
            unsafe {
                let _ = self.key.set_value::<()>(null_mut());
            }
        }
    }
}

/// A macro for declaring thread-local variables using the platform-native `Tls`.
///
/// # Example
///
/// ```rust
/// veloq_tls::veloq_tls! {
///     pub static FOO: i32 = 42;
///     static BAR: String = "hello".to_string();
/// }
/// ```
#[macro_export]
macro_rules! veloq_tls {
    (
        $(
            $(#[$attr:meta])*
            $vis:vis static $name:ident : $t:ty = $init:expr;
        )*
    ) => {
        $(
            $(#[$attr])*
            $vis static $name: $crate::Tls<$t> = $crate::Tls::new();

            #[allow(non_camel_case_types)]
            #[allow(dead_code)]
            $vis struct $name {}

            impl $name {
                #[inline(always)]
                $vis fn init() -> $t {
                    $init
                }
            }
        )*
    };
}
