#![no_std]

extern crate alloc;

mod error;
mod shared;
mod sys;
mod tls;

// Re-export internal items for submodules
pub(crate) use shared::{ResetGuard, is_sentinel, sentinel_ptr};
pub(crate) use sys::{AtomicKey, Key, SystermKey};

// Public exports
pub use error::{TlsError, TlsErrorKind};
pub use tls::Tls;

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
