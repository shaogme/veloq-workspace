mod error;
mod shared;
mod tls;

// Re-export internal items for submodules
pub(crate) use shared::{RawKey, ResetGuard, is_sentinel, sentinel_ptr};

// Public exports
pub use error::TlsError;
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
            $vis static $name: $crate::Tls<$t> = $crate::Tls::new(|| $init);
        )*
    };
}
