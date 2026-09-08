#[macro_export]
macro_rules! vec {
    () => {
        $crate::alloc_crate::vec![]
    };
    ($elem:expr; $n:expr) => {
        $crate::alloc_crate::vec![$elem; $n]
    };
    ($($x:expr),+ $(,)?) => {
        $crate::alloc_crate::vec![$($x),+]
    };
}

#[macro_export]
macro_rules! format {
    ($($arg:tt)*) => {
        $crate::alloc_crate::format!($($arg)*)
    };
}

/// 创建 NonZero<T> 的宏
/// - 输入 0：编译失败
/// - 输入非 0 字面量/常量：编译通过，且无运行时开销
#[macro_export]
macro_rules! nz {
    ($value:expr) => {{
        // 1. 利用匿名常量强制进行编译时检查
        // 如果 $value 为 0，assert! 会 panic，导致编译中断
        const _: () = assert!($value != 0, "nz! macro: Value cannot be zero!");

        // 2. 如果上面通过了，说明 $value 肯定不为 0
        // 使用 unsafe 块调用 new_unchecked
        unsafe { $crate::num::NonZero::new_unchecked($value) }
    }};
}

#[macro_export]
macro_rules! print {
    ($($arg:tt)*) => {
        $crate::io::_print(core::format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! println {
    () => {
        $crate::print!("\n")
    };
    ($fmt:expr) => {
        $crate::print!(core::concat!($fmt, "\n"))
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::print!(core::concat!($fmt, "\n"), $($arg)*)
    };
}

#[macro_export]
macro_rules! eprint {
    ($($arg:tt)*) => {
        $crate::io::_eprint(core::format_args!($($arg)*))
    };
}

#[macro_export]
macro_rules! eprintln {
    () => {
        $crate::eprint!("\n")
    };
    ($fmt:expr) => {
        $crate::eprint!(core::concat!($fmt, "\n"))
    };
    ($fmt:expr, $($arg:tt)*) => {
        $crate::eprint!(core::concat!($fmt, "\n"), $($arg)*)
    };
}
