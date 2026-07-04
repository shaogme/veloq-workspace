#[macro_export]
macro_rules! vec {
    () => {
        $crate::alloc::vec![]
    };
    ($elem:expr; $n:expr) => {
        $crate::alloc::vec![$elem; $n]
    };
    ($($x:expr),+ $(,)?) => {
        $crate::alloc::vec![$($x),+]
    };
}

#[macro_export]
macro_rules! format {
    ($($arg:tt)*) => {
        $crate::alloc::format!($($arg)*)
    };
}
