#![deny(warnings)]

#[cfg(any(target_os = "linux", target_os = "android"))]
include!("../src/io_uring_bench.rs");

#[cfg(not(any(target_os = "linux", target_os = "android")))]
fn main() {}
