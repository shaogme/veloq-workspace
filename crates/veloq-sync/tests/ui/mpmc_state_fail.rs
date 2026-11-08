//! MPMC State is an internal implementation detail.

use veloq_sync::mpmc::Inner;

fn main() {
    let _ = Inner::<i32, _, _>::new(0);
}
