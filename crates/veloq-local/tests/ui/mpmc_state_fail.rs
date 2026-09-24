//! MPMC State is an internal implementation detail.

use veloq_local::mpmc;

fn main() {
    let state = mpmc::State::<i32>::unbounded();
    let _ = state.split();
}
