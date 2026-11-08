//! MPMC State is an internal implementation detail.

use veloq_local::mpmc::State;

fn main() {
    let state = State::<i32>::unbounded();
    let _ = state.split();
}
