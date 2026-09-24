//! MPMC State is an internal implementation detail.

use veloq_sync::mpmc;

fn main() {
    let state = mpmc::State::<i32, mpmc::flavor::Unbounded, _>::new(0);
    let _ = state.split();
}
