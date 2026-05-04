use std::num::NonZeroUsize;
use veloq::runtime::{Runtime, context};
use veloq_buf::{BufPool, UniformSlot, heap::ThreadMemoryMultiplier, nz};

#[test]
fn runtime_binds_buf_pool_to_current_thread() {
    let runtime = Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(NonZeroUsize::new(1).expect("1 is non-zero"))
        .build()
        .expect("failed to build runtime");

    runtime.block_on(async {
        let pool = context::current_pool().expect("buffer pool should be bound");
        assert!(pool.alloc(nz!(64)).is_some());
    });
}
