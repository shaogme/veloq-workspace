use veloq::runtime::Runtime;
use veloq_buf::{BufPool, UniformSlot, heap::ThreadMemoryMultiplier, nz};

#[test]
fn runtime_binds_buf_pool_to_current_thread() {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .scope(async |ctx| {
            let pool = ctx.buf_pool();
            assert!(pool.alloc(nz!(64)).is_some());
        })
        .unwrap();
}
