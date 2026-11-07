use veloq::{nz, runtime::Runtime};
use veloq_buf::{BufPool, UniformSlot};

#[test]
fn runtime_binds_buf_pool_to_current_thread() {
    Runtime::builder(UniformSlot::new(4 * 4 * 1024 * 1024))
        .worker_count(Some(nz!(1)))
        .scope(async |ctx| {
            let pool = ctx.buf_pool();
            assert!(pool.alloc_full(nz!(64)).is_some());
        })
        .unwrap();
}
