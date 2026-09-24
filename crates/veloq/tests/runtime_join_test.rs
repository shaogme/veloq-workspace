use veloq::{
    buf::{UniformSlot, heap::ThreadMemoryMultiplier},
    nz,
    runtime::{Runtime, join},
};

#[test]
fn runtime_join_macro_is_available_through_the_facade() {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(1))))
        .worker_count(Some(nz!(1)))
        .scope(async |_| {
            assert_eq!(join!(async { 11usize }, async { 22usize }), (11, 22));
        })
        .unwrap();
}
