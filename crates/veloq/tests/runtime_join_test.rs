use veloq::{
    buf::UniformSlot,
    nz,
    runtime::{Runtime, join},
};

#[test]
fn runtime_join_macro_is_available_through_the_facade() {
    Runtime::builder(UniformSlot::new(4 * 1024 * 1024))
        .worker_count(Some(nz!(1)))
        .scope(async |_| {
            assert_eq!(join!(async { 11usize }, async { 22usize }), (11, 22));
        })
        .unwrap();
}
