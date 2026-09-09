#[test]
fn compile_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/block_on_ctx_escape_fail.rs");
    t.compile_fail("tests/ui/scope_spawn_boxed_lifetime_fail.rs");
    t.compile_fail("tests/ui/task_lifetime_fail.rs");

    #[cfg(not(windows))]
    t.compile_fail("tests/ui/join_handle_not_sync.rs");

    #[cfg(windows)]
    t.compile_fail("tests/ui/windows/join_handle_not_sync.rs");
}
