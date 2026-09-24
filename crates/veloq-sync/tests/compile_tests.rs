#[test]
#[cfg(not(feature = "loom"))]
fn compile_tests() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/oneshot_escape_fail.rs");
    t.compile_fail("tests/ui/mpsc_escape_fail.rs");
    t.compile_fail("tests/ui/mpmc_escape_fail.rs");
    t.compile_fail("tests/ui/mpmc_state_fail.rs");
    t.compile_fail("tests/ui/broadcast_escape_fail.rs");
    t.compile_fail("tests/ui/watch_escape_fail.rs");
    t.pass("tests/ui/pass.rs");
}
