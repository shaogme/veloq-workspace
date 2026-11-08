use trybuild::TestCases;

#[test]
fn compile_tests() {
    let t = TestCases::new();
    #[cfg(not(windows))]
    {
        t.compile_fail("tests/ui/oneshot_escape_fail.rs");
        t.compile_fail("tests/ui/mpsc_escape_fail.rs");
        t.compile_fail("tests/ui/mpmc_escape_fail.rs");
        t.compile_fail("tests/ui/broadcast_escape_fail.rs");
        t.compile_fail("tests/ui/watch_escape_fail.rs");
    }
    #[cfg(windows)]
    {
        t.compile_fail("tests/ui/windows/oneshot_escape_fail.rs");
        t.compile_fail("tests/ui/windows/mpsc_escape_fail.rs");
        t.compile_fail("tests/ui/windows/mpmc_escape_fail.rs");
        t.compile_fail("tests/ui/windows/broadcast_escape_fail.rs");
        t.compile_fail("tests/ui/windows/watch_escape_fail.rs");
    }
    t.compile_fail("tests/ui/mpmc_state_fail.rs");
    t.pass("tests/ui/pass.rs");
}
