#[test]
fn receiver_is_not_sync() {
    let test_cases = trybuild::TestCases::new();

    #[cfg(not(windows))]
    test_cases.compile_fail("tests/ui/mpsc_receiver_not_sync.rs");

    #[cfg(windows)]
    test_cases.compile_fail("tests/ui/windows/mpsc_receiver_not_sync.rs");
}
