#![cfg(feature = "std")]

use veloq_std::panic::{AssertUnwindSafe, PanicResult, catch_unwind, resume_unwind};

#[test]
fn catch_unwind_captures_and_downcasts_payloads() {
    let result: PanicResult<()> = catch_unwind(AssertUnwindSafe::new(|| panic!("&str payload")));
    let payload = result.expect_err("the closure should panic");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"&str payload"));
    assert!(payload.as_any().is::<&str>());

    let result = catch_unwind(AssertUnwindSafe::new(|| {
        std::panic::panic_any(String::from("String payload"));
    }));
    let payload = result.expect_err("the closure should panic");
    assert_eq!(
        payload.downcast_ref::<String>().map(String::as_str),
        Some("String payload")
    );
}

#[test]
fn resume_unwind_preserves_the_original_payload() {
    let caught = catch_unwind(AssertUnwindSafe::new(|| panic!("original payload")))
        .expect_err("the first closure should panic");
    let resumed = catch_unwind(AssertUnwindSafe::new(|| resume_unwind(caught)));
    let payload = resumed.expect_err("resume_unwind should panic");
    assert_eq!(payload.downcast_ref::<&str>(), Some(&"original payload"));
}

#[test]
fn panic_result_has_a_single_public_error_type() {
    fn assert_send<T: Send>() {}
    assert_send::<veloq_std::panic::PanicPayload>();

    let result: PanicResult<i32> = catch_unwind(AssertUnwindSafe::new(|| 42));
    assert_eq!(result.expect("the closure should return normally"), 42);
}
