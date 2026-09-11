#![cfg(not(feature = "std"))]

use veloq_std::panic::{AssertUnwindSafe, PanicResult, catch_unwind};

#[test]
fn no_std_panic_api_executes_and_uses_the_unified_result_type() {
    let result: PanicResult<u32> = catch_unwind(AssertUnwindSafe::new(|| 42));
    assert_eq!(
        result.expect("no_std catch_unwind should return normally"),
        42
    );
}
