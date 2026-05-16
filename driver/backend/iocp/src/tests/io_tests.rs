use crate::config::IocpConfig;
use crate::driver::IocpDriver;
use crate::tests::{submit_test_op, wait_completion};
use veloq_buf::NoopRegistrar;
use veloq_driver_core::op::Timeout;

#[test]
fn test_iocp_timeout() {
    let mut driver: IocpDriver =
        IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    let timeout_op = Timeout {
        duration: std::time::Duration::from_millis(100),
    };

    let (user_data, generation) = submit_test_op(&mut driver, timeout_op);

    let start = std::time::Instant::now();
    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(1),
    );
    assert!(res.is_ok(), "Timeout should succeed");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "Should wait at least ~100ms, got {:?}",
        elapsed
    );
}
