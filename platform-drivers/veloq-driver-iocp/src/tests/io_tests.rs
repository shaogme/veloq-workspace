use veloq_driver_core::driver::Driver;
use veloq_driver_core::op::IntoPlatformOp;
use veloq_driver_core::op::Timeout;
use crate::driver::IocpDriver;
use crate::config::IocpConfig;
use crate::ops::IocpOp;
use crate::tests::wait_completion;

#[test]
fn test_iocp_timeout() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default()).unwrap();

    let timeout_op = Timeout {
        duration: std::time::Duration::from_millis(100),
    };

    let (iocp_kernel, _timeout_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(timeout_op);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, veloq_driver_core::driver::SubmitBinder::new())
        .into_inner()
        .expect("submit timeout failed");

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
