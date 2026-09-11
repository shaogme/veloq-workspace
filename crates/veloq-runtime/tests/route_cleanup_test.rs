use veloq_std::future::Ready;

use veloq_runtime::{error::RuntimeError, runtime::Runtime};

#[test]
fn route_job_panic_is_published_to_the_route_future() {
    let result = Runtime::<(), _>::scope(async |ctx| {
        ctx.route_to(0, || -> Ready<()> { panic!("route job panic") })
            .expect("route task should be queued")
            .await
    })
    .expect("runtime should finish after consuming route error");

    let error = result.expect_err("route job panic must become a route error");
    assert!(matches!(
        error.inner(),
        RuntimeError::InvariantViolation {
            site: "RuntimeCtx::route_to::RouteJobTask::poll_raw",
            ..
        }
    ));
}
