use veloq_runtime::{
    error::Result,
    runtime::{
        context::{IdleDecision, IdleWaitStrategy},
        RuntimeBuilder, RuntimeShared,
    },
};

struct CustomExtra;

fn idle_custom(_: &RuntimeShared<CustomExtra>) -> Result<IdleDecision> {
    Ok(IdleDecision::wait(IdleWaitStrategy::block()))
}

fn park_custom(_: &RuntimeShared<CustomExtra>, _: IdleWaitStrategy) -> Result<()> {
    Ok(())
}

fn main() {
    let _ = RuntimeBuilder::new()
        .with_idle_hook(idle_custom)
        .with_park_hook(park_custom);
    let _ = RuntimeBuilder::new()
        .with_park_hook(park_custom)
        .with_idle_hook(idle_custom);
    let _ = RuntimeBuilder::new()
        .with_park_hook(park_custom)
        .with_park_hook(park_custom);
    let _ = RuntimeBuilder::new()
        .with_idle_hook(idle_custom)
        .with_idle_hook(idle_custom);
    let _ = RuntimeBuilder::new().scope(async |_| ());
}
