use veloq_runtime::{
    error::Result,
    runtime::{
        context::{IdleDecision, IdleWaitStrategy},
        RuntimeBuilder, RuntimeShared,
    },
};

struct CustomExtra;

fn park_unit(_: &RuntimeShared<()>, _: IdleWaitStrategy) -> Result<()> {
    Ok(())
}

fn idle_custom(_: &RuntimeShared<CustomExtra>) -> Result<IdleDecision> {
    Ok(IdleDecision::wait(IdleWaitStrategy::block()))
}

fn main() {
    let _ = RuntimeBuilder::new()
        .with_park_hook(park_unit)
        .with_idle_hook(idle_custom);
}
