#[cfg(feature = "test-hooks")]
use veloq::runtime::context::Ctx;

use std::sync::Mutex;
use veloq::std::sync::Arc;
use veloq::{nz, runtime::Runtime, runtime::scope};
use veloq_buf::{
    AnyBufPool, BufPool, BufResult, BufferRegistrar, PoolInitContext, PoolKind, PoolTopology,
    RegionInfo, RegistrationCapability, UniformPoolState, UniformSlot,
};

#[cfg(feature = "test-hooks")]
use veloq_driver_native::driver::test_hooks::DriverTestHooks;

fn assert_registered_slot(info: RegionInfo, message: &str) {
    assert_eq!(info.pool_kind, PoolKind::SlotBased, "{message}");
    assert!(matches!(
        info.registration,
        RegistrationCapability::RegisteredChunk(_)
    ));
}

fn assert_unregistered_heap(info: RegionInfo, message: &str) {
    assert_eq!(info.pool_kind, PoolKind::Heap, "{message}");
    assert_eq!(
        info.registration,
        RegistrationCapability::Unregistered,
        "{message}"
    );
}

#[derive(Clone)]
struct RegistrationWorkerRecorder {
    workers: Arc<Mutex<Vec<usize>>>,
}

impl PoolTopology for RegistrationWorkerRecorder {
    type State = Arc<UniformPoolState>;

    fn init(&self, context: PoolInitContext) -> BufResult<Self::State> {
        *self.workers.lock().expect("worker recorder lock") =
            context.registration_workers().to_vec();
        UniformSlot::new(0).init(context)
    }

    fn build(
        &self,
        state: &Self::State,
        worker_idx: usize,
        registrar: &dyn BufferRegistrar,
    ) -> BufResult<AnyBufPool> {
        UniformSlot::new(0).build(state, worker_idx, registrar)
    }
}

fn recorded_registration_workers(selection: Option<Vec<usize>>) -> Vec<usize> {
    let workers = Arc::new(Mutex::new(Vec::new()));
    let topology = RegistrationWorkerRecorder {
        workers: workers.clone(),
    };
    let mut builder = Runtime::builder(topology).worker_count(Some(nz!(3)));
    if let Some(selection) = selection {
        builder = builder.buffer_registration_workers(selection);
    }
    builder
        .scope(async |_| {})
        .expect("runtime should initialize with the requested registration workers");
    workers.lock().expect("worker recorder lock").clone()
}

#[cfg(feature = "test-hooks")]
fn current_chunk_register_attempts(ctx: Ctx<'_>) -> u64 {
    ctx.driver(|driver| {
        let hooks = &driver as &dyn DriverTestHooks;
        hooks.debug_chunk_register_attempts()
    })
}

fn run_fixed_capacity_fallback() {
    let builder = Runtime::builder(UniformSlot::new(4 * 1024 * 1024))
        .worker_count(Some(nz!(1)))
        .buffer_registration_workers(vec![0]);
    builder
        .scope(async |ctx| {
            let pool = ctx.buf_pool();
            let mut registered = Vec::new();
            let mut fallback = None;
            for _ in 0..16 {
                let buf = pool
                    .alloc_full(nz!(1024 * 1024))
                    .expect("pool or heap allocation should succeed");
                let info = buf.resolve_region_info();
                if info.pool_kind == PoolKind::Heap {
                    assert_unregistered_heap(
                        info,
                        "pool exhaustion should not create or register another chunk",
                    );
                    fallback = Some(buf);
                    break;
                }
                assert_registered_slot(info, "slot allocation must come from a registered chunk");
                registered.push(buf);
            }
            assert!(
                fallback.is_some(),
                "fixed capacity should eventually be exhausted"
            );
        })
        .unwrap();
}

fn run_no_lazy_registration_check() {
    let builder = Runtime::builder(UniformSlot::new(4 * 1024 * 1024))
        .worker_count(Some(nz!(1)))
        .buffer_registration_workers(vec![0]);
    builder
        .scope(async |ctx| {
            #[cfg(feature = "test-hooks")]
            let before = current_chunk_register_attempts(ctx);
            let pool = ctx.buf_pool();
            let mut registered = Vec::new();
            let mut fallback = None;
            for _ in 0..16 {
                let buf = pool
                    .alloc_full(nz!(1024 * 1024))
                    .expect("pool or heap allocation should succeed");
                let info = buf.resolve_region_info();
                if info.pool_kind == PoolKind::Heap {
                    assert_unregistered_heap(
                        info,
                        "heap fallback must carry explicit unregistered capability",
                    );
                    fallback = Some(buf);
                    break;
                }
                assert_registered_slot(info, "slot allocation must come from a registered chunk");
                registered.push(buf);
            }
            assert!(
                fallback.is_some(),
                "fixed capacity should eventually be exhausted"
            );

            for _ in 0..128 {
                ctx.yield_now().await;
            }

            #[cfg(feature = "test-hooks")]
            {
                let after = current_chunk_register_attempts(ctx);
                assert_eq!(
                    after, before,
                    "pool exhaustion must not register a new chunk: before={before}, after={after}"
                );
            }
        })
        .unwrap();
}

fn run_multithreaded_fixed_capacity_fallback() {
    let builder = Runtime::builder(UniformSlot::new(4 * 1024 * 1024))
        .worker_count(Some(nz!(2)))
        .buffer_registration_workers(vec![0, 1]);
    builder
        .scope(async |ctx| {
            let pool = ctx.buf_pool();
            let mut registered = Vec::new();
            let mut fallback = None;
            for _ in 0..16 {
                let buf = pool
                    .alloc_full(nz!(1024 * 1024))
                    .expect("pool or heap allocation should succeed");
                let info = buf.resolve_region_info();
                if info.pool_kind == PoolKind::Heap {
                    assert_unregistered_heap(
                        info,
                        "shared pool exhaustion should use unregistered heap memory",
                    );
                    fallback = Some(buf);
                    break;
                }
                assert_registered_slot(info, "slot allocation must come from a registered chunk");
                registered.push(buf);
            }
            assert!(
                fallback.is_some(),
                "fixed shared capacity should be exhausted"
            );

            scope!(ctx, async |scope| {
                let mut handles = Vec::new();
                for _ in 0..4 {
                    handles.push(scope.spawn_boxed(async move {
                        ctx.yield_now().await;
                        let buf = ctx
                            .buf_pool()
                            .alloc_full(nz!(4096))
                            .expect("worker heap fallback allocation");
                        let info = buf.resolve_region_info();
                        (info.pool_kind, info.registration)
                    }));
                }

                for handle in handles {
                    let (pool_kind, registration) = handle.await.expect("worker task failed");
                    match pool_kind {
                        PoolKind::SlotBased => assert!(matches!(
                            registration,
                            RegistrationCapability::RegisteredChunk(_)
                        )),
                        PoolKind::Heap => {
                            assert_eq!(registration, RegistrationCapability::Unregistered)
                        }
                    }
                }
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn test_fixed_capacity_falls_back_to_heap() {
    run_fixed_capacity_fallback();
}

#[test]
fn test_pool_exhaustion_does_not_register_lazily() {
    run_no_lazy_registration_check();
}

#[test]
fn test_multithreaded_fixed_capacity_fallback() {
    run_multithreaded_fixed_capacity_fallback();
}

#[test]
fn runtime_defaults_to_all_buffer_registration_workers() {
    assert_eq!(recorded_registration_workers(None), vec![0, 1, 2]);
}

#[test]
fn runtime_accepts_no_buffer_registration_workers() {
    assert!(recorded_registration_workers(Some(Vec::new())).is_empty());
}

#[test]
fn runtime_accepts_a_buffer_registration_worker_subset() {
    assert_eq!(recorded_registration_workers(Some(vec![2])), vec![2]);
}
