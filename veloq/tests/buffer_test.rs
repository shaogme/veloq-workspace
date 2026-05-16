use std::num::NonZeroUsize;

use veloq::config::BufferRegistrationMode;
use veloq::runtime::Runtime;
#[cfg(feature = "test-hooks")]
use veloq::runtime::context::RuntimeContext;
use veloq_buf::{BufPool, UniformSlot, heap::ThreadMemoryMultiplier, nz};

#[cfg(feature = "test-hooks")]
use veloq_driver_native::driver::test_hooks::DriverTestHooks;

fn build_runtime(worker_threads: usize, mode: BufferRegistrationMode) -> Runtime<UniformSlot> {
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(1))))
        .worker_count(NonZeroUsize::new(worker_threads).expect("worker_threads must be > 0"))
        .with_config(|c| c.iocp_registration_mode(mode).uring_registration_mode(mode))
        .build()
        .expect("failed to build runtime")
}

#[cfg(feature = "test-hooks")]
fn current_chunk_register_attempts(ctx: RuntimeContext) -> u64 {
    ctx.driver(|driver| {
        let hooks = driver as &dyn DriverTestHooks;
        hooks.debug_chunk_register_attempts()
    })
}

fn run_auto_expansion_single_worker(mode: BufferRegistrationMode) {
    let runtime = build_runtime(1, mode);
    runtime.block_on(async |ctx| {
        let pool = ctx.buf_pool();
        let alloc_size = nz!(1024 * 1024);

        let mut bufs = Vec::new();
        let mut expanded_chunk_id = None;
        for i in 0..16 {
            let buf = pool
                .alloc(alloc_size)
                .unwrap_or_else(|| panic!("allocation failed before expansion validation, i={i}"));
            let info = buf.resolve_region_info();
            assert_ne!(
                info.id,
                u16::MAX,
                "auto expansion should not fallback to heap buffer"
            );
            if info.id != 0 {
                expanded_chunk_id = Some(info.id);
            }
            bufs.push(buf);
            if expanded_chunk_id.is_some() {
                break;
            }
        }

        let expanded_id = expanded_chunk_id.expect("auto expansion did not produce a new chunk");

        // Ensure expanded chunk is actually usable.
        let mut post_expansion_count = 0usize;
        for _ in 0..8 {
            let buf = pool.alloc(alloc_size).expect("allocation failed");
            if buf.resolve_region_info().id == expanded_id {
                post_expansion_count += 1;
            }
            bufs.push(buf);
        }
        assert!(
            post_expansion_count > 0,
            "expanded chunk should be reusable for subsequent allocations"
        );
    });
}

fn run_expansion_immediate_registration_check(
    mode: BufferRegistrationMode,
    _should_immediate: bool,
) {
    let runtime = build_runtime(1, mode);
    runtime.block_on(async |ctx| {
        let pool = ctx.buf_pool();
        let alloc_size = nz!(1024 * 1024);

        #[cfg(feature = "test-hooks")]
        let before = current_chunk_register_attempts(ctx);

        let mut bufs = Vec::new();
        let mut expanded = false;
        for i in 0..16 {
            let buf = pool
                .alloc(alloc_size)
                .unwrap_or_else(|| panic!("allocation failed while triggering expansion, i={i}"));
            let info = buf.resolve_region_info();
            assert_ne!(info.id, u16::MAX, "expansion should not fallback to heap");
            if info.id != 0 {
                expanded = true;
            }
            bufs.push(buf);
            if expanded {
                break;
            }
        }
        assert!(expanded, "failed to trigger expansion");

        // Cross at least one executor budget boundary so check_for_memory_updates runs again.
        for _ in 0..128 {
            ctx.yield_now().await;
        }

        #[cfg(feature = "test-hooks")]
        {
            let after = current_chunk_register_attempts(ctx);
            if _should_immediate {
                assert!(
                    after > before,
                    "compatible mode should eagerly register new chunk after expansion: before={before}, after={after}"
                );
            } else {
                assert_eq!(
                    after, before,
                    "strict mode should not eagerly register after expansion without I/O submit: before={before}, after={after}"
                );
            }
        }
    });
}

fn run_auto_expansion_multithread(mode: BufferRegistrationMode) {
    let runtime = build_runtime(2, mode);
    runtime.block_on(async |ctx| {
        let pool = ctx.buf_pool();
        let alloc_size = nz!(1024 * 1024);

        let mut holding = Vec::new();
        let mut expanded_chunk_id = None;
        for i in 0..16 {
            let buf = pool
                .alloc(alloc_size)
                .unwrap_or_else(|| panic!("worker0 allocation failed, i={i}"));
            let info = buf.resolve_region_info();
            assert_ne!(
                info.id,
                u16::MAX,
                "expansion path should not fallback to heap"
            );
            if info.id != 0 {
                expanded_chunk_id = Some(info.id);
                holding.push(buf);
                break;
            }
            holding.push(buf);
        }
        let expanded_id = expanded_chunk_id.expect("worker0 did not trigger pool auto expansion");

        ctx.scope(async |s| {
            let mut handles = Vec::new();
            for _ in 0..4 {
                handles.push(s.spawn_boxed(async move {
                    ctx.yield_now().await;
                    let pool = ctx.buf_pool();
                    let buf = pool.alloc(alloc_size).expect("worker allocation failed");
                    buf.resolve_region_info().id
                }));
            }

            for h in handles {
                let chunk_id = h.await.expect("task failed");
                assert_ne!(chunk_id, u16::MAX, "worker should not fallback to heap");
                assert_ne!(chunk_id, 0, "worker should see expanded chunk");
                assert!(
                    chunk_id >= expanded_id,
                    "worker chunk_id should be on or after expanded chunk"
                );
            }
        })
        .await;
    });
}

#[test]
fn test_memory_auto_expansion_strict_mode() {
    run_auto_expansion_single_worker(BufferRegistrationMode::Strict);
}

#[test]
fn test_memory_auto_expansion_compatible_mode() {
    run_auto_expansion_single_worker(BufferRegistrationMode::Compatible);
}

#[test]
fn test_expansion_does_not_immediately_register_in_strict_mode() {
    run_expansion_immediate_registration_check(BufferRegistrationMode::Strict, false);
}

#[test]
fn test_expansion_immediately_registers_in_compatible_mode() {
    run_expansion_immediate_registration_check(BufferRegistrationMode::Compatible, true);
}

#[test]
fn test_multithreaded_auto_expansion_strict_mode() {
    run_auto_expansion_multithread(BufferRegistrationMode::Strict);
}

#[test]
fn test_multithreaded_auto_expansion_compatible_mode() {
    run_auto_expansion_multithread(BufferRegistrationMode::Compatible);
}
