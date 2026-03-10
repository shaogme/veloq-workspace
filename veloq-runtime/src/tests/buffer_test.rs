use crate::config::{BufferRegistrationMode, Config, IocpConfig, UringConfig};
use crate::runtime::Runtime;
use veloq_buf::{BufPool, UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_driver::driver::test_hooks::DriverTestHooks;

fn build_runtime(worker_threads: usize, mode: BufferRegistrationMode) -> Runtime<UniformSlot> {
    let config = Config::default()
        .worker_threads(worker_threads)
        .uring(UringConfig::default().registration_mode(mode))
        .iocp(IocpConfig::default().registration_mode(mode));

    Runtime::builder()
        .config(config)
        .with_topology(UniformSlot::new(ThreadMemoryMultiplier(nz!(1))))
        .build()
        .expect("failed to build runtime")
}

fn current_chunk_register_attempts() -> u64 {
    let weak = crate::runtime::context::current().driver();
    let driver = weak.upgrade().expect("driver dropped unexpectedly");
    driver.borrow().debug_chunk_register_attempts()
}

fn run_auto_expansion_single_worker(mode: BufferRegistrationMode) {
    let runtime = build_runtime(1, mode);
    runtime.block_on(async move {
        let pool = crate::runtime::context::current_pool().expect("buffer pool not found");
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
            if let Some(buf) = pool.alloc(alloc_size)
                && buf.resolve_region_info().id == expanded_id
            {
                post_expansion_count += 1;
                bufs.push(buf);
            }
        }
        assert!(
            post_expansion_count > 0,
            "expanded chunk should be reusable for subsequent allocations"
        );
    });
}

fn run_expansion_immediate_registration_check(
    mode: BufferRegistrationMode,
    should_immediate: bool,
) {
    let runtime = build_runtime(1, mode);
    runtime.block_on(async move {
        let pool = crate::runtime::context::current_pool().expect("buffer pool not found");
        let alloc_size = nz!(1024 * 1024);
        let before = current_chunk_register_attempts();

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
            crate::runtime::context::yield_now().await;
        }

        let after = current_chunk_register_attempts();
        if should_immediate {
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
    });
}

fn run_auto_expansion_multithread(mode: BufferRegistrationMode) {
    let runtime = build_runtime(2, mode);
    runtime.block_on(async move {
        let pool = crate::runtime::context::current_pool().expect("no pool");
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

        let handles: Vec<_> = (0..4)
            .map(|_| {
                crate::runtime::context::spawn(async move {
                    crate::runtime::context::yield_now().await;
                    let pool = crate::runtime::context::current_pool().expect("pool missing");
                    let buf = pool.alloc(alloc_size).expect("worker allocation failed");
                    buf.resolve_region_info().id
                })
            })
            .collect();

        for h in handles {
            let chunk_id = h.await;
            assert_ne!(chunk_id, u16::MAX, "worker should not fallback to heap");
            assert_ne!(chunk_id, 0, "worker should see expanded chunk");
            assert!(
                chunk_id >= expanded_id,
                "worker chunk_id should be on or after expanded chunk"
            );
        }
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
