#![cfg(feature = "test-hooks")]
#![cfg(not(feature = "loom"))]
#![cfg(any(target_os = "linux", target_os = "android"))]

//! 固定缓冲区启动注册与未注册 I/O 回退测试。
//! Startup registration and unregistered-I/O fallback tests for fixed buffers.

use veloq_std::{
    env,
    fs::{File, OpenOptions, remove_file},
    io::{Read, Write},
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{NativeAtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use veloq_buf::{
    BufPool, BufferRegion, FixedBuf, LockedMemoryBudget, SlotBasedPool,
    heap::{GlobalAllocatorConfig, GlobalSlotPool},
};
use veloq_driver_core::DriverResult;
use veloq_driver_core::{
    BufferRegistrationStatus, CompletionRecord, CompletionRecordParts, CompletionValue, DriveMode,
    Driver, DriverSubmitResult, IntoPlatformOp, OpToken, PollRecordResult,
    ReadFixed as CoreReadFixed, ReadRaw as CoreReadRaw, RegisterFd, SlotError,
    WriteFixed as CoreWriteFixed, WriteRaw as CoreWriteRaw, test_hooks::DriverTestHooks,
};
use veloq_driver_uring::{
    IoFd, RawHandle, UringConfig, UringDriveLimits, UringDriver, UringError, UringOp,
    UringRawHandle, UringSlotSpec,
};

type ReadFixed = CoreReadFixed<UringRawHandle>;
type ReadRaw = CoreReadRaw<UringRawHandle>;
type WriteFixed = CoreWriteFixed<UringRawHandle>;
type WriteRaw = CoreWriteRaw<UringRawHandle>;

static TEMP_FILE_ID: NativeAtomicUsize = NativeAtomicUsize::new(0);

struct CleanupFile(PathBuf);

impl CleanupFile {
    fn new(label: &str) -> Self {
        let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
        let path = env::temp_dir().join(format!(
            "veloq-uring-buffer-{label}-{}-{id}.tmp",
            process::id()
        ));
        let _ = remove_file(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for CleanupFile {
    fn drop(&mut self) {
        let _ = remove_file(&self.0);
    }
}

fn new_driver_or_skip(budget: LockedMemoryBudget) -> Option<UringDriver<'static>> {
    let config = UringConfig::default()
        .with_entries(NonZeroU32::new(64).expect("non-zero ring entries"))
        .drive_limits(UringDriveLimits::for_entries(64))
        .locked_memory_budget(budget);
    match UringDriver::new(config) {
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping fixed-buffer registration test: {report}");
            None
        }
    }
}

fn slot_backing_pool() -> &'static Arc<GlobalSlotPool> {
    static POOL: veloq_std::sync::OnceLock<Arc<GlobalSlotPool>> = veloq_std::sync::OnceLock::new();
    POOL.get_or_init(|| {
        Arc::new(
            GlobalSlotPool::new(GlobalAllocatorConfig {
                total_memory: 4 * 1024 * 1024,
            })
            .expect("create slot pool"),
        )
    })
}

fn slot_buffers(count: usize, len: usize) -> Vec<FixedBuf> {
    let pool = slot_backing_pool().clone();
    let registered_chunks = pool.chunk_infos().into_iter().map(|info| info.id).collect();
    let slot_pool = SlotBasedPool::with_registered_chunks(pool, 0, registered_chunks)
        .expect("registered chunks belong to the pool");
    (0..count)
        .map(|_| {
            slot_pool
                .alloc(NonZeroUsize::new(4096).expect("non-zero buffer size"), len)
                .expect("allocate slot buffer")
        })
        .collect()
}

fn open_file(label: &str, initial: &[u8]) -> (CleanupFile, File) {
    let cleanup = CleanupFile::new(label);
    let mut file = File::create(cleanup.path()).expect("create test file");
    file.write_all(initial).expect("write initial test data");
    drop(file);
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(cleanup.path())
        .expect("reopen test file");
    (cleanup, file)
}

fn register_file(driver: &mut UringDriver<'static>, file: &File) -> IoFd {
    let raw = RawHandle::new(UringRawHandle::for_file(file.as_raw_fd()));
    driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .expect("register test file")
        .into_iter()
        .next()
        .expect("registered file handle")
}

fn register_chunk(
    driver: &mut UringDriver<'static>,
    buffer: &FixedBuf,
) -> DriverResult<BufferRegistrationStatus, SlotError<UringSlotSpec>> {
    let info = buffer.resolve_region_info();
    let pool = slot_backing_pool();
    let chunk_info = pool
        .chunk_info(info.id)
        .expect("slot chunk belongs to pool");
    // SAFETY: `chunk_info` comes from `pool`, which is retained by the registration region. / 安全性：`chunk_info` 来自 `pool`，注册区域会保留该 pool。
    let mut region = unsafe { BufferRegion::from_chunk_info(chunk_info, pool.clone()) };
    driver.register_buffer(region.registration_request())
}

fn submit<T>(driver: &mut UringDriver<'static>, op: T) -> OpToken
where
    T: IntoPlatformOp<UringSlotSpec>,
{
    let (kernel, payload) = T::into_kernel_and_payload(op);
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve operation");
    slot.set_payload(T::payload_into_erased(payload));
    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Submitted(_) => {
            let token = slot.persist().token();
            driver.completion_table().mark_waiting(token);
            token
        }
        DriverSubmitResult::Failed { report, status } => {
            panic!("operation submission failed: status={status:?}, error={report}")
        }
    }
}

fn wait_completion(
    driver: &mut UringDriver<'static>,
    token: OpToken,
) -> CompletionRecord<UringSlotSpec> {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "uring operation timed out");
        driver.drive(DriveMode::Poll).expect("drive operation");
        match driver.completion_table().try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => return record,
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion record unavailable: {kind:?}")
            }
            PollRecordResult::Pending => std::thread::sleep(Duration::from_millis(2)),
        }
    }
}

fn take_read_completion<T>(
    driver: &mut UringDriver<'static>,
    token: OpToken,
    take_buffer: fn(T) -> FixedBuf,
) -> (usize, FixedBuf)
where
    T: IntoPlatformOp<UringSlotSpec, SubmitPayload = T, RecordPayload = T, Output = T>,
{
    let record = wait_completion(driver, token);
    let CompletionRecordParts {
        event,
        payload,
        mut cleanup,
        ..
    } = record.into_parts();
    cleanup.disarm();
    let payload = T::try_record_from_erased(payload.expect("read completion payload"))
        .expect("unexpected read payload type");
    let buffer = take_buffer(payload);
    let result = usize::from_event_res::<UringError>(event.res()).expect("read completion");
    (result, buffer)
}

fn take_write_completion<T>(driver: &mut UringDriver<'static>, token: OpToken) -> usize
where
    T: IntoPlatformOp<UringSlotSpec, SubmitPayload = T, RecordPayload = T, Output = T>,
{
    let record = wait_completion(driver, token);
    let CompletionRecordParts {
        event,
        payload,
        mut cleanup,
        ..
    } = record.into_parts();
    cleanup.disarm();
    let _payload = T::try_record_from_erased(payload.expect("write completion payload"))
        .expect("unexpected write payload type");
    usize::from_event_res::<UringError>(event.res()).expect("write completion")
}

fn fixed_read(buf: FixedBuf, fd: IoFd) -> ReadFixed {
    ReadFixed {
        fd,
        buf,
        offset: 0,
        buf_offset: 0,
    }
}

fn fixed_write(buf: FixedBuf, fd: IoFd) -> WriteFixed {
    WriteFixed {
        fd,
        buf,
        offset: 0,
        buf_offset: 0,
    }
}

#[test]
fn fixed_buffer_registration_failure_propagates() {
    let mut buffers = slot_buffers(1, 4096);
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::default()) else {
        return;
    };
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        hooks.debug_inject_register_buffers_update_failure(libc::EIO);
    }

    let buffer = buffers.pop().expect("registration buffer");
    let report = register_chunk(&mut driver, &buffer).expect_err("buffer registration must fail");
    assert_eq!(*report.inner(), UringError::Registration);
}

#[test]
fn unknown_startup_registration_quarantines_the_ledger() {
    let mut buffers = slot_buffers(1, 4096);
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::default()) else {
        return;
    };
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        hooks.debug_inject_register_buffers_update_unknown(libc::EIO);
    }

    let buffer = buffers.pop().expect("registration buffer");
    let report = register_chunk(&mut driver, &buffer).expect_err("unknown result must fail");
    assert_eq!(*report.inner(), UringError::Registration);

    let report = register_chunk(&mut driver, &buffer)
        .expect_err("quarantined registration must reject another update");
    assert_eq!(*report.inner(), UringError::InvalidState);
    let hooks = &driver as &dyn DriverTestHooks;
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 1);
}

#[test]
fn bitset_failure_clears_kernel_slot_during_startup_registration() {
    let mut buffers = slot_buffers(1, 4096);
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::default()) else {
        return;
    };
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        hooks.debug_inject_register_buffers_update_sequence(&[None, None]);
        hooks.debug_inject_bitset_set_failure();
    }

    let buffer = buffers.pop().expect("registration buffer");
    let report = register_chunk(&mut driver, &buffer).expect_err("injected bitset error");
    assert_eq!(*report.inner(), UringError::InvalidState);
    let hooks = &driver as &dyn DriverTestHooks;
    assert!(!hooks.debug_chunk_registered(0));
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 0);
}

#[test]
fn failed_startup_cleanup_quarantines_the_fixed_buffer_registry() {
    let mut buffers = slot_buffers(2, 4096);
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::default()) else {
        return;
    };
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        hooks.debug_inject_register_buffers_update_sequence(&[None]);
        hooks.debug_inject_register_buffers_update_unknown(libc::EIO);
        hooks.debug_inject_bitset_set_failure();
    }

    let first = buffers.pop().expect("first registration buffer");
    let report = register_chunk(&mut driver, &first).expect_err("cleanup failure must fail");
    assert_eq!(*report.inner(), UringError::InvalidState);

    let second = buffers.pop().expect("second registration buffer");
    let report = register_chunk(&mut driver, &second)
        .expect_err("quarantined registry must reject later registration");
    assert_eq!(*report.inner(), UringError::InvalidState);
    let hooks = &driver as &dyn DriverTestHooks;
    assert!(!hooks.debug_chunk_registered(0));
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 0);
}

#[test]
fn budget_exhaustion_keeps_fixed_read_on_raw_io() {
    let mut buffers = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("budget-read", b"budget-read");
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::new(0)) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    let buffer = buffers.pop().expect("registration buffer");
    let status =
        register_chunk(&mut driver, &buffer).expect("budget exhaustion must skip registration");
    assert!(matches!(status, BufferRegistrationStatus::BudgetExceeded));
    let attempts_after_startup = (&driver as &dyn DriverTestHooks).debug_chunk_register_attempts();

    let raw_buf = FixedBuf::alloc_heap_full(NonZeroUsize::new(4096).unwrap())
        .expect("allocate unregistered fallback buffer");
    let token = submit(&mut driver, fixed_read(raw_buf, fd));
    let (read, read_buf) =
        take_read_completion::<ReadFixed>(&mut driver, token, |payload| payload.buf);
    assert_eq!(read, b"budget-read".len());
    assert_eq!(&read_buf.as_slice()[..read], b"budget-read");
    assert_eq!(
        (&driver as &dyn DriverTestHooks).debug_chunk_register_attempts(),
        attempts_after_startup,
        "submission must not register a buffer lazily"
    );
}

#[test]
fn budget_exhaustion_keeps_fixed_write_on_raw_io() {
    let mut buffers = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("budget-write", &[]);
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::new(0)) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    let buffer = buffers.pop().expect("registration buffer");
    let status =
        register_chunk(&mut driver, &buffer).expect("budget exhaustion must skip registration");
    assert!(matches!(status, BufferRegistrationStatus::BudgetExceeded));

    let mut raw_buf = FixedBuf::alloc_heap(NonZeroUsize::new(4096).unwrap(), b"budget-write".len())
        .expect("allocate raw write buffer");
    raw_buf.spare_capacity_mut()[..b"budget-write".len()].copy_from_slice(b"budget-write");
    let token = submit(&mut driver, fixed_write(raw_buf, fd));
    let written = take_write_completion::<WriteFixed>(&mut driver, token);
    assert_eq!(written, b"budget-write".len());

    let mut check = File::open(_file_path.path()).expect("reopen written file");
    let mut content = Vec::new();
    check.read_to_end(&mut content).expect("read written file");
    assert_eq!(content, b"budget-write");
}

#[test]
fn unregistered_fixed_read_survives_backlog_retry() {
    let (_file_path, file) = open_file("budget-backlog", b"budget-backlog");
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::new(0)) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    (&mut driver as &mut dyn DriverTestHooks).debug_inject_push_entry_failure();

    let raw_buf = FixedBuf::alloc_heap_full(NonZeroUsize::new(4096).unwrap())
        .expect("allocate raw read buffer");
    let token = submit(&mut driver, fixed_read(raw_buf, fd));
    let events = (&mut driver as &mut dyn DriverTestHooks).debug_control_plane_events();
    assert!(
        !events
            .iter()
            .any(|event| event.contains("CleanupHintInsert")),
        "a Full stage must roll back cleanup metadata and its observer event"
    );
    let (read, read_buf) =
        take_read_completion::<ReadFixed>(&mut driver, token, |payload| payload.buf);
    assert_eq!(read, b"budget-backlog".len());
    assert_eq!(&read_buf.as_slice()[..read], b"budget-backlog");
}

#[test]
fn budget_exhaustion_accepts_unregistered_raw_read_and_write_entries() {
    let (_file_path, file) = open_file("raw-entries", b"raw-read");
    let Some(mut driver) = new_driver_or_skip(LockedMemoryBudget::new(0)) else {
        return;
    };
    let raw = UringRawHandle::for_file(file.as_raw_fd());
    let read_buf = FixedBuf::alloc_heap_full(NonZeroUsize::new(4096).unwrap())
        .expect("allocate raw read buffer");
    let token = submit(
        &mut driver,
        ReadRaw {
            fd: raw,
            buf: read_buf,
            offset: 0,
            buf_offset: 0,
        },
    );
    let (read, read_buf) =
        take_read_completion::<ReadRaw>(&mut driver, token, |payload| payload.buf);
    assert_eq!(read, b"raw-read".len());
    assert_eq!(&read_buf.as_slice()[..read], b"raw-read");

    let mut write_buf = FixedBuf::alloc_heap(NonZeroUsize::new(4096).unwrap(), b"raw-write".len())
        .expect("allocate raw write buffer");
    write_buf.spare_capacity_mut()[..b"raw-write".len()].copy_from_slice(b"raw-write");
    let token = submit(
        &mut driver,
        WriteRaw {
            fd: raw,
            buf: write_buf,
            offset: 0,
            buf_offset: 0,
        },
    );
    let written = take_write_completion::<WriteRaw>(&mut driver, token);
    assert_eq!(written, b"raw-write".len());
}
