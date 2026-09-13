#![cfg(feature = "test-hooks")]

//! 固定缓冲区注册失败时的严格/兼容模式回归测试。

use veloq_std::{
    env,
    fs::{File, OpenOptions, remove_file},
    io::{Read, Write},
    num::{NonZeroU32, NonZeroUsize},
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use veloq_buf::{
    BufPool, BufResult, BufferRegion, BufferRegistrar, FixedBuf, SlotBasedPool,
    heap::{ChunkId, ChunkInfo, GlobalAllocatorConfig, GlobalSlotPool},
};
use veloq_driver_core::{
    driver::{
        CompletionRecord, CompletionValue, DriveMode, Driver, DriverSubmitResult, OpToken,
        PollRecordResult, RegisterFd, SubmitStatus, test_hooks::DriverTestHooks,
    },
    op::{
        IntoPlatformOp,
        types::{
            ReadFixed as CoreReadFixed, ReadRaw as CoreReadRaw, WriteFixed as CoreWriteFixed,
            WriteRaw as CoreWriteRaw,
        },
    },
};
use veloq_driver_uring::{
    IoFd, RawHandle, UringConfig, UringDriver, UringError, UringOp, UringRawHandle, UringSlotSpec,
    UringUserPayload,
};

type ReadFixed = CoreReadFixed<UringRawHandle>;
type ReadRaw = CoreReadRaw<UringRawHandle>;
type WriteFixed = CoreWriteFixed<UringRawHandle>;
type WriteRaw = CoreWriteRaw<UringRawHandle>;

static TEMP_FILE_ID: AtomicUsize = AtomicUsize::new(0);

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

#[derive(Clone, Copy)]
struct ChunkRegistrar {
    info: ChunkInfo,
}

impl BufferRegistrar for ChunkRegistrar {
    fn register(&self, regions: &[BufferRegion]) -> BufResult<Vec<ChunkId>> {
        Ok(regions.iter().map(BufferRegion::id).collect())
    }

    fn resolve_chunk_info(&self, chunk_id: ChunkId) -> Option<ChunkInfo> {
        (chunk_id == self.info.id).then_some(self.info)
    }
}

fn new_driver_or_skip(
    mode: veloq_driver_uring::BufferRegistrationMode,
    registrar: &'static ChunkRegistrar,
) -> Option<UringDriver<'static>> {
    let config = UringConfig {
        entries: NonZeroU32::new(64).expect("non-zero ring entries"),
        registration_mode: mode,
        ..UringConfig::default()
    };
    match UringDriver::new(config, registrar) {
        Ok(driver) => Some(driver),
        Err(report) => {
            eprintln!("skipping fixed-buffer fallback test: {report}");
            None
        }
    }
}

fn slot_buffers(count: usize, len: usize) -> (Vec<FixedBuf>, &'static ChunkRegistrar) {
    let pool = Arc::new(
        GlobalSlotPool::new(GlobalAllocatorConfig {
            total_memory: 4 * 1024 * 1024,
        })
        .expect("create slot pool"),
    );
    let registrar = Box::leak(Box::new(ChunkRegistrar {
        info: pool.global_info(),
    }));
    let slot_pool = SlotBasedPool::new(pool);
    let buffers = (0..count)
        .map(|_| {
            slot_pool
                .alloc(NonZeroUsize::new(4096).expect("non-zero buffer size"), len)
                .expect("allocate slot buffer")
        })
        .collect();
    (buffers, registrar)
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

fn take_read_completion(
    driver: &mut UringDriver<'static>,
    token: OpToken,
    expected_variant: fn(&UringUserPayload) -> bool,
) -> (usize, FixedBuf) {
    let record = wait_completion(driver, token);
    let CompletionRecord {
        event,
        payload,
        mut cleanup,
        ..
    } = record;
    cleanup.disarm();
    assert!(
        expected_variant(&payload),
        "unexpected read payload variant"
    );
    let buf = match payload {
        UringUserPayload::ReadFixed(op) => op.buf,
        UringUserPayload::ReadRaw(op) => op.buf,
        other => panic!(
            "unexpected read payload: {:?}",
            core::mem::discriminant(&other)
        ),
    };
    let result = usize::from_event_res::<UringError>(event.res()).expect("read completion");
    (result, buf)
}

fn take_write_completion(
    driver: &mut UringDriver<'static>,
    token: OpToken,
    expected_variant: fn(&UringUserPayload) -> bool,
) -> usize {
    let record = wait_completion(driver, token);
    let CompletionRecord {
        event,
        payload,
        mut cleanup,
        ..
    } = record;
    cleanup.disarm();
    assert!(
        expected_variant(&payload),
        "unexpected write payload variant"
    );
    assert!(matches!(
        payload,
        UringUserPayload::WriteFixed(_) | UringUserPayload::WriteRaw(_)
    ));
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
fn strict_mode_rejects_registration_failure_and_recovers_payload() {
    let (mut buffers, registrar) = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("strict", b"strict-mode");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Strict,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if hooks.debug_fixed_buffers_available() {
            hooks.debug_inject_register_buffers_update_failure(libc::EIO);
        }
    }

    let (kernel, payload) = <ReadFixed as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(
        fixed_read(buffers.pop().expect("read buffer"), fd),
    );
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve operation");
    slot.set_payload(<ReadFixed as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), UringError::Registration),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("strict registration failure must be void, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("strict registration failure was submitted"),
    }

    assert!(matches!(
        slot.recover_payload(),
        Some(UringUserPayload::ReadFixed(_))
    ));
}

#[test]
fn compatible_mode_rejects_unknown_update_instead_of_falling_back() {
    let (mut buffers, registrar) = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("unknown-update", b"unknown-update");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Compatible,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        hooks.debug_inject_register_buffers_update_unknown(libc::EIO);
    }

    let (kernel, payload) = <ReadFixed as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(
        fixed_read(buffers.pop().expect("unknown-update buffer"), fd),
    );
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver
        .reserve_op()
        .expect("reserve unknown-update operation");
    slot.set_payload(<ReadFixed as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), UringError::Registration),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("unknown update must be void, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("unknown update must not be submitted"),
    }

    assert!(matches!(
        slot.recover_payload(),
        Some(UringUserPayload::ReadFixed(_))
    ));
    let hooks = &driver as &dyn DriverTestHooks;
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 1);
    assert_eq!(hooks.debug_raw_buffer_fallbacks(), 0);
}

#[test]
fn bitset_failure_clears_kernel_slot_before_returning_error() {
    let (mut buffers, registrar) = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("bitset-cleanup", b"bitset-cleanup");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Strict,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        // The first outcome is the registration syscall and the second is the cleanup syscall.
        hooks.debug_inject_register_buffers_update_sequence(&[None, None]);
        hooks.debug_inject_bitset_set_failure();
    }

    let (kernel, payload) = <ReadFixed as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(
        fixed_read(buffers.pop().expect("bitset test buffer"), fd),
    );
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve operation");
    slot.set_payload(<ReadFixed as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));

    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), UringError::InvalidState),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("bitset failure must be void, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("bitset failure was submitted"),
    }

    assert!(matches!(
        slot.recover_payload(),
        Some(UringUserPayload::ReadFixed(_))
    ));
    let hooks = &driver as &dyn DriverTestHooks;
    assert!(!hooks.debug_chunk_registered(0));
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 0);
}

#[test]
fn failed_bitset_cleanup_quarantines_the_fixed_buffer_registry() {
    let (mut buffers, registrar) = slot_buffers(2, 4096);
    let (_file_path, file) = open_file("bitset-quarantine", b"bitset-quarantine");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Strict,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        if !hooks.debug_fixed_buffers_available() {
            return;
        }
        // The cleanup failure leaves the kernel slot unknown, so all later buffer submissions must
        // fail until this ring is rebuilt.
        hooks.debug_inject_register_buffers_update_sequence(&[None]);
        hooks.debug_inject_register_buffers_update_unknown(libc::EIO);
        hooks.debug_inject_bitset_set_failure();
    }

    let (kernel, payload) = <ReadFixed as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(
        fixed_read(buffers.pop().expect("first quarantine buffer"), fd),
    );
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve first operation");
    slot.set_payload(<ReadFixed as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));
    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), UringError::InvalidState),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("quarantine trigger must be void, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("quarantine trigger was submitted"),
    }
    assert!(matches!(
        slot.recover_payload(),
        Some(UringUserPayload::ReadFixed(_))
    ));

    let (kernel, payload) = <ReadFixed as IntoPlatformOp<UringSlotSpec>>::into_kernel_and_payload(
        fixed_read(buffers.pop().expect("second quarantine buffer"), fd),
    );
    let mut kernel_op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve second operation");
    slot.set_payload(<ReadFixed as IntoPlatformOp<UringSlotSpec>>::payload_into_erased(payload));
    match slot.submit(&mut kernel_op) {
        DriverSubmitResult::Failed {
            report,
            status: SubmitStatus::Void,
        } => assert_eq!(*report.inner(), UringError::InvalidState),
        DriverSubmitResult::Failed { status, .. } => {
            panic!("quarantined registry must be void, got {status:?}")
        }
        DriverSubmitResult::Submitted(_) => panic!("quarantined registry submitted I/O"),
    }
    assert!(matches!(
        slot.recover_payload(),
        Some(UringUserPayload::ReadFixed(_))
    ));
    let hooks = &driver as &dyn DriverTestHooks;
    assert!(!hooks.debug_chunk_registered(0));
    assert_eq!(hooks.debug_chunk_register_attempts(), 1);
    assert_eq!(hooks.debug_chunk_register_failures(), 0);
}

#[test]
fn compatible_mode_falls_back_for_fixed_read_and_respects_cooldown() {
    let (mut buffers, registrar) = slot_buffers(2, 4096);
    let (_file_path, file) = open_file("compatible-read", b"compatible-read");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Compatible,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    let fixed_available = {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        let fixed_available = hooks.debug_fixed_buffers_available();
        if fixed_available {
            hooks.debug_inject_register_buffers_update_failure(libc::EIO);
        }
        fixed_available
    };

    let first = submit(&mut driver, fixed_read(buffers.pop().unwrap(), fd));
    let (read, read_buf) = take_read_completion(&mut driver, first, |payload| {
        matches!(payload, UringUserPayload::ReadFixed(_))
    });
    assert_eq!(read, b"compatible-read".len());
    assert_eq!(&read_buf.as_slice()[..read], b"compatible-read");

    let (attempts_after_first, failures_after_first, fallbacks_after_first, skipped_before_second) = {
        let hooks = &driver as &dyn DriverTestHooks;
        (
            hooks.debug_chunk_register_attempts(),
            hooks.debug_chunk_register_failures(),
            hooks.debug_raw_buffer_fallbacks(),
            hooks.debug_chunk_register_skipped_recent_failure(),
        )
    };

    let second = submit(&mut driver, fixed_read(buffers.pop().unwrap(), fd));
    let (read, read_buf) = take_read_completion(&mut driver, second, |payload| {
        matches!(payload, UringUserPayload::ReadFixed(_))
    });
    assert_eq!(read, b"compatible-read".len());
    assert_eq!(&read_buf.as_slice()[..read], b"compatible-read");

    let hooks = &driver as &dyn DriverTestHooks;
    if fixed_available {
        assert_eq!(attempts_after_first, 1);
        assert_eq!(failures_after_first, 1);
        assert_eq!(hooks.debug_chunk_register_attempts(), attempts_after_first);
        assert_eq!(hooks.debug_chunk_register_failures(), failures_after_first);
        assert!(
            hooks.debug_chunk_register_skipped_recent_failure() > skipped_before_second,
            "the second submission must use the registration cooldown"
        );
    } else {
        assert_eq!(attempts_after_first, 0);
        assert_eq!(failures_after_first, 0);
        assert_eq!(hooks.debug_chunk_register_attempts(), attempts_after_first);
    }
    assert!(hooks.debug_raw_buffer_fallbacks() > fallbacks_after_first);
}

#[test]
fn compatible_mode_falls_back_for_fixed_write() {
    let (mut buffers, registrar) = slot_buffers(1, b"compatible-write".len());
    let (_file_path, file) = open_file("compatible-write", &[]);
    buffers[0].as_slice_mut()[..b"compatible-write".len()].copy_from_slice(b"compatible-write");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Compatible,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    let hooks = &mut driver as &mut dyn DriverTestHooks;
    if hooks.debug_fixed_buffers_available() {
        hooks.debug_inject_register_buffers_update_failure(libc::EIO);
    }

    let token = submit(&mut driver, fixed_write(buffers.pop().unwrap(), fd));
    let written = take_write_completion(&mut driver, token, |payload| {
        matches!(payload, UringUserPayload::WriteFixed(_))
    });
    assert_eq!(written, b"compatible-write".len());

    let mut check = File::open(_file_path.path()).expect("reopen written file");
    let mut content = Vec::new();
    check.read_to_end(&mut content).expect("read written file");
    assert_eq!(content, b"compatible-write");
    let hooks = &driver as &dyn DriverTestHooks;
    assert_eq!(hooks.debug_raw_buffer_fallbacks(), 1);
}

#[test]
fn compatible_mode_backlog_retry_preserves_raw_fallback() {
    let (mut buffers, registrar) = slot_buffers(1, 4096);
    let (_file_path, file) = open_file("compatible-backlog", b"compatible-backlog");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Compatible,
        registrar,
    ) else {
        return;
    };
    let fd = register_file(&mut driver, &file);
    let fixed_available = {
        let hooks = &mut driver as &mut dyn DriverTestHooks;
        let fixed_available = hooks.debug_fixed_buffers_available();
        if fixed_available {
            hooks.debug_inject_register_buffers_update_failure(libc::EIO);
        }
        hooks.debug_inject_push_entry_failure();
        fixed_available
    };

    let token = submit(
        &mut driver,
        fixed_read(buffers.pop().expect("backlog read buffer"), fd),
    );
    let (read, read_buf) = take_read_completion(&mut driver, token, |payload| {
        matches!(payload, UringUserPayload::ReadFixed(_))
    });
    assert_eq!(read, b"compatible-backlog".len());
    assert_eq!(&read_buf.as_slice()[..read], b"compatible-backlog");

    let hooks = &driver as &dyn DriverTestHooks;
    assert!(
        hooks.debug_raw_buffer_fallbacks() >= 2,
        "initial submission and backlog retry must both preserve raw fallback"
    );
    if fixed_available {
        assert_eq!(hooks.debug_chunk_register_attempts(), 1);
        assert_eq!(hooks.debug_chunk_register_failures(), 1);
        assert!(hooks.debug_chunk_register_skipped_recent_failure() >= 1);
    } else {
        assert_eq!(hooks.debug_chunk_register_attempts(), 0);
    }
}

#[test]
fn compatible_mode_accepts_raw_read_and_write_entries() {
    let (mut buffers, registrar) = slot_buffers(2, b"raw-write".len());
    let (_file_path, file) = open_file("raw-entries", b"raw-read");
    buffers[0].as_slice_mut()[..b"raw-write".len()].copy_from_slice(b"raw-write");
    let Some(mut driver) = new_driver_or_skip(
        veloq_driver_uring::BufferRegistrationMode::Compatible,
        registrar,
    ) else {
        return;
    };
    let raw = UringRawHandle::for_file(file.as_raw_fd());
    let read = submit(
        &mut driver,
        ReadRaw {
            fd: raw,
            buf: buffers.pop().unwrap(),
            offset: 0,
            buf_offset: 0,
        },
    );
    let (read, read_buf) = take_read_completion(&mut driver, read, |payload| {
        matches!(payload, UringUserPayload::ReadRaw(_))
    });
    assert_eq!(read, b"raw-read".len());
    assert_eq!(&read_buf.as_slice()[..read], b"raw-read");

    let write = submit(
        &mut driver,
        WriteRaw {
            fd: raw,
            buf: buffers.pop().unwrap(),
            offset: 0,
            buf_offset: 0,
        },
    );
    let written = take_write_completion(&mut driver, write, |payload| {
        matches!(payload, UringUserPayload::WriteRaw(_))
    });
    assert_eq!(written, b"raw-write".len());
}
