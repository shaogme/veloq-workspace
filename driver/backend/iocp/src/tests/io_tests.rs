use crate::config::{IoFd, IocpConfig, IocpHandle, RawHandle};
use crate::driver::IocpDriver;
use crate::tests::{complete_from_record, submit_test_op, wait_completion, wait_completion_record};
use std::num::NonZeroUsize;
use std::os::windows::fs::OpenOptionsExt;
use std::os::windows::io::AsRawHandle;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use veloq_buf::{FixedBuf, NoopRegistrar};
use veloq_driver_core::driver::RegisterFd;
use veloq_driver_core::op::{ReadFixed, ReadRaw, Timeout, WriteFixed, WriteRaw};
use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_OVERLAPPED;

static NEXT_TEMP_FILE_ID: AtomicU64 = AtomicU64::new(0);

fn new_driver() -> IocpDriver<'static> {
    IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap()
}

fn temp_file_path(label: &str) -> PathBuf {
    let id = NEXT_TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "veloq-iocp-{label}-{}-{id}.tmp",
        std::process::id()
    ))
}

fn open_overlapped_temp_file(label: &str) -> (PathBuf, std::fs::File) {
    let path = temp_file_path(label);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .custom_flags(FILE_FLAG_OVERLAPPED)
        .open(&path)
        .expect("open overlapped temp file failed");
    (path, file)
}

fn remove_temp_file(path: PathBuf, file: std::fs::File) {
    drop(file);
    std::fs::remove_file(&path).unwrap_or_else(|err| {
        panic!("remove temp file {} failed: {err}", path.display());
    });
}

fn register_borrowed_file(driver: &mut IocpDriver<'_>, file: &std::fs::File) -> IoFd {
    let raw = RawHandle::new(IocpHandle::for_file(file.as_raw_handle() as _));
    driver
        .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
        .expect("register temp file failed")
        .into_iter()
        .next()
        .expect("register_files returned empty")
}

fn fixed_buf_from_bytes(bytes: &[u8]) -> FixedBuf {
    let mut buf = FixedBuf::alloc_heap(NonZeroUsize::new(bytes.len()).expect("non-empty buffer"))
        .expect("heap buffer allocation failed");
    buf.spare_capacity_mut()[..bytes.len()].copy_from_slice(bytes);
    buf.set_len(bytes.len());
    buf
}

fn fixed_buf(len: usize) -> FixedBuf {
    FixedBuf::alloc_heap(NonZeroUsize::new(len).expect("non-empty buffer"))
        .expect("heap buffer allocation failed")
}

fn submit_registered_write(driver: &mut IocpDriver<'_>, fd: IoFd, offset: u64, bytes: &[u8]) {
    let op = WriteFixed {
        fd,
        buf: fixed_buf_from_bytes(bytes),
        offset,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(driver, op);
    let written = wait_completion(driver, user_data, generation, Duration::from_secs(5))
        .expect("registered write completion failed");
    assert_eq!(written, bytes.len());
}

fn read_registered(driver: &mut IocpDriver<'_>, fd: IoFd, offset: u64, len: usize) -> Vec<u8> {
    let op = ReadFixed {
        fd,
        buf: fixed_buf(len),
        offset,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(driver, op);
    let record = wait_completion_record(driver, user_data, generation, Duration::from_secs(5))
        .expect("registered read completion missing");
    let completion = complete_from_record::<ReadFixed>(record);
    let (result, mut op) = completion.into_parts();
    let bytes = result.expect("registered read completion failed");
    op.buf.set_len(bytes);
    op.buf.as_slice().to_vec()
}

fn submit_raw_write(driver: &mut IocpDriver<'_>, handle: IocpHandle, offset: u64, bytes: &[u8]) {
    let op = WriteRaw {
        fd: handle,
        buf: fixed_buf_from_bytes(bytes),
        offset,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(driver, op);
    let written = wait_completion(driver, user_data, generation, Duration::from_secs(5))
        .expect("raw write completion failed");
    assert_eq!(written, bytes.len());
}

fn read_raw(driver: &mut IocpDriver<'_>, handle: IocpHandle, offset: u64, len: usize) -> Vec<u8> {
    let op = ReadRaw {
        fd: handle,
        buf: fixed_buf(len),
        offset,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(driver, op);
    let record = wait_completion_record(driver, user_data, generation, Duration::from_secs(5))
        .expect("raw read completion missing");
    let completion = complete_from_record::<ReadRaw<IocpHandle>>(record);
    let (result, mut op) = completion.into_parts();
    let bytes = result.expect("raw read completion failed");
    op.buf.set_len(bytes);
    op.buf.as_slice().to_vec()
}

#[test]
fn test_iocp_timeout() {
    let mut driver = new_driver();

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

#[test]
fn test_iocp_registered_file_read_write_respects_nonzero_offsets() {
    let mut driver = new_driver();
    let (path, file) = open_overlapped_temp_file("registered-offset");
    let fd = register_borrowed_file(&mut driver, &file);

    submit_registered_write(&mut driver, fd, 8, b"tail");
    submit_registered_write(&mut driver, fd, 0, b"head");

    assert_eq!(read_registered(&mut driver, fd, 8, 4), b"tail".to_vec());

    let mut expected = b"head".to_vec();
    expected.extend_from_slice(&[0; 4]);
    expected.extend_from_slice(b"tail");
    assert_eq!(
        read_registered(&mut driver, fd, 0, expected.len()),
        expected
    );

    driver.unregister_files(vec![fd]).unwrap();
    remove_temp_file(path, file);
}

#[test]
fn test_iocp_raw_file_read_write_respects_nonzero_offsets() {
    let mut driver = new_driver();
    let (path, file) = open_overlapped_temp_file("raw-offset");
    let handle = IocpHandle::for_file(file.as_raw_handle() as _);

    submit_raw_write(&mut driver, handle, 9, b"raw-tail");
    submit_raw_write(&mut driver, handle, 0, b"raw-a");

    assert_eq!(read_raw(&mut driver, handle, 9, 8), b"raw-tail".to_vec());

    let mut expected = b"raw-a".to_vec();
    expected.extend_from_slice(&[0; 4]);
    expected.extend_from_slice(b"raw-tail");
    assert_eq!(read_raw(&mut driver, handle, 0, expected.len()), expected);

    remove_temp_file(path, file);
}
