#![cfg(feature = "std")]

use core::sync::atomic::{AtomicUsize, Ordering};

use veloq_std::{
    ffi::OsStr,
    fs::{File, FileTimes, OpenOptions, read, read_to_string, remove_file, write},
    io::{ErrorKind, IoSlice, IoSliceMut, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    time::Duration,
};

#[cfg(unix)]
use veloq_std::os::unix::{
    fd::{AsFd, AsRawFd, FromRawFd, IntoRawFd},
    fs::{
        FileExt as UnixFileExt, MetadataExt as UnixMetadataExt,
        OpenOptionsExt as UnixOpenOptionsExt, PermissionsExt as UnixPermissionsExt,
    },
};

#[cfg(windows)]
use veloq_std::os::windows::{
    fs::{
        FileExt as WinFileExt, MetadataExt as WinMetadataExt, OpenOptionsExt as WinOpenOptionsExt,
    },
    io::{AsHandle, AsRawHandle, FromRawHandle, IntoRawHandle},
};

#[cfg(feature = "std")]
use std::{env, process::id};

static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

struct TempFileGuard {
    path: PathBuf,
}

impl TempFileGuard {
    fn new(name: &str) -> Self {
        let count = TEST_COUNTER.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "std")]
        let base = env::temp_dir().to_str().unwrap().to_string();
        #[cfg(not(feature = "std"))]
        let base = String::from("target");

        let mut path = PathBuf::from(base.as_str());
        let filename = format!("veloq_test_{}_{}_{}.tmp", id(), name, count);
        path.push(filename.as_str());

        // Remove if exists
        let _ = remove_file(path.as_path());

        Self { path }
    }

    fn path(&self) -> &Path {
        self.path.as_path()
    }
}

impl Drop for TempFileGuard {
    fn drop(&mut self) {
        let _ = remove_file(self.path.as_path());
    }
}

#[test]
fn test_path_operations() {
    let p = Path::new("foo/bar/baz.txt");
    assert_eq!(p.file_name(), Some(OsStr::new("baz.txt")));
    assert_eq!(p.extension(), Some(OsStr::new("txt")));
    assert_eq!(p.parent(), Some(Path::new("foo/bar")));

    let mut pb = PathBuf::from("foo");
    pb.push("bar");
    pb.push("baz.txt");
    assert_eq!(pb.file_name(), Some(OsStr::new("baz.txt")));
    assert!(pb.pop());
    assert_eq!(pb.file_name(), Some(OsStr::new("bar")));

    #[cfg(not(windows))]
    let root = Path::new("/a/b/c");
    #[cfg(windows)]
    let root = Path::new(r"C:\a\b\c");
    assert!(root.is_absolute());
    assert!(!p.is_absolute());
}

#[test]
fn test_file_create_write_read() {
    let guard = TempFileGuard::new("create_write_read");
    let path = guard.path();

    let data = b"Hello, veloq-std file system!";
    {
        let mut file = File::create(path).expect("failed to create file");
        file.write_all(data).expect("failed to write data");
        file.flush().expect("failed to flush");
    }

    {
        let mut file = File::open(path).expect("failed to open file");
        let mut read_buf = Vec::new();
        file.read_to_end(&mut read_buf)
            .expect("failed to read data");
        assert_eq!(read_buf.as_slice(), data);
    }
}

#[test]
fn test_file_seek() {
    let guard = TempFileGuard::new("seek");
    let path = guard.path();

    let data = b"0123456789ABCDEF";
    {
        let mut file = File::create(path).expect("failed to create file");
        file.write_all(data).expect("failed to write data");
    }

    let mut file = File::open(path).expect("failed to open file");

    let pos = file.seek(SeekFrom::Start(10)).expect("failed to seek");
    assert_eq!(pos, 10);

    let mut buf = [0u8; 6];
    file.read_exact(&mut buf).expect("failed to read exact");
    assert_eq!(&buf, b"ABCDEF");

    let pos = file.stream_position().expect("failed to get position");
    assert_eq!(pos, 16);

    file.rewind().expect("failed to rewind");
    assert_eq!(file.stream_position().unwrap(), 0);
}

#[test]
fn test_file_vectored_io() {
    let guard = TempFileGuard::new("vectored_io");
    let path = guard.path();

    let s1 = b"Veloq ";
    let s2 = b"Vectored ";
    let s3 = b"I/O";

    {
        let mut file = File::create(path).expect("failed to create file");
        let slices = [IoSlice::new(s1), IoSlice::new(s2), IoSlice::new(s3)];
        let total = file.write_vectored(&slices).expect("write_vectored failed");
        assert!(total > 0);
        file.flush().expect("flush failed");
    }

    {
        let mut file = File::open(path).expect("failed to open file");
        let mut b1 = [0u8; 6];
        let mut b2 = [0u8; 9];
        let mut b3 = [0u8; 3];
        let mut slices_mut = [
            IoSliceMut::new(&mut b1),
            IoSliceMut::new(&mut b2),
            IoSliceMut::new(&mut b3),
        ];
        let total = file
            .read_vectored(&mut slices_mut)
            .expect("read_vectored failed");
        assert!(total > 0);
    }
}

#[test]
fn test_file_set_len_and_metadata() {
    let guard = TempFileGuard::new("set_len");
    let path = guard.path();

    let file = File::create(path).expect("failed to create file");
    file.set_len(1024).expect("failed to set_len");

    let metadata = file.metadata().expect("failed to query metadata");
    assert_eq!(metadata.len(), 1024);
    assert!(!metadata.is_empty());
    assert!(metadata.is_file());
    assert!(!metadata.is_dir());

    file.set_len(0).expect("failed to truncate to 0");
    let metadata = file.metadata().expect("failed to query metadata");
    assert_eq!(metadata.len(), 0);
    assert!(metadata.is_empty());
}

#[test]
fn test_file_try_clone() {
    let guard = TempFileGuard::new("try_clone");
    let path = guard.path();

    let mut writer = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .expect("failed to open file for read and write");
    let mut cloner = writer.try_clone().expect("failed to clone file");

    writer.write_all(b"cloned hello").expect("write failed");
    writer.sync_all().expect("sync failed");

    cloner.rewind().expect("rewind failed");
    let mut read_buf = Vec::new();
    cloner.read_to_end(&mut read_buf).expect("read failed");
    assert_eq!(read_buf.as_slice(), b"cloned hello");
}

#[test]
fn test_file_locks() {
    let guard = TempFileGuard::new("locks");
    let path = guard.path();

    let file = File::create(path).expect("failed to create file");
    file.lock().expect("exclusive lock failed");
    file.unlock().expect("unlock failed");

    file.lock_shared().expect("shared lock failed");
    file.unlock().expect("unlock failed");

    file.try_lock().expect("try_lock failed");
    file.unlock().expect("unlock failed");

    file.try_lock_shared().expect("try_lock_shared failed");
    file.unlock().expect("unlock failed");
}

#[test]
fn test_convenience_functions() {
    let guard = TempFileGuard::new("convenience");
    let path = guard.path();

    write(path, "convenience test content").expect("write failed");

    let text = read_to_string(path).expect("read_to_string failed");
    assert_eq!(text, "convenience test content");

    let bytes = read(path).expect("read failed");
    assert_eq!(bytes, b"convenience test content");
}

#[test]
fn test_open_options_create_new() {
    let guard = TempFileGuard::new("create_new");
    let path = guard.path();

    let file = File::create_new(path).expect("first create_new should succeed");
    drop(file);

    let err = File::create_new(path);
    assert!(err.is_err());
    assert_eq!(err.err().unwrap().kind(), ErrorKind::AlreadyExists);
}

#[cfg(unix)]
#[test]
fn test_unix_file_ext() {
    let guard = TempFileGuard::new("unix_ext");
    let path = guard.path();

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .mode(0o644)
        .open(path)
        .expect("open with mode failed");

    let wrote = file.write_at(b"MIDDLE", 10).expect("write_at failed");
    assert_eq!(wrote, 6);

    let mut buf = [0u8; 6];
    let read_len = file.read_at(&mut buf, 10).expect("read_at failed");
    assert_eq!(read_len, 6);
    assert_eq!(&buf, b"MIDDLE");

    let raw = file.as_raw_fd();
    assert!(raw >= 0);
    let borrowed = file.as_fd();
    assert_eq!(borrowed.as_raw_fd(), raw);

    let meta = file.metadata().unwrap();
    assert_ne!(meta.ino(), 0);
    assert!(meta.size() >= 16);
    assert_eq!(meta.permissions().mode() & 0o777, 0o644);

    let fd = IntoRawFd::into_raw_fd(file);
    let reopened: File = unsafe { FromRawFd::from_raw_fd(fd) };
    assert_eq!(AsRawFd::as_raw_fd(&reopened), raw);
    let borrowed_trait = AsFd::as_fd(&reopened);
    assert_eq!(borrowed_trait.as_raw_fd(), raw);
}

#[cfg(windows)]
#[test]
fn test_windows_file_ext() {
    let guard = TempFileGuard::new("windows_ext");
    let path = guard.path();

    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true);
    WinOpenOptionsExt::share_mode(&mut opts, 7);
    let file = opts.open(path).expect("open failed");

    let wrote = file
        .seek_write(b"WIN_MIDDLE", 10)
        .expect("seek_write failed");
    assert_eq!(wrote, 10);

    let mut buf = [0u8; 10];
    let read_len = file.seek_read(&mut buf, 10).expect("seek_read failed");
    assert_eq!(read_len, 10);
    assert_eq!(&buf, b"WIN_MIDDLE");

    let raw = file.as_raw_handle();
    assert!(!raw.is_null());
    let borrowed = file.as_handle();
    assert_eq!(borrowed.as_raw_handle(), raw);

    let meta = file.metadata().unwrap();
    assert!(meta.file_size() >= 20);

    let handle = IntoRawHandle::into_raw_handle(file);
    let reopened: File = unsafe { FromRawHandle::from_raw_handle(handle) };
    assert_eq!(AsRawHandle::as_raw_handle(&reopened), raw);
    let borrowed_trait = AsHandle::as_handle(&reopened);
    assert_eq!(borrowed_trait.as_raw_handle(), raw);
}

#[test]
fn test_file_times() {
    let guard = TempFileGuard::new("times");
    let path = guard.path();

    let file = File::create(path).expect("failed to create file");
    let times = FileTimes::new()
        .set_accessed(Duration::from_secs(100_000))
        .set_modified(Duration::from_secs(200_000));
    let _ = file.set_times(times);
}

#[test]
fn test_remove_file() {
    let guard = TempFileGuard::new("remove_file");
    let path = guard.path();

    // 1. Write file and verify existence
    write(path, b"hello remove_file").expect("write test file failed");
    assert_eq!(read(path).unwrap(), b"hello remove_file");

    // 2. Remove file
    remove_file(path).expect("remove_file should succeed");

    // 3. Verify it no longer exists
    let err = File::open(path).expect_err("file should no longer exist");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // 4. Repeated removal returns NotFound
    let err = remove_file(path).expect_err("second remove_file should fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // 5. Non-existent path returns NotFound
    let non_existent = Path::new("definitely_non_existent_file_123456.tmp");
    let err = remove_file(non_existent).expect_err("non-existent file removal should fail");
    assert_eq!(err.kind(), ErrorKind::NotFound);

    // 6. Path containing null byte returns InvalidInput
    let invalid_path = Path::new("invalid\0file.tmp");
    let err = remove_file(invalid_path).expect_err("null-byte path removal should fail");
    assert_eq!(err.kind(), ErrorKind::InvalidInput);
}
