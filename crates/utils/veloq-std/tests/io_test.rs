use core::fmt::{self, Display, Formatter};

use veloq_std::const_io_error;
use veloq_std::io::{
    Cursor, Error, ErrorKind, IoSlice, IoSliceMut, RawOsError, Read, Result, Seek, SeekFrom,
    SimpleMessage, Stderr, Stdin, Stdout, Write, copy, empty, repeat, sink, stderr, stdin, stdout,
};

#[cfg(feature = "std")]
use std::io::ErrorKind as StdErrorKind;

#[derive(Debug, PartialEq, Eq)]
struct CustomTestError {
    detail: &'static str,
}

impl Display for CustomTestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "custom error: {}", self.detail)
    }
}

impl core::error::Error for CustomTestError {}

#[test]
fn test_error_from_kind() {
    let err = Error::from(ErrorKind::NotFound);
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert!(err.raw_os_error().is_none());
    assert_eq!(format!("{err}"), "entity not found");
}

#[test]
fn test_error_new_and_downcast() {
    let custom = CustomTestError {
        detail: "test payload",
    };
    let err = Error::new(ErrorKind::AlreadyExists, custom);
    assert_eq!(err.kind(), ErrorKind::AlreadyExists);
    assert!(err.raw_os_error().is_none());
    assert_eq!(format!("{err}"), "custom error: test payload");

    let downcasted = err.downcast::<CustomTestError>().expect("downcast failed");
    assert_eq!(
        *downcasted,
        CustomTestError {
            detail: "test payload"
        }
    );
}

#[test]
fn test_error_other_and_into_inner() {
    let err = Error::other("something went wrong");
    assert_eq!(err.kind(), ErrorKind::Other);
    assert_eq!(format!("{err}"), "something went wrong");

    let inner = err.into_inner();
    assert!(inner.is_some());
}

#[test]
fn test_raw_os_error() {
    let code: RawOsError = 2; // ENOENT on Linux
    let err = Error::from_raw_os_error(code);
    assert_eq!(err.raw_os_error(), Some(code));
    #[cfg(target_os = "linux")]
    assert_eq!(err.kind(), ErrorKind::NotFound);

    let display_str = format!("{err}");
    assert!(display_str.contains("(os error 2)"));
}

#[test]
fn test_last_os_error() {
    let err = Error::last_os_error();
    assert!(err.raw_os_error().is_some());
}

#[test]
fn test_const_error_and_predefined_constants() {
    const MY_CONST: Error = const_io_error!(ErrorKind::BrokenPipe, "pipe broken");
    assert_eq!(MY_CONST.kind(), ErrorKind::BrokenPipe);
    assert_eq!(format!("{MY_CONST}"), "pipe broken");

    assert_eq!(Error::INVALID_UTF8.kind(), ErrorKind::InvalidData);
    assert_eq!(Error::READ_EXACT_EOF.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(Error::UNEXPECTED_EOF.kind(), ErrorKind::UnexpectedEof);
    assert_eq!(Error::WRITE_ZERO.kind(), ErrorKind::WriteZero);
    assert_eq!(Error::TIMED_OUT.kind(), ErrorKind::TimedOut);
}

#[test]
fn test_is_interrupted() {
    let err = Error::from(ErrorKind::Interrupted);
    assert!(err.is_interrupted());

    let err2 = Error::from(ErrorKind::TimedOut);
    assert!(!err2.is_interrupted());
}

#[test]
fn test_result_type_alias() {
    fn produces_ok() -> Result<u32> {
        Ok(42)
    }

    fn produces_err() -> Result<u32> {
        Err(Error::from(ErrorKind::PermissionDenied))
    }

    assert_eq!(produces_ok().unwrap(), 42);
    assert_eq!(
        produces_err().unwrap_err().kind(),
        ErrorKind::PermissionDenied
    );
}

#[test]
fn test_send_sync() {
    fn assert_send_sync<T: Send + Sync + 'static>() {}
    assert_send_sync::<Error>();
    assert_send_sync::<ErrorKind>();
    assert_send_sync::<SimpleMessage>();
}

#[cfg(feature = "std")]
#[test]
fn test_std_error_conversion() {
    use std::io::Error as StdIoError;

    let std_err = StdIoError::new(StdErrorKind::NotFound, "file missing");
    let err: Error = std_err.into();
    assert_eq!(err.kind(), ErrorKind::NotFound);
    assert_eq!(format!("{err}"), "file missing");

    let back_to_std: StdIoError = err.into();
    assert_eq!(back_to_std.kind(), StdErrorKind::NotFound);

    let os_err = Error::from_raw_os_error(22);
    let std_os_err: StdIoError = os_err.into();
    assert_eq!(std_os_err.raw_os_error(), Some(22));

    let back_from_std: Error = std_os_err.into();
    assert_eq!(back_from_std.raw_os_error(), Some(22));
}

#[test]
fn test_read_slice() {
    let mut data: &[u8] = b"hello world";
    let mut buf = [0u8; 5];
    assert_eq!(data.read(&mut buf).unwrap(), 5);
    assert_eq!(&buf, b"hello");
    assert_eq!(data, b" world");

    let mut rest = [0u8; 10];
    assert_eq!(data.read(&mut rest).unwrap(), 6);
    assert_eq!(&rest[..6], b" world");
    assert_eq!(data.read(&mut rest).unwrap(), 0);
}

#[test]
fn test_read_exact() {
    let mut data: &[u8] = b"12345";
    let mut buf = [0u8; 5];
    data.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"12345");

    let mut buf2 = [0u8; 1];
    let err = data.read_exact(&mut buf2).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::UnexpectedEof);
}

#[test]
fn test_read_to_end_and_to_string() {
    use veloq_std::{string::String, vec::Vec};

    let mut data: &[u8] = b"rust 2024 edition";
    let mut vec = Vec::new();
    let n = data.read_to_end(&mut vec).unwrap();
    assert_eq!(n, 17);
    assert_eq!(vec, b"rust 2024 edition");

    let mut data2: &[u8] = b"hello async";
    let mut s = String::new();
    let n2 = data2.read_to_string(&mut s).unwrap();
    assert_eq!(n2, 11);
    assert_eq!(s, "hello async");
}

#[test]
fn test_write_slice_and_vec() {
    use veloq_std::vec::Vec;

    let mut buf = [0u8; 8];
    {
        let mut slice = &mut buf[..];
        let n = slice.write(b"abcd").unwrap();
        assert_eq!(n, 4);
        slice.write_all(b"efgh").unwrap();
        assert_eq!(slice.write(b"i").unwrap(), 0);
    }
    assert_eq!(&buf, b"abcdefgh");

    let mut v = Vec::new();
    v.write_all(b"hello ").unwrap();
    write!(v, "format {}", 42).unwrap();
    assert_eq!(v, b"hello format 42");
}

#[test]
fn test_cursor_read_write_seek() {
    use veloq_std::vec::Vec;

    let mut c = Cursor::new(Vec::new());
    c.write_all(b"hello world").unwrap();
    assert_eq!(c.position(), 11);

    c.rewind().unwrap();
    assert_eq!(c.position(), 0);

    let mut buf = [0u8; 5];
    c.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"hello");

    let pos = c.seek(SeekFrom::Current(1)).unwrap();
    assert_eq!(pos, 6);

    let mut buf2 = [0u8; 5];
    c.read_exact(&mut buf2).unwrap();
    assert_eq!(&buf2, b"world");

    assert_eq!(c.stream_len().unwrap(), 11);
    assert_eq!(c.stream_position().unwrap(), 11);

    c.seek(SeekFrom::End(-5)).unwrap();
    let mut buf3 = [0u8; 5];
    c.read_exact(&mut buf3).unwrap();
    assert_eq!(&buf3, b"world");
}

#[test]
fn test_copy_sink_empty_repeat() {
    use veloq_std::vec::Vec;

    let mut src: &[u8] = b"hello";
    let mut dst = Vec::new();
    let n = copy(&mut src, &mut dst).unwrap();
    assert_eq!(n, 5);
    assert_eq!(dst, b"hello");

    let mut s = sink();
    assert_eq!(s.write(b"drop me").unwrap(), 7);

    let mut e = empty();
    let mut b = [0u8; 4];
    assert_eq!(e.read(&mut b).unwrap(), 0);
    assert_eq!(e.seek(SeekFrom::Start(10)).unwrap(), 0);

    let mut r = repeat(42);
    let mut r_buf = [0u8; 4];
    assert_eq!(r.read(&mut r_buf).unwrap(), 4);
    assert_eq!(r_buf, [42, 42, 42, 42]);
}

#[test]
fn test_read_adaptors() {
    use veloq_std::vec::Vec;

    let src: &[u8] = b"abcdef";
    let mut taken = src.take(3);
    let mut buf = Vec::new();
    taken.read_to_end(&mut buf).unwrap();
    assert_eq!(buf, b"abc");

    let r1: &[u8] = b"abc";
    let r2: &[u8] = b"def";
    let mut chained = r1.chain(r2);
    let mut chain_buf = Vec::new();
    chained.read_to_end(&mut chain_buf).unwrap();
    assert_eq!(chain_buf, b"abcdef");

    let r3: &[u8] = b"xyz";
    let bytes: Result<Vec<u8>> = r3.bytes().collect();
    assert_eq!(bytes.unwrap(), b"xyz");
}

#[test]
fn test_ioslice() {
    let buf = b"hello world";
    let mut slice = IoSlice::new(buf);
    assert_eq!(&*slice, b"hello world");
    slice.advance(6);
    assert_eq!(&*slice, b"world");

    let mut mut_buf = *b"hello world";
    let mut mut_slice = IoSliceMut::new(&mut mut_buf);
    assert_eq!(&*mut_slice, b"hello world");
    mut_slice.advance(6);
    assert_eq!(&*mut_slice, b"world");
}

#[test]
fn test_stdio() {
    fn assert_types<R: Read, W: Write>() {}
    assert_types::<Stdin, Stdout>();
    assert_types::<Stdin, Stderr>();

    let mut out: Stdout = stdout();
    out.write_all(b"").unwrap();
    out.flush().unwrap();
    let _out_lock = out.lock();

    let mut err: Stderr = stderr();
    err.write_all(b"").unwrap();
    err.flush().unwrap();
    let _err_lock = err.lock();

    let in_handle: Stdin = stdin();
    let _in_lock = in_handle.lock();
}

#[cfg(unix)]
#[test]
fn test_stdio_unix_raw_fd() {
    use veloq_std::os::fd::AsRawFd;

    let in_handle = stdin();
    let out_handle = stdout();
    let err_handle = stderr();

    assert_eq!(in_handle.as_raw_fd(), libc::STDIN_FILENO);
    assert_eq!(out_handle.as_raw_fd(), libc::STDOUT_FILENO);
    assert_eq!(err_handle.as_raw_fd(), libc::STDERR_FILENO);
}

#[cfg(windows)]
#[test]
fn test_stdio_windows_raw_handle() {
    use veloq_std::os::windows::io::AsRawHandle;

    let in_handle = stdin();
    let out_handle = stdout();
    let err_handle = stderr();

    let _ = in_handle.as_raw_handle();
    let _ = out_handle.as_raw_handle();
    let _ = err_handle.as_raw_handle();
}
