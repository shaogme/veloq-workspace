use core::fmt::{self, Display, Formatter};

use veloq_std::const_io_error;
use veloq_std::io::{Error, ErrorKind, RawOsError, Result, SimpleMessage};

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
