use diagweave::{report::Report, union};
use std::io;

union! {
    pub enum Error =
        io::Error as Io |
        veloq_driver_native::error::DriverErrorKind as DriverKind |
        veloq_driver_native::error::Error as Driver |
        veloq_buf::BufError as Buf
}

pub type Result<T> = std::result::Result<T, Report<Error>>;

#[inline]
pub fn from_io_error(error: io::Error) -> Report<Error> {
    Report::new(Error::from(error))
}

#[inline]
pub fn from_report<E>(report: Report<E>) -> Report<Error>
where
    Error: From<E>,
    E: std::error::Error + Send + Sync + 'static,
{
    report.map_err(Error::from)
}

#[inline]
pub fn to_io_error(error: Report<Error>) -> io::Error {
    io::Error::other(error)
}
