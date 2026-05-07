use diagweave::{report::Report, union};
use std::io;

union! {
    pub enum Error =
        io::Error as Io |
        veloq_driver::error::DriverErrorKind as DriverKind |
        veloq_driver::error::Error as Driver
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
pub fn from_driver_report<E>(report: Report<E>) -> Report<Error>
where
    Error: From<E>,
    E: std::error::Error + Send + Sync + 'static,
{
    from_report(report)
}

#[inline]
pub fn to_io_error(error: Report<Error>) -> io::Error {
    io::Error::other(error)
}
