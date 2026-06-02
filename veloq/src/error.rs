use diagweave::{report::Report, union};
use std::io;

union! {
    pub enum Error =
        io::Error as Io |
        crate::net::error::NetError as Net |
        veloq_driver_native::error::DriverErrorKind as DriverKind |
        veloq_driver_native::error::Error as Driver |
        veloq_buf::BufError as Buf |
        crate::fs::error::FsError as Fs
}

pub type Result<T> = std::result::Result<T, Report<Error>>;