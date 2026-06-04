use diagweave::{report::Report, union};

union! {
    pub enum Error =
        crate::net::error::NetError as Net |
        veloq_driver_native::error::DriverErrorKind as DriverKind |
        veloq_driver_native::error::Error as Driver |
        veloq_buf::BufError as Buf |
        crate::fs::error::FsError as Fs |
        veloq_runtime::error::RuntimeError as Runtime
}

pub type Result<T> = std::result::Result<T, Report<Error>>;
