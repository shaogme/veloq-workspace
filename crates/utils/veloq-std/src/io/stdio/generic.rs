use crate::io::{ErrorKind, Result};

pub(crate) struct Platform;

impl Platform {
    pub(crate) fn read_stdin(_buf: &mut [u8]) -> Result<usize> {
        Err(ErrorKind::Unsupported.into())
    }

    pub(crate) fn write_stdout(_buf: &[u8]) -> Result<usize> {
        Err(ErrorKind::Unsupported.into())
    }

    pub(crate) fn write_stderr(_buf: &[u8]) -> Result<usize> {
        Err(ErrorKind::Unsupported.into())
    }

    #[inline]
    pub(crate) fn flush_stdout() -> Result<()> {
        Ok(())
    }

    #[inline]
    pub(crate) fn flush_stderr() -> Result<()> {
        Ok(())
    }
}
