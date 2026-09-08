use crate::io::{ErrorKind, Result};

pub fn read_stdin(_buf: &mut [u8]) -> Result<usize> {
    Err(ErrorKind::Unsupported.into())
}

pub fn write_stdout(_buf: &[u8]) -> Result<usize> {
    Err(ErrorKind::Unsupported.into())
}

pub fn write_stderr(_buf: &[u8]) -> Result<usize> {
    Err(ErrorKind::Unsupported.into())
}

#[inline]
pub fn flush_stdout() -> Result<()> {
    Ok(())
}

#[inline]
pub fn flush_stderr() -> Result<()> {
    Ok(())
}
