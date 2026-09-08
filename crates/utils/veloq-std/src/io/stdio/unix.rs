use core::cmp;

use crate::io::{Error, Result};

pub fn read_stdin(buf: &mut [u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let len = cmp::min(buf.len(), isize::MAX as usize);
    let ret = unsafe { libc::read(libc::STDIN_FILENO, buf.as_mut_ptr().cast(), len) };
    if ret < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

pub fn write_stdout(buf: &[u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let len = cmp::min(buf.len(), isize::MAX as usize);
    let ret = unsafe { libc::write(libc::STDOUT_FILENO, buf.as_ptr().cast(), len) };
    if ret < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

pub fn write_stderr(buf: &[u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let len = cmp::min(buf.len(), isize::MAX as usize);
    let ret = unsafe { libc::write(libc::STDERR_FILENO, buf.as_ptr().cast(), len) };
    if ret < 0 {
        Err(Error::last_os_error())
    } else {
        Ok(ret as usize)
    }
}

#[inline]
pub fn flush_stdout() -> Result<()> {
    Ok(())
}

#[inline]
pub fn flush_stderr() -> Result<()> {
    Ok(())
}
