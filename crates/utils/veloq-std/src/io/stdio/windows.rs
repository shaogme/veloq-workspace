use core::{cmp, ptr::null_mut};

use crate::io::{Error, ErrorKind, Result};

use windows_sys::Win32::{
    Foundation::INVALID_HANDLE_VALUE,
    Storage::FileSystem::{ReadFile, WriteFile},
    System::Console::{GetStdHandle, STD_ERROR_HANDLE, STD_INPUT_HANDLE, STD_OUTPUT_HANDLE},
};

pub fn read_stdin(buf: &mut [u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let handle = unsafe { GetStdHandle(STD_INPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(ErrorKind::BrokenPipe.into());
    }
    let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
    let mut bytes_read = 0u32;
    let res = unsafe {
        ReadFile(
            handle,
            buf.as_mut_ptr().cast(),
            len,
            &mut bytes_read,
            null_mut(),
        )
    };
    if res == 0 {
        let err = Error::last_os_error();
        if err.kind() == ErrorKind::BrokenPipe {
            Ok(0)
        } else {
            Err(err)
        }
    } else {
        Ok(bytes_read as usize)
    }
}

pub fn write_stdout(buf: &[u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let handle = unsafe { GetStdHandle(STD_OUTPUT_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(ErrorKind::BrokenPipe.into());
    }
    let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
    let mut bytes_written = 0u32;
    let res = unsafe {
        WriteFile(
            handle,
            buf.as_ptr().cast(),
            len,
            &mut bytes_written,
            null_mut(),
        )
    };
    if res == 0 {
        Err(Error::last_os_error())
    } else {
        Ok(bytes_written as usize)
    }
}

pub fn write_stderr(buf: &[u8]) -> Result<usize> {
    if buf.is_empty() {
        return Ok(0);
    }
    let handle = unsafe { GetStdHandle(STD_ERROR_HANDLE) };
    if handle.is_null() || handle == INVALID_HANDLE_VALUE {
        return Err(ErrorKind::BrokenPipe.into());
    }
    let len = cmp::min(buf.len(), u32::MAX as usize) as u32;
    let mut bytes_written = 0u32;
    let res = unsafe {
        WriteFile(
            handle,
            buf.as_ptr().cast(),
            len,
            &mut bytes_written,
            null_mut(),
        )
    };
    if res == 0 {
        Err(Error::last_os_error())
    } else {
        Ok(bytes_written as usize)
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
