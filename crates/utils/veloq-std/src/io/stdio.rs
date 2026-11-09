#[cfg(windows)]
mod windows;

#[cfg(not(any(unix, windows)))]
mod generic;

use core::fmt;

use crate::{
    io::{IoSlice, IoSliceMut, Read, Result, Write},
    sync::{NativeReentrantMutex, NativeReentrantMutexGuard},
};

#[cfg(not(any(unix, windows)))]
use generic as sys;
#[cfg(unix)]
use unix as sys;
#[cfg(windows)]
use windows as sys;

#[cfg(unix)]
mod unix {
    use core::cmp;

    use crate::io::{Error, Result};
    use libc::{STDERR_FILENO, STDIN_FILENO, STDOUT_FILENO, read, write};

    pub(crate) struct Platform;

    impl Platform {
        pub(crate) fn read_stdin(buf: &mut [u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let len = cmp::min(buf.len(), isize::MAX as usize);
            let ret = unsafe { read(STDIN_FILENO, buf.as_mut_ptr().cast(), len) };
            if ret < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(ret as usize)
            }
        }

        pub(crate) fn write_stdout(buf: &[u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let len = cmp::min(buf.len(), isize::MAX as usize);
            let ret = unsafe { write(STDOUT_FILENO, buf.as_ptr().cast(), len) };
            if ret < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(ret as usize)
            }
        }

        pub(crate) fn write_stderr(buf: &[u8]) -> Result<usize> {
            if buf.is_empty() {
                return Ok(0);
            }
            let len = cmp::min(buf.len(), isize::MAX as usize);
            let ret = unsafe { write(STDERR_FILENO, buf.as_ptr().cast(), len) };
            if ret < 0 {
                Err(Error::last_os_error())
            } else {
                Ok(ret as usize)
            }
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
}

fn stdin_mutex() -> &'static NativeReentrantMutex<()> {
    static LOCK: NativeReentrantMutex<()> = NativeReentrantMutex::new(());
    &LOCK
}

fn stdout_mutex() -> &'static NativeReentrantMutex<()> {
    static LOCK: NativeReentrantMutex<()> = NativeReentrantMutex::new(());
    &LOCK
}

fn stderr_mutex() -> &'static NativeReentrantMutex<()> {
    static LOCK: NativeReentrantMutex<()> = NativeReentrantMutex::new(());
    &LOCK
}

/// A handle to the standard input stream of a process.
#[derive(Default)]
pub struct Stdin {
    _priv: (),
}

/// A locked reference to the [`Stdin`] handle.
#[must_use = "if unused stdin will immediately unlock"]
pub struct StdinLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

impl Stdin {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self { _priv: () }
    }

    /// Locks this handle to the standard input stream, returning a [`StdinLock`].
    #[inline]
    pub fn lock(&self) -> StdinLock<'static> {
        StdinLock {
            _guard: stdin_mutex().lock(),
        }
    }
}

impl Read for Stdin {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        self.lock().read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        self.lock().read_vectored(bufs)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.lock().read_exact(buf)
    }
}

impl Read for &Stdin {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (*self).lock().read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        (*self).lock().read_vectored(bufs)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (*self).lock().read_exact(buf)
    }
}

impl Read for StdinLock<'_> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        sys::Platform::read_stdin(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter_mut().find(|buf| !buf.is_empty()) else {
            return Ok(0);
        };
        sys::Platform::read_stdin(buf)
    }
}

impl fmt::Debug for Stdin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stdin").finish_non_exhaustive()
    }
}

impl fmt::Debug for StdinLock<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StdinLock").finish_non_exhaustive()
    }
}

/// A handle to the standard output stream of a process.
#[derive(Default)]
pub struct Stdout {
    _priv: (),
}

/// A locked reference to the [`Stdout`] handle.
#[must_use = "if unused stdout will immediately unlock"]
pub struct StdoutLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

impl Stdout {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self { _priv: () }
    }

    /// Locks this handle to the standard output stream, returning a [`StdoutLock`].
    #[inline]
    pub fn lock(&self) -> StdoutLock<'static> {
        StdoutLock {
            _guard: stdout_mutex().lock(),
        }
    }
}

impl Write for Stdout {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.lock().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.lock().flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.lock().write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.lock().write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> Result<()> {
        self.lock().write_fmt(args)
    }
}

impl Write for &Stdout {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (*self).lock().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (*self).lock().flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (*self).lock().write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (*self).lock().write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> Result<()> {
        (*self).lock().write_fmt(args)
    }
}

impl Write for StdoutLock<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        sys::Platform::write_stdout(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        sys::Platform::flush_stdout()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter().find(|buf| !buf.is_empty()) else {
            return Ok(0);
        };
        sys::Platform::write_stdout(buf)
    }
}

impl fmt::Debug for Stdout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stdout").finish_non_exhaustive()
    }
}

impl fmt::Debug for StdoutLock<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StdoutLock").finish_non_exhaustive()
    }
}

/// A handle to the standard error stream of a process.
#[derive(Default)]
pub struct Stderr {
    _priv: (),
}

/// A locked reference to the [`Stderr`] handle.
#[must_use = "if unused stderr will immediately unlock"]
pub struct StderrLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

impl Stderr {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self { _priv: () }
    }

    /// Locks this handle to the standard error stream, returning a [`StderrLock`].
    #[inline]
    pub fn lock(&self) -> StderrLock<'static> {
        StderrLock {
            _guard: stderr_mutex().lock(),
        }
    }
}

impl Write for Stderr {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.lock().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        self.lock().flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        self.lock().write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.lock().write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> Result<()> {
        self.lock().write_fmt(args)
    }
}

impl Write for &Stderr {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (*self).lock().write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (*self).lock().flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (*self).lock().write_vectored(bufs)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (*self).lock().write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, args: fmt::Arguments<'_>) -> Result<()> {
        (*self).lock().write_fmt(args)
    }
}

impl Write for StderrLock<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        sys::Platform::write_stderr(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        sys::Platform::flush_stderr()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter().find(|buf| !buf.is_empty()) else {
            return Ok(0);
        };
        sys::Platform::write_stderr(buf)
    }
}

impl fmt::Debug for Stderr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stderr").finish_non_exhaustive()
    }
}

impl fmt::Debug for StderrLock<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StderrLock").finish_non_exhaustive()
    }
}

impl Stdout {
    #[doc(hidden)]
    #[inline]
    pub fn print(args: fmt::Arguments<'_>) {
        if let Err(error) = Self::new().write_fmt(args) {
            panic!("failed printing to stdout: {error}");
        }
    }
}

impl Stderr {
    #[doc(hidden)]
    #[inline]
    pub fn print(args: fmt::Arguments<'_>) {
        if let Err(error) = Self::new().write_fmt(args) {
            panic!("failed printing to stderr: {error}");
        }
    }
}
