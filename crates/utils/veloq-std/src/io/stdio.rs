#[cfg(unix)]
pub mod unix;

#[cfg(windows)]
pub mod windows;

#[cfg(not(any(unix, windows)))]
pub mod generic;

use core::fmt;

use crate::io::{Error, IoSlice, IoSliceMut, Read, Result, Write};
use crate::sync::{NativeReentrantMutex, NativeReentrantMutexGuard};

#[cfg(unix)]
use unix as sys;

#[cfg(windows)]
use windows as sys;

#[cfg(not(any(unix, windows)))]
use generic as sys;

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
pub struct StdinLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

/// Constructs a new handle to the standard input of the current process.
#[inline]
#[must_use]
pub fn stdin() -> Stdin {
    Stdin { _priv: () }
}

impl Stdin {
    /// Locks this handle to the standard input stream, returning a [`StdinLock`].
    #[inline]
    pub fn lock(&self) -> StdinLock<'static> {
        StdinLock {
            _guard: stdin_mutex().lock(),
        }
    }
}

impl Read for Stdin {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let _guard = stdin_mutex().lock();
        sys::read_stdin(buf)
    }

    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let _guard = stdin_mutex().lock();
        let Some(buf) = bufs.iter_mut().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::read_stdin(buf)
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        let _guard = stdin_mutex().lock();
        while !buf.is_empty() {
            match sys::read_stdin(buf) {
                Ok(0) => break,
                Ok(n) => {
                    let (_, rest) = buf.split_at_mut(n);
                    buf = rest;
                }
                Err(ref e) if e.is_interrupted() => continue,
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(Error::READ_EXACT_EOF)
        } else {
            Ok(())
        }
    }
}

impl Read for StdinLock<'_> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        sys::read_stdin(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter_mut().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::read_stdin(buf)
    }

    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            match sys::read_stdin(buf) {
                Ok(0) => break,
                Ok(n) => {
                    let (_, rest) = buf.split_at_mut(n);
                    buf = rest;
                }
                Err(ref e) if e.is_interrupted() => continue,
                Err(e) => return Err(e),
            }
        }
        if !buf.is_empty() {
            Err(Error::READ_EXACT_EOF)
        } else {
            Ok(())
        }
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
pub struct StdoutLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

/// Constructs a new handle to the standard output of the current process.
#[inline]
#[must_use]
pub fn stdout() -> Stdout {
    Stdout { _priv: () }
}

impl Stdout {
    /// Locks this handle to the standard output stream, returning a [`StdoutLock`].
    #[inline]
    pub fn lock(&self) -> StdoutLock<'static> {
        StdoutLock {
            _guard: stdout_mutex().lock(),
        }
    }
}

impl Write for Stdout {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let _guard = stdout_mutex().lock();
        sys::write_stdout(buf)
    }

    fn flush(&mut self) -> Result<()> {
        let _guard = stdout_mutex().lock();
        sys::flush_stdout()
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let _guard = stdout_mutex().lock();
        let Some(buf) = bufs.iter().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::write_stdout(buf)
    }

    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        let _guard = stdout_mutex().lock();
        while !buf.is_empty() {
            let n = sys::write_stdout(buf)?;
            if n == 0 {
                return Err(Error::WRITE_ZERO);
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        let mut lock = self.lock();
        lock.write_fmt(fmt)
    }
}

impl Write for StdoutLock<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        sys::write_stdout(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        sys::flush_stdout()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::write_stdout(buf)
    }

    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let n = sys::write_stdout(buf)?;
            if n == 0 {
                return Err(Error::WRITE_ZERO);
            }
            buf = &buf[n..];
        }
        Ok(())
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
pub struct StderrLock<'a> {
    _guard: NativeReentrantMutexGuard<'a, ()>,
}

/// Constructs a new handle to the standard error of the current process.
#[inline]
#[must_use]
pub fn stderr() -> Stderr {
    Stderr { _priv: () }
}

impl Stderr {
    /// Locks this handle to the standard error stream, returning a [`StderrLock`].
    #[inline]
    pub fn lock(&self) -> StderrLock<'static> {
        StderrLock {
            _guard: stderr_mutex().lock(),
        }
    }
}

impl Write for Stderr {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let _guard = stderr_mutex().lock();
        sys::write_stderr(buf)
    }

    fn flush(&mut self) -> Result<()> {
        let _guard = stderr_mutex().lock();
        sys::flush_stderr()
    }

    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let _guard = stderr_mutex().lock();
        let Some(buf) = bufs.iter().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::write_stderr(buf)
    }

    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        let _guard = stderr_mutex().lock();
        while !buf.is_empty() {
            let n = sys::write_stderr(buf)?;
            if n == 0 {
                return Err(Error::WRITE_ZERO);
            }
            buf = &buf[n..];
        }
        Ok(())
    }

    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        let mut lock = self.lock();
        lock.write_fmt(fmt)
    }
}

impl Write for StderrLock<'_> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        sys::write_stderr(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        sys::flush_stderr()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        sys::write_stderr(buf)
    }

    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            let n = sys::write_stderr(buf)?;
            if n == 0 {
                return Err(Error::WRITE_ZERO);
            }
            buf = &buf[n..];
        }
        Ok(())
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

#[doc(hidden)]
pub fn _print(args: fmt::Arguments<'_>) {
    let mut out = stdout().lock();
    let _ = out.write_fmt(args);
}

#[doc(hidden)]
pub fn _eprint(args: fmt::Arguments<'_>) {
    let mut err = stderr().lock();
    let _ = err.write_fmt(args);
}
