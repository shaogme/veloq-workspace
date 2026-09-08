use core::cmp;

use crate::alloc_crate as alloc;
use crate::io::{ErrorKind, Read, Result, Seek, SeekFrom, Write};

use alloc::{boxed::Box, vec::Vec};

/// A `Cursor` wraps an in-memory buffer and provides it with a [`Seek`] implementation.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Cursor<T> {
    inner: T,
    pos: u64,
}

impl<T> Cursor<T> {
    /// Creates a new cursor wrapping the provided buffer.
    #[inline]
    #[must_use]
    pub const fn new(inner: T) -> Cursor<T> {
        Cursor { inner, pos: 0 }
    }

    /// Consumes this cursor, returning the underlying value.
    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Gets a reference to the underlying value in this cursor.
    #[inline]
    #[must_use]
    pub const fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying value in this cursor.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    /// Returns the current position of this cursor.
    #[inline]
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.pos
    }

    /// Sets the position of this cursor.
    #[inline]
    pub fn set_position(&mut self, pos: u64) {
        self.pos = pos;
    }
}

impl<T: AsRef<[u8]>> Read for Cursor<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let slice = self.inner.as_ref();
        if self.pos >= slice.len() as u64 {
            return Ok(0);
        }
        let pos = self.pos as usize;
        let mut remaining = &slice[pos..];
        let n = remaining.read(buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl<T: AsRef<[u8]>> Seek for Cursor<T> {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        let (base, offset) = match pos {
            SeekFrom::Start(n) => {
                self.pos = n;
                return Ok(n);
            }
            SeekFrom::End(n) => (self.inner.as_ref().len() as u64, n),
            SeekFrom::Current(n) => (self.pos, n),
        };
        let new_pos = if offset >= 0 {
            base.checked_add(offset as u64)
        } else {
            base.checked_sub(offset.unsigned_abs())
        };
        match new_pos {
            Some(p) => {
                self.pos = p;
                Ok(p)
            }
            None => Err(ErrorKind::InvalidInput.into()),
        }
    }

    #[inline]
    fn stream_position(&mut self) -> Result<u64> {
        Ok(self.pos)
    }
}

impl Write for Cursor<&mut [u8]> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let pos = self.pos;
        let slice = &mut self.inner[..];
        if pos >= slice.len() as u64 {
            return Ok(0);
        }
        let mut target = &mut slice[pos as usize..];
        let n = target.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Write for Cursor<Vec<u8>> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let pos = self.pos as usize;
        let len = self.inner.len();
        if pos < len {
            let write_len = cmp::min(len - pos, buf.len());
            self.inner[pos..pos + write_len].copy_from_slice(&buf[..write_len]);
            if write_len < buf.len() {
                self.inner.extend_from_slice(&buf[write_len..]);
            }
        } else {
            self.inner.resize(pos, 0);
            self.inner.extend_from_slice(buf);
        }
        self.pos += buf.len() as u64;
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Write for Cursor<Box<[u8]>> {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let pos = self.pos;
        let slice = &mut self.inner[..];
        if pos >= slice.len() as u64 {
            return Ok(0);
        }
        let mut target = &mut slice[pos as usize..];
        let n = target.write(buf)?;
        self.pos += n as u64;
        Ok(n)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}
