use core::{
    cmp, fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
};

use crate::alloc_crate as alloc;
use crate::io::{Error, ErrorKind, Result};

use alloc::{string::String, vec::Vec};

/// Enumeration of possible methods to seek within an I/O object.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum SeekFrom {
    /// Sets the offset to the provided number of bytes from the start of the stream.
    Start(u64),
    /// Sets the offset to the size of this object plus the specified number of bytes.
    End(i64),
    /// Sets the offset to the current position plus the specified number of bytes.
    Current(i64),
}

/// A buffer to read data into.
#[repr(transparent)]
pub struct IoSliceMut<'a> {
    vec: &'a mut [u8],
    _phantom: PhantomData<&'a mut [u8]>,
}

impl<'a> IoSliceMut<'a> {
    /// Creates a new `IoSliceMut` wrapping a byte slice.
    #[inline]
    #[must_use]
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self {
            vec: buf,
            _phantom: PhantomData,
        }
    }

    /// Advances the internal slice by `n` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.len()`.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        assert!(
            n <= self.vec.len(),
            "advancing IoSliceMut beyond its length"
        );
        let rest = core::mem::take(&mut self.vec);
        let (_, rest) = rest.split_at_mut(n);
        self.vec = rest;
    }
}

impl Deref for IoSliceMut<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.vec
    }
}

impl DerefMut for IoSliceMut<'_> {
    #[inline]
    fn deref_mut(&mut self) -> &mut [u8] {
        self.vec
    }
}

impl fmt::Debug for IoSliceMut<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.vec, f)
    }
}

/// A buffer to write data from.
#[repr(transparent)]
#[derive(Copy, Clone)]
pub struct IoSlice<'a> {
    vec: &'a [u8],
    _phantom: PhantomData<&'a [u8]>,
}

impl<'a> IoSlice<'a> {
    /// Creates a new `IoSlice` wrapping a byte slice.
    #[inline]
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self {
            vec: buf,
            _phantom: PhantomData,
        }
    }

    /// Advances the internal slice by `n` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `n > self.len()`.
    #[inline]
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.vec.len(), "advancing IoSlice beyond its length");
        self.vec = &self.vec[n..];
    }
}

impl Deref for IoSlice<'_> {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.vec
    }
}

impl fmt::Debug for IoSlice<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.vec, f)
    }
}

/// The `Read` trait allows for reading bytes from a source.
pub trait Read {
    /// Pull some bytes from this source into the specified buffer, returning how many bytes were read.
    fn read(&mut self, buf: &mut [u8]) -> Result<usize>;

    /// Like `read`, except that it reads into a slice of buffers.
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter_mut().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        self.read(buf)
    }

    /// Determines if this `Read`er has an efficient `read_vectored` implementation.
    #[inline]
    fn is_read_vectored(&self) -> bool {
        false
    }

    /// Read all bytes until EOF in this source, placing them into `buf`.
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        let mut total = 0;
        let mut probe = [0u8; 4096];
        loop {
            match self.read(&mut probe) {
                Ok(0) => break Ok(total),
                Ok(n) => {
                    buf.extend_from_slice(&probe[..n]);
                    total += n;
                }
                Err(ref e) if e.is_interrupted() => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// Read all bytes until EOF in this source, appending them to `buf`.
    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        let mut bytes = Vec::new();
        let n = self.read_to_end(&mut bytes)?;
        match core::str::from_utf8(&bytes) {
            Ok(s) => {
                buf.push_str(s);
                Ok(n)
            }
            Err(_) => Err(Error::INVALID_UTF8),
        }
    }

    /// Read the exact number of bytes required to fill `buf`.
    fn read_exact(&mut self, mut buf: &mut [u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.read(buf) {
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

    /// Borrows this reader by reference.
    #[inline]
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }

    /// Transforms this `Read` instance to an `Iterator` over its bytes.
    #[inline]
    fn bytes(self) -> Bytes<Self>
    where
        Self: Sized,
    {
        Bytes { inner: self }
    }

    /// Creates an adaptor which will chain this stream with another.
    #[inline]
    fn chain<R: Read>(self, next: R) -> Chain<Self, R>
    where
        Self: Sized,
    {
        Chain {
            first: self,
            second: next,
            done_first: false,
        }
    }

    /// Creates an adaptor which will read at most `limit` bytes from it.
    #[inline]
    fn take(self, limit: u64) -> Take<Self>
    where
        Self: Sized,
    {
        Take { inner: self, limit }
    }
}

/// A trait for objects which are byte-oriented sinks.
pub trait Write {
    /// Write a buffer into this writer, returning how many bytes were written.
    fn write(&mut self, buf: &[u8]) -> Result<usize>;

    /// Flush this output stream, ensuring that all intermediately buffered contents reach their destination.
    fn flush(&mut self) -> Result<()>;

    /// Like `write`, except that it writes from a slice of buffers.
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        let Some(buf) = bufs.iter().find(|b| !b.is_empty()) else {
            return Ok(0);
        };
        self.write(buf)
    }

    /// Determines if this `Write`r has an efficient `write_vectored` implementation.
    #[inline]
    fn is_write_vectored(&self) -> bool {
        false
    }

    /// Attempts to write an entire buffer into this writer.
    fn write_all(&mut self, mut buf: &[u8]) -> Result<()> {
        while !buf.is_empty() {
            match self.write(buf) {
                Ok(0) => return Err(Error::WRITE_ZERO),
                Ok(n) => buf = &buf[n..],
                Err(ref e) if e.is_interrupted() => continue,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Writes a formatted string into this writer, returning any error encountered.
    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        struct Adaptor<'a, T: ?Sized + 'a> {
            inner: &'a mut T,
            error: Result<()>,
        }

        impl<T: Write + ?Sized> fmt::Write for Adaptor<'_, T> {
            fn write_str(&mut self, s: &str) -> fmt::Result {
                match self.inner.write_all(s.as_bytes()) {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        self.error = Err(e);
                        Err(fmt::Error)
                    }
                }
            }
        }

        let mut adaptor = Adaptor {
            inner: self,
            error: Ok(()),
        };
        match fmt::write(&mut adaptor, fmt) {
            Ok(()) => Ok(()),
            Err(..) => {
                if adaptor.error.is_err() {
                    adaptor.error
                } else {
                    Err(ErrorKind::InvalidData.into())
                }
            }
        }
    }

    /// Borrows this writer by reference.
    #[inline]
    fn by_ref(&mut self) -> &mut Self
    where
        Self: Sized,
    {
        self
    }
}

/// The `Seek` trait provides a cursor which can be moved within a stream of bytes.
pub trait Seek {
    /// Seek to an offset, in bytes, in a stream.
    fn seek(&mut self, pos: SeekFrom) -> Result<u64>;

    /// Rewind to the beginning of a stream.
    #[inline]
    fn rewind(&mut self) -> Result<()> {
        self.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    /// Returns the current seek position from the start of the stream.
    #[inline]
    fn stream_position(&mut self) -> Result<u64> {
        self.seek(SeekFrom::Current(0))
    }

    /// Returns the length of this stream (in bytes).
    fn stream_len(&mut self) -> Result<u64> {
        let old_pos = self.stream_position()?;
        let len = self.seek(SeekFrom::End(0))?;
        if old_pos != len {
            self.seek(SeekFrom::Start(old_pos))?;
        }
        Ok(len)
    }
}

/// An iterator over `u8` values of a reader.
#[derive(Debug)]
pub struct Bytes<R> {
    inner: R,
}

impl<R: Read> Iterator for Bytes<R> {
    type Item = Result<u8>;

    fn next(&mut self) -> Option<Result<u8>> {
        let mut byte = 0;
        loop {
            return match self.inner.read(core::slice::from_mut(&mut byte)) {
                Ok(0) => None,
                Ok(..) => Some(Ok(byte)),
                Err(ref e) if e.is_interrupted() => continue,
                Err(e) => Some(Err(e)),
            };
        }
    }
}

/// Adaptor chaining two readers together.
#[derive(Debug)]
pub struct Chain<T, U> {
    first: T,
    second: U,
    done_first: bool,
}

impl<T, U> Chain<T, U> {
    /// Consumes the `Chain`, returning the wrapped readers.
    #[inline]
    pub fn into_inner(self) -> (T, U) {
        (self.first, self.second)
    }

    /// Gets references to the underlying readers in this `Chain`.
    #[inline]
    pub fn get_ref(&self) -> (&T, &U) {
        (&self.first, &self.second)
    }

    /// Gets mutable references to the underlying readers in this `Chain`.
    #[inline]
    pub fn get_mut(&mut self) -> (&mut T, &mut U) {
        (&mut self.first, &mut self.second)
    }
}

impl<T: Read, U: Read> Read for Chain<T, U> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if !self.done_first {
            match self.first.read(buf)? {
                0 if !buf.is_empty() => self.done_first = true,
                n => return Ok(n),
            }
        }
        self.second.read(buf)
    }
}

/// Reader adaptor which limits the bytes read from an underlying reader.
#[derive(Debug)]
pub struct Take<T> {
    inner: T,
    limit: u64,
}

impl<T> Take<T> {
    /// Returns the number of bytes that can be read before this instance will return EOF.
    #[inline]
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Sets the number of bytes that can be read before this instance will return EOF.
    #[inline]
    pub fn set_limit(&mut self, limit: u64) {
        self.limit = limit;
    }

    /// Consumes the `Take`, returning the wrapped reader.
    #[inline]
    pub fn into_inner(self) -> T {
        self.inner
    }

    /// Gets a reference to the underlying reader.
    #[inline]
    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    /// Gets a mutable reference to the underlying reader.
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: Read> Read for Take<T> {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        if self.limit == 0 {
            return Ok(0);
        }

        let max = cmp::min(buf.len() as u64, self.limit) as usize;
        let n = self.inner.read(&mut buf[..max])?;
        self.limit -= n as u64;
        Ok(n)
    }
}

/// Copies the entire contents of a reader into a writer.
pub fn copy<R: ?Sized + Read, W: ?Sized + Write>(reader: &mut R, writer: &mut W) -> Result<u64> {
    let mut buf = [0u8; 8192];
    let mut written = 0;
    loop {
        let len = match reader.read(&mut buf) {
            Ok(0) => return Ok(written),
            Ok(len) => len,
            Err(ref e) if e.is_interrupted() => continue,
            Err(e) => return Err(e),
        };
        writer.write_all(&buf[..len])?;
        written += len as u64;
    }
}

/// A writer which consumes and ignores all provided bytes.
#[derive(Copy, Clone, Debug, Default)]
#[non_exhaustive]
pub struct Sink;

/// Creates an instance of a writer which will successfully consume all data.
#[inline]
#[must_use]
pub const fn sink() -> Sink {
    Sink
}

impl Write for Sink {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        Ok(buf.len())
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

/// A reader which is always at EOF.
#[derive(Copy, Clone, Debug, Default)]
pub struct Empty {
    _priv: (),
}

/// Creates a value that is always at EOF for reads.
#[inline]
#[must_use]
pub const fn empty() -> Empty {
    Empty { _priv: () }
}

impl Read for Empty {
    #[inline]
    fn read(&mut self, _buf: &mut [u8]) -> Result<usize> {
        Ok(0)
    }
}

impl Seek for Empty {
    #[inline]
    fn seek(&mut self, _pos: SeekFrom) -> Result<u64> {
        Ok(0)
    }

    #[inline]
    fn stream_len(&mut self) -> Result<u64> {
        Ok(0)
    }

    #[inline]
    fn stream_position(&mut self) -> Result<u64> {
        Ok(0)
    }
}

/// A reader which infinitely repeats a single byte.
#[derive(Debug)]
pub struct Repeat {
    byte: u8,
}

/// Creates an instance of a reader that infinitely repeats one byte.
#[inline]
#[must_use]
pub const fn repeat(byte: u8) -> Repeat {
    Repeat { byte }
}

impl Read for Repeat {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        buf.fill(self.byte);
        Ok(buf.len())
    }
}
