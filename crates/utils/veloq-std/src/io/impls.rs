use core::{cmp, fmt};

use crate::alloc_crate as alloc;
use crate::io::{Error, IoSlice, IoSliceMut, Read, Result, Seek, SeekFrom, Write};

use alloc::{boxed::Box, string::String, vec::Vec};

impl Read for &[u8] {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let amt = cmp::min(buf.len(), self.len());
        let (a, b) = self.split_at(amt);
        buf[..amt].copy_from_slice(a);
        *self = b;
        Ok(amt)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        if buf.len() > self.len() {
            *self = &self[self.len()..];
            return Err(Error::READ_EXACT_EOF);
        }
        let (a, b) = self.split_at(buf.len());
        buf.copy_from_slice(a);
        *self = b;
        Ok(())
    }
}

impl Write for &mut [u8] {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let amt = cmp::min(buf.len(), self.len());
        let (a, b) = core::mem::take(self).split_at_mut(amt);
        a.copy_from_slice(&buf[..amt]);
        *self = b;
        Ok(amt)
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        if self.write(buf)? == buf.len() {
            Ok(())
        } else {
            Err(Error::WRITE_ZERO)
        }
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Write for Vec<u8> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        self.extend_from_slice(buf);
        Ok(())
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl<R: Read + ?Sized> Read for &mut R {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        (**self).read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        (**self).is_read_vectored()
    }

    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        (**self).read_to_end(buf)
    }

    #[inline]
    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        (**self).read_to_string(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact(buf)
    }
}

impl<R: Read + ?Sized> Read for Box<R> {
    #[inline]
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        (**self).read(buf)
    }

    #[inline]
    fn read_vectored(&mut self, bufs: &mut [IoSliceMut<'_>]) -> Result<usize> {
        (**self).read_vectored(bufs)
    }

    #[inline]
    fn is_read_vectored(&self) -> bool {
        (**self).is_read_vectored()
    }

    #[inline]
    fn read_to_end(&mut self, buf: &mut Vec<u8>) -> Result<usize> {
        (**self).read_to_end(buf)
    }

    #[inline]
    fn read_to_string(&mut self, buf: &mut String) -> Result<usize> {
        (**self).read_to_string(buf)
    }

    #[inline]
    fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        (**self).read_exact(buf)
    }
}

impl<W: Write + ?Sized> Write for &mut W {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (**self).write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (**self).write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        (**self).is_write_vectored()
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (**self).write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        (**self).write_fmt(fmt)
    }
}

impl<W: Write + ?Sized> Write for Box<W> {
    #[inline]
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        (**self).write(buf)
    }

    #[inline]
    fn flush(&mut self) -> Result<()> {
        (**self).flush()
    }

    #[inline]
    fn write_vectored(&mut self, bufs: &[IoSlice<'_>]) -> Result<usize> {
        (**self).write_vectored(bufs)
    }

    #[inline]
    fn is_write_vectored(&self) -> bool {
        (**self).is_write_vectored()
    }

    #[inline]
    fn write_all(&mut self, buf: &[u8]) -> Result<()> {
        (**self).write_all(buf)
    }

    #[inline]
    fn write_fmt(&mut self, fmt: fmt::Arguments<'_>) -> Result<()> {
        (**self).write_fmt(fmt)
    }
}

impl<S: Seek + ?Sized> Seek for &mut S {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }

    #[inline]
    fn rewind(&mut self) -> Result<()> {
        (**self).rewind()
    }

    #[inline]
    fn stream_position(&mut self) -> Result<u64> {
        (**self).stream_position()
    }

    #[inline]
    fn stream_len(&mut self) -> Result<u64> {
        (**self).stream_len()
    }
}

impl<S: Seek + ?Sized> Seek for Box<S> {
    #[inline]
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        (**self).seek(pos)
    }

    #[inline]
    fn rewind(&mut self) -> Result<()> {
        (**self).rewind()
    }

    #[inline]
    fn stream_position(&mut self) -> Result<u64> {
        (**self).stream_position()
    }

    #[inline]
    fn stream_len(&mut self) -> Result<u64> {
        (**self).stream_len()
    }
}
