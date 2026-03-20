#![cfg(feature = "compat")]
use futures::{AsyncReadExt, AsyncWriteExt};
use std::alloc::{Layout, alloc, dealloc};
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use veloq_buf::{DeallocParams, FixedBuf, PoolVTable, RegionInfo};
use veloq_runtime::io::{AsyncBufRead, AsyncBufWrite, Compat};

// --- Mock Pool ---
struct MockPool;

static MOCK_VTABLE: PoolVTable = PoolVTable {
    dealloc: mock_dealloc,
    resolve_region_info: mock_resolve,
};

unsafe fn mock_dealloc(_pool_data: NonNull<()>, params: DeallocParams) {
    let layout = Layout::from_size_align(params.cap.get(), 1).unwrap();
    unsafe { dealloc(params.ptr.as_ptr(), layout) };
}

unsafe fn mock_resolve(_pool_data: NonNull<()>, _buf: &FixedBuf) -> RegionInfo {
    RegionInfo {
        id: 0,
        offset: 0,
        cookie: 0,
    }
}

impl MockPool {
    fn alloc(size: usize) -> FixedBuf {
        let size = NonZeroUsize::new(size).unwrap();
        let layout = Layout::from_size_align(size.get(), 1).unwrap();
        let ptr = unsafe { alloc(layout) };
        let ptr = NonNull::new(ptr).unwrap();
        unsafe { FixedBuf::new(ptr, size, NonNull::dangling(), &MOCK_VTABLE, 0) }
    }
}

// --- Mock IO ---
#[derive(Clone)]
struct MockIo {
    read_data: Arc<Vec<u8>>,
    write_data: Arc<Mutex<Vec<u8>>>,
    read_pos: Arc<Mutex<usize>>,
}

impl MockIo {
    fn new(data: Vec<u8>) -> Self {
        Self {
            read_data: Arc::new(data),
            write_data: Arc::new(Mutex::new(Vec::new())),
            read_pos: Arc::new(Mutex::new(0)),
        }
    }
}

impl AsyncBufRead for MockIo {
    fn read(&self, mut buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>> {
        let read_data = self.read_data.clone();
        let read_pos = self.read_pos.clone();
        async move {
            let mut pos = read_pos.lock().unwrap();
            let avail = read_data.len().saturating_sub(*pos);
            let cap = buf.capacity();
            let n = std::cmp::min(avail, cap);

            {
                let dest = buf.spare_capacity_mut();
                dest[..n].copy_from_slice(&read_data[*pos..*pos + n]);
            }
            *pos += n;

            if n > 0 {
                buf.set_len(n);
            }

            Ok((n, buf))
        }
    }

    fn read_exact(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>> {
        // Mock read already fills as much as possible, for simplicity in test:
        self.read(buf)
    }
}

impl AsyncBufWrite for MockIo {
    fn write(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>> {
        let write_data = self.write_data.clone();
        async move {
            let n = buf.len();
            let mut data = write_data.lock().unwrap();
            data.extend_from_slice(buf.as_slice());
            Ok((n, buf))
        }
    }

    fn write_all(&self, buf: FixedBuf) -> impl Future<Output = io::Result<(usize, FixedBuf)>> {
        self.write(buf)
    }

    fn flush(&self) -> impl Future<Output = io::Result<()>> {
        async { Ok(()) }
    }

    fn shutdown(&self) -> impl Future<Output = io::Result<()>> {
        async { Ok(()) }
    }
}

#[test]
fn test_compat_read() {
    futures::executor::block_on(async {
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let mock = MockIo::new(data.clone());
        let buf = MockPool::alloc(4); // Small buffer to force multiple reads (8 bytes / 4 = 2 reads)
        let mut compat = Compat::new(mock, buf);

        let mut output = Vec::new();
        let n = compat.read_to_end(&mut output).await.unwrap();

        assert_eq!(n, 8);
        assert_eq!(output, data);
    });
}

#[test]
fn test_compat_write() {
    futures::executor::block_on(async {
        let data = vec![1u8, 2, 3, 4, 5, 6, 7, 8];
        let mock = MockIo::new(Vec::new());
        let buf = MockPool::alloc(4); // Small buffer (2 writes)
        let mut compat = Compat::new(mock.clone(), buf);

        compat.write_all(&data).await.unwrap();
        compat.flush().await.unwrap();

        let written = mock.write_data.lock().unwrap().clone();
        assert_eq!(written, data);
    });
}
