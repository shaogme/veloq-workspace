use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use std::num::NonZeroUsize;
use veloq_buf::{nz, BufPool, PoolTopology, FixedBuf};
use veloq_runtime::{LocalExecutor, spawn_local};
use veloq_runtime::net::{tcp::LocalTcpListener, tcp::LocalTcpStream, udp::LocalUdpSocket};

fn create_local_executor() -> LocalExecutor {
    LocalExecutor::builder().build(move |registrar| {
        use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};
        let multiplier = ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(16) });
        let topology = UniformSlot::new(multiplier);
        let global_pool = topology.create_pool(1).expect("Failed to create global pool");
        topology.build(&global_pool, 0, registrar)
    })
}

async fn read_exact(stream: &LocalTcpStream, mut buf: FixedBuf, mut total_len: usize) -> FixedBuf {
    buf.set_len(0);
    while total_len > 0 {
        let (res, b) = stream.recv(buf).await;
        let n = res.unwrap();
        if n == 0 {
            panic!("Unexpected EOF");
        }
        buf = b;
        let current_len = buf.len();
        buf.set_len(current_len + n);
        total_len -= n;
    }
    buf
}

async fn write_all(stream: &LocalTcpStream, mut buf: FixedBuf, total_len: usize) -> FixedBuf {
    let mut written = 0;
    while written < total_len {
        let (res, b) = stream.send(buf).await;
        let n = res.unwrap();
        if n == 0 {
            panic!("Unexpected EOF during write");
        }
        buf = b;
        written += n;
        
        // We need to shift the unwritten part of the buffer if we had a partial write.
        // For simplicity in the benchmark since we reuse the buffer we can just assume 
        // standard send. But strictly speaking, FixedBuf doesn't have an easy way 
        // to slice the front off. However, LocalTcpStream::send sends the whole `buf`.
        // To implement a true `write_all` with FixedBuf, we'd need to shift contents.
        // For this benchmark, since send sends the *entire* valid length of `buf`, 
        // we can just shift the data to the front by `n`.
        if written < total_len {
            let remaining = buf.len() - n;
            unsafe {
                std::ptr::copy(buf.as_ptr().add(n), buf.as_mut_ptr(), remaining);
            }
            buf.set_len(remaining);
        }
    }
    buf
}

fn benchmark_tcp(c: &mut Criterion) {
    let mut group = c.benchmark_group("tcp_throughput");
    let payload_size = 4096;
    let iterations = 10000;
    group.throughput(Throughput::Bytes((payload_size * iterations) as u64));

    group.bench_function("veloq_tcp", |b| {
        let mut exec = create_local_executor();
        b.iter(|| {
            exec.block_on(async {
                let listener = LocalTcpListener::bind("127.0.0.1:0").unwrap();
                let addr = listener.local_addr().unwrap();
                
                let server = spawn_local(async move {
                    let (stream, _): (LocalTcpStream, _) = listener.accept().await.unwrap();
                    let pool = veloq_runtime::runtime::context::current_pool().unwrap();
                    for _ in 0..iterations {
                        let mut buf = pool.alloc(nz!(4096)).unwrap();
                        buf = read_exact(&stream, buf, 4096).await;
                        buf.set_len(4096); // Reset length for writing
                        buf = write_all(&stream, buf, 4096).await;
                    }
                });

                let client = spawn_local(async move {
                    let stream = LocalTcpStream::connect(addr).await.unwrap();
                    let pool = veloq_runtime::runtime::context::current_pool().unwrap();
                    for _ in 0..iterations {
                        let mut buf = pool.alloc(nz!(4096)).unwrap();
                        buf.set_len(4096);
                        buf = write_all(&stream, buf, 4096).await;
                        buf = read_exact(&stream, buf, 4096).await;
                    }
                });
                
                server.await;
                client.await;
            });
        });
    });

    group.bench_function("tokio_tcp", |b| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        b.iter(|| {
            rt.block_on(async {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                
                let server = tokio::spawn(async move {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut buf = vec![0u8; 4096];
                    for _ in 0..iterations {
                        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut buf).await.unwrap();
                        tokio::io::AsyncWriteExt::write_all(&mut stream, &buf).await.unwrap();
                    }
                });

                let client = tokio::spawn(async move {
                    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
                    let mut buf = vec![0u8; 4096];
                    for _ in 0..iterations {
                        tokio::io::AsyncWriteExt::write_all(&mut stream, &buf).await.unwrap();
                        tokio::io::AsyncReadExt::read_exact(&mut stream, &mut buf).await.unwrap();
                    }
                });
                
                server.await.unwrap();
                client.await.unwrap();
            });
        });
    });
    group.finish();
}

fn benchmark_udp(c: &mut Criterion) {
    let mut group = c.benchmark_group("udp_throughput");
    let payload_size = 4096;
    let iterations = 10000;
    group.throughput(Throughput::Bytes((payload_size * iterations) as u64));

    group.bench_function("veloq_udp", |b| {
        let mut exec = create_local_executor();
        b.iter(|| {
            exec.block_on(async {
                let server_sock = LocalUdpSocket::bind("127.0.0.1:0").unwrap();
                let server_addr = server_sock.local_addr().unwrap();
                
                let client_sock = LocalUdpSocket::bind("127.0.0.1:0").unwrap();
                let client_addr = client_sock.local_addr().unwrap();
                let _ = client_addr;

                let server = spawn_local(async move {
                    let pool = veloq_runtime::runtime::context::current_pool().unwrap();
                    for _ in 0..iterations {
                        let mut buf = pool.alloc(nz!(4096)).unwrap();
                        let datagram = server_sock.recv_stream(buf).await.unwrap();
                        let addr = datagram.addr;
                        buf = datagram.buf;
                        buf.set_len(4096);
                        let (res, _) = server_sock.send_to(buf, addr).await;
                        res.unwrap();
                    }
                });

                let client = spawn_local(async move {
                    let pool = veloq_runtime::runtime::context::current_pool().unwrap();
                    for _ in 0..iterations {
                        let mut buf = pool.alloc(nz!(4096)).unwrap();
                        buf.set_len(4096);
                        let (res, b) = client_sock.send_to(buf, server_addr).await;
                        res.unwrap();
                        buf = b;
                        let _datagram = client_sock.recv_stream(buf).await.unwrap();
                    }
                });
                
                server.await;
                client.await;
            });
        });
    });

    group.bench_function("tokio_udp", |b| {
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        b.iter(|| {
            rt.block_on(async {
                let server_sock = std::sync::Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
                let server_addr = server_sock.local_addr().unwrap();
                
                let client_sock = std::sync::Arc::new(tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap());
                let client_addr = client_sock.local_addr().unwrap();
                let _ = client_addr;

                let s = server_sock.clone();
                let server = tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    for _ in 0..iterations {
                        let (len, addr) = s.recv_from(&mut buf).await.unwrap();
                        s.send_to(&buf[..len], addr).await.unwrap();
                    }
                });

                let c = client_sock.clone();
                let client = tokio::spawn(async move {
                    let mut buf = vec![0u8; 4096];
                    for _ in 0..iterations {
                        c.send_to(&buf, server_addr).await.unwrap();
                        let (_len, _) = c.recv_from(&mut buf).await.unwrap();
                    }
                });
                
                server.await.unwrap();
                client.await.unwrap();
            });
        });
    });
    group.finish();
}

criterion_group!(benches, benchmark_tcp, benchmark_udp);
criterion_main!(benches);
