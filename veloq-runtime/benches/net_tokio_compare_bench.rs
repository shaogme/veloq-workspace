use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::io;
use std::num::NonZeroUsize;
use std::time::Duration;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{
    TcpListener as TokioTcpListener, TcpStream as TokioTcpStream, UdpSocket as TokioUdpSocket,
};
use veloq_buf::nz;
use veloq_runtime::config::Config;
use veloq_runtime::io::{AsyncBufRead, AsyncBufWrite};
use veloq_runtime::net::{TcpListener, TcpStream, UdpSocket};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::spawn;
use veloq_runtime::yield_now;

const PAYLOAD_SIZE: usize = 8192;
const ROUNDS: usize = 2048;

fn alloc_fixed(size: NonZeroUsize) -> veloq_buf::FixedBuf {
    let mut buf = veloq_runtime::runtime::context::alloc(size);
    buf.set_len(size.get());
    buf
}

async fn run_veloq_tcp_roundtrip(payload_size: NonZeroUsize, rounds: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind veloq tcp listener failed");
    let listen_addr = listener
        .local_addr()
        .expect("get veloq tcp listen addr failed");

    let server_h = spawn(async move {
        let (stream, _) = listener.accept().await.expect("veloq tcp accept failed");
        let mut io_buf = alloc_fixed(payload_size);
        for _ in 0..rounds {
            let (_, buf) = stream
                .read_exact(io_buf)
                .await
                .expect("veloq tcp server read_exact failed");
            io_buf = buf;

            let (_, buf) = stream
                .write_all(io_buf)
                .await
                .expect("veloq tcp server write_all failed");
            io_buf = buf;
        }
    });

    let stream = TcpStream::connect(listen_addr)
        .await
        .expect("veloq tcp connect failed");

    let mut write_buf = alloc_fixed(payload_size);
    for b in write_buf.as_slice_mut() {
        *b = 0x5A;
    }
    let mut read_buf = alloc_fixed(payload_size);

    for _ in 0..rounds {
        let (_, buf) = stream
            .write_all(write_buf)
            .await
            .expect("veloq tcp client write_all failed");
        write_buf = buf;

        let (_, buf) = stream
            .read_exact(read_buf)
            .await
            .expect("veloq tcp client read_exact failed");
        read_buf = buf;
    }

    server_h.await;
}

async fn run_veloq_tcp_roundtrip_reuse_socket(
    payload_size: NonZeroUsize,
    rounds_per_iter: usize,
    iters: u64,
) {
    let total_rounds = rounds_per_iter
        .checked_mul(iters as usize)
        .expect("veloq tcp total rounds overflow");
    run_veloq_tcp_roundtrip(payload_size, total_rounds).await;
}

async fn run_tokio_tcp_roundtrip(payload_size: usize, rounds: usize) {
    let listener = TokioTcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind tokio tcp listener failed");
    let listen_addr = listener
        .local_addr()
        .expect("get tokio tcp listen addr failed");

    let server_h = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.expect("tokio tcp accept failed");
        let mut io_buf = vec![0_u8; payload_size];
        for _ in 0..rounds {
            stream
                .read_exact(&mut io_buf)
                .await
                .expect("tokio tcp server read_exact failed");
            stream
                .write_all(&io_buf)
                .await
                .expect("tokio tcp server write_all failed");
        }
    });

    let mut stream = TokioTcpStream::connect(listen_addr)
        .await
        .expect("tokio tcp connect failed");

    let write_buf = vec![0x5A_u8; payload_size];
    let mut read_buf = vec![0_u8; payload_size];

    for _ in 0..rounds {
        stream
            .write_all(&write_buf)
            .await
            .expect("tokio tcp client write_all failed");
        stream
            .read_exact(&mut read_buf)
            .await
            .expect("tokio tcp client read_exact failed");
    }

    server_h.await.expect("tokio tcp server join failed");
}

async fn run_tokio_tcp_roundtrip_reuse_socket(
    payload_size: usize,
    rounds_per_iter: usize,
    iters: u64,
) {
    let total_rounds = rounds_per_iter
        .checked_mul(iters as usize)
        .expect("tokio tcp total rounds overflow");
    run_tokio_tcp_roundtrip(payload_size, total_rounds).await;
}

async fn tokio_udp_read_exact(socket: &TokioUdpSocket, buf: &mut [u8]) -> io::Result<()> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = socket.recv(&mut buf[total..]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "failed to fill whole buffer",
            ));
        }
        total += n;
    }
    Ok(())
}

async fn tokio_udp_write_all(socket: &TokioUdpSocket, buf: &[u8]) -> io::Result<()> {
    let mut total = 0usize;
    while total < buf.len() {
        let n = socket.send(&buf[total..]).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write whole buffer",
            ));
        }
        total += n;
    }
    Ok(())
}

async fn run_veloq_udp_roundtrip(payload_size: NonZeroUsize, rounds: usize) {
    let server = UdpSocket::bind("127.0.0.1:0").expect("bind veloq udp server failed");
    let client = UdpSocket::bind("127.0.0.1:0").expect("bind veloq udp client failed");
    let server_addr = server
        .local_addr()
        .expect("get veloq udp server addr failed");
    let client_addr = client
        .local_addr()
        .expect("get veloq udp client addr failed");

    // Prime UDP recv credits on RIO path to avoid first-packet timing windows.
    server
        .recv_ready(payload_size, 8)
        .await
        .expect("veloq udp server recv_ready failed");
    client
        .recv_ready(payload_size, 8)
        .await
        .expect("veloq udp client recv_ready failed");

    server
        .connect(client_addr)
        .await
        .expect("veloq udp server connect failed");
    client
        .connect(server_addr)
        .await
        .expect("veloq udp client connect failed");

    let server_h = spawn(async move {
        let mut io_buf = alloc_fixed(payload_size);
        for _ in 0..rounds {
            let (_, buf) = server
                .read_exact(io_buf)
                .await
                .expect("veloq udp server read_exact failed");
            io_buf = buf;

            let (_, buf) = server
                .write_all(io_buf)
                .await
                .expect("veloq udp server write_all failed");
            io_buf = buf;
        }
    });

    // Let server task run once before the first client send.
    yield_now().await;

    let mut write_buf = alloc_fixed(payload_size);
    for b in write_buf.as_slice_mut() {
        *b = 0xA5;
    }
    let mut read_buf = alloc_fixed(payload_size);

    for _ in 0..rounds {
        let (_, buf) = client
            .write_all(write_buf)
            .await
            .expect("veloq udp client write_all failed");
        write_buf = buf;

        let (_, buf) = client
            .read_exact(read_buf)
            .await
            .expect("veloq udp client read_exact failed");
        read_buf = buf;
    }

    server_h.await;
}

async fn run_veloq_udp_roundtrip_reuse_socket(
    payload_size: NonZeroUsize,
    rounds_per_iter: usize,
    iters: u64,
) {
    let total_rounds = rounds_per_iter
        .checked_mul(iters as usize)
        .expect("veloq udp total rounds overflow");
    run_veloq_udp_roundtrip(payload_size, total_rounds).await;
}

async fn run_tokio_udp_roundtrip(payload_size: usize, rounds: usize) {
    let server = TokioUdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind tokio udp server failed");
    let client = TokioUdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind tokio udp client failed");
    let server_addr = server
        .local_addr()
        .expect("get tokio udp server addr failed");
    let client_addr = client
        .local_addr()
        .expect("get tokio udp client addr failed");

    server
        .connect(client_addr)
        .await
        .expect("tokio udp server connect failed");
    client
        .connect(server_addr)
        .await
        .expect("tokio udp client connect failed");

    let server_h = tokio::spawn(async move {
        let mut io_buf = vec![0_u8; payload_size];
        for _ in 0..rounds {
            tokio_udp_read_exact(&server, &mut io_buf)
                .await
                .expect("tokio udp server read_exact failed");
            tokio_udp_write_all(&server, &io_buf)
                .await
                .expect("tokio udp server write_all failed");
        }
    });

    let write_buf = vec![0xA5_u8; payload_size];
    let mut read_buf = vec![0_u8; payload_size];

    for _ in 0..rounds {
        tokio_udp_write_all(&client, &write_buf)
            .await
            .expect("tokio udp client write_all failed");
        tokio_udp_read_exact(&client, &mut read_buf)
            .await
            .expect("tokio udp client read_exact failed");
    }

    server_h.await.expect("tokio udp server join failed");
}

async fn run_tokio_udp_roundtrip_reuse_socket(
    payload_size: usize,
    rounds_per_iter: usize,
    iters: u64,
) {
    let total_rounds = rounds_per_iter
        .checked_mul(iters as usize)
        .expect("tokio udp total rounds overflow");
    run_tokio_udp_roundtrip(payload_size, total_rounds).await;
}

fn benchmark_tcp(c: &mut Criterion) {
    let mut group = c.benchmark_group("net_tcp_roundtrip");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));

    let payload_nz = nz!(PAYLOAD_SIZE);
    let total_bytes = (PAYLOAD_SIZE as u64) * (ROUNDS as u64);
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_with_input(
        BenchmarkId::new("veloq", PAYLOAD_SIZE),
        &PAYLOAD_SIZE,
        |b, _| {
            b.iter_custom(|iters| {
                let runtime = Runtime::builder()
                    .config(Config::default().worker_threads(1))
                    .build()
                    .expect("build veloq runtime failed");

                let start = Instant::now();
                runtime.block_on(run_veloq_tcp_roundtrip_reuse_socket(
                    payload_nz, ROUNDS, iters,
                ));
                start.elapsed()
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("tokio", PAYLOAD_SIZE),
        &PAYLOAD_SIZE,
        |b, _| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime failed");
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(run_tokio_tcp_roundtrip_reuse_socket(
                    PAYLOAD_SIZE,
                    ROUNDS,
                    iters,
                ));
                start.elapsed()
            });
        },
    );

    group.finish();
}

fn benchmark_udp(c: &mut Criterion) {
    let mut group = c.benchmark_group("net_udp_roundtrip");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(20));

    let payload_nz = nz!(PAYLOAD_SIZE);
    let total_bytes = (PAYLOAD_SIZE as u64) * (ROUNDS as u64);
    group.throughput(Throughput::Bytes(total_bytes));

    group.bench_with_input(
        BenchmarkId::new("veloq", PAYLOAD_SIZE),
        &PAYLOAD_SIZE,
        |b, _| {
            b.iter_custom(|iters| {
                let runtime = Runtime::builder()
                    .config(Config::default().worker_threads(1))
                    .build()
                    .expect("build veloq runtime failed");

                let start = Instant::now();
                runtime.block_on(run_veloq_udp_roundtrip_reuse_socket(
                    payload_nz, ROUNDS, iters,
                ));
                start.elapsed()
            });
        },
    );

    group.bench_with_input(
        BenchmarkId::new("tokio", PAYLOAD_SIZE),
        &PAYLOAD_SIZE,
        |b, _| {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build tokio runtime failed");
            b.iter_custom(|iters| {
                let start = Instant::now();
                runtime.block_on(run_tokio_udp_roundtrip_reuse_socket(
                    PAYLOAD_SIZE,
                    ROUNDS,
                    iters,
                ));
                start.elapsed()
            });
        },
    );

    group.finish();
}

criterion_group!(benches, benchmark_tcp, benchmark_udp);
criterion_main!(benches);
