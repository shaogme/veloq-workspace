use std::num::NonZeroUsize;
use std::time::Instant;
use veloq_runtime::net::UdpSocket;
use veloq_runtime::runtime::Runtime;

fn main() {
    let rt = Runtime::builder().build().unwrap();

    rt.block_on(async {
        let addr = "127.0.0.1:0";
        let socket = UdpSocket::bind(addr).unwrap();
        let capacity = NonZeroUsize::new(1024).unwrap();

        // 1. Warm up
        for _ in 0..10 {
            socket.recv_ready(capacity, 100).await.unwrap();
        }

        // 2. Measure
        let start = Instant::now();
        for _ in 0..100 {
            socket.recv_ready(capacity, 100).await.unwrap();
        }
        let elapsed = start.elapsed();
        println!("Elapsed for 100 recv_ready (10000 buffers): {:?}", elapsed);
    });
}
