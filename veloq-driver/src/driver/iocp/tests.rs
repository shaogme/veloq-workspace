use super::ext::Extensions;
use super::*;

use crate::Socket;
use crate::config::IocpConfig;
use crate::driver::Driver;
use crate::op::{
    Accept, Connect, IntoPlatformOp, IoFd, OpLifecycle, Recv, SendTo, Timeout, UdpRecvStream,
    UdpRefill,
};
use std::net::TcpListener;
use std::os::windows::io::IntoRawSocket;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
}

#[test]
fn test_extensions_load() {
    let ext = Extensions::new();
    assert!(ext.is_ok(), "Extensions should load on Windows");
}

#[test]
fn test_driver_creation() {
    let driver: Result<IocpDriver, io::Error> = IocpDriver::new(IocpConfig::default());
    assert!(driver.is_ok(), "Driver should be created");
}

#[test]
fn test_iocp_accept() {
    let mut driver: IocpDriver =
        IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    // Listener (Bind to random port)
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();

    // Acceptor - pre-create the socket for AcceptEx
    let acceptor = Socket::new_tcp_v4().expect("Acceptor socket creation failed");
    let acceptor_handle = acceptor.into_raw();

    // Prepare Accept Op using OpLifecycle
    let mut accept_op = Accept::into_op(listener_handle.into(), acceptor_handle);
    accept_op.accept_socket = acceptor_handle;

    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(accept_op));

    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit accept failed");

    // Connect Client in background
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::net::TcpStream::connect(addr).expect("Client connect failed");
    });

    // Poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Test timed out");
        }

        driver.process_completions();

        let mut op_out = None;
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let iocp_op = op_out.unwrap();
                assert!(res.is_ok(), "Accept failed: {:?}", res.err());
                let op =
                    <Accept as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(iocp_op);
                assert!(op.remote_addr.is_some(), "Remote addr should be populated");
                unsafe {
                    if let Some(fd) = op.fd.raw() {
                        windows_sys::Win32::Foundation::CloseHandle(fd.into());
                    }
                    let s = op.accept_socket;
                    windows_sys::Win32::Foundation::CloseHandle(s.into());
                }
                break;
            }
            Poll::Pending => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn test_iocp_connect() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default()).unwrap();

    // Listener
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();

    // Client Socket
    let client = Socket::new_tcp_v4().unwrap();
    let client_handle = client.into_raw();

    // Create Connect Op manually as it doesn't have into_op
    use crate::socket_addr_to_storage;
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);

    let connect_op = Connect {
        fd: crate::op::IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };

    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(connect_op));
    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit connect failed");

    // Poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Connect Timed out");
        }
        driver.process_completions();
        let mut op_out = None;
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let _ = op_out.unwrap();
                assert!(res.is_ok(), "Connect failed: {:?}", res.err());
                unsafe { windows_sys::Win32::Foundation::CloseHandle(client_handle.into()) };
                break;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
}

#[test]
fn test_iocp_timeout() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default()).unwrap();

    let timeout_op = Timeout {
        duration: std::time::Duration::from_millis(100),
    };

    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(timeout_op));
    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit timeout failed");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        // Safety timeout
        if start.elapsed() > std::time::Duration::from_secs(1) {
            panic!("Timeout Op didn't complete in time");
        }

        driver.process_completions();

        let mut op_out = None;
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let _ = op_out.unwrap();
                assert!(res.is_ok(), "Timeout should succeed");
                let elapsed = start.elapsed();
                assert!(
                    elapsed >= std::time::Duration::from_millis(50),
                    "Should wait at least ~100ms, got {:?}",
                    elapsed
                );
                break;
            }
            Poll::Pending => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn test_iocp_recv_with_buffer_pool() {
    use veloq_buf::BufPool;

    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

    // Setup GlobalAlloc
    // 10x multiplier -> 20MB
    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);

    let global_pool = topology.create_pool(1).expect("Create pool failed");

    // Build pool with noop registrar; chunk registration is explicitly controlled below.
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    // Setup server listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Create RIO-capable client socket and connect via driver op.
    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_handle = client.into_raw();
    let (addr_storage, addr_len) = crate::socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: crate::op::IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let mut connect_iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(connect_op));
    let connect_user_data = driver.reserve_op().unwrap().0;
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op)
        .expect("submit connect failed");

    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        use std::io::Write;
        stream.write_all(b"Hello Buffer").unwrap();
    });

    // Alloc buffer
    let buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("Failed to alloc buffer");

    // Strict RIO path: ensure the exact chunk backing this buffer is registered in the driver.
    let region = buf.resolve_region_info();
    let chunk = global_pool
        .chunk_info(region.id)
        .expect("Chunk info for buffer not found");
    driver
        .register_chunk(region.id, chunk.ptr.as_ptr(), chunk.len.get())
        .expect("register chunk failed");

    // Poll connect completion before issuing recv.
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let connect_start = std::time::Instant::now();
    loop {
        if connect_start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Connect timed out");
        }
        driver.process_completions();
        let mut op_out = None;
        match driver.poll_op(connect_user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                assert!(res.is_ok(), "Connect failed: {:?}", res.err());
                let _ = op_out.unwrap();
                break;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }

    // Create Recv Op
    let recv_op = Recv {
        fd: crate::op::IoFd::Raw(client_handle),
        buf,
    };

    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit recv failed");

    // Poll
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Recv timed out");
        }
        driver.process_completions();

        let mut op_out = None;
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let iocp_op = op_out.unwrap();
                assert!(res.is_ok(), "Recv failed: {:?}", res.err());
                let bytes_read = res.unwrap();
                assert_eq!(bytes_read, 12);

                let mut op =
                    <Recv as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(iocp_op);
                op.buf.set_len(bytes_read);
                assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

                unsafe { windows_sys::Win32::Foundation::CloseHandle(client_handle.into()) };
                break;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
    server_thread.join().unwrap();
}

#[test]
fn test_rio_cancel_poll_returns_aborted_without_hang() {
    use std::io::Write;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use veloq_buf::BufPool;
    use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    });

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_handle = client.into_raw();
    let (addr_storage, addr_len) = crate::socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: crate::op::IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let mut connect_iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(connect_op));
    let connect_user_data = driver.reserve_op().unwrap().0;
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op)
        .expect("submit connect failed");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let connect_start = Instant::now();
    loop {
        if connect_start.elapsed() > Duration::from_secs(5) {
            panic!("Connect timed out");
        }
        driver.process_completions();
        let mut op_out = None;
        match driver.poll_op(connect_user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                assert!(res.is_ok(), "Connect failed: {:?}", res.err());
                let _ = op_out.unwrap();
                break;
            }
            Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
        }
    }

    let buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("Failed to alloc buffer");
    let region = buf.resolve_region_info();
    let chunk = global_pool
        .chunk_info(region.id)
        .expect("Chunk info for buffer not found");
    driver
        .register_chunk(region.id, chunk.ptr.as_ptr(), chunk.len.get())
        .expect("register chunk failed");

    let recv_op = Recv {
        fd: crate::op::IoFd::Raw(client_handle),
        buf,
    };
    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit recv failed");

    driver.cancel_op(user_data);

    let poll_start = Instant::now();
    let mut polled = false;
    while poll_start.elapsed() < Duration::from_secs(1) {
        driver.process_completions();
        let mut op_out = None;
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let _ = op_out.expect("cancelled op should still be returned");
                let err = res.expect_err("cancelled op should return aborted");
                assert_eq!(
                    err.raw_os_error(),
                    Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
                );
                polled = true;
                break;
            }
            Poll::Pending => std::thread::sleep(Duration::from_millis(5)),
        }
    }
    assert!(polled, "cancel后 poll 不应卡住");

    let _ = tx_send.send(());
    server_thread.join().unwrap();
    unsafe { windows_sys::Win32::Foundation::CloseHandle(client_handle.into()) };
}

#[test]
fn test_rio_cancel_late_completion_recycles_slot_after_drain() {
    use std::io::Write;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};
    use veloq_buf::BufPool;
    use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    });

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_handle = client.into_raw();
    let (addr_storage, addr_len) = crate::socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: crate::op::IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let mut connect_iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(connect_op));
    let connect_user_data = driver.reserve_op().unwrap().0;
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op)
        .expect("submit connect failed");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let connect_start = Instant::now();
    loop {
        if connect_start.elapsed() > Duration::from_secs(5) {
            panic!("Connect timed out");
        }
        driver.process_completions();
        let mut op_out = None;
        match driver.poll_op(connect_user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                assert!(res.is_ok(), "Connect failed: {:?}", res.err());
                let _ = op_out.unwrap();
                break;
            }
            Poll::Pending => std::thread::sleep(Duration::from_millis(10)),
        }
    }

    let buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("Failed to alloc buffer");
    let region = buf.resolve_region_info();
    let chunk = global_pool
        .chunk_info(region.id)
        .expect("Chunk info for buffer not found");
    driver
        .register_chunk(region.id, chunk.ptr.as_ptr(), chunk.len.get())
        .expect("register chunk failed");

    let recv_op = Recv {
        fd: crate::op::IoFd::Raw(client_handle),
        buf,
    };
    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (user_data, _) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op)
        .expect("submit recv failed");

    driver.cancel_op(user_data);

    let mut op_out = None;
    let res = loop {
        driver.process_completions();
        match driver.poll_op(user_data, &mut cx, &mut op_out) {
            Poll::Ready(res) => break res,
            Poll::Pending => std::thread::sleep(Duration::from_millis(5)),
        }
    };
    let _ = op_out.expect("cancelled op should still be returned");
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        err.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    assert!(
        !driver.ops.free_indices.contains(&user_data),
        "late RIO completion 前不应回收槽位"
    );

    let _ = tx_send.send(());
    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_secs(2) {
        driver.process_completions();
        if driver.ops.free_indices.contains(&user_data) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(
        driver.ops.free_indices.contains(&user_data),
        "晚到 RIO completion 到来后应回收槽位"
    );

    server_thread.join().unwrap();
    unsafe { windows_sys::Win32::Foundation::CloseHandle(client_handle.into()) };
}

#[test]
fn test_rio_extensions_load() {
    let _ext = Extensions::new().expect("RIO Extensions should load");
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path() {
    use std::time::{Duration, Instant};
    use veloq_buf::BufPool;
    use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    let server = Socket::new_udp_v4().expect("server socket create failed");
    let client = Socket::new_udp_v4().expect("client socket create failed");

    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    client
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("client bind failed");

    let server_addr = server.local_addr().expect("server local_addr failed");
    let client_addr = client.local_addr().expect("client local_addr failed");

    let server_handle = server.into_raw();
    let client_handle = client.into_raw();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    let mut send_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("send alloc failed");
    let test_data = b"rio-udp-sendto-regression";
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
    send_buf.set_len(test_data.len());

    let recv_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("recv alloc failed");

    // Register backing chunks for strict RIO path
    let send_region = send_buf.resolve_region_info();
    let send_chunk = global_pool
        .chunk_info(send_region.id)
        .expect("send chunk not found");
    driver
        .register_chunk(
            send_region.id,
            send_chunk.ptr.as_ptr(),
            send_chunk.len.get(),
        )
        .expect("register send chunk failed");

    let recv_region = recv_buf.resolve_region_info();
    if recv_region.id != send_region.id {
        let recv_chunk = global_pool
            .chunk_info(recv_region.id)
            .expect("recv chunk not found");
        driver
            .register_chunk(
                recv_region.id,
                recv_chunk.ptr.as_ptr(),
                recv_chunk.len.get(),
            )
            .expect("register recv chunk failed");
    }

    // Provide initial buffer to pool via UdpRefill
    let refill_op = UdpRefill {
        fd: IoFd::Raw(server_handle),
        buf: Some(recv_buf),
    };
    let mut refill_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(refill_op));
    let (refill_ud, _) = driver.reserve_op().expect("reserve refill op failed");
    let _ = driver
        .submit(refill_ud, &mut refill_iocp)
        .expect("submit refill failed");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Poll refill completion
    loop {
        driver.process_completions();
        let mut op_out = None;
        if let Poll::Ready(res) = driver.poll_op(refill_ud, &mut cx, &mut op_out) {
            res.expect("refill failed");
            break;
        }
    }

    let recv_op = UdpRecvStream {
        fd: IoFd::Raw(server_handle),
        buf: None,
        addr: None,
        result: None,
    };
    let send_op = SendTo {
        fd: IoFd::Raw(client_handle),
        buf: send_buf,
        addr: server_addr,
    };

    let mut recv_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (recv_ud, _) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp)
        .expect("submit udp_recv_stream failed");

    let mut send_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(send_op));
    let (send_ud, _) = driver.reserve_op().expect("reserve send op failed");
    let _ = driver
        .submit(send_ud, &mut send_iocp)
        .expect("submit send_to failed");

    let start = Instant::now();

    let mut send_done = false;
    let mut recv_done = false;

    while !(send_done && recv_done) {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("RIO UDP send_to/recv_stream regression test timed out");
        }
        driver.process_completions();

        if !send_done {
            let mut op_out = None;
            if let Poll::Ready(res) = driver.poll_op(send_ud, &mut cx, &mut op_out) {
                let sent = res.expect("send_to completion failed");
                assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
                let _ = op_out.expect("send_to op missing");
                send_done = true;
            }
        }

        if !recv_done {
            let mut op_out = None;
            if let Poll::Ready(res) = driver.poll_op(recv_ud, &mut cx, &mut op_out) {
                let bytes = res.expect("udp_recv_stream completion failed");
                let iocp_op = op_out.expect("udp_recv_stream op missing");
                let recv_out =
                    <UdpRecvStream as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(
                        iocp_op,
                    );
                let datagram = recv_out.result.expect("datagram missing in result");
                assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
                assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
                assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");
                recv_done = true;
            }
        }

        if !(send_done && recv_done) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize);
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path_ipv6() {
    use std::time::{Duration, Instant};
    use veloq_buf::BufPool;
    use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    let server = Socket::new_udp_v6().expect("server v6 socket create failed");
    let client = Socket::new_udp_v6().expect("client v6 socket create failed");

    // Some Windows environments disable IPv6 loopback. Skip gracefully in that case.
    if let Err(e) = server.bind("[::1]:0".parse().unwrap()) {
        println!("IPv6 loopback unavailable for server bind, skip: {}", e);
        return;
    }
    if let Err(e) = client.bind("[::1]:0".parse().unwrap()) {
        println!("IPv6 loopback unavailable for client bind, skip: {}", e);
        return;
    }

    let server_addr = server.local_addr().expect("server local_addr failed");
    let client_addr = client.local_addr().expect("client local_addr failed");

    let server_handle = server.into_raw();
    let client_handle = client.into_raw();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    let mut send_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("send alloc failed");
    let test_data = b"rio-udp-sendto-regression-ipv6";
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
    send_buf.set_len(test_data.len());

    let recv_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("recv alloc failed");

    let send_region = send_buf.resolve_region_info();
    let send_chunk = global_pool
        .chunk_info(send_region.id)
        .expect("send chunk not found");
    driver
        .register_chunk(
            send_region.id,
            send_chunk.ptr.as_ptr(),
            send_chunk.len.get(),
        )
        .expect("register send chunk failed");

    let recv_region = recv_buf.resolve_region_info();
    if recv_region.id != send_region.id {
        let recv_chunk = global_pool
            .chunk_info(recv_region.id)
            .expect("recv chunk not found");
        driver
            .register_chunk(
                recv_region.id,
                recv_chunk.ptr.as_ptr(),
                recv_chunk.len.get(),
            )
            .expect("register recv chunk failed");
    }

    // Provide initial buffer to pool via UdpRefill
    let refill_op = UdpRefill {
        fd: IoFd::Raw(server_handle),
        buf: Some(recv_buf),
    };
    let mut refill_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(refill_op));
    let (refill_ud, _) = driver.reserve_op().expect("reserve refill op failed");
    let _ = driver
        .submit(refill_ud, &mut refill_iocp)
        .expect("submit refill failed");

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    // Poll refill completion
    loop {
        driver.process_completions();
        let mut op_out = None;
        if let Poll::Ready(res) = driver.poll_op(refill_ud, &mut cx, &mut op_out) {
            res.expect("refill failed");
            break;
        }
    }

    let recv_op = UdpRecvStream {
        fd: IoFd::Raw(server_handle),
        buf: None,
        addr: None,
        result: None,
    };
    let send_op = SendTo {
        fd: IoFd::Raw(client_handle),
        buf: send_buf,
        addr: server_addr,
    };

    let mut recv_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (recv_ud, _) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp)
        .expect("submit udp_recv_stream failed");

    let mut send_iocp = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(send_op));
    let (send_ud, _) = driver.reserve_op().expect("reserve send op failed");
    let _ = driver
        .submit(send_ud, &mut send_iocp)
        .expect("submit send_to failed");

    let start = Instant::now();

    let mut send_done = false;
    let mut recv_done = false;

    while !(send_done && recv_done) {
        if start.elapsed() > Duration::from_secs(5) {
            panic!("RIO UDP IPv6 send_to/recv_stream regression test timed out");
        }
        driver.process_completions();

        if !send_done {
            let mut op_out = None;
            if let Poll::Ready(res) = driver.poll_op(send_ud, &mut cx, &mut op_out) {
                let sent = res.expect("send_to completion failed");
                assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
                let _ = op_out.expect("send_to op missing");
                send_done = true;
            }
        }

        if !recv_done {
            let mut op_out = None;
            if let Poll::Ready(res) = driver.poll_op(recv_ud, &mut cx, &mut op_out) {
                let bytes = res.expect("udp_recv_stream completion failed");
                let iocp_op = op_out.expect("udp_recv_stream op missing");
                let recv_out =
                    <UdpRecvStream as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(
                        iocp_op,
                    );
                let datagram = recv_out.result.expect("datagram missing in result");
                assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
                assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
                assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");
                recv_done = true;
            }
        }

        if !(send_done && recv_done) {
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize);
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}

#[test]
fn test_rio_udp_recv_pool_burst_waiters_raise_target() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_handle = server.into_raw();
    let raw_handle = server_handle.handle as windows_sys::Win32::Foundation::HANDLE;

    let mut submitted = Vec::new();
    const BURST_WAITERS: usize = 12;

    for _ in 0..BURST_WAITERS {
        let recv_op = UdpRecvStream {
            fd: IoFd::Raw(server_handle),
            buf: None,
            addr: None,
            result: None,
        };
        let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
        let (ud, _) = driver.reserve_op().expect("reserve recv op failed");
        let _ = driver
            .submit(ud, &mut iocp_op)
            .expect("submit udp_recv_stream failed");
        submitted.push(ud);
    }

    let stats = driver
        .rio_state
        .udp_pool_debug_stats(raw_handle)
        .expect("udp pool stats missing");
    assert!(
        stats.target_credits > 4,
        "burst waiters should raise target credits, stats={stats:?}"
    );
    assert!(
        stats.target_credits <= stats.max_credits,
        "target should not exceed max, stats={stats:?}"
    );
    assert_eq!(stats.waiters_len, BURST_WAITERS);

    for ud in submitted {
        driver.cancel_op(ud);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut op_out = None;
        match driver.poll_op(ud, &mut cx, &mut op_out) {
            Poll::Ready(res) => {
                let _ = op_out.expect("cancelled op should be returned");
                let err = res.expect_err("cancelled udp_recv_stream should fail");
                assert_eq!(
                    err.raw_os_error(),
                    Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
                );
            }
            Poll::Pending => panic!("cancelled waiter should be immediately pollable"),
        }
    }

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}

#[test]
fn test_rio_udp_recv_waiter_does_not_increment_outstanding_count() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_handle = server.into_raw();
    let raw_handle = server_handle.handle as windows_sys::Win32::Foundation::HANDLE;

    let recv_op = UdpRecvStream {
        fd: IoFd::Raw(server_handle),
        buf: None,
        addr: None,
        result: None,
    };
    let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
    let (ud, _) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(ud, &mut iocp_op)
        .expect("submit udp_recv_stream failed");

    let stats = driver
        .rio_state
        .udp_pool_debug_stats(raw_handle)
        .expect("udp pool stats missing");

    assert_eq!(
        driver.rio_state.outstanding_count, stats.in_flight,
        "only real in-flight RIO receives should contribute to outstanding_count; stats={stats:?}"
    );

    driver.cancel_op(ud);
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut op_out = None;
    match driver.poll_op(ud, &mut cx, &mut op_out) {
        Poll::Ready(res) => {
            let _ = op_out.expect("cancelled op should be returned");
            let err = res.expect_err("cancelled udp_recv_stream should fail");
            assert_eq!(
                err.raw_os_error(),
                Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
            );
        }
        Poll::Pending => panic!("cancelled waiter should be immediately pollable"),
    }

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}

#[test]
fn test_rio_udp_recv_pool_idle_falls_back_to_min_target() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_handle = server.into_raw();
    let raw_handle = server_handle.handle as windows_sys::Win32::Foundation::HANDLE;

    let mut submitted = Vec::new();
    const BURST_WAITERS: usize = 12;

    for _ in 0..BURST_WAITERS {
        let recv_op = UdpRecvStream {
            fd: IoFd::Raw(server_handle),
            buf: None,
            addr: None,
            result: None,
        };
        let mut iocp_op = Some(IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op));
        let (ud, _) = driver.reserve_op().expect("reserve recv op failed");
        let _ = driver
            .submit(ud, &mut iocp_op)
            .expect("submit udp_recv_stream failed");
        submitted.push(ud);
    }

    for ud in submitted {
        driver.cancel_op(ud);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut op_out = None;
        match driver.poll_op(ud, &mut cx, &mut op_out) {
            Poll::Ready(_) => {}
            Poll::Pending => panic!("cancelled waiter should be immediately pollable"),
        }
    }

    // No waiters/no queue during idle ticks -> target should decay to min.
    // Decay policy is gradual; tick until reaching min (with an upper bound).
    const MAX_IDLE_TICKS: usize = 4096;
    let mut stats = driver
        .rio_state
        .udp_pool_debug_stats(raw_handle)
        .expect("udp pool stats missing");
    for _ in 0..MAX_IDLE_TICKS {
        if stats.target_credits == stats.min_credits {
            break;
        }
        driver
            .rio_state
            .debug_tick_udp_pool_idle(raw_handle, 1, &*driver.registrar)
            .expect("idle tick failed");
        stats = driver
            .rio_state
            .udp_pool_debug_stats(raw_handle)
            .expect("udp pool stats missing");
    }
    assert_eq!(
        stats.target_credits, stats.min_credits,
        "idle should fall back to min credits, stats={stats:?}"
    );
    assert_eq!(stats.waiters_len, 0);

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}
