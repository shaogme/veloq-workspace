use super::ext::Extensions;
use super::*;

use crate::IocpConfig;
use crate::Socket;
use std::net::TcpListener;
use std::os::windows::io::IntoRawSocket;
use std::sync::atomic::Ordering;
use veloq_driver_core::driver::{Driver, SubmitBinder, encode_completion_token, event_res_to_io};
use veloq_driver_core::op::{
    Accept as AcceptBase, Connect as ConnectBase, IntoPlatformOp, OpLifecycle, Recv as RecvBase,
    SendTo as SendToBase, Timeout, UdpRecvStream as UdpRecvStreamBase, UdpRefill as UdpRefillBase,
};

type Accept = AcceptBase<crate::RawHandle, crate::SockAddrStorage>;
type Connect = ConnectBase<crate::RawHandle, crate::SockAddrStorage>;
type Recv = RecvBase<crate::RawHandle>;
type SendTo = SendToBase<crate::RawHandle>;
type UdpRecvStream = UdpRecvStreamBase<crate::RawHandle>;
type UdpRefill = UdpRefillBase<crate::RawHandle>;
type IoFd = crate::IoFd;

fn remote_free_contains(driver: &IocpDriver, needle: usize) -> bool {
    let mut cur = driver.ops.shared.remote_free_head.load(Ordering::Acquire);
    while cur
        != veloq_driver_core::slot::SlotTable::<crate::op::IocpOp, crate::op::OverlappedEntry>::NULL_INDEX
    {
        if cur == needle {
            return true;
        }
        cur = driver.ops.shared.slots[cur]
            .next_free
            .load(Ordering::Relaxed);
    }
    false
}

fn wait_completion(
    driver: &mut IocpDriver,
    user_data: usize,
    generation: u32,
    timeout: std::time::Duration,
) -> io::Result<usize> {
    let start = std::time::Instant::now();
    let token = encode_completion_token(user_data, generation);
    loop {
        if start.elapsed() > timeout {
            panic!(
                "wait completion timed out: user_data={}, generation={}",
                user_data, generation
            );
        }
        driver.process_completions();
        if let Some(ev) = driver.try_take_completion(token) {
            return event_res_to_io(ev.res);
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
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

    // Prepare Accept Op using OpLifecycle
    let accept_op = Accept::into_op((listener_handle as usize).into(), ());

    let (iocp_kernel, accept_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(accept_op);
    let mut accept_payload = Some(accept_payload);
    let mut iocp_op = Some(iocp_kernel);

    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit accept failed");

    // Connect Client in background
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::net::TcpStream::connect(addr).expect("Client connect failed");
    });

    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    );
    assert!(res.is_ok(), "Accept failed: {:?}", res.err());
    let op =
        <Accept as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
            accept_payload
                .take()
                .expect("accept payload missing on completion"),
        );
    assert!(op.remote_addr.is_some(), "Remote addr should be populated");
    unsafe {
        if let Some(fd) = op.fd.raw() {
            windows_sys::Win32::Networking::WinSock::closesocket(fd.handle as usize);
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
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };

    let (iocp_kernel, _connect_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(connect_op);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit connect failed");

    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    );
    assert!(res.is_ok(), "Connect failed: {:?}", res.err());
    unsafe { windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize) };
}

#[test]
fn test_iocp_timeout() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default()).unwrap();

    let timeout_op = Timeout {
        duration: std::time::Duration::from_millis(100),
    };

    let (iocp_kernel, _timeout_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(timeout_op);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit timeout failed");

    let start = std::time::Instant::now();
    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(1),
    );
    assert!(res.is_ok(), "Timeout should succeed");
    let elapsed = start.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_millis(50),
        "Should wait at least ~100ms, got {:?}",
        elapsed
    );
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
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(connect_op);
    let mut connect_iocp_op = Some(connect_kernel);
    let connect_user_data = driver.reserve_op().unwrap().0;
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op, SubmitBinder::new())
        .into_inner()
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
    let connect_gen = driver.ops.local[connect_user_data].platform_data.generation;
    let connect_res = wait_completion(
        &mut driver,
        connect_user_data,
        connect_gen,
        std::time::Duration::from_secs(5),
    );
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

    // Create Recv Op
    let recv_op = Recv {
        fd: IoFd::Raw(client_handle),
        buf,
    };

    let (iocp_kernel, recv_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
    let mut recv_payload = Some(recv_payload);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit recv failed");

    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    );
    assert!(res.is_ok(), "Recv failed: {:?}", res.err());
    let bytes_read = res.unwrap();
    assert_eq!(bytes_read, 12);

    let mut op =
        <Recv as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
            recv_payload
                .take()
                .expect("recv payload missing on completion"),
        );
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

    unsafe { windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize) };
    server_thread.join().unwrap();
}

#[test]
fn test_rio_cancel_poll_returns_aborted_without_hang() {
    use std::io::Write;
    use std::sync::mpsc;
    use std::time::Duration;
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
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(connect_op);
    let mut connect_iocp_op = Some(connect_kernel);
    let (connect_user_data, connect_generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit connect failed");

    let connect_res = wait_completion(
        &mut driver,
        connect_user_data,
        connect_generation,
        Duration::from_secs(5),
    );
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

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
        fd: IoFd::Raw(client_handle),
        buf,
    };
    let (iocp_kernel, _recv_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit recv failed");

    driver.cancel_op(user_data);

    let res = wait_completion(&mut driver, user_data, generation, Duration::from_secs(1));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        err.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    let _ = tx_send.send(());
    server_thread.join().unwrap();
    unsafe { windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize) };
}

#[test]
fn test_rio_cancel_late_completion_recycles_slot_after_drain() {
    use std::io::Write;
    use std::sync::mpsc;
    use std::time::Duration;
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
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(connect_op);
    let mut connect_iocp_op = Some(connect_kernel);
    let (connect_user_data, connect_generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit connect failed");

    let connect_res = wait_completion(
        &mut driver,
        connect_user_data,
        connect_generation,
        Duration::from_secs(5),
    );
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

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
        fd: IoFd::Raw(client_handle),
        buf,
    };
    let (iocp_kernel, _recv_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
    let mut iocp_op = Some(iocp_kernel);
    let (user_data, generation) = driver.reserve_op().unwrap();
    let _ = driver
        .submit(user_data, &mut iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit recv failed");

    driver.cancel_op(user_data);

    let res = wait_completion(&mut driver, user_data, generation, Duration::from_secs(1));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        err.raw_os_error(),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    assert!(
        remote_free_contains(&driver, user_data),
        "取消完成后应立即回收槽位"
    );

    let _ = tx_send.send(());
    let drain_start = std::time::Instant::now();
    while drain_start.elapsed() < Duration::from_secs(2) {
        driver.process_completions();
        if remote_free_contains(&driver, user_data) {
            break;
        }
        std::thread::sleep(Duration::from_millis(5));
    }

    assert!(
        remote_free_contains(&driver, user_data),
        "晚到 RIO completion 到来后槽位仍应保持可复用"
    );

    server_thread.join().unwrap();
    unsafe { windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize) };
}

#[test]
fn test_rio_extensions_load() {
    let _ext = Extensions::new().expect("RIO Extensions should load");
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path() {
    use std::time::Duration;
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
    let (refill_kernel, _refill_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(refill_op);
    let mut refill_iocp = Some(refill_kernel);
    let (refill_ud, refill_gen) = driver.reserve_op().expect("reserve refill op failed");
    let _ = driver
        .submit(refill_ud, &mut refill_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit refill failed");

    wait_completion(&mut driver, refill_ud, refill_gen, Duration::from_secs(5))
        .expect("refill failed");

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

    let (recv_kernel, recv_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
    let mut recv_payload = Some(recv_payload);
    let mut recv_iocp = Some(recv_kernel);
    let (recv_ud, recv_gen) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit udp_recv_stream failed");

    let (send_kernel, _send_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(send_op);
    let mut send_iocp = Some(send_kernel);
    let (send_ud, send_gen) = driver.reserve_op().expect("reserve send op failed");
    let _ = driver
        .submit(send_ud, &mut send_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit send_to failed");

    let sent = wait_completion(&mut driver, send_ud, send_gen, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let bytes = wait_completion(&mut driver, recv_ud, recv_gen, Duration::from_secs(5))
        .expect("udp_recv_stream completion failed");
    let recv_out = <UdpRecvStream as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
        recv_payload
            .take()
            .expect("udp_recv_stream payload missing on completion"),
    );
    let datagram = recv_out.result.expect("datagram missing in result");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
    assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");

    unsafe {
        windows_sys::Win32::Networking::WinSock::closesocket(client_handle.handle as usize);
        windows_sys::Win32::Networking::WinSock::closesocket(server_handle.handle as usize);
    }
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path_ipv6() {
    use std::time::Duration;
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
    let (refill_kernel, _refill_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(refill_op);
    let mut refill_iocp = Some(refill_kernel);
    let (refill_ud, refill_gen) = driver.reserve_op().expect("reserve refill op failed");
    let _ = driver
        .submit(refill_ud, &mut refill_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit refill failed");

    wait_completion(&mut driver, refill_ud, refill_gen, Duration::from_secs(5))
        .expect("refill failed");

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

    let (recv_kernel, recv_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
    let mut recv_payload = Some(recv_payload);
    let mut recv_iocp = Some(recv_kernel);
    let (recv_ud, recv_gen) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit udp_recv_stream failed");

    let (send_kernel, _send_payload) =
        IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(send_op);
    let mut send_iocp = Some(send_kernel);
    let (send_ud, send_gen) = driver.reserve_op().expect("reserve send op failed");
    let _ = driver
        .submit(send_ud, &mut send_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit send_to failed");

    let sent = wait_completion(&mut driver, send_ud, send_gen, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let bytes = wait_completion(&mut driver, recv_ud, recv_gen, Duration::from_secs(5))
        .expect("udp_recv_stream completion failed");
    let recv_out = <UdpRecvStream as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
        recv_payload
            .take()
            .expect("udp_recv_stream payload missing on completion"),
    );
    let datagram = recv_out.result.expect("datagram missing in result");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
    assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");

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
        let (iocp_kernel, recv_payload) =
            IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
        let mut iocp_op = Some(iocp_kernel);
        let (ud, generation) = driver.reserve_op().expect("reserve recv op failed");
        let _ = driver
            .submit(ud, &mut iocp_op, SubmitBinder::new())
            .into_inner()
            .expect("submit udp_recv_stream failed");
        submitted.push((ud, generation, recv_payload));
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

    for (ud, generation, recv_payload) in submitted {
        driver.cancel_op(ud);
        let _ = <UdpRecvStream as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
            recv_payload,
        );
        let res = wait_completion(
            &mut driver,
            ud,
            generation,
            std::time::Duration::from_secs(1),
        );
        let err = res.expect_err("cancelled udp_recv_stream should fail");
        assert_eq!(
            err.raw_os_error(),
            Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
        );
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
        let (iocp_kernel, recv_payload) =
            IntoPlatformOp::<crate::op::IocpOp>::into_kernel_and_payload(recv_op);
        let mut iocp_op = Some(iocp_kernel);
        let (ud, generation) = driver.reserve_op().expect("reserve recv op failed");
        let _ = driver
            .submit(ud, &mut iocp_op, SubmitBinder::new())
            .into_inner()
            .expect("submit udp_recv_stream failed");
        submitted.push((ud, generation, recv_payload));
    }

    for (ud, generation, recv_payload) in submitted {
        driver.cancel_op(ud);
        let _ = <UdpRecvStream as veloq_driver_core::op::IntoPlatformOp<crate::op::IocpOp>>::from_user_payload(
            recv_payload,
        );
        let _ = wait_completion(
            &mut driver,
            ud,
            generation,
            std::time::Duration::from_secs(1),
        );
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
