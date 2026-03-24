use crate::config::{IoFd, IocpConfig, IocpHandle};
use crate::driver::IocpDriver;
use crate::net::addr::{SockAddrStorage, socket_addr_to_storage};
use crate::net::socket::Socket;
use crate::ops::IocpOp;
use crate::tests::{remote_free_contains, wait_completion};
use std::io::Write;
use std::net::TcpListener;
use std::os::windows::io::IntoRawSocket;
use std::time::Duration;
use veloq_buf::BufPool;
use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_driver_core::driver::{Driver, SubmitBinder};
use veloq_driver_core::op::{Accept as AcceptBase, Connect as ConnectBase, Recv as RecvBase};
use veloq_driver_core::op::{IntoPlatformOp, OpLifecycle};

type Accept = AcceptBase<IocpHandle, SockAddrStorage>;
type Connect = ConnectBase<IocpHandle, SockAddrStorage>;
type Recv = RecvBase<IocpHandle>;

#[test]
fn test_iocp_accept() {
    let mut driver: IocpDriver =
        IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    // Listener (Bind to random port)
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();

    // Prepare Accept Op using OpLifecycle
    let accept_op = Accept::into_op(IocpHandle::for_socket(listener_handle as usize as _), ());

    let (iocp_kernel, accept_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(accept_op);
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
    let op = Accept::from_user_payload(
        accept_payload
            .take()
            .expect("accept payload missing on completion"),
    );
    assert!(op.remote_addr.is_some(), "Remote addr should be populated");

    // SAFETY: Closing the socket handle is required to release OS resources.
    unsafe {
        if let Some(fd) = op.fd.raw() {
            drop(Socket::from_raw(fd));
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
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);

    let connect_op = Connect {
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };

    let (iocp_kernel, _connect_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(connect_op);
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

    // SAFETY: Closing the socket handle is required to release OS resources.
    unsafe { drop(Socket::from_raw(client_handle)) };
}

#[test]
fn test_iocp_recv_with_buffer_pool() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    // Setup GlobalAlloc
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
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(connect_op);
    let mut connect_iocp_op = Some(connect_kernel);
    let connect_user_data = driver.reserve_op().unwrap().0;
    let _ = driver
        .submit(connect_user_data, &mut connect_iocp_op, SubmitBinder::new())
        .into_inner()
        .expect("submit connect failed");

    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
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
    let connect_gen = driver.ops.local[connect_user_data]
        .entry
        .platform_data
        .generation;
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
        buf_offset: 0,
    };

    let (iocp_kernel, recv_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
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

    let mut op = Recv::from_user_payload(
        recv_payload
            .take()
            .expect("recv payload missing on completion"),
    );
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

    // SAFETY: Closing the socket handle is required to release OS resources.
    unsafe { drop(Socket::from_raw(client_handle)) };
    server_thread.join().unwrap();
}

#[test]
fn test_rio_cancel_poll_returns_aborted_without_hang() {
    use std::sync::mpsc;

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
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(connect_op);
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
        buf_offset: 0,
    };
    let (iocp_kernel, _recv_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
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
    // SAFETY: Closing the socket handle is required to release OS resources.
    unsafe { drop(Socket::from_raw(client_handle)) };
}

#[test]
fn test_rio_cancel_late_completion_recycles_slot_after_drain() {
    use std::sync::mpsc;

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
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_kernel, _connect_payload) =
        IntoPlatformOp::<IocpOp>::into_kernel_and_payload(connect_op);
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
        buf_offset: 0,
    };
    let (iocp_kernel, _recv_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
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
    // SAFETY: Closing the socket handle is required to release OS resources.
    unsafe { drop(Socket::from_raw(client_handle)) };
}
