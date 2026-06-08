use crate::config::{IoFd, IocpConfig};
use crate::driver::IocpDriver;
use crate::net::addr::{SockAddrStorage, socket_addr_to_storage};
use crate::net::socket::Socket;
use crate::tests::{
    complete_from_record, completion_os_error_code, remote_free_contains, submit_test_op,
    wait_completion, wait_completion_record,
};
use std::io::Write;
use std::net::TcpListener;
use std::os::windows::io::IntoRawSocket;
use std::time::Duration;
use veloq_buf::BufPool;
use veloq_buf::{NoopRegistrar, PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_driver_core::driver::{DriveMode, Driver, RegisterFd};
use veloq_driver_core::op::{Accept, Connect, Recv};

fn register_owned_socket(driver: &mut IocpDriver, socket: Socket) -> IoFd {
    let handle = socket.into_owned_raw();
    driver
        .register_files(vec![RegisterFd::Owned(handle)])
        .expect("register socket failed")
        .into_iter()
        .next()
        .expect("register_files returned empty")
}

#[test]
fn test_iocp_accept() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar))
        .expect("Driver creation failed");

    // Listener (Bind to random port)
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();
    let listener_owned = unsafe {
        crate::OwnedRawHandle::from_raw_owned(crate::RawHandle::new(
            crate::config::IocpHandle::for_socket(listener_handle as usize as _),
        ))
    };
    let listen_fd = driver
        .register_files(vec![RegisterFd::Owned(listener_owned)])
        .expect("register listener failed")
        .into_iter()
        .next()
        .expect("register listener returned empty");

    let accept_op = Accept {
        fd: listen_fd,
        addr: SockAddrStorage::default(),
        addr_len: std::mem::size_of::<SockAddrStorage>() as u32,
        remote_addr: None,
    };

    let (user_data, generation) = submit_test_op(&mut driver, accept_op);

    // Connect Client in background
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::net::TcpStream::connect(addr).expect("Client connect failed");
    });

    let record = wait_completion_record(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    )
    .expect("Accept failed");
    let completion = complete_from_record::<Accept<SockAddrStorage>>(record);
    let (accepted, op) = completion.into_parts();
    let _accepted = accepted.expect("Accept failed");
    assert!(op.remote_addr.is_some(), "Remote addr should be populated");

    driver.unregister_files(vec![listen_fd]).unwrap();
}

#[test]
fn test_iocp_connect() {
    let mut driver: IocpDriver =
        IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    // Listener
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();

    // Client Socket
    let client = Socket::new_tcp_v4().unwrap();
    let client_fd = register_owned_socket(&mut driver, client);

    // Create Connect Op manually as it doesn't have into_op
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);

    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };

    let (user_data, generation) = submit_test_op(&mut driver, connect_op);

    let res = wait_completion(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    );
    assert!(res.is_ok(), "Connect failed: {:?}", res.err());

    driver.unregister_files(vec![client_fd]).unwrap();
}

#[test]
fn test_iocp_recv_with_buffer_pool() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    // Setup GlobalAlloc
    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);

    let global_pool = topology.create_pool(1).expect("Create pool failed");

    // Build pool with noop registrar; chunk registration is explicitly controlled below.
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    // Setup server listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Create RIO-capable client socket and connect via driver op.
    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_user_data, connect_generation) = submit_test_op(&mut driver, connect_op);

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
    let connect_res = wait_completion(
        &mut driver,
        connect_user_data,
        connect_generation,
        std::time::Duration::from_secs(5),
    );
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

    // Create Recv Op
    let recv_op = Recv {
        fd: client_fd,
        buf,
        buf_offset: 0,
    };

    let (user_data, generation) = submit_test_op(&mut driver, recv_op);

    let record = wait_completion_record(
        &mut driver,
        user_data,
        generation,
        std::time::Duration::from_secs(5),
    )
    .expect("recv completion missing");
    let completion = complete_from_record::<Recv>(record);
    let (result, mut op) = completion.into_parts();
    let bytes_read = result.expect("Recv failed");
    assert_eq!(bytes_read, 12);
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

    driver.unregister_files(vec![client_fd]).unwrap();
    server_thread.join().unwrap();
}

#[test]
fn test_unregister_owned_socket_waits_for_inflight_recv() {
    use std::sync::mpsc;

    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx_send, rx_send) = mpsc::channel::<()>();

    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"recv-after-unregister").unwrap();
    });

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_user_data, connect_generation) = submit_test_op(&mut driver, connect_op);
    wait_completion(
        &mut driver,
        connect_user_data,
        connect_generation,
        Duration::from_secs(5),
    )
    .expect("Connect failed");

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
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(&mut driver, recv_op);

    driver
        .unregister_files(vec![client_fd])
        .expect("unregister while recv in flight should defer cleanup");

    let _ = tx_send.send(());
    let record = wait_completion_record(&mut driver, user_data, generation, Duration::from_secs(5))
        .expect("recv completion missing");
    let completion = complete_from_record::<Recv>(record);
    let (result, mut op) = completion.into_parts();
    let bytes_read = result.expect("Recv failed after unregister");
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..bytes_read], b"recv-after-unregister");

    server_thread.join().unwrap();
}

#[test]
fn test_rio_cancel_poll_returns_aborted_without_hang() {
    use std::sync::mpsc;

    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    });

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_user_data, connect_generation) = submit_test_op(&mut driver, connect_op);

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
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(&mut driver, recv_op);

    let _ = driver.cancel_op(veloq_driver_core::driver::CancelRequest::user_visible(
        veloq_driver_core::driver::OpToken::new(user_data, generation),
    ));
    let _ = tx_send.send(());

    let res = wait_completion(&mut driver, user_data, generation, Duration::from_secs(5));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        completion_os_error_code(&err),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    server_thread.join().unwrap();
    driver.unregister_files(vec![client_fd]).unwrap();
}

#[test]
fn test_rio_cancel_late_completion_recycles_slot_after_drain() {
    use std::sync::mpsc;

    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar)).unwrap();

    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    });

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let (connect_user_data, connect_generation) = submit_test_op(&mut driver, connect_op);

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
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let (user_data, generation) = submit_test_op(&mut driver, recv_op);

    let _ = driver.cancel_op(veloq_driver_core::driver::CancelRequest::user_visible(
        veloq_driver_core::driver::OpToken::new(user_data, generation),
    ));

    assert!(
        !remote_free_contains(&driver, user_data),
        "取消后真实 RIO completion 到来前不应回收槽位"
    );

    let _ = tx_send.send(());

    let res = wait_completion(&mut driver, user_data, generation, Duration::from_secs(5));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        completion_os_error_code(&err),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    let drain_start = std::time::Instant::now();
    while drain_start.elapsed() < Duration::from_secs(2) {
        let _ = driver.drive(DriveMode::Poll);
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
    driver.unregister_files(vec![client_fd]).unwrap();
}
