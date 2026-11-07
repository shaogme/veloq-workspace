use crate::{
    OwnedRawHandle, RawHandle, SockAddrStorage, Socket,
    config::{IoFd, IocpConfig, IocpHandle},
    driver::IocpDriver,
    op::{Accept, AcceptMulti, Connect, Recv, RecvMulti, RecvProvided},
    socket_addr_to_storage,
    tests::TestSupport,
};
use veloq_buf::{BufPool, LockedMemoryBudget, UniformSlot};
use veloq_driver_core::{CancelRequest, DriveMode, Driver, RegisterFd};
use veloq_std::{
    io::Write,
    mem,
    net::{TcpListener, TcpStream},
    nz,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
    vec,
};
use windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED;

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
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    // Listener (Bind to random port)
    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();
    let listener_owned = unsafe {
        OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(
            listener_handle as usize as _,
        )))
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
        addr_len: mem::size_of::<SockAddrStorage>() as u32,
        remote_addr: None,
    };

    let token = TestSupport::submit_test_op(&mut driver, accept_op);

    // Connect Client in background
    let _ = thread::spawn(move || {
        let _ = thread::sleep(Duration::from_millis(50));
        TcpStream::connect(addr).expect("Client connect failed");
    });

    let record = TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
        .expect("Accept failed");
    let completion = TestSupport::complete_from_record::<Accept>(record);
    let (accepted, op) = completion.into_parts();
    let _accepted = accepted.expect("Accept failed");
    assert!(op.remote_addr.is_some(), "Remote addr should be populated");

    driver.unregister_files(vec![listen_fd]).unwrap();
}

#[test]
fn test_iocp_accept_multi_rearms_after_each_connection() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();
    let listener_owned = unsafe {
        OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(
            listener_handle as usize as _,
        )))
    };
    let listen_fd = driver
        .register_files(vec![RegisterFd::Owned(listener_owned)])
        .expect("register listener failed")
        .into_iter()
        .next()
        .expect("register listener returned empty");

    let token = TestSupport::submit_test_op(&mut driver, AcceptMulti { fd: listen_fd });

    let first_client = thread::spawn(move || {
        let _ = thread::sleep(Duration::from_millis(50));
        TcpStream::connect(addr).expect("first client connect failed");
    })
    .expect("first client thread spawn failed");
    let first_record =
        TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
            .expect("first AcceptMulti completion missing");
    assert!(
        first_record.continuation().is_more(),
        "AcceptMulti must remain armed after the first completion"
    );
    let first_completion = TestSupport::complete_from_record::<AcceptMulti>(first_record);
    let (first_result, _first_item) = first_completion.into_parts();
    let _first_accepted = first_result.expect("first AcceptMulti result failed");
    first_client.join().expect("first client thread panicked");

    let second_client = thread::spawn(move || {
        TcpStream::connect(addr).expect("second client connect failed");
    })
    .expect("second client thread spawn failed");
    let second_record =
        TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
            .expect("second AcceptMulti completion missing");
    assert!(
        second_record.continuation().is_more(),
        "AcceptMulti replacement must remain armed after the second completion"
    );
    let second_completion = TestSupport::complete_from_record::<AcceptMulti>(second_record);
    let (second_result, _second_item) = second_completion.into_parts();
    let _second_accepted = second_result.expect("second AcceptMulti result failed");
    second_client.join().expect("second client thread panicked");

    let _ = driver.cancel_op(CancelRequest::user_visible(token));
    let cancel_result = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    assert!(
        cancel_result.is_err(),
        "cancelled AcceptMulti must produce a terminal error"
    );

    driver.unregister_files(vec![listen_fd]).unwrap();
}

#[test]
fn test_iocp_accept_multi_cancel_does_not_rearm_after_completion_race() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");

    let std_listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = std_listener.local_addr().unwrap();
    let listener_handle = std_listener.into_raw_socket();
    let listener_owned = unsafe {
        OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(
            listener_handle as usize as _,
        )))
    };
    let listen_fd = driver
        .register_files(vec![RegisterFd::Owned(listener_owned)])
        .expect("register listener failed")
        .into_iter()
        .next()
        .expect("register listener returned empty");

    let token = TestSupport::submit_test_op(&mut driver, AcceptMulti { fd: listen_fd });
    let client = thread::spawn(move || {
        let _stream = TcpStream::connect(addr).expect("client connect failed");
        let _ = thread::sleep(Duration::from_millis(100));
    })
    .expect("client thread spawn failed");

    // Let the AcceptEx completion reach the kernel completion queue before cancellation. Depending
    // on timing, CancelIoEx reports either Submitted or ERROR_NOT_FOUND; both paths must settle
    // the logical stream without arming another AcceptEx request.
    let _ = thread::sleep(Duration::from_millis(100));
    let _ = driver.cancel_op(CancelRequest::user_visible(token));
    let result = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    assert!(
        result.is_err(),
        "cancelled AcceptMulti completion must not be delivered as a record"
    );

    client.join().expect("client thread panicked");
    driver.unregister_files(vec![listen_fd]).unwrap();
}

#[test]
fn test_iocp_connect() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

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

    let token = TestSupport::submit_test_op(&mut driver, connect_op);

    let res = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    assert!(res.is_ok(), "Connect failed: {:?}", res.err());

    driver.unregister_files(vec![client_fd]).unwrap();
}

#[test]
fn test_iocp_recv_with_buffer_pool() {
    let budget = LockedMemoryBudget::new(64 * 1024 * 1024);
    let mut driver =
        IocpDriver::new(IocpConfig::default().locked_memory_budget(budget.clone())).unwrap();

    // Setup GlobalAlloc
    let pool_capacity_bytes = 10 * 4 * 1024 * 1024;
    let topology = UniformSlot::new(pool_capacity_bytes);

    // Build a pool whose chunks are registered before allocation.
    let reg_pool = TestSupport::build_registered_pool(&mut driver, &topology, budget, 0);

    // Setup server listener
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    // Create RIO-capable client socket and connect via driver op.
    let client = Socket::new_rio_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let connect_token = TestSupport::submit_test_op(&mut driver, connect_op);

    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"Hello Buffer").unwrap();
    })
    .unwrap();

    // Alloc buffer
    let buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("Failed to alloc buffer");

    // Poll connect completion before issuing recv.
    let connect_res =
        TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5));
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

    let token = TestSupport::submit_test_op(&mut driver, recv_op);

    let record = TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
        .expect("recv completion missing");
    let completion = TestSupport::complete_from_record::<Recv>(record);
    let (result, mut op) = completion.into_parts();
    let bytes_read = result.expect("Recv failed");
    assert_eq!(bytes_read, 12);
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

    driver.unregister_files(vec![client_fd]).unwrap();
    server_thread.join().unwrap();
}

#[test]
fn test_iocp_recv_provided_uses_backend_owned_buffer() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"Hello Provided").unwrap();
    })
    .unwrap();

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_token = TestSupport::submit_test_op(
        &mut driver,
        Connect {
            fd: client_fd,
            addr: addr_storage,
            addr_len: addr_len as u32,
        },
    );
    TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5))
        .expect("Connect failed");

    let token = TestSupport::submit_test_op(&mut driver, RecvProvided { fd: client_fd });
    let record = TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
        .expect("RecvProvided completion missing");
    let completion = TestSupport::complete_from_record::<RecvProvided>(record);
    let (result, provided) = completion.into_parts();
    let received = result.expect("RecvProvided failed");
    let buffer = provided
        .buf
        .expect("successful RecvProvided completion must carry a buffer");
    assert_eq!(received, 14);
    assert_eq!(buffer.len(), received);
    assert_eq!(buffer.as_slice(), b"Hello Provided");

    driver.unregister_files(vec![client_fd]).unwrap();
    server_thread.join().unwrap();
}

#[test]
fn test_iocp_recv_multi_delivers_data_and_eof() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.write_all(b"Hello Multi").unwrap();
    })
    .unwrap();

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_token = TestSupport::submit_test_op(
        &mut driver,
        Connect {
            fd: client_fd,
            addr: addr_storage,
            addr_len: addr_len as u32,
        },
    );
    TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5))
        .expect("Connect failed");

    let token = TestSupport::submit_test_op(&mut driver, RecvMulti { fd: client_fd });
    let data_record =
        TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
            .expect("RecvMulti data completion missing");
    assert!(
        data_record.continuation().is_more(),
        "RecvMulti must rearm after a data completion"
    );
    let data_completion = TestSupport::complete_from_record::<RecvMulti>(data_record);
    let (data_result, data_payload) = data_completion.into_parts();
    let data_len = data_result.expect("RecvMulti data completion failed");
    let data_buffer = data_payload
        .buf
        .expect("RecvMulti data completion must carry a buffer");
    assert_eq!(data_len, b"Hello Multi".len());
    assert_eq!(data_buffer.as_slice(), b"Hello Multi");

    let eof_record =
        TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
            .expect("RecvMulti EOF completion missing");
    assert!(
        eof_record.continuation().is_final(),
        "RecvMulti EOF must terminate the logical receive operation"
    );
    let eof_completion = TestSupport::complete_from_record::<RecvMulti>(eof_record);
    let (eof_result, eof_payload) = eof_completion.into_parts();
    assert_eq!(eof_result.expect("RecvMulti EOF completion failed"), 0);
    assert_eq!(
        eof_payload
            .buf
            .expect("RecvMulti EOF completion must carry a terminal buffer")
            .len(),
        0
    );

    driver.unregister_files(vec![client_fd]).unwrap();
    server_thread.join().unwrap();
}

#[test]
fn test_iocp_recv_multi_cancel_returns_aborted() {
    let mut driver = IocpDriver::new(IocpConfig::default()).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx_release, rx_release) = mpsc::channel::<()>();
    let server_thread = thread::spawn(move || {
        let (_stream, _) = listener.accept().unwrap();
        let _ = rx_release.recv();
    })
    .unwrap();

    let client = Socket::new_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_token = TestSupport::submit_test_op(
        &mut driver,
        Connect {
            fd: client_fd,
            addr: addr_storage,
            addr_len: addr_len as u32,
        },
    );
    TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5))
        .expect("Connect failed");

    let token = TestSupport::submit_test_op(&mut driver, RecvMulti { fd: client_fd });
    driver
        .cancel_op(CancelRequest::user_visible(token))
        .expect("RecvMulti cancellation request failed");
    let _ = tx_release.send(());

    let result = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    let error = result.expect_err("cancelled RecvMulti should return an error");
    assert_eq!(
        TestSupport::completion_os_error_code(&error),
        Some(ERROR_OPERATION_ABORTED as i32)
    );

    server_thread.join().unwrap();
    driver.unregister_files(vec![client_fd]).unwrap();
}

#[test]
fn test_unregister_owned_socket_waits_for_inflight_recv() {
    TestSupport::init_test_logger();
    let budget = LockedMemoryBudget::new(64 * 1024 * 1024);
    let mut driver =
        IocpDriver::new(IocpConfig::default().locked_memory_budget(budget.clone())).unwrap();

    let pool_capacity_bytes = 10 * 4 * 1024 * 1024;
    let topology = UniformSlot::new(pool_capacity_bytes);
    let reg_pool = TestSupport::build_registered_pool(&mut driver, &topology, budget, 0);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx_send, rx_send) = mpsc::channel::<()>();

    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"recv-after-unregister").unwrap();
    })
    .unwrap();

    let client = Socket::new_rio_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let connect_token = TestSupport::submit_test_op(&mut driver, connect_op);
    TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5))
        .expect("Connect failed");

    let buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("Failed to alloc buffer");
    let recv_op = Recv {
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let token = TestSupport::submit_test_op(&mut driver, recv_op);

    driver
        .unregister_files(vec![client_fd])
        .expect("unregister while recv in flight should defer cleanup");

    let _ = tx_send.send(());
    let record = TestSupport::wait_completion_record(&mut driver, token, Duration::from_secs(5))
        .expect("recv completion missing");
    let completion = TestSupport::complete_from_record::<Recv>(record);
    let (result, mut op) = completion.into_parts();
    let bytes_read = result.expect("Recv failed after unregister");
    op.buf.set_len(bytes_read);
    assert_eq!(&op.buf.as_slice()[..bytes_read], b"recv-after-unregister");

    server_thread.join().unwrap();
}

#[test]
fn test_rio_cancel_poll_returns_aborted_without_hang() {
    let budget = LockedMemoryBudget::new(64 * 1024 * 1024);
    let mut driver =
        IocpDriver::new(IocpConfig::default().locked_memory_budget(budget.clone())).unwrap();

    let pool_capacity_bytes = 10 * 4 * 1024 * 1024;
    let topology = UniformSlot::new(pool_capacity_bytes);
    let reg_pool = TestSupport::build_registered_pool(&mut driver, &topology, budget, 0);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    })
    .unwrap();

    let client = Socket::new_rio_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let connect_token = TestSupport::submit_test_op(&mut driver, connect_op);

    let connect_res =
        TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5));
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

    let buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("Failed to alloc buffer");
    let recv_op = Recv {
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let token = TestSupport::submit_test_op(&mut driver, recv_op);

    let _ = driver.cancel_op(CancelRequest::user_visible(token));
    let _ = tx_send.send(());

    let res = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        TestSupport::completion_os_error_code(&err),
        Some(ERROR_OPERATION_ABORTED as i32)
    );

    server_thread.join().unwrap();
    driver.unregister_files(vec![client_fd]).unwrap();
}

#[test]
fn test_rio_cancel_late_completion_recycles_slot_after_drain() {
    let budget = LockedMemoryBudget::new(64 * 1024 * 1024);
    let mut driver =
        IocpDriver::new(IocpConfig::default().locked_memory_budget(budget.clone())).unwrap();

    let pool_capacity_bytes = 10 * 4 * 1024 * 1024;
    let topology = UniformSlot::new(pool_capacity_bytes);
    let reg_pool = TestSupport::build_registered_pool(&mut driver, &topology, budget, 0);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let (tx_send, rx_send) = mpsc::channel::<()>();
    let server_thread = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let _ = rx_send.recv();
        stream.write_all(b"late").unwrap();
    })
    .unwrap();

    let client = Socket::new_rio_tcp_v4().expect("client socket create failed");
    let client_fd = register_owned_socket(&mut driver, client);
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);
    let connect_op = Connect {
        fd: client_fd,
        addr: addr_storage,
        addr_len: addr_len as u32,
    };
    let connect_token = TestSupport::submit_test_op(&mut driver, connect_op);

    let connect_res =
        TestSupport::wait_completion(&mut driver, connect_token, Duration::from_secs(5));
    assert!(
        connect_res.is_ok(),
        "Connect failed: {:?}",
        connect_res.err()
    );

    let buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("Failed to alloc buffer");
    let recv_op = Recv {
        fd: client_fd,
        buf,
        buf_offset: 0,
    };
    let token = TestSupport::submit_test_op(&mut driver, recv_op);

    let _ = driver.cancel_op(CancelRequest::user_visible(token));

    assert!(
        !TestSupport::slot_is_idle(&mut driver, token.index()),
        "取消后真实 RIO completion 到来前槽位应保持活跃"
    );

    let _ = tx_send.send(());

    let res = TestSupport::wait_completion(&mut driver, token, Duration::from_secs(5));
    let err = res.expect_err("cancelled op should return aborted");
    assert_eq!(
        TestSupport::completion_os_error_code(&err),
        Some(ERROR_OPERATION_ABORTED as i32)
    );

    let drain_start = Instant::now();
    while drain_start.elapsed() < Duration::from_secs(2) {
        let _ = driver.drive(DriveMode::Poll).expect("drive failed");
        if TestSupport::slot_is_idle(&mut driver, token.index()) {
            break;
        }
        let _ = thread::sleep(Duration::from_millis(5));
    }

    assert!(
        TestSupport::slot_is_idle(&mut driver, token.index()),
        "晚到 RIO completion 到来后槽位应归为空闲"
    );

    server_thread.join().unwrap();
    driver.unregister_files(vec![client_fd]).unwrap();
}
