use crate::{
    config::{IoFd, IocpConfig},
    driver::IocpDriver,
    net::socket::Socket,
    op::{SendTo, UdpRecvFrom},
    tests::{
        complete_from_record, completion_os_error_code, submit_test_op, wait_completion,
        wait_completion_record,
    },
};
use veloq_buf::{
    BufPool, FixedBuf, NoopRegistrar, PoolTopology, UniformSlot,
    heap::{GlobalSlotPool, ThreadMemoryMultiplier},
};
use veloq_driver_core::driver::{CancelRequest, Driver, RegisterFd};
use veloq_std::{
    net::UdpSocket,
    num::NonZeroUsize,
    nz, println,
    sync::Arc,
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

fn register_buf_chunk(
    driver: &mut IocpDriver,
    global_pool: &Arc<GlobalSlotPool>,
    buf: &FixedBuf,
    label: &'static str,
) {
    let region = buf.resolve_region_info();
    let chunk = global_pool
        .chunk_info(region.id)
        .unwrap_or_else(|| panic!("{label} chunk not found"));
    driver
        .register_buffer(region.id, chunk.ptr.as_ptr(), chunk.len.get())
        .unwrap_or_else(|_| panic!("register {label} chunk failed"));
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path() {
    let registrar = NoopRegistrar;
    let mut driver =
        IocpDriver::new(IocpConfig::default(), &registrar).expect("Driver creation failed");

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

    let server_fd = register_owned_socket(&mut driver, server);
    let client_fd = register_owned_socket(&mut driver, client);

    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let test_data = b"rio-udp-sendto-regression";
    let mut send_buf = reg_pool
        .alloc(nz!(8192), test_data.len())
        .expect("send alloc failed");
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);

    let recv_buf = reg_pool.alloc_full(nz!(8192)).expect("recv alloc failed");
    register_buf_chunk(&mut driver, &global_pool, &send_buf, "send");
    register_buf_chunk(&mut driver, &global_pool, &recv_buf, "recv");

    let recv_op = UdpRecvFrom {
        fd: server_fd,
        buf: recv_buf,
        buf_offset: 0,
        addr: None,
    };
    let send_op = SendTo {
        fd: client_fd,
        buf: send_buf,
        buf_offset: 0,
        addr: server_addr,
    };

    let recv_token = submit_test_op(&mut driver, recv_op);
    let send_token = submit_test_op(&mut driver, send_op);

    let sent = wait_completion(&mut driver, send_token, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let recv_completion = complete_from_record::<UdpRecvFrom>(
        wait_completion_record(&mut driver, recv_token, Duration::from_secs(5))
            .expect("udp_recv_from completion missing"),
    );
    let (recv_result, recv_out) = recv_completion.into_parts();
    let bytes = recv_result.expect("udp_recv_from completion failed");
    let recv_addr = recv_out.addr.expect("recv_from addr missing");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&recv_out.buf.as_slice()[..bytes], test_data);
    assert_eq!(recv_addr, client_addr, "recv_from source addr mismatch");

    driver.unregister_files(vec![client_fd, server_fd]).unwrap();
}

#[test]
fn test_rio_udp_single_send_reaches_receiver_with_multiple_armed_receives() {
    const SINGLE_DATAGRAM_BUDGET: Duration = Duration::from_secs(15);

    let registrar = NoopRegistrar;
    let mut driver =
        IocpDriver::new(IocpConfig::default(), &registrar).expect("Driver creation failed");

    let source = Socket::new_udp_v4().expect("source socket create failed");
    let relay = Socket::new_udp_v4().expect("relay socket create failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    let probe = Socket::new_udp_v4().expect("probe socket create failed");
    for socket in [&source, &relay, &server, &probe] {
        socket
            .bind("127.0.0.1:0".parse().unwrap())
            .expect("UDP socket bind failed");
    }

    let relay_addr = relay.local_addr().expect("relay local_addr failed");
    let source_addr = source.local_addr().expect("source local_addr failed");
    let source_fd = register_owned_socket(&mut driver, source);
    let relay_fd = register_owned_socket(&mut driver, relay);
    let server_fd = register_owned_socket(&mut driver, server);
    let probe_fd = register_owned_socket(&mut driver, probe);

    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let test_data = b"single-iocp-datagram";
    let mut send_buf = reg_pool
        .alloc(nz!(8192), test_data.len())
        .expect("send alloc failed");
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
    let relay_recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("relay receive alloc failed");
    let server_recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("server receive alloc failed");
    let probe_recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("probe receive alloc failed");
    let source_recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("source receive alloc failed");
    for (buf, label) in [
        (&send_buf, "send"),
        (&relay_recv_buf, "relay receive"),
        (&server_recv_buf, "server receive"),
        (&probe_recv_buf, "probe receive"),
        (&source_recv_buf, "source receive"),
    ] {
        register_buf_chunk(&mut driver, &global_pool, buf, label);
    }

    let relay_recv_token = submit_test_op(
        &mut driver,
        UdpRecvFrom {
            fd: relay_fd,
            buf: relay_recv_buf,
            buf_offset: 0,
            addr: None,
        },
    );
    let server_recv_token = submit_test_op(
        &mut driver,
        UdpRecvFrom {
            fd: server_fd,
            buf: server_recv_buf,
            buf_offset: 0,
            addr: None,
        },
    );
    let probe_recv_token = submit_test_op(
        &mut driver,
        UdpRecvFrom {
            fd: probe_fd,
            buf: probe_recv_buf,
            buf_offset: 0,
            addr: None,
        },
    );
    let source_recv_token = submit_test_op(
        &mut driver,
        UdpRecvFrom {
            fd: source_fd,
            buf: source_recv_buf,
            buf_offset: 0,
            addr: None,
        },
    );
    let send_token = submit_test_op(
        &mut driver,
        SendTo {
            fd: source_fd,
            buf: send_buf,
            buf_offset: 0,
            addr: relay_addr,
        },
    );

    let deadline = Instant::now() + SINGLE_DATAGRAM_BUDGET;
    let sent = wait_completion(
        &mut driver,
        send_token,
        deadline.saturating_duration_since(Instant::now()),
    )
    .expect("single UDP send did not complete within 15 seconds");
    assert_eq!(sent, test_data.len(), "single UDP send bytes mismatch");

    let relay_completion = complete_from_record::<UdpRecvFrom>(
        wait_completion_record(
            &mut driver,
            relay_recv_token,
            deadline.saturating_duration_since(Instant::now()),
        )
        .expect("relay did not receive the single UDP datagram within 15 seconds"),
    );
    let (relay_result, relay_op) = relay_completion.into_parts();
    let bytes = relay_result.expect("relay receive failed");
    assert_eq!(bytes, test_data.len(), "relay receive bytes mismatch");
    assert_eq!(&relay_op.buf.as_slice()[..bytes], test_data);
    assert_eq!(
        relay_op.addr.expect("relay source address missing"),
        source_addr
    );

    // These receives intentionally remain in flight to preserve the original topology. The
    // driver's fast-drop path owns their RIO cleanup; waiting for cancellation here would test a
    // separate cancellation race instead of single-datagram delivery.
    let _ = (server_recv_token, probe_recv_token, source_recv_token);
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path_ipv6() {
    let registrar = NoopRegistrar;
    let mut driver =
        IocpDriver::new(IocpConfig::default(), &registrar).expect("Driver creation failed");

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

    let server_fd = register_owned_socket(&mut driver, server);
    let client_fd = register_owned_socket(&mut driver, client);

    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let test_data = b"rio-udp-sendto-regression-ipv6";
    let mut send_buf = reg_pool
        .alloc(nz!(8192), test_data.len())
        .expect("send alloc failed");
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);

    let recv_buf = reg_pool.alloc_full(nz!(8192)).expect("recv alloc failed");
    register_buf_chunk(&mut driver, &global_pool, &send_buf, "send");
    register_buf_chunk(&mut driver, &global_pool, &recv_buf, "recv");

    let recv_op = UdpRecvFrom {
        fd: server_fd,
        buf: recv_buf,
        buf_offset: 0,
        addr: None,
    };
    let send_op = SendTo {
        fd: client_fd,
        buf: send_buf,
        buf_offset: 0,
        addr: server_addr,
    };

    let recv_token = submit_test_op(&mut driver, recv_op);
    let send_token = submit_test_op(&mut driver, send_op);

    let sent = wait_completion(&mut driver, send_token, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let recv_completion = complete_from_record::<UdpRecvFrom>(
        wait_completion_record(&mut driver, recv_token, Duration::from_secs(5))
            .expect("udp_recv_from completion missing"),
    );
    let (recv_result, recv_out) = recv_completion.into_parts();
    let bytes = recv_result.expect("udp_recv_from completion failed");
    let recv_addr = recv_out.addr.expect("recv_from addr missing");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&recv_out.buf.as_slice()[..bytes], test_data);
    assert_eq!(recv_addr, client_addr, "recv_from source addr mismatch");

    driver.unregister_files(vec![client_fd, server_fd]).unwrap();
}

#[test]
fn test_rio_udp_recv_from_cancel_reports_aborted() {
    let registrar = NoopRegistrar;
    let mut driver =
        IocpDriver::new(IocpConfig::default(), &registrar).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_addr = server.local_addr().expect("server local_addr failed");
    let server_fd = register_owned_socket(&mut driver, server);
    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let recv_buf = reg_pool.alloc_full(nz!(8192)).expect("recv alloc failed");
    register_buf_chunk(&mut driver, &global_pool, &recv_buf, "recv");

    let recv_op = UdpRecvFrom {
        fd: server_fd,
        buf: recv_buf,
        buf_offset: 0,
        addr: None,
    };
    let token = submit_test_op(&mut driver, recv_op);

    let _ = driver.cancel_op(CancelRequest::user_visible(token));
    let client = UdpSocket::bind("127.0.0.1:0").expect("client bind failed");
    client
        .send_to(b"cancel-drain", server_addr)
        .expect("client send_to failed");
    let err = wait_completion(&mut driver, token, Duration::from_secs(5))
        .expect_err("cancelled udp_recv_from should fail");
    assert_eq!(
        completion_os_error_code(&err),
        Some(ERROR_OPERATION_ABORTED as i32)
    );

    driver.unregister_files(vec![server_fd]).unwrap();
}
