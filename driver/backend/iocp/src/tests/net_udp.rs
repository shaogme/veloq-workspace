use crate::config::{IoFd, IocpConfig};
use crate::driver::IocpDriver;
use crate::net::socket::Socket;
use crate::op::IocpOp;
use crate::tests::{completion_os_error_code, wait_completion};
use std::time::Duration;
use veloq_buf::BufPool;
use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};
use veloq_driver_core::driver::{Driver, RegisterFd, SubmitBinder};
use veloq_driver_core::op::{IntoPlatformOp, SendTo, UdpRecvStream};

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
fn test_rio_udp_send_to_recv_from_address_path() {
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

    let server_fd = register_owned_socket(&mut driver, server);
    let client_fd = register_owned_socket(&mut driver, client);

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

    let recv_op = UdpRecvStream {
        fd: server_fd,
        buf: None,
        addr: None,
        result: None,
    };
    let send_op = SendTo {
        fd: client_fd,
        buf: send_buf,
        buf_offset: 0,
        addr: server_addr,
    };

    let (recv_kernel, recv_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
    let mut recv_payload = Some(recv_payload);
    let mut recv_iocp = Some(recv_kernel);
    let (recv_ud, recv_gen) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit udp_recv_stream failed");

    let (send_kernel, _send_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(send_op);
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
    let recv_out = UdpRecvStream::from_user_payload(
        recv_payload
            .take()
            .expect("udp_recv_stream payload missing on completion"),
    );
    let datagram = recv_out.result.expect("datagram missing in result");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
    assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");

    driver.unregister_files(vec![client_fd, server_fd]).unwrap();
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path_ipv6() {
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

    let server_fd = register_owned_socket(&mut driver, server);
    let client_fd = register_owned_socket(&mut driver, client);

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

    let recv_op = UdpRecvStream {
        fd: server_fd,
        buf: None,
        addr: None,
        result: None,
    };
    let send_op = SendTo {
        fd: client_fd,
        buf: send_buf,
        buf_offset: 0,
        addr: server_addr,
    };

    let (recv_kernel, recv_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
    let mut recv_payload = Some(recv_payload);
    let mut recv_iocp = Some(recv_kernel);
    let (recv_ud, recv_gen) = driver.reserve_op().expect("reserve recv op failed");
    let _ = driver
        .submit(recv_ud, &mut recv_iocp, SubmitBinder::new())
        .into_inner()
        .expect("submit udp_recv_stream failed");

    let (send_kernel, _send_payload) = IntoPlatformOp::<IocpOp>::into_kernel_and_payload(send_op);
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
    let recv_out = UdpRecvStream::from_user_payload(
        recv_payload
            .take()
            .expect("udp_recv_stream payload missing on completion"),
    );
    let datagram = recv_out.result.expect("datagram missing in result");
    assert_eq!(bytes, test_data.len(), "recv_from bytes mismatch");
    assert_eq!(&datagram.buf.as_slice()[..bytes], test_data);
    assert_eq!(datagram.addr, client_addr, "recv_from source addr mismatch");

    driver.unregister_files(vec![client_fd, server_fd]).unwrap();
}

#[test]
fn test_rio_udp_recv_pool_burst_waiters_raise_target() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_fd = register_owned_socket(&mut driver, server);
    let socket_key = match &driver.registered_files[server_fd.fixed_index() as usize] {
        Some(crate::config::RegisteredHandle::Owned(h)) => h.raw().actor_key(),
        Some(crate::config::RegisteredHandle::Weak(h)) => h.raw().actor_key(),
        None => panic!("server fd missing after registration"),
    };

    let mut submitted = Vec::new();
    const BURST_WAITERS: usize = 12;

    for _ in 0..BURST_WAITERS {
        let recv_op = UdpRecvStream {
            fd: server_fd,
            buf: None,
            addr: None,
            result: None,
        };
        let (iocp_kernel, recv_payload) =
            IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
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
        .udp_pool_debug_stats(socket_key)
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
        let _ = UdpRecvStream::from_user_payload(recv_payload);
        let res = wait_completion(
            &mut driver,
            ud,
            generation,
            std::time::Duration::from_secs(1),
        );
        let err = res.expect_err("cancelled udp_recv_stream should fail");
        assert_eq!(
            completion_os_error_code(&err),
            Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
        );
    }

    driver.unregister_files(vec![server_fd]).unwrap();
}

#[test]
fn test_rio_udp_recv_pool_idle_falls_back_to_min_target() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_fd = register_owned_socket(&mut driver, server);
    let socket_key = match &driver.registered_files[server_fd.fixed_index() as usize] {
        Some(crate::config::RegisteredHandle::Owned(h)) => h.raw().actor_key(),
        Some(crate::config::RegisteredHandle::Weak(h)) => h.raw().actor_key(),
        None => panic!("server fd missing after registration"),
    };

    let mut submitted = Vec::new();
    const BURST_WAITERS: usize = 12;

    for _ in 0..BURST_WAITERS {
        let recv_op = UdpRecvStream {
            fd: server_fd,
            buf: None,
            addr: None,
            result: None,
        };
        let (iocp_kernel, recv_payload) =
            IntoPlatformOp::<IocpOp>::into_kernel_and_payload(recv_op);
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
        let _ = UdpRecvStream::from_user_payload(recv_payload);
        let _ = wait_completion(
            &mut driver,
            ud,
            generation,
            std::time::Duration::from_secs(1),
        );
    }

    const MAX_IDLE_TICKS: usize = 4096;
    let mut stats = driver
        .rio_state
        .udp_pool_debug_stats(socket_key)
        .expect("udp pool stats missing");
    for _ in 0..MAX_IDLE_TICKS {
        if stats.target_credits == stats.min_credits {
            break;
        }
        driver
            .rio_state
            .debug_tick_udp_pool_idle(socket_key, 1, &*driver.registrar)
            .expect("idle tick failed");
        stats = driver
            .rio_state
            .udp_pool_debug_stats(socket_key)
            .expect("udp pool stats missing");
    }
    assert_eq!(
        stats.target_credits, stats.min_credits,
        "idle should fall back to min credits, stats={stats:?}"
    );
    assert_eq!(stats.waiters_len, 0);

    driver.unregister_files(vec![server_fd]).unwrap();
}
