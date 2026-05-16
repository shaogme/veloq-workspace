use crate::config::{IoFd, IocpConfig};
use crate::driver::IocpDriver;
use crate::net::socket::Socket;
use crate::tests::{
    complete_from_record, completion_os_error_code, submit_test_op, wait_completion,
    wait_completion_record,
};
use std::time::Duration;
use veloq_buf::{BufPool, FixedBuf, NoopRegistrar};
use veloq_buf::{
    PoolTopology, UniformSlot,
    heap::{GlobalSlotPool, ThreadMemoryMultiplier},
};
use veloq_driver_core::driver::{Driver, RegisterFd};
use veloq_driver_core::op::{SendTo, UdpRecvFrom};

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
    global_pool: &std::sync::Arc<GlobalSlotPool>,
    buf: &FixedBuf,
    label: &'static str,
) {
    let region = buf.resolve_region_info();
    let chunk = global_pool
        .chunk_info(region.id)
        .unwrap_or_else(|| panic!("{label} chunk not found"));
    driver
        .register_chunk(region.id, chunk.ptr.as_ptr(), chunk.len.get())
        .unwrap_or_else(|_| panic!("register {label} chunk failed"));
}

#[test]
fn test_rio_udp_send_to_recv_from_address_path() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar))
        .expect("Driver creation failed");

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

    let recv_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("recv alloc failed");
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

    let (recv_ud, recv_gen) = submit_test_op(&mut driver, recv_op);
    let (send_ud, send_gen) = submit_test_op(&mut driver, send_op);

    let sent = wait_completion(&mut driver, send_ud, send_gen, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let recv_completion = complete_from_record::<UdpRecvFrom>(
        wait_completion_record(&mut driver, recv_ud, recv_gen, Duration::from_secs(5))
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
fn test_rio_udp_send_to_recv_from_address_path_ipv6() {
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar))
        .expect("Driver creation failed");

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

    let recv_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("recv alloc failed");
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

    let (recv_ud, recv_gen) = submit_test_op(&mut driver, recv_op);
    let (send_ud, send_gen) = submit_test_op(&mut driver, send_op);

    let sent = wait_completion(&mut driver, send_ud, send_gen, Duration::from_secs(5))
        .expect("send_to completion failed");
    assert_eq!(sent, test_data.len(), "send_to bytes mismatch");
    let recv_completion = complete_from_record::<UdpRecvFrom>(
        wait_completion_record(&mut driver, recv_ud, recv_gen, Duration::from_secs(5))
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
    let mut driver = IocpDriver::new(IocpConfig::default(), Box::new(NoopRegistrar))
        .expect("Driver creation failed");
    let server = Socket::new_udp_v4().expect("server socket create failed");
    server
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("server bind failed");
    let server_fd = register_owned_socket(&mut driver, server);
    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("Create pool failed");
    let reg_pool = topology.build(&global_pool, 0, Box::new(veloq_buf::NoopRegistrar));

    let recv_buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("recv alloc failed");
    register_buf_chunk(&mut driver, &global_pool, &recv_buf, "recv");

    let recv_op = UdpRecvFrom {
        fd: server_fd,
        buf: recv_buf,
        buf_offset: 0,
        addr: None,
    };
    let (ud, generation) = submit_test_op(&mut driver, recv_op);

    driver.cancel_op(ud);
    let err = wait_completion(
        &mut driver,
        ud,
        generation,
        std::time::Duration::from_secs(1),
    )
    .expect_err("cancelled udp_recv_from should fail");
    assert_eq!(
        completion_os_error_code(&err),
        Some(windows_sys::Win32::Foundation::ERROR_OPERATION_ABORTED as i32)
    );

    driver.unregister_files(vec![server_fd]).unwrap();
}
