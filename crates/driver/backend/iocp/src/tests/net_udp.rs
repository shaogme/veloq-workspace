use crate::{
    config::{IoFd, IocpConfig},
    driver::IocpDriver,
    net::socket::Socket,
    op::{SendTo, UdpRecvMulti},
    tests::{complete_from_record, submit_test_op, wait_completion, wait_completion_record},
};
use veloq_buf::{
    BufPool, FixedBuf, NoopRegistrar, PoolTopology, UniformSlot,
    heap::{GlobalSlotPool, ThreadMemoryMultiplier},
};
use veloq_driver_core::{
    driver::{CancelRequest, Driver, RegisterFd},
    platform::receive_pump::{ReceivePumpConfig, ReceivePumpState, ReceiveSlot},
};
use veloq_std::{num::NonZeroUsize, nz, sync::Arc, time::Duration, vec};

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

fn receive_pump(buffer: FixedBuf) -> ReceivePumpState {
    let config = ReceivePumpConfig {
        kernel_capacity: NonZeroUsize::new(1).expect("non-zero receive depth"),
        queue_capacity: NonZeroUsize::new(1).expect("non-zero queue capacity"),
        datagram_capacity: NonZeroUsize::new(8192).expect("non-zero datagram capacity"),
        close_timeout: Duration::from_secs(5),
    };
    ReceivePumpState::try_new(config, vec![ReceiveSlot::new(0, buffer)].into_boxed_slice())
        .expect("valid receive pump")
}

#[test]
fn test_rio_udp_recv_multi_delivers_datagram_and_arms_replacement() {
    let registrar = NoopRegistrar;
    let mut driver =
        IocpDriver::new(IocpConfig::default(), &registrar).expect("Driver creation failed");

    let source = Socket::new_udp_v4().expect("source socket create failed");
    let receiver = Socket::new_udp_v4().expect("receiver socket create failed");
    source
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("source bind failed");
    receiver
        .bind("127.0.0.1:0".parse().unwrap())
        .expect("receiver bind failed");
    let source_addr = source.local_addr().expect("source local_addr failed");
    let receiver_addr = receiver.local_addr().expect("receiver local_addr failed");

    let source_fd = register_owned_socket(&mut driver, source);
    let receiver_fd = register_owned_socket(&mut driver, receiver);

    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(10).unwrap());
    let topology = UniformSlot::new(multiplier);
    let global_pool = topology.create_pool(1).expect("create pool failed");
    let reg_pool = topology
        .build(&global_pool, 0, &veloq_buf::NoopRegistrar)
        .expect("build buffer pool failed");

    let test_data = b"rio-udp-recv-multi";
    let mut send_buf = reg_pool
        .alloc(nz!(8192), test_data.len())
        .expect("send allocation failed");
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
    let recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("receive allocation failed");
    register_buf_chunk(&mut driver, &global_pool, &send_buf, "send");
    register_buf_chunk(&mut driver, &global_pool, &recv_buf, "receive");

    let recv_token = submit_test_op(
        &mut driver,
        UdpRecvMulti::from_backend(receiver_fd, receive_pump(recv_buf)),
    );
    let send_token = submit_test_op(
        &mut driver,
        SendTo {
            fd: source_fd,
            buf: send_buf,
            buf_offset: 0,
            addr: receiver_addr,
        },
    );

    assert_eq!(
        wait_completion(&mut driver, send_token, Duration::from_secs(5))
            .expect("send_to completion failed"),
        test_data.len()
    );

    let record = wait_completion_record(&mut driver, recv_token, Duration::from_secs(5))
        .expect("udp_recv_multi completion missing");
    let completion = complete_from_record::<UdpRecvMulti>(record);
    let (recv_result, packet) = completion.into_parts();
    let bytes = recv_result.expect("udp_recv_multi completion failed");
    assert_eq!(bytes, test_data.len());
    assert_eq!(&packet.buf.as_slice()[..bytes], test_data);
    assert_eq!(packet.addr, source_addr);

    let _ = driver.cancel_op(CancelRequest::user_visible(recv_token));
}
