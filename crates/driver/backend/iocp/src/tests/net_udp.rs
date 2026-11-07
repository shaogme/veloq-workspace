use crate::{
    config::{IoFd, IocpConfig},
    driver::IocpDriver,
    net::socket::Socket,
    op::{SendTo, UdpRecvMulti},
    tests::{
        complete_from_record, completion_os_error_code, submit_test_op, wait_completion,
        wait_completion_record,
    },
};
use veloq_buf::{BufPool, FixedBuf, LockedMemoryBudget, UniformSlot};
use veloq_driver_core::{
    CancelRequest, CompletionContinuation, Driver, ReceivePumpConfig, ReceivePumpState,
    ReceiveSlot, RegisterFd,
};
use veloq_std::{num::NonZeroUsize, nz, time::Duration, vec, vec::Vec};
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

fn receive_pump(buffer: FixedBuf) -> ReceivePumpState {
    receive_pump_with_buffers(vec![buffer], 1)
}

fn receive_pump_with_buffers(buffers: Vec<FixedBuf>, depth: usize) -> ReceivePumpState {
    let config = ReceivePumpConfig::new(
        NonZeroUsize::new(depth).expect("non-zero receive depth"),
        NonZeroUsize::new(1).expect("non-zero queue capacity"),
        NonZeroUsize::new(8192).expect("non-zero datagram capacity"),
        Duration::from_secs(5),
    );
    let slots = buffers
        .into_iter()
        .enumerate()
        .map(|(slot_id, buffer)| {
            ReceiveSlot::new(u32::try_from(slot_id).expect("slot id fits u32"), buffer)
        })
        .collect::<Vec<_>>()
        .into_boxed_slice();
    ReceivePumpState::try_new(config, slots).expect("valid receive pump")
}

#[test]
fn test_rio_udp_recv_multi_delivers_datagram_and_arms_replacement() {
    let budget = LockedMemoryBudget::new(64 * 1024 * 1024);
    let mut driver = IocpDriver::new(IocpConfig::default().locked_memory_budget(budget.clone()))
        .expect("Driver creation failed");

    let source = Socket::new_rio_udp_v4().expect("source socket create failed");
    let receiver = Socket::new_rio_udp_v4().expect("receiver socket create failed");
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

    let pool_capacity_bytes = 10 * 4 * 1024 * 1024;
    let topology = UniformSlot::new(pool_capacity_bytes);
    let reg_pool = super::build_registered_pool(&mut driver, &topology, budget, 0);

    let test_data = b"rio-udp-recv-multi";
    let mut send_buf = reg_pool
        .alloc(nz!(8192), test_data.len())
        .expect("send allocation failed");
    send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
    let recv_buf = reg_pool
        .alloc_full(nz!(8192))
        .expect("receive allocation failed");
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

#[test]
fn test_raw_udp_recv_multi_runs_with_multiple_overlapped_requests() {
    let mut driver = IocpDriver::new(IocpConfig::default()).expect("Driver creation failed");
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

    let first_receive_buffer =
        FixedBuf::alloc_heap(nz!(8192), 0).expect("first receive buffer allocation failed");
    let second_receive_buffer =
        FixedBuf::alloc_heap(nz!(8192), 0).expect("second receive buffer allocation failed");
    let recv_token = submit_test_op(
        &mut driver,
        UdpRecvMulti::from_backend(
            receiver_fd,
            receive_pump_with_buffers(vec![first_receive_buffer, second_receive_buffer], 2),
        ),
    );

    for test_data in [b"raw-udp-first".as_slice(), b"raw-udp-second".as_slice()] {
        let mut send_buf =
            FixedBuf::alloc_heap(nz!(128), test_data.len()).expect("send buffer allocation failed");
        send_buf.spare_capacity_mut()[..test_data.len()].copy_from_slice(test_data);
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
            .expect("raw udp_recv_multi completion missing");
        assert_eq!(record.continuation(), CompletionContinuation::More);
        let completion = complete_from_record::<UdpRecvMulti>(record);
        let (recv_result, packet) = completion.into_parts();
        let bytes = recv_result.expect("raw udp_recv_multi completion failed");
        assert_eq!(&packet.buf.as_slice()[..bytes], test_data);
        assert_eq!(packet.addr, source_addr);
    }

    driver
        .cancel_op(CancelRequest::user_visible(recv_token))
        .expect("raw udp_recv_multi cancellation failed");
    let error = wait_completion(&mut driver, recv_token, Duration::from_secs(5))
        .expect_err("cancelled raw udp_recv_multi should return an error");
    assert_eq!(
        completion_os_error_code(&error),
        Some(ERROR_OPERATION_ABORTED as i32)
    );
}
