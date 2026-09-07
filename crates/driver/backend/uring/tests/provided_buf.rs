//! provided buffer ring 的驱动级测试。
//!
//! 每个用例都会在缺 `IORING_REGISTER_PBUF_RING` 的内核（< 5.19）上自行跳过——那是仓库声明
//! 支持的区间的一部分，不是失败。

use std::{
    num::{NonZeroU16, NonZeroU32, NonZeroUsize},
    os::fd::RawFd,
    time::{Duration, Instant},
};

use veloq_buf::{AnyBufPool, BufPool, FixedBuf, NoopRegistrar};
use veloq_driver_core::{
    driver::{
        CancelRequest, CompletionRecord, DriveMode, Driver, DriverSubmitResult, OpToken,
        PollRecordResult, RegisterFd,
    },
    op::{
        IntoPlatformOp,
        types::{RecvMulti as CoreRecvMulti, RecvProvided as CoreRecvProvided},
    },
};
use veloq_driver_uring::{
    IoFd, ProvidedBufConfig, RawHandle, UringConfig, UringDriver, UringOp, UringRawHandle,
    UringSlotSpec, UringUserPayload,
};

type RecvProvided = CoreRecvProvided<UringRawHandle>;
type RecvMulti = CoreRecvMulti<UringRawHandle>;

/// 一个只会走堆分配的池。
///
/// 这些用例关心的是环的记账，不是池的实现——用最朴素的那个，失败就一定是环的问题。
#[derive(Debug, Clone)]
struct HeapPool;

impl BufPool for HeapPool {
    fn alloc(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf> {
        FixedBuf::alloc_heap(cap, len).ok()
    }
}

fn new_driver_or_skip(entries: u16, buf_size: usize) -> Option<UringDriver<'static>> {
    let config = UringConfig {
        entries: NonZeroU32::new(64).unwrap(),
        provided_buffers: Some(ProvidedBufConfig {
            entries: NonZeroU16::new(entries).unwrap(),
            buf_size: NonZeroUsize::new(buf_size).unwrap(),
        }),
        ..UringConfig::default()
    };
    static REGISTRAR: NoopRegistrar = NoopRegistrar;

    let mut driver = match UringDriver::new(config, &REGISTRAR) {
        Ok(driver) => driver,
        Err(report) => {
            eprintln!("skipping provided-buffer test: {report}");
            return None;
        }
    };
    driver
        .attach_buffer_pool(AnyBufPool::new(HeapPool))
        .expect("attaching a buffer pool must not fail");

    if !driver.capabilities().provided_buffers {
        eprintln!("skipping provided-buffer test: kernel has no IORING_REGISTER_PBUF_RING");
        return None;
    }
    Some(driver)
}

/// 一对已连接的 `AF_UNIX` socket，省掉一整套 TCP 握手。
struct SocketPair {
    rx: RawFd,
    tx: RawFd,
}

impl SocketPair {
    fn new() -> Self {
        let mut fds = [0i32; 2];
        // SAFETY: `fds` 是一个长度为 2 的数组，正是 `socketpair` 要写入的形状。
        let rc = unsafe { libc::socketpair(libc::AF_UNIX, libc::SOCK_STREAM, 0, fds.as_mut_ptr()) };
        assert_eq!(
            rc,
            0,
            "socketpair failed: {}",
            std::io::Error::last_os_error()
        );
        Self {
            rx: fds[0],
            tx: fds[1],
        }
    }

    fn send(&self, data: &[u8]) {
        // SAFETY: `data` 是一段活着的切片，`tx` 是本结构持有的 fd。
        let written = unsafe { libc::write(self.tx, data.as_ptr().cast(), data.len()) };
        assert_eq!(written, data.len() as isize, "short write to socketpair");
    }

    /// 关掉发送端的写方向：接收端读到 EOF，而两个 fd 都还活着。
    fn shutdown_tx(&self) {
        // SAFETY: `tx` 是本结构持有的 fd。
        let rc = unsafe { libc::shutdown(self.tx, libc::SHUT_WR) };
        assert_eq!(
            rc,
            0,
            "shutdown failed: {}",
            std::io::Error::last_os_error()
        );
    }

    fn register_rx(&self, driver: &mut UringDriver<'static>) -> IoFd {
        let raw = RawHandle::new(UringRawHandle::for_socket(self.rx));
        driver
            .register_files(vec![RegisterFd::Borrowed(raw.borrow())])
            .expect("registering the receiving socket")
            .into_iter()
            .next()
            .expect("register_files returned nothing")
    }
}

impl Drop for SocketPair {
    fn drop(&mut self) {
        // SAFETY: 两个 fd 都由本结构拥有，且只在这里关闭一次。
        unsafe {
            libc::close(self.rx);
            libc::close(self.tx);
        }
    }
}

fn submit_op<T>(driver: &mut UringDriver<'static>, user_op: T) -> OpToken
where
    T: IntoPlatformOp<UringSlotSpec>,
{
    let (kernel, payload) = T::into_kernel_and_payload(user_op);
    let mut op: Option<UringOp> = Some(kernel);
    let mut slot = driver.reserve_op().expect("reserve op failed");
    slot.set_payload(T::payload_into_erased(payload));
    match slot.submit(&mut op) {
        DriverSubmitResult::Submitted(_) => {
            let token = slot.persist().token();
            driver.completion_table().mark_waiting(token);
            token
        }
        DriverSubmitResult::Failed { report, status } => {
            panic!(
                "submit {} failed: status={status:?}, error={report}",
                std::any::type_name::<T>()
            )
        }
    }
}

fn submit_recv_provided(driver: &mut UringDriver<'static>, fd: IoFd) -> OpToken {
    submit_op(driver, RecvProvided { fd })
}

/// 提交一条 multishot recv。
///
/// 返回 `None` 表示内核不认识它——`IORING_OP_RECV` 从 5.6 就在，它的 multishot 变体要 6.0，
/// 而仓库声明的最低内核是 5.6，所以那是支持区间的一部分而不是失败。
///
/// 判据是「提交之后立刻有没有一条完成」：被接受的 multishot 在数据到来之前什么都不产出，
/// 被拒绝的那条 `-EINVAL` 由 `io_uring_enter` 同步产生，第一次 drive 就在队列里。
fn arm_recv_multi(driver: &mut UringDriver<'static>, fd: IoFd) -> Option<OpToken> {
    let token = submit_op(driver, RecvMulti { fd });
    for _ in 0..3 {
        driver.drive(DriveMode::Poll).expect("drive failed");
        match driver.completion_table().try_take_record(token).unwrap() {
            PollRecordResult::Pending => std::thread::sleep(Duration::from_millis(1)),
            PollRecordResult::Ready(record) => {
                assert_eq!(
                    record.event.res(),
                    -libc::EINVAL,
                    "a multishot recv must not complete before any data arrives"
                );
                eprintln!("skipping multishot-recv test: kernel has no multishot recv");
                return None;
            }
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion record unavailable right after submit: {kind:?}")
            }
        }
    }
    Some(token)
}

/// 取消一条已经 orphan 掉的操作，并把它的终态完成收干净。
///
/// 少了这一步就把 driver 丢掉，等于在内核仍持有环指针时反注册并 munmap——`UringDriver` 的
/// 析构确实会 close ring fd 让内核取消一切在途操作，但那发生在 `unregister_buf_ring` 之后。
///
/// 「收干净了」的判据是 token 变成 `Unavailable`：slot 归还的同时 generation 推进，这正是
/// 终态完成落地的证据。
fn drain_cancelled(driver: &mut UringDriver<'static>, token: OpToken) {
    driver
        .cancel_op(CancelRequest::abandon(token))
        .expect("cancel failed");

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        driver.drive(DriveMode::Poll).expect("drive failed");
        if matches!(
            driver.completion_table().try_take_record(token),
            Ok(PollRecordResult::Unavailable { .. })
        ) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the cancelled operation never settled"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// 取一条完成，返回 `(CQE 结果, 内核挑给它的 buffer)`。
fn take_completion(driver: &mut UringDriver<'static>, token: OpToken) -> (i32, Option<FixedBuf>) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline, "provided-buffer recv timed out");
        driver.drive(DriveMode::Poll).expect("drive failed");
        let table = driver.completion_table();
        match table.try_take_record(token).unwrap() {
            PollRecordResult::Ready(record) => {
                let CompletionRecord {
                    event,
                    payload,
                    mut cleanup,
                    ..
                } = record;
                cleanup.disarm();
                let buf = match payload {
                    UringUserPayload::ProvidedBuf(provided) => provided.buf,
                    other => panic!(
                        "a RecvProvided completion must carry a ProvidedBuf, got kind {:?}",
                        std::mem::discriminant(&other)
                    ),
                };
                return (event.res(), buf);
            }
            PollRecordResult::Unavailable { kind, .. } => {
                panic!("completion record unavailable: {kind:?}")
            }
            PollRecordResult::Pending => std::thread::sleep(Duration::from_millis(2)),
        }
    }
}

/// 连续收 N 次（N 远大于环的深度），每次都拿到数据，环始终填得回来。
///
/// 这条用例锚定的是「移交 + 补充」：交出去的 buffer 不回环，回环的是从池里新取的一个。做
/// 成「借出 + 归还」的话前 4 次一样绿，第 5 次就没 buffer 了。
#[test]
fn provided_buffers_are_recycled_after_each_completion() {
    const ENTRIES: u16 = 4;
    const ROUNDS: usize = 16;

    let Some(mut driver) = new_driver_or_skip(ENTRIES, 256) else {
        return;
    };
    let sock = SocketPair::new();
    let fd = sock.register_rx(&mut driver);

    for round in 0..ROUNDS {
        let payload = format!("round-{round}");
        sock.send(payload.as_bytes());

        let token = submit_recv_provided(&mut driver, fd);
        let (res, buf) = take_completion(&mut driver, token);

        assert_eq!(
            res,
            payload.len() as i32,
            "round {round} read the wrong length"
        );
        let buf = buf.unwrap_or_else(|| panic!("round {round} produced no buffer"));
        assert_eq!(buf.as_slice(), payload.as_bytes(), "round {round} content");
        assert_eq!(
            buf.capacity(),
            256,
            "the ring must hand out its own buffers"
        );
    }

    let stats = driver.provided_buf_stats().expect("the ring is registered");
    assert_eq!(stats.handed_out, ROUNDS as u64);
    // 初始填满 ENTRIES 个，之后每交出一个补一个。
    assert_eq!(stats.refilled, ENTRIES as u64 + ROUNDS as u64);
    assert_eq!(stats.refill_failed, 0);
    assert_eq!(
        stats.available, ENTRIES,
        "every consumed entry must have been replaced"
    );

    driver.unregister_files(vec![fd]).unwrap();
}

/// 环被掏空时内核报 `-ENOBUFS`，而不是随便写进别人的内存。
///
/// 单发 recv 对此没有特殊处理——那一次以错误完成，用户可以重试。这里断言的是错误确实到得
/// 了用户手上，并且被记进了诊断。
#[test]
fn an_exhausted_ring_completes_with_enobufs() {
    const ENTRIES: u16 = 2;
    const CONCURRENT: usize = 4;

    let Some(mut driver) = new_driver_or_skip(ENTRIES, 256) else {
        return;
    };

    let socks: Vec<SocketPair> = (0..CONCURRENT).map(|_| SocketPair::new()).collect();
    let fds: Vec<IoFd> = socks.iter().map(|s| s.register_rx(&mut driver)).collect();

    // 先把数据都准备好，再一次性提交：内核在处理每条 SQE 时就会取走一个 buffer，而补充要
    // 等我们收割 CQE，所以第 ENTRIES + 1 条起必然找不到 buffer。
    for sock in &socks {
        sock.send(b"x");
    }
    let tokens: Vec<OpToken> = fds
        .iter()
        .map(|&fd| submit_recv_provided(&mut driver, fd))
        .collect();

    let mut delivered = 0usize;
    let mut enobufs = 0usize;
    for token in tokens {
        let (res, buf) = take_completion(&mut driver, token);
        if res == -libc::ENOBUFS {
            assert!(buf.is_none(), "an ENOBUFS completion cannot carry a buffer");
            enobufs += 1;
        } else {
            assert_eq!(res, 1);
            assert!(buf.is_some(), "a successful recv must carry its buffer");
            delivered += 1;
        }
    }

    assert!(
        delivered >= ENTRIES as usize,
        "the ring must serve at least its own depth, served {delivered}"
    );
    assert!(
        enobufs > 0,
        "{CONCURRENT} concurrent recvs against a ring of {ENTRIES} must exhaust it"
    );

    let stats = driver.provided_buf_stats().expect("the ring is registered");
    assert_eq!(stats.exhausted, enobufs as u64);
    assert_eq!(
        stats.available, ENTRIES,
        "the ring must be full again once every completion has been processed"
    );

    driver.unregister_files(fds).unwrap();
}

/// 一条被放弃的 recv 收到完成时，它选中的 buffer 要原样还回环。
///
/// 「取消不等于结束」在 provided buffer 上的形态：内核已经把 buffer 从环里取走并写好了
/// 数据，而这条完成没人要。不还回去的话每取消一次环就永久少一个 bid——几次之后所有 recv
/// 都会 `-ENOBUFS`，而现场看起来像是「消费方太慢」。
#[test]
fn a_discarded_completion_returns_its_buffer_to_the_ring() {
    const ENTRIES: u16 = 2;

    let Some(mut driver) = new_driver_or_skip(ENTRIES, 256) else {
        return;
    };
    let sock = SocketPair::new();
    let fd = sock.register_rx(&mut driver);

    // 数据还没来就放弃这次 recv：slot 转 `InFlightOrphaned`，token 仍然有效。
    let token = submit_recv_provided(&mut driver, fd);
    driver.drive(DriveMode::Poll).expect("drive failed");
    driver.completion_table().mark_orphaned(token);

    // 现在才把数据送来：内核取走一个 buffer、投递一条谁也不要的完成。
    sock.send(b"orphaned");
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        driver.drive(DriveMode::Poll).expect("drive failed");
        let stats = driver.provided_buf_stats().expect("the ring is registered");
        if stats.returned > 0 {
            assert_eq!(stats.handed_out, 0, "nobody consumed this completion");
            assert_eq!(
                stats.available, ENTRIES,
                "a discarded completion must leave the ring full"
            );
            // 还回环用的是原来那个 buffer，不是新从池里取的。
            assert_eq!(stats.refilled, ENTRIES as u64);
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the orphaned completion never arrived"
        );
        std::thread::sleep(Duration::from_millis(2));
    }

    // 环没被削短：后面的 recv 照常拿得到 buffer。
    sock.send(b"after");
    let token = submit_recv_provided(&mut driver, fd);
    let (res, buf) = take_completion(&mut driver, token);
    assert_eq!(res, b"after".len() as i32);
    assert_eq!(buf.expect("buffer after an orphan").as_slice(), b"after");

    driver.unregister_files(vec![fd]).unwrap();
}

/// 一次提交，N 条完成，每条各带一个内核挑的 buffer。
///
/// 这条用例同时锚定阶段 1 与阶段 2 的交集：token 跨越 N 条完成始终有效（slot 没被归还、
/// generation 没推进），而每条完成各自从环里取走一个 buffer 又各自补回去。ROUNDS 特意大于
/// 环深——补充路径要是没接上，第 ENTRIES + 1 轮就会 `-ENOBUFS`。
#[test]
fn recv_multi_delivers_every_chunk_from_one_submission() {
    const ENTRIES: u16 = 4;
    const ROUNDS: usize = 12;

    let Some(mut driver) = new_driver_or_skip(ENTRIES, 256) else {
        return;
    };
    let sock = SocketPair::new();
    let fd = sock.register_rx(&mut driver);

    let Some(token) = arm_recv_multi(&mut driver, fd) else {
        driver.unregister_files(vec![fd]).unwrap();
        return;
    };

    for round in 0..ROUNDS {
        // 一次一条：等这一轮的完成到手再发下一条，否则内核会把两轮的数据并进一个 buffer。
        let payload = format!("round-{round}");
        sock.send(payload.as_bytes());

        let (res, buf) = take_completion(&mut driver, token);
        assert_eq!(
            res,
            payload.len() as i32,
            "round {round} read the wrong length"
        );
        let buf = buf.unwrap_or_else(|| panic!("round {round} produced no buffer"));
        assert_eq!(buf.as_slice(), payload.as_bytes(), "round {round} content");
    }

    let stats = driver.provided_buf_stats().expect("the ring is registered");
    assert_eq!(stats.handed_out, ROUNDS as u64);
    assert_eq!(stats.refilled, ENTRIES as u64 + ROUNDS as u64);
    assert_eq!(
        stats.available, ENTRIES,
        "every consumed entry must have been replaced"
    );

    // 对端关闭：内核以 `res == 0` 结束这条 multishot，流的终点就是这一条。
    sock.shutdown_tx();
    let (res, _buf) = take_completion(&mut driver, token);
    assert_eq!(res, 0, "EOF must arrive as a zero-length completion");

    driver.unregister_files(vec![fd]).unwrap();
}

/// 一条被放弃的 multishot recv 收到完成时，它选中的 buffer 要原样还回环。
///
/// 与 [`a_discarded_completion_returns_its_buffer_to_the_ring`] 的差别在于「取消不等于结
/// 束」在 multishot 上还要多说一句：内核会**接着**投递，每一条都带一个 bid。所以这里发两
/// 次数据，断言环两次都补得回来——只处理第一条的实现在这里会被抓住。
#[test]
fn a_cancelled_recv_multi_returns_its_buffer_to_the_ring() {
    const ENTRIES: u16 = 2;

    let Some(mut driver) = new_driver_or_skip(ENTRIES, 256) else {
        return;
    };
    let sock = SocketPair::new();
    let fd = sock.register_rx(&mut driver);

    let Some(token) = arm_recv_multi(&mut driver, fd) else {
        driver.unregister_files(vec![fd]).unwrap();
        return;
    };

    // 数据还没来就放弃：slot 转 `InFlightOrphaned`，而 multishot 仍在内核里。
    driver.completion_table().mark_orphaned(token);

    for expected_returns in 1..=2u64 {
        sock.send(b"orphaned");
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            driver.drive(DriveMode::Poll).expect("drive failed");
            let stats = driver.provided_buf_stats().expect("the ring is registered");
            if stats.returned >= expected_returns {
                assert_eq!(stats.handed_out, 0, "nobody consumed these completions");
                assert_eq!(
                    stats.available, ENTRIES,
                    "a discarded completion must leave the ring full"
                );
                // 还回环用的是原来那个 buffer，不是新从池里取的。
                assert_eq!(stats.refilled, ENTRIES as u64);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "orphaned completion {expected_returns} never arrived"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }

    // 环没被削短——上面每一轮的 `available == ENTRIES` 就是这句话。这里不再另开一条 recv
    // 来验证：那条 multishot 还在内核里 armed 着，会跟新 recv 抢同一个 socket 上的数据。
    drain_cancelled(&mut driver, token);
    driver.unregister_files(vec![fd]).unwrap();
}
