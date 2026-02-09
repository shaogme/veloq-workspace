use super::ext::Extensions;
use super::*;

use crate::Socket;
use crate::config::IocpConfig;
use crate::driver::Driver;
use crate::op::{Accept, Connect, IntoPlatformOp, OpLifecycle, Recv, Timeout};
use std::net::TcpListener;
use std::os::windows::io::IntoRawSocket;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

fn noop_waker() -> Waker {
    unsafe fn clone(_: *const ()) -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn wake(_: *const ()) {}
    unsafe fn wake_by_ref(_: *const ()) {}
    unsafe fn drop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, wake, wake_by_ref, drop);
    unsafe { Waker::from_raw(RawWaker::new(std::ptr::null(), &VTABLE)) }
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

    // Acceptor - pre-create the socket for AcceptEx
    let acceptor = Socket::new_tcp_v4().expect("Acceptor socket creation failed");
    let acceptor_handle = acceptor.into_raw();

    // Prepare Accept Op using OpLifecycle
    let mut accept_op = Accept::into_op(listener_handle.into(), acceptor_handle.into());
    accept_op.accept_socket = acceptor_handle.into();

    let iocp_op = IntoPlatformOp::<IocpDriver>::into_platform_op(accept_op);

    let user_data = driver.reserve_op();
    let _ = driver.submit(user_data, iocp_op);

    // Connect Client in background
    std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(50));
        std::net::TcpStream::connect(addr).expect("Client connect failed");
    });

    // Poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Test timed out");
        }

        driver.process_completions();

        match driver.poll_op(user_data, &mut cx) {
            Poll::Ready((res, iocp_op)) => {
                assert!(res.is_ok(), "Accept failed: {:?}", res.err());
                let op =
                    <Accept as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(iocp_op);
                assert!(op.remote_addr.is_some(), "Remote addr should be populated");
                unsafe {
                    if let Some(fd) = op.fd.raw() {
                        windows_sys::Win32::Foundation::CloseHandle(fd.into());
                    }
                    let s = op.accept_socket;
                    windows_sys::Win32::Foundation::CloseHandle(s.into());
                }
                break;
            }
            Poll::Pending => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
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
    let client_handle = client.into_raw().into();

    // Create Connect Op manually as it doesn't have into_op
    use crate::socket_addr_to_storage;
    let (addr_storage, addr_len) = socket_addr_to_storage(addr);

    let connect_op = Connect {
        fd: crate::op::IoFd::Raw(client_handle),
        addr: addr_storage,
        addr_len: addr_len as u32,
    };

    let iocp_op = IntoPlatformOp::<IocpDriver>::into_platform_op(connect_op);
    let user_data = driver.reserve_op();
    let _ = driver.submit(user_data, iocp_op);

    // Poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Connect Timed out");
        }
        driver.process_completions();
        match driver.poll_op(user_data, &mut cx) {
            Poll::Ready((res, _)) => {
                assert!(res.is_ok(), "Connect failed: {:?}", res.err());
                unsafe { windows_sys::Win32::Foundation::CloseHandle(client_handle.into()) };
                break;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
}

#[test]
fn test_iocp_timeout() {
    let mut driver: IocpDriver = IocpDriver::new(IocpConfig::default()).unwrap();

    let timeout_op = Timeout {
        duration: std::time::Duration::from_millis(100),
    };

    let iocp_op = IntoPlatformOp::<IocpDriver>::into_platform_op(timeout_op);
    let user_data = driver.reserve_op();
    let _ = driver.submit(user_data, iocp_op);

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        // Safety timeout
        if start.elapsed() > std::time::Duration::from_secs(1) {
            panic!("Timeout Op didn't complete in time");
        }

        driver.process_completions();

        match driver.poll_op(user_data, &mut cx) {
            Poll::Ready((res, _)) => {
                assert!(res.is_ok(), "Timeout should succeed");
                let elapsed = start.elapsed();
                assert!(
                    elapsed >= std::time::Duration::from_millis(50),
                    "Should wait at least ~100ms, got {:?}",
                    elapsed
                );
                break;
            }
            Poll::Pending => {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
    }
}

#[test]
fn test_iocp_recv_with_buffer_pool() {
    use std::cell::RefCell;
    use std::rc::Rc;
    use veloq_buf::BufPool;

    let driver = Rc::new(RefCell::new(
        IocpDriver::new(IocpConfig::default()).unwrap(),
    ));

    use veloq_buf::{BlockTopology, ThreadMemoryMultiplier, UniformBlock};

    // Setup GlobalAlloc
    // 10x multiplier -> 20MB
    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(10).unwrap());
    let topology = UniformBlock::hybrid(multiplier);

    let global_pool = topology.create_pool(1).expect("Create pool failed");

    // Wrapper to use driver as registrar
    struct LegacyDriverRegistrar {
        driver: Rc<RefCell<IocpDriver>>,
    }

    impl veloq_buf::BufferRegistrar for LegacyDriverRegistrar {
        fn register(&self, regions: &[veloq_buf::BufferRegion]) -> std::io::Result<Vec<usize>> {
            self.driver.borrow_mut().register_buffer_regions(regions)
        }
    }

    let registrar = Box::new(LegacyDriverRegistrar {
        driver: driver.clone(),
    });

    let reg_pool = topology.build_for_worker(global_pool, 0, registrar);

    // Setup connection
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let client_thread = std::thread::spawn(move || {
        let mut stream = std::net::TcpStream::connect(addr).unwrap();
        use std::io::Write;
        stream.write_all(b"Hello Buffer").unwrap();
    });

    let (stream, _) = listener.accept().unwrap();
    let stream_handle = stream.into_raw_socket().into();

    // Alloc buffer
    let buf = reg_pool
        .alloc(std::num::NonZeroUsize::new(8192).unwrap())
        .expect("Failed to alloc buffer");

    // Create Recv Op
    let recv_op = Recv {
        fd: crate::op::IoFd::Raw(stream_handle),
        buf,
    };

    let iocp_op = IntoPlatformOp::<IocpDriver>::into_platform_op(recv_op);
    let user_data = driver.borrow_mut().reserve_op();
    let _ = driver.borrow_mut().submit(user_data, iocp_op);

    // Poll
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let start = std::time::Instant::now();

    loop {
        if start.elapsed() > std::time::Duration::from_secs(5) {
            panic!("Recv timed out");
        }
        driver.borrow_mut().process_completions();

        match driver.borrow_mut().poll_op(user_data, &mut cx) {
            Poll::Ready((res, iocp_op)) => {
                assert!(res.is_ok(), "Recv failed: {:?}", res.err());
                let bytes_read = res.unwrap();
                assert_eq!(bytes_read, 12);

                let mut op =
                    <Recv as crate::op::IntoPlatformOp<IocpDriver>>::from_platform_op(iocp_op);
                op.buf
                    .set_len(std::num::NonZeroUsize::new(bytes_read).unwrap());
                assert_eq!(&op.buf.as_slice()[..12], b"Hello Buffer");

                unsafe { windows_sys::Win32::Foundation::CloseHandle(stream_handle.into()) };
                break;
            }
            Poll::Pending => std::thread::sleep(std::time::Duration::from_millis(10)),
        }
    }
    client_thread.join().unwrap();
}

#[test]
fn test_rio_extensions_load() {
    let ext = Extensions::new().expect("Extensions failed to load");
    if ext.rio_table.is_some() {
        println!("RIO Extensions loaded successfully");
    } else {
        eprintln!("RIO Extensions not available (this is expected on Windows 7 or older)");
    }
}
