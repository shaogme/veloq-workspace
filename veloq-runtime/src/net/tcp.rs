use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::net::common::InnerSocket;
use crate::runtime::context::submit;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    Accept, Connect, DetachedSubmitter, IoFd, LocalSubmitter, Op, OpLifecycle, OpSubmitter,
    ReadFixed, WriteFixed,
};

// ============================================================================
// Generic TCP Socket
// ============================================================================

pub struct GenericTcpListener<S: OpSubmitter> {
    pub(crate) inner: InnerSocket,
    pub(crate) submitter: S,
}

pub struct GenericTcpStream<S: OpSubmitter> {
    pub(crate) inner: InnerSocket,
    pub(crate) submitter: S,
}

pub type LocalTcpListener = GenericTcpListener<LocalSubmitter>;
pub type LocalTcpStream = GenericTcpStream<LocalSubmitter>;

pub type TcpListener = GenericTcpListener<DetachedSubmitter>;
pub type TcpStream = GenericTcpStream<DetachedSubmitter>;

// ============================================================================
// Constructors and Helpers
// ============================================================================

fn bind_listener_inner<A: ToSocketAddrs>(addr: A) -> io::Result<InnerSocket> {
    let addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;

    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4()?
    } else {
        Socket::new_tcp_v6()?
    };

    socket.bind(addr)?;
    socket.listen(1024)?; // backlog

    Ok(InnerSocket::new(socket.into_raw()))
}

fn new_stream_inner(addr: &SocketAddr) -> io::Result<InnerSocket> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4()?
    } else {
        Socket::new_tcp_v6()?
    };
    Ok(InnerSocket::new(socket.into_raw()))
}

// ============================================================================
// GenericTcpListener Implementation
// ============================================================================

impl<S: OpSubmitter> GenericTcpListener<S> {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(addr)?,
            submitter: S::from_current_context()?,
        })
    }

    pub async fn accept(&self) -> io::Result<(GenericTcpStream<S>, SocketAddr)> {
        let op = Accept::prepare_op(self.inner.raw())?;

        // Wait for connection
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let op = op_back.expect("Accept op lost");

        // Check result and get fd, addr
        let (fd, addr) = op.into_output(res)?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(fd),
            submitter: self.submitter.clone(),
        };

        Ok((stream, addr))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
}

// ============================================================================
// GenericTcpStream Implementation
// ============================================================================

impl<S: OpSubmitter> GenericTcpStream<S> {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let inner = new_stream_inner(&addr)?;
        Self::connect_from_inner(inner, addr).await
    }

    pub(crate) async fn connect_from_inner(
        inner: InnerSocket,
        addr: SocketAddr,
    ) -> io::Result<Self> {
        let submitter = S::from_current_context()?;

        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: IoFd::Raw(inner.raw()),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };

        let (res, _) = submit(&submitter, Op::new(op)).await.into_inner();
        res?;

        Ok(Self { inner, submitter })
    }

    pub async fn recv(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = ReadFixed {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            offset: 0,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .unwrap_or_else(|| panic!("Op buffer lost"));
        (res, buf)
    }

    pub async fn send(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = WriteFixed {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            offset: 0,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .unwrap_or_else(|| panic!("Op buffer lost"));
        (res, buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufRead for GenericTcpStream<S> {
    fn read(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = (io::Result<usize>, FixedBuf)> {
        self.recv(buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufWrite for GenericTcpStream<S> {
    fn write(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = (io::Result<usize>, FixedBuf)> {
        self.send(buf)
    }

    fn flush(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }
}
