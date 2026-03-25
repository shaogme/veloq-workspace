use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::net::common::InnerSocket;
use crate::runtime::context::submit;
use error_stack::Report;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, Send as OpSend,
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

#[inline]
fn driver_err<E>(err: Report<E>) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::other(err.to_string())
}

// ============================================================================
// Constructors and Helpers
// ============================================================================

fn bind_listener_inner<A: ToSocketAddrs>(addr: A) -> io::Result<InnerSocket> {
    let addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;

    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().map_err(driver_err)?
    } else {
        Socket::new_tcp_v6().map_err(driver_err)?
    };

    socket.bind(addr).map_err(driver_err)?;
    socket.listen(1024).map_err(driver_err)?; // backlog
    let local_addr = socket.local_addr().map_err(driver_err)?;

    InnerSocket::new(socket.into_owned_raw().into_raw(), Some(local_addr))
}

fn new_stream_inner(addr: &SocketAddr) -> io::Result<InnerSocket> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().map_err(driver_err)?
    } else {
        Socket::new_tcp_v6().map_err(driver_err)?
    };
    InnerSocket::new(socket.into_owned_raw().into_raw(), None)
}

// ============================================================================
// GenericTcpListener Implementation
// ============================================================================

impl<S: OpSubmitter> GenericTcpListener<S> {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(addr)?,
            submitter: S::from_current_context(),
        })
    }

    pub async fn accept(&self) -> io::Result<(GenericTcpStream<S>, SocketAddr)> {
        let op = Accept {
            fd: self.inner.fd(),
            addr: veloq_driver::SockAddrStorage::default(),
            addr_len: std::mem::size_of::<veloq_driver::SockAddrStorage>() as u32,
            remote_addr: None,
        };

        // Wait for connection
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let op =
            op_back.ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Accept op lost"))?;

        // Completion value is the accepted socket handle; address comes from payload sideband.
        let accepted = res.map_err(driver_err)?;
        let addr = op.remote_addr.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "Accept completed without remote address",
            )
        })?;

        let stream = GenericTcpStream {
            inner: InnerSocket::new(accepted.into_raw(), None)?,
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
        let submitter = S::from_current_context();

        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: inner.fd(),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };

        let (res, _) = submit(&submitter, Op::new(op)).await.into_inner();
        res.map_err(driver_err)?;

        Ok(Self { inner, submitter })
    }

    pub async fn recv(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.recv_subset(buf, 0).await
    }

    pub async fn send(&self, buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        self.send_subset(buf, 0).await
    }

    pub async fn recv_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = Recv {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        let op = OpSend {
            fd: self.inner.fd(),
            buf,
            buf_offset,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Op buffer lost"))?;
        Ok((res.map_err(driver_err)?, buf))
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufRead for GenericTcpStream<S> {
    fn read(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = io::Result<(usize, FixedBuf)>> {
        self.recv(buf)
    }

    async fn read_exact(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.recv_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ));
            }
            total += n;
        }
        Ok((total, buf))
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufWrite for GenericTcpStream<S> {
    fn write(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = io::Result<(usize, FixedBuf)>> {
        self.send(buf)
    }

    async fn write_all(&self, mut buf: FixedBuf) -> io::Result<(usize, FixedBuf)> {
        let target = buf.len();
        let mut total = 0;
        while total < target {
            let (n, b) = self.send_subset(buf, total).await?;
            buf = b;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            total += n;
        }
        Ok((total, buf))
    }

    fn flush(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    fn shutdown(&self) -> impl std::future::Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }
}
