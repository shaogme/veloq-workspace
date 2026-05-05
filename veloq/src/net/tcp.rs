use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::rc::Rc;
use std::sync::Arc;

use crate::net::common::{InnerSocket, SocketToken, SocketTokenPtr};
use crate::runtime::context::submit;
use error_stack::Report;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    Accept, Connect, DetachedSubmitter, LocalSubmitter, Op, OpSubmitter, Recv, Send as OpSend,
};

#[derive(Clone)]
pub struct GenericTcpListener<S: OpSubmitter, P: SocketTokenPtr> {
    pub(crate) inner: InnerSocket<P>,
    pub(crate) submitter: S,
}

#[derive(Clone)]
pub struct GenericTcpStream<S: OpSubmitter, P: SocketTokenPtr> {
    pub(crate) inner: InnerSocket<P>,
    pub(crate) submitter: S,
}

pub type LocalTcpListener = GenericTcpListener<LocalSubmitter, Rc<SocketToken>>;
pub type LocalTcpStream = GenericTcpStream<LocalSubmitter, Rc<SocketToken>>;

pub type TcpListener = GenericTcpListener<DetachedSubmitter, Arc<SocketToken>>;
pub type TcpStream = GenericTcpStream<DetachedSubmitter, Arc<SocketToken>>;

#[inline]
fn driver_err<E>(err: Report<E>) -> io::Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    io::Error::other(err.to_string())
}

fn bind_listener_inner<A: ToSocketAddrs, P: SocketTokenPtr>(addr: A) -> io::Result<InnerSocket<P>> {
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
    socket.listen(1024).map_err(driver_err)?;
    let local_addr = socket.local_addr().map_err(driver_err)?;

    InnerSocket::new(socket.into_owned_raw().into_raw(), Some(local_addr))
}

fn new_stream_inner<P: SocketTokenPtr>(addr: &SocketAddr) -> io::Result<InnerSocket<P>> {
    let socket = if addr.is_ipv4() {
        Socket::new_tcp_v4().map_err(driver_err)?
    } else {
        Socket::new_tcp_v6().map_err(driver_err)?
    };
    InnerSocket::new(socket.into_owned_raw().into_raw(), None)
}

impl<S: OpSubmitter + Copy, P: SocketTokenPtr> GenericTcpListener<S, P> {
    async fn accept_direct(&self) -> io::Result<(GenericTcpStream<S, P>, SocketAddr)> {
        let op = Accept {
            fd: self.inner.fd(),
            addr: veloq_driver::SockAddrStorage::default(),
            addr_len: std::mem::size_of::<veloq_driver::SockAddrStorage>() as u32,
            remote_addr: None,
        };

        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let op =
            op_back.ok_or_else(|| io::Error::new(io::ErrorKind::BrokenPipe, "Accept op lost"))?;

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

impl<S: OpSubmitter + Copy, P: SocketTokenPtr> GenericTcpStream<S, P> {
    async fn connect_from_inner_direct(
        inner: InnerSocket<P>,
        submitter: S,
        addr: SocketAddr,
    ) -> io::Result<Self> {
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

    async fn recv_subset_direct(
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

    async fn send_subset_direct(
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

impl LocalTcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(addr)?,
            submitter: LocalSubmitter,
        })
    }

    pub async fn accept(&self) -> io::Result<(LocalTcpStream, SocketAddr)> {
        self.accept_direct().await
    }
}

impl TcpListener {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_listener_inner(addr)?,
            submitter: DetachedSubmitter::new(),
        })
    }

    pub async fn accept(&self) -> io::Result<(TcpStream, SocketAddr)> {
        self.accept_direct().await
    }
}

impl LocalTcpStream {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let inner = new_stream_inner(&addr)?;
        Self::connect_from_inner_direct(inner, LocalSubmitter, addr).await
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
        self.recv_subset_direct(buf, buf_offset).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }
}

impl TcpStream {
    pub async fn connect(addr: SocketAddr) -> io::Result<Self> {
        let inner = new_stream_inner(&addr)?;
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), addr).await
    }

    pub(crate) async fn connect_from_inner(
        inner: InnerSocket<Arc<SocketToken>>,
        addr: SocketAddr,
    ) -> io::Result<Self> {
        Self::connect_from_inner_direct(inner, DetachedSubmitter::new(), addr).await
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
        self.recv_subset_direct(buf, buf_offset).await
    }

    pub async fn send_subset(
        &self,
        buf: FixedBuf,
        buf_offset: usize,
    ) -> io::Result<(usize, FixedBuf)> {
        self.send_subset_direct(buf, buf_offset).await
    }
}

impl crate::io::AsyncBufRead for LocalTcpStream {
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

impl crate::io::AsyncBufRead for TcpStream {
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

impl crate::io::AsyncBufWrite for LocalTcpStream {
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

impl crate::io::AsyncBufWrite for TcpStream {
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
