use std::io;
use std::net::{SocketAddr, ToSocketAddrs};

use crate::net::common::InnerSocket;
use crate::runtime::context::submit;
use veloq_buf::FixedBuf;
use veloq_driver::Socket;
use veloq_driver::op::{
    Connect, DetachedSubmitter, IoFd, LocalSubmitter, Op, OpSubmitter, Recv as OpRecv,
    Send as OpSend, SendTo, UdpRecvDatagram, UdpRecvStream, UdpRefill,
};

// ============================================================================
// Generic UDP Socket
// ============================================================================

pub struct GenericUdpSocket<S: OpSubmitter> {
    pub(crate) inner: InnerSocket,
    pub(crate) submitter: S,
}

pub type LocalUdpSocket = GenericUdpSocket<LocalSubmitter>;
pub type UdpSocket = GenericUdpSocket<DetachedSubmitter>;

// ============================================================================
// Constructors
// ============================================================================

fn bind_inner<A: ToSocketAddrs>(addr: A) -> io::Result<InnerSocket> {
    let addr = addr
        .to_socket_addrs()?
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "No address provided"))?;

    let socket = if addr.is_ipv4() {
        Socket::new_udp_v4()?
    } else {
        Socket::new_udp_v6()?
    };

    socket.bind(addr)?;

    Ok(InnerSocket::new(socket.into_raw()))
}

impl LocalUdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: LocalSubmitter,
        })
    }
}

impl UdpSocket {
    pub fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(Self {
            inner: bind_inner(addr)?,
            submitter: DetachedSubmitter::new()?,
        })
    }
}

// ============================================================================
// Shared Implementation
// ============================================================================

impl<S: OpSubmitter> GenericUdpSocket<S> {
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    pub async fn send_to(
        &self,
        buf: FixedBuf,
        target: SocketAddr,
    ) -> (io::Result<usize>, FixedBuf) {
        let op = SendTo {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
            addr: target,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .unwrap_or_else(|| panic!("Op buffer lost"));
        (res, buf)
    }

    pub async fn recv_stream(&self, buf: FixedBuf) -> io::Result<UdpRecvDatagram> {
        const UDP_PREFILL_CREDITS: usize = 4;
        let refill_capacity = buf.capacity();

        let refill_op = UdpRefill {
            fd: IoFd::Raw(self.inner.raw()),
            buf: Some(buf),
        };
        let (refill_res, refill_back_opt) = submit(&self.submitter, Op::new(refill_op))
            .await
            .into_inner();
        let refill_back = refill_back_opt.ok_or_else(|| io::Error::other("UdpRefill op lost"))?;
        refill_res?;

        // Best-effort top-up to absorb burst packets on RIO pooled recv path.
        for _ in 1..UDP_PREFILL_CREDITS {
            let Some(extra_cap) = std::num::NonZeroUsize::new(refill_capacity) else {
                break;
            };
            let Ok(extra_buf) = FixedBuf::alloc_heap(extra_cap) else {
                break;
            };

            let top_up = UdpRefill {
                fd: IoFd::Raw(self.inner.raw()),
                buf: Some(extra_buf),
            };
            let (top_up_res, _top_up_back) =
                submit(&self.submitter, Op::new(top_up)).await.into_inner();
            // Non-fatal: main recv path still proceeds with at least one refill buffer.
            let _ = top_up_res;
        }

        let op = UdpRecvStream {
            fd: IoFd::Raw(self.inner.raw()),
            buf: refill_back.buf,
            addr: None,
            result: None,
        };
        let (res, op_back_opt) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let mut op_back = op_back_opt.ok_or_else(|| io::Error::other("UdpRecvStream op lost"))?;
        let n = res?;

        if let Some(datagram) = op_back.result.take() {
            return Ok(datagram);
        }

        let mut recv_buf = op_back
            .buf
            .take()
            .ok_or_else(|| io::Error::other("udp recv_stream buffer missing"))?;
        recv_buf.set_len(n);
        let addr = op_back.addr.unwrap_or_else(|| "0.0.0.0:0".parse().unwrap());
        Ok(UdpRecvDatagram {
            buf: recv_buf,
            addr,
        })
    }

    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let (raw_addr, raw_addr_len) = veloq_driver::socket_addr_to_storage(addr);
        #[allow(clippy::unnecessary_cast)]
        let op = Connect {
            fd: IoFd::Raw(self.inner.raw()),
            addr: raw_addr,
            addr_len: raw_addr_len as u32,
        };
        let (res, _) = submit(&self.submitter, Op::new(op)).await.into_inner();
        res.map(|_| ())
    }

    pub async fn send(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = OpSend {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .unwrap_or_else(|| panic!("Op buffer lost"));
        (res, buf)
    }

    pub async fn recv(&self, buf: FixedBuf) -> (io::Result<usize>, FixedBuf) {
        let op = OpRecv {
            fd: IoFd::Raw(self.inner.raw()),
            buf,
        };
        let (res, op_back) = submit(&self.submitter, Op::new(op)).await.into_inner();
        let buf = op_back
            .map(|o| o.buf)
            .unwrap_or_else(|| panic!("Op buffer lost"));
        (res, buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufRead for GenericUdpSocket<S> {
    fn read(
        &self,
        buf: FixedBuf,
    ) -> impl std::future::Future<Output = (io::Result<usize>, FixedBuf)> {
        self.recv(buf)
    }
}

impl<S: OpSubmitter> crate::io::AsyncBufWrite for GenericUdpSocket<S> {
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
