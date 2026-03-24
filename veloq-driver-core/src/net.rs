use crate::SockAddr;
use std::io;
use std::net::SocketAddr;

/// 平台套接字抽象，由各 driver 后端提供具体实现。
pub trait PlatformSocket: Sized + Send + 'static {
    type Handle: crate::RawHandleMeta;

    fn new_tcp_v4() -> io::Result<Self>;
    fn new_tcp_v6() -> io::Result<Self>;
    fn new_udp_v4() -> io::Result<Self>;
    fn new_udp_v6() -> io::Result<Self>;

    fn bind(&self, addr: SocketAddr) -> io::Result<()>;
    fn listen(&self, backlog: i32) -> io::Result<()>;
    fn connect(&self, addr: SocketAddr) -> io::Result<()>;

    fn into_owned_raw(self) -> crate::OwnedRawHandle<Self::Handle>;

    /// # Safety
    ///
    /// `handle` 必须是有效底层句柄，并满足所有权语义。
    unsafe fn from_raw(handle: Self::Handle) -> Self;

    fn local_addr(&self) -> io::Result<SocketAddr>;

    fn set_nodelay(&self, nodelay: bool) -> io::Result<()>;
    fn set_recv_buffer_size(&self, size: usize) -> io::Result<()>;
    fn set_send_buffer_size(&self, size: usize) -> io::Result<()>;
    fn set_reuse_address(&self, reuse: bool) -> io::Result<()>;
    fn set_keepalive(&self, keepalive: bool) -> io::Result<()>;
    fn set_ttl(&self, ttl: u32) -> io::Result<()>;
    fn set_broadcast(&self, broadcast: bool) -> io::Result<()>;
}

/// 平台地址存储编解码抽象。
pub trait SocketAddrCodec: SockAddr {
    type Len: Copy + Send + 'static;

    fn to_socket_addr(buf: &[u8]) -> io::Result<SocketAddr>;
    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len);
}
