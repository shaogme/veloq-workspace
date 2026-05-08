use crate::SockAddr;
use diagweave::report::Report;
use std::net::SocketAddr;

/// 平台套接字抽象，由各 driver 后端提供具体实现。
pub trait PlatformSocket: Sized + Send {
    type Handle: crate::RawHandleMeta;
    type Error: std::error::Error + Send + Sync;

    fn new_tcp_v4() -> Result<Self, Report<Self::Error>>;
    fn new_tcp_v6() -> Result<Self, Report<Self::Error>>;
    fn new_udp_v4() -> Result<Self, Report<Self::Error>>;
    fn new_udp_v6() -> Result<Self, Report<Self::Error>>;

    fn bind(&self, addr: SocketAddr) -> Result<(), Report<Self::Error>>;
    fn listen(&self, backlog: i32) -> Result<(), Report<Self::Error>>;
    fn connect(&self, addr: SocketAddr) -> Result<(), Report<Self::Error>>;

    fn into_owned_raw(self) -> crate::OwnedRawHandle<Self::Handle>;

    /// # Safety
    ///
    /// `handle` 必须是有效底层句柄，并满足所有权语义。
    unsafe fn from_raw(handle: Self::Handle) -> Self;

    fn local_addr(&self) -> Result<SocketAddr, Report<Self::Error>>;

    fn set_nodelay(&self, nodelay: bool) -> Result<(), Report<Self::Error>>;
    fn set_recv_buffer_size(&self, size: usize) -> Result<(), Report<Self::Error>>;
    fn set_send_buffer_size(&self, size: usize) -> Result<(), Report<Self::Error>>;
    fn set_reuse_address(&self, reuse: bool) -> Result<(), Report<Self::Error>>;
    fn set_keepalive(&self, keepalive: bool) -> Result<(), Report<Self::Error>>;
    fn set_ttl(&self, ttl: u32) -> Result<(), Report<Self::Error>>;
    fn set_broadcast(&self, broadcast: bool) -> Result<(), Report<Self::Error>>;
}

/// 平台地址存储编解码抽象。
pub trait SocketAddrCodec: SockAddr {
    type Len: Copy + Send;
    type Error: std::error::Error + Send + Sync;

    fn to_socket_addr(buf: &[u8]) -> Result<SocketAddr, Report<Self::Error>>;
    fn socket_addr_to_storage(addr: SocketAddr) -> (Self, Self::Len);
}
