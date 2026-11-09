//! Networking primitives for TCP/UDP communication without depending on the standard library.

mod addr;
pub(crate) mod sys;
mod tcp;
mod udp;

pub use self::{
    addr::ToSocketAddrs,
    tcp::{Incoming, TcpListener, TcpStream},
    udp::UdpSocket,
};
pub use core::net::*;

/// Possible values which can be passed to the [`TcpStream::shutdown`] method.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Shutdown {
    /// The reading portion of the [`TcpStream`] should be shut down.
    Read,
    /// The writing portion of the [`TcpStream`] should be shut down.
    Write,
    /// Both the reading and the writing portions of the [`TcpStream`] should be shut down.
    Both,
}
