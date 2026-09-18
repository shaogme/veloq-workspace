use veloq::{
    buf::FixedBuf,
    std::{marker::PhantomData, net::SocketAddr, vec::Vec},
    sync::{mpmc::BoundedOwnedSender, oneshot},
};

use crate::{
    endpoint::{Command, ConnectionKey},
    error::{Error, Result},
    packet::{ConnectionId, MessageSequence},
    session::SendReceipt,
};

type CommandSender = BoundedOwnedSender<Command>;
type Reply<T> = oneshot::OwnedSender<Result<T>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Shutdown {
    Read,
    Write,
    Both,
}

pub struct Message {
    pub sequence: MessageSequence,
    pub payload: FixedBuf,
}

impl Message {
    pub fn as_slice(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn into_fixed_buf(self) -> FixedBuf {
        self.payload
    }
}

pub(crate) enum SendPayload {
    Bytes(Vec<u8>),
    Buffer(FixedBuf),
}

pub struct Connection<'rt> {
    pub(crate) command: CommandSender,
    pub(crate) key: ConnectionKey,
    pub(crate) max_payload: usize,
    pub(crate) drop_command: bool,
    marker: PhantomData<&'rt ()>,
}

impl<'rt> Connection<'rt> {
    pub(crate) fn new(command: CommandSender, key: ConnectionKey, max_payload: usize) -> Self {
        Self {
            command,
            key,
            max_payload,
            drop_command: true,
            marker: PhantomData,
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.key.peer()
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.key.connection_id()
    }

    pub async fn send(&self, payload: FixedBuf) -> Result<SendReceipt> {
        if payload.len() > self.max_payload {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Send {
                key: self.key,
                payload: SendPayload::Buffer(payload),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn send_bytes(&self, payload: &[u8]) -> Result<SendReceipt> {
        if payload.len() > self.max_payload {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Send {
                key: self.key,
                payload: SendPayload::Bytes(payload.to_vec()),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn recv(&mut self) -> Result<Message> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Recv {
                key: self.key,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn shutdown(&self, how: Shutdown) -> Result<()> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Shutdown {
                key: self.key,
                how,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn close(mut self) -> Result<()> {
        self.drop_command = false;
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Close {
                key: self.key,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub(crate) fn suppress_drop(&mut self) {
        self.drop_command = false;
    }
}

impl Drop for Connection<'_> {
    fn drop(&mut self) {
        if self.drop_command {
            let _ = self.command.try_send(Command::Drop { key: self.key });
        }
    }
}

pub(crate) type ConnectionReply = Reply<()>;
