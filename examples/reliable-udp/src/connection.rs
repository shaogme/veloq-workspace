use core::sync::atomic::{AtomicUsize, Ordering};

use veloq::{
    buf::FixedBuf,
    std::{marker::PhantomData, net::SocketAddr, sync::Arc, vec::Vec},
    sync::{mpmc::BoundedOwnedSender, oneshot},
};

use crate::{
    endpoint::{Command, ConnectionKey},
    error::{Error, Result},
    packet::{ConnectionId, MessageId, StreamId, StreamSequence},
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionErrorCode {
    Protocol,
    Application(u32),
}

pub struct StreamMessage {
    pub stream_id: StreamId,
    pub stream_sequence: StreamSequence,
    pub message_id: MessageId,
    pub payload: FixedBuf,
}

impl StreamMessage {
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
    pub(crate) max_message_size: usize,
    pub(crate) drop_command: bool,
    marker: PhantomData<&'rt ()>,
}

impl<'rt> Connection<'rt> {
    pub(crate) fn new(command: CommandSender, key: ConnectionKey, max_message_size: usize) -> Self {
        Self {
            command,
            key,
            max_message_size,
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

    pub async fn open_stream(&self) -> Result<Stream<'rt>> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::OpenStream {
                key: self.key,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        let stream_id = response.await.map_err(|_| Error::EndpointClosed)??;
        Ok(Stream::new(
            self.command.clone(),
            self.key,
            stream_id,
            self.max_message_size,
        ))
    }

    pub async fn accept_stream(&self) -> Result<Stream<'rt>> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::AcceptStream {
                key: self.key,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        let stream_id = response.await.map_err(|_| Error::EndpointClosed)??;
        Ok(Stream::new(
            self.command.clone(),
            self.key,
            stream_id,
            self.max_message_size,
        ))
    }

    pub async fn abort(&self, code: ConnectionErrorCode) -> Result<()> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::Abort {
                key: self.key,
                code,
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

pub struct Stream<'rt> {
    command: CommandSender,
    key: ConnectionKey,
    stream_id: StreamId,
    max_message_size: usize,
    drop_command: bool,
    drop_once: Option<Arc<AtomicUsize>>,
    marker: PhantomData<&'rt ()>,
}

impl<'rt> Stream<'rt> {
    pub(crate) fn new(
        command: CommandSender,
        key: ConnectionKey,
        stream_id: StreamId,
        max_message_size: usize,
    ) -> Self {
        Self {
            command,
            key,
            stream_id,
            max_message_size,
            drop_command: true,
            drop_once: None,
            marker: PhantomData,
        }
    }

    pub fn id(&self) -> StreamId {
        self.stream_id
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.key.peer()
    }

    pub fn connection_id(&self) -> ConnectionId {
        self.key.connection_id()
    }

    pub async fn send(&self, payload: FixedBuf) -> Result<SendReceipt> {
        if payload.len() > self.max_message_size {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamSend {
                key: self.key,
                stream_id: self.stream_id,
                payload: SendPayload::Buffer(payload),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn send_bytes(&self, payload: &[u8]) -> Result<SendReceipt> {
        if payload.len() > self.max_message_size {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamSend {
                key: self.key,
                stream_id: self.stream_id,
                payload: SendPayload::Bytes(payload.to_vec()),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn recv(&mut self) -> Result<StreamMessage> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamRecv {
                key: self.key,
                stream_id: self.stream_id,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn shutdown(&self, how: Shutdown) -> Result<()> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamShutdown {
                key: self.key,
                stream_id: self.stream_id,
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
            .send(Command::StreamClose {
                key: self.key,
                stream_id: self.stream_id,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub fn split(mut self) -> (SendStream<'rt>, RecvStream<'rt>) {
        self.drop_command = false;
        let drop_once = Arc::new(AtomicUsize::new(2));
        let send = SendStream {
            command: self.command.clone(),
            key: self.key,
            stream_id: self.stream_id,
            max_message_size: self.max_message_size,
            drop_command: true,
            drop_once: Some(drop_once.clone()),
            marker: PhantomData,
        };
        let recv = RecvStream {
            command: self.command.clone(),
            key: self.key,
            stream_id: self.stream_id,
            drop_command: true,
            drop_once: Some(drop_once),
            marker: PhantomData,
        };
        (send, recv)
    }
}

impl Drop for Stream<'_> {
    fn drop(&mut self) {
        if self.drop_command
            && self
                .drop_once
                .as_ref()
                .is_none_or(|refs| refs.fetch_sub(1, Ordering::Relaxed) == 1)
        {
            let _ = self.command.try_send(Command::StreamDrop {
                key: self.key,
                stream_id: self.stream_id,
            });
        }
    }
}

pub struct SendStream<'rt> {
    command: CommandSender,
    key: ConnectionKey,
    stream_id: StreamId,
    max_message_size: usize,
    drop_command: bool,
    drop_once: Option<Arc<AtomicUsize>>,
    marker: PhantomData<&'rt ()>,
}

impl Clone for SendStream<'_> {
    fn clone(&self) -> Self {
        if let Some(refs) = &self.drop_once {
            refs.fetch_add(1, Ordering::Relaxed);
        }
        Self {
            command: self.command.clone(),
            key: self.key,
            stream_id: self.stream_id,
            max_message_size: self.max_message_size,
            drop_command: true,
            drop_once: self.drop_once.clone(),
            marker: PhantomData,
        }
    }
}

impl SendStream<'_> {
    pub fn id(&self) -> StreamId {
        self.stream_id
    }

    pub async fn send(&self, payload: FixedBuf) -> Result<SendReceipt> {
        if payload.len() > self.max_message_size {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamSend {
                key: self.key,
                stream_id: self.stream_id,
                payload: SendPayload::Buffer(payload),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }

    pub async fn send_bytes(&self, payload: &[u8]) -> Result<SendReceipt> {
        if payload.len() > self.max_message_size {
            return Err(Error::MessageTooLarge);
        }
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamSend {
                key: self.key,
                stream_id: self.stream_id,
                payload: SendPayload::Bytes(payload.to_vec()),
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }
}

impl Drop for SendStream<'_> {
    fn drop(&mut self) {
        if self.drop_command
            && self
                .drop_once
                .as_ref()
                .is_some_and(|refs| refs.fetch_sub(1, Ordering::Relaxed) == 1)
        {
            let _ = self.command.try_send(Command::StreamDrop {
                key: self.key,
                stream_id: self.stream_id,
            });
        }
    }
}

pub struct RecvStream<'rt> {
    command: CommandSender,
    key: ConnectionKey,
    stream_id: StreamId,
    drop_command: bool,
    drop_once: Option<Arc<AtomicUsize>>,
    marker: PhantomData<&'rt ()>,
}

impl RecvStream<'_> {
    pub fn id(&self) -> StreamId {
        self.stream_id
    }

    pub async fn recv(&mut self) -> Result<StreamMessage> {
        let (reply, response) = oneshot::owned_channel();
        self.command
            .send(Command::StreamRecv {
                key: self.key,
                stream_id: self.stream_id,
                reply,
            })
            .await
            .map_err(|_| Error::EndpointClosed)?;
        response.await.map_err(|_| Error::EndpointClosed)?
    }
}

impl Drop for RecvStream<'_> {
    fn drop(&mut self) {
        if self.drop_command
            && self
                .drop_once
                .as_ref()
                .is_some_and(|refs| refs.fetch_sub(1, Ordering::Relaxed) == 1)
        {
            let _ = self.command.try_send(Command::StreamDrop {
                key: self.key,
                stream_id: self.stream_id,
            });
        }
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
