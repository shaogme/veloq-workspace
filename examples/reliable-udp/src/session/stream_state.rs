use veloq::std::{
    collections::{HashMap, VecDeque},
    vec::Vec,
};

use crate::{
    config::Config,
    error::{Error, Result},
    packet::StreamId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StreamLifecycle {
    Opening,
    Open,
    HalfClosedLocal,
    HalfClosedRemote,
    Closed,
    Reset,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct StreamState {
    pub(super) lifecycle: StreamLifecycle,
    pub(super) open_retries: u8,
}

impl StreamState {
    pub(super) fn new(lifecycle: StreamLifecycle) -> Self {
        Self {
            lifecycle,
            open_retries: 0,
        }
    }

    pub(super) fn ensure_sendable(&self) -> Result<()> {
        match self.lifecycle {
            StreamLifecycle::Open | StreamLifecycle::HalfClosedRemote => Ok(()),
            StreamLifecycle::Opening => Err(Error::StreamOpenRejected),
            StreamLifecycle::HalfClosedLocal | StreamLifecycle::Closed => Err(Error::StreamClosed),
            StreamLifecycle::Reset => Err(Error::StreamReset),
        }
    }

    pub(super) fn close_local(&mut self) {
        self.lifecycle = match self.lifecycle {
            StreamLifecycle::Open => StreamLifecycle::HalfClosedLocal,
            StreamLifecycle::HalfClosedRemote => StreamLifecycle::Closed,
            current => current,
        };
    }

    pub(super) fn close_remote(&mut self) {
        self.lifecycle = match self.lifecycle {
            StreamLifecycle::Open => StreamLifecycle::HalfClosedRemote,
            StreamLifecycle::HalfClosedLocal => StreamLifecycle::Closed,
            current => current,
        };
    }

    pub(super) fn retry_open(&mut self, max_retries: u8) -> bool {
        if self.lifecycle != StreamLifecycle::Opening || self.open_retries >= max_retries {
            return false;
        }
        self.open_retries = self.open_retries.saturating_add(1);
        true
    }
}

pub(super) struct StreamRegistry {
    states: HashMap<StreamId, StreamState>,
    local_is_client: bool,
    pub(super) next_local_stream_id: StreamId,
    pub(super) max_remote_streams: usize,
    accept_capacity: usize,
    opening: Vec<StreamId>,
    accept_ready: VecDeque<StreamId>,
}

impl StreamRegistry {
    pub(super) fn new(role_is_client: bool, config: &Config) -> Self {
        let first = if role_is_client { 1 } else { 2 };
        Self {
            states: HashMap::default(),
            local_is_client: role_is_client,
            next_local_stream_id: StreamId::new(first).expect("stream ID is non-zero"),
            max_remote_streams: config.max_streams.get(),
            accept_capacity: config.stream_accept_capacity.get(),
            opening: Vec::new(),
            accept_ready: VecDeque::new(),
        }
    }

    pub(super) fn get(&self, id: StreamId) -> Option<&StreamState> {
        self.states.get(&id)
    }

    pub(super) fn get_mut(&mut self, id: StreamId) -> Option<&mut StreamState> {
        self.states.get_mut(&id)
    }

    pub(super) fn contains(&self, id: StreamId) -> bool {
        self.states.contains_key(&id)
    }

    pub(super) fn create_local(&mut self, config: &Config) -> Result<StreamId> {
        if self.states.len() >= config.max_streams.get()
            || self.opening.len() >= config.max_pending_streams.get()
        {
            return Err(Error::TooManyStreams);
        }
        let id = self.next_local_stream_id;
        self.next_local_stream_id = id.next_after();
        self.states
            .insert(id, StreamState::new(StreamLifecycle::Opening));
        self.opening.push(id);
        Ok(id)
    }

    pub(super) fn mark_open(&mut self, id: StreamId) -> Result<()> {
        if id.is_client_initiated() != self.local_is_client {
            return Err(Error::InvalidStreamId);
        }
        let state = self.states.get_mut(&id).ok_or(Error::InvalidStreamId)?;
        if state.lifecycle != StreamLifecycle::Opening {
            return Err(Error::StreamOpenRejected);
        }
        state.lifecycle = StreamLifecycle::Open;
        self.opening.retain(|current| *current != id);
        Ok(())
    }

    pub(super) fn create_remote(&mut self, id: StreamId, config: &Config) -> Result<()> {
        if self.states.contains_key(&id) {
            return Err(Error::DuplicateStreamOpen);
        }
        if self.states.len() >= config.max_streams.get()
            || self.remote_ordinal(id) > self.max_remote_streams
        {
            return Err(Error::TooManyStreams);
        }
        if self.accept_ready.len() >= self.accept_capacity {
            return Err(Error::StreamReceiveWindowClosed);
        }
        self.states
            .insert(id, StreamState::new(StreamLifecycle::Open));
        self.accept_ready.push_back(id);
        Ok(())
    }

    pub(super) fn pop_accept(&mut self) -> Option<StreamId> {
        self.accept_ready.pop_front()
    }

    pub(super) fn mark_reset(&mut self, id: StreamId) -> Result<()> {
        let state = self.states.get_mut(&id).ok_or(Error::InvalidStreamId)?;
        state.lifecycle = StreamLifecycle::Reset;
        self.opening.retain(|current| *current != id);
        Ok(())
    }

    fn remote_ordinal(&self, id: StreamId) -> usize {
        usize::try_from(id.get().saturating_add(1) / 2).unwrap_or(usize::MAX)
    }
}
