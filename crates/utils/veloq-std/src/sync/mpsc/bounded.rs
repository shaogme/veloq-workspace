use super::{RecvError, RecvTimeoutError, SendError, TryRecvError, TrySendError};
use crate::{
    collections::VecDeque,
    mem,
    sync::{Arc, UnpoisonedCondvar, UnpoisonedMutex},
    time::{Duration, Instant},
};

struct Rendezvous<T> {
    value: Option<T>,
    delivered: bool,
    sender_waiting: bool,
}

struct State<T> {
    queue: VecDeque<T>,
    capacity: usize,
    senders: usize,
    receiver_alive: bool,
    waiting_receivers: usize,
    rendezvous: Option<Rendezvous<T>>,
}

pub(super) struct Shared<T> {
    state: UnpoisonedMutex<State<T>>,
    available: UnpoisonedCondvar,
    space: UnpoisonedCondvar,
}

/// The sending-half of a bounded synchronous channel.
pub struct SyncSender<T> {
    pub(super) inner: Arc<Shared<T>>,
}

impl<T> Clone for SyncSender<T> {
    fn clone(&self) -> Self {
        self.inner.state.lock().senders += 1;
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Drop for SyncSender<T> {
    fn drop(&mut self) {
        let mut state = self.inner.state.lock();
        state.senders -= 1;
        if state.senders == 0 {
            self.inner.available.notify_all();
            self.inner.space.notify_all();
        }
    }
}

impl<T> core::fmt::Debug for SyncSender<T> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SyncSender").finish_non_exhaustive()
    }
}

impl<T> SyncSender<T> {
    /// Sends a value on this channel, blocking until capacity is available.
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        let mut state = self.inner.state.lock();

        if state.capacity == 0 {
            while state.receiver_alive
                && (state.rendezvous.is_some() || state.waiting_receivers == 0)
            {
                state = self.inner.space.wait(state);
            }

            if !state.receiver_alive {
                return Err(SendError(value));
            }

            state.rendezvous = Some(Rendezvous {
                value: Some(value),
                delivered: false,
                sender_waiting: true,
            });
            self.inner.available.notify_one();

            loop {
                if state
                    .rendezvous
                    .as_ref()
                    .is_some_and(|rendezvous| rendezvous.delivered)
                {
                    state.rendezvous.take();
                    self.inner.space.notify_all();
                    return Ok(());
                }

                if !state.receiver_alive {
                    let value = state
                        .rendezvous
                        .take()
                        .and_then(|rendezvous| rendezvous.value)
                        .expect("rendezvous value must be present before delivery");
                    self.inner.space.notify_all();
                    return Err(SendError(value));
                }

                state = self.inner.space.wait(state);
            }
        }

        while state.receiver_alive && state.queue.len() >= state.capacity {
            state = self.inner.space.wait(state);
        }

        if !state.receiver_alive {
            return Err(SendError(value));
        }

        state.queue.push_back(value);
        self.inner.available.notify_one();
        Ok(())
    }

    /// Attempts to send a value without blocking.
    pub fn try_send(&self, value: T) -> Result<(), TrySendError<T>> {
        let mut state = self.inner.state.lock();

        if !state.receiver_alive {
            return Err(TrySendError::Disconnected(value));
        }

        if state.capacity == 0 {
            if state.rendezvous.is_some() || state.waiting_receivers == 0 {
                return Err(TrySendError::Full(value));
            }

            state.rendezvous = Some(Rendezvous {
                value: Some(value),
                delivered: false,
                sender_waiting: false,
            });
            self.inner.available.notify_one();
            return Ok(());
        }

        if state.queue.len() >= state.capacity {
            return Err(TrySendError::Full(value));
        }

        state.queue.push_back(value);
        self.inner.available.notify_one();
        Ok(())
    }
}

impl<T> Shared<T> {
    pub(super) fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            state: UnpoisonedMutex::new(State {
                queue: VecDeque::new(),
                capacity,
                senders: 1,
                receiver_alive: true,
                waiting_receivers: 0,
                rendezvous: None,
            }),
            available: UnpoisonedCondvar::new(),
            space: UnpoisonedCondvar::new(),
        })
    }

    pub(super) fn close(&self) {
        let (queue, rendezvous) = {
            let mut state = self.state.lock();
            state.receiver_alive = false;
            let queue = mem::take(&mut state.queue);
            let rendezvous = match state.rendezvous.as_ref() {
                Some(rendezvous) if !rendezvous.sender_waiting => state
                    .rendezvous
                    .take()
                    .and_then(|rendezvous| rendezvous.value),
                _ => None,
            };
            self.available.notify_all();
            self.space.notify_all();
            (queue, rendezvous)
        };

        drop(queue);
        drop(rendezvous);
    }

    fn take_rendezvous_value(state: &mut State<T>) -> Option<T> {
        let (value, remove) = {
            let rendezvous = state.rendezvous.as_mut()?;
            rendezvous.value.as_ref()?;
            rendezvous.delivered = true;
            (rendezvous.value.take(), !rendezvous.sender_waiting)
        };

        if remove {
            state.rendezvous.take();
        }
        value
    }

    pub(super) fn try_recv(&self) -> Result<T, TryRecvError> {
        let mut state = self.state.lock();

        if state.capacity == 0 {
            if let Some(value) = Self::take_rendezvous_value(&mut state) {
                self.space.notify_all();
                return Ok(value);
            }
        } else if let Some(value) = state.queue.pop_front() {
            self.space.notify_one();
            return Ok(value);
        }

        if state.senders == 0 {
            if state.capacity > 0 {
                if let Some(value) = state.queue.pop_front() {
                    return Ok(value);
                }
            } else if let Some(value) = Self::take_rendezvous_value(&mut state) {
                self.space.notify_all();
                return Ok(value);
            }
            Err(TryRecvError::Disconnected)
        } else {
            Err(TryRecvError::Empty)
        }
    }

    pub(super) fn recv(&self) -> Result<T, RecvError> {
        let mut state = self.state.lock();

        if state.capacity == 0 {
            loop {
                if let Some(value) = Self::take_rendezvous_value(&mut state) {
                    self.space.notify_all();
                    return Ok(value);
                }
                if state.senders == 0 {
                    return Err(RecvError);
                }

                state.waiting_receivers += 1;
                self.space.notify_one();
                state = self.available.wait(state);
                state.waiting_receivers -= 1;
            }
        }

        loop {
            if let Some(value) = state.queue.pop_front() {
                self.space.notify_one();
                return Ok(value);
            }
            if state.senders == 0 {
                if let Some(value) = state.queue.pop_front() {
                    return Ok(value);
                }
                return Err(RecvError);
            }
            state = self.available.wait(state);
        }
    }

    pub(super) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
        let deadline = Instant::now() + timeout;
        let mut state = self.state.lock();

        if state.capacity == 0 {
            loop {
                if let Some(value) = Self::take_rendezvous_value(&mut state) {
                    self.space.notify_all();
                    return Ok(value);
                }
                if state.senders == 0 {
                    return Err(RecvTimeoutError::Disconnected);
                }

                let now = Instant::now();
                if now >= deadline {
                    return Err(RecvTimeoutError::Timeout);
                }

                state.waiting_receivers += 1;
                self.space.notify_one();
                let (next_state, _) = self.available.wait_timeout(state, deadline - now);
                state = next_state;
                state.waiting_receivers -= 1;
            }
        }

        loop {
            if let Some(value) = state.queue.pop_front() {
                self.space.notify_one();
                return Ok(value);
            }
            if state.senders == 0 {
                if let Some(value) = state.queue.pop_front() {
                    return Ok(value);
                }
                return Err(RecvTimeoutError::Disconnected);
            }

            let now = Instant::now();
            if now >= deadline {
                return Err(RecvTimeoutError::Timeout);
            }
            let (next_state, _) = self.available.wait_timeout(state, deadline - now);
            state = next_state;
        }
    }
}
