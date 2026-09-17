use veloq::std::{collections::VecDeque, time::Duration, vec::Vec};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramAction {
    Deliver,
    Drop,
    Duplicate,
    Delay(Duration),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualDatagram {
    pub from: usize,
    pub to: usize,
    pub payload: Vec<u8>,
}

#[derive(Debug)]
struct ScheduledDatagram {
    datagram: VirtualDatagram,
    deliver_at: Duration,
}

/// A deterministic in-memory datagram link for protocol tests.
///
/// The link has no socket or clock of its own. Callers explicitly advance its
/// clock and choose whether each subsequently sent datagram is delivered,
/// dropped, duplicated, or delayed. Reordering can be introduced by calling
/// [`Self::reorder_pending`].
#[derive(Debug, Default)]
pub struct VirtualDatagramHarness {
    now: Duration,
    actions: VecDeque<DatagramAction>,
    pending: VecDeque<ScheduledDatagram>,
}

impl VirtualDatagramHarness {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn at(now: Duration) -> Self {
        Self {
            now,
            ..Self::default()
        }
    }

    pub fn push_action(&mut self, action: DatagramAction) {
        self.actions.push_back(action);
    }

    pub fn send(&mut self, from: usize, to: usize, payload: Vec<u8>) {
        let action = self.actions.pop_front().unwrap_or(DatagramAction::Deliver);
        match action {
            DatagramAction::Deliver => self.enqueue(from, to, payload, Duration::ZERO),
            DatagramAction::Drop => {}
            DatagramAction::Duplicate => {
                self.enqueue(from, to, payload.clone(), Duration::ZERO);
                self.enqueue(from, to, payload, Duration::ZERO);
            }
            DatagramAction::Delay(delay) => self.enqueue(from, to, payload, delay),
        }
    }

    pub fn advance(&mut self, elapsed: Duration) {
        self.now = self.now.saturating_add(elapsed);
    }

    pub fn now(&self) -> Duration {
        self.now
    }

    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    pub fn reorder_pending(&mut self) {
        if self.pending.len() > 1 {
            let first = self.pending.pop_front().expect("length checked above");
            let last = self.pending.pop_back().expect("length checked above");
            self.pending.push_front(last);
            self.pending.push_back(first);
        }
    }

    pub fn recv(&mut self) -> Option<VirtualDatagram> {
        let index = self
            .pending
            .iter()
            .position(|entry| entry.deliver_at <= self.now)?;
        self.pending.remove(index).map(|entry| entry.datagram)
    }

    fn enqueue(&mut self, from: usize, to: usize, payload: Vec<u8>, delay: Duration) {
        self.pending.push_back(ScheduledDatagram {
            datagram: VirtualDatagram { from, to, payload },
            deliver_at: self.now.saturating_add(delay),
        });
    }
}
