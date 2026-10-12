use veloq_driver_core::driver::OpToken;
use veloq_std::{vec, vec::Vec};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BacklogEntry {
    token: OpToken,
}

impl BacklogEntry {
    pub(crate) const fn token(self) -> OpToken {
        self.token
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BacklogError {
    Capacity,
    Duplicate,
    GenerationMismatch,
    Missing,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct BacklogLink {
    entry: Option<BacklogEntry>,
    prev: Option<usize>,
    next: Option<usize>,
}

/// Fixed-capacity, generation-aware FIFO backlog.
///
/// The slot index is the node address.  A node can only contain one generation, so removing a
/// stale token can never remove a newer operation that reused the same registry slot.  Unlinking
/// updates both neighbours directly; there are no stale queue nodes and no membership map to
/// maintain in the hot path.
/// 这是固定容量且按 generation 区分的 FIFO 队列；slot 索引就是节点地址，直接摘链可避免旧节点或成员映射。
pub(crate) struct SubmissionBacklog {
    links: Vec<BacklogLink>,
    head: Option<usize>,
    tail: Option<usize>,
    len: usize,
    peak_len: usize,
}

impl SubmissionBacklog {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            links: vec![BacklogLink::default(); capacity],
            head: None,
            tail: None,
            len: 0,
            peak_len: 0,
        }
    }

    pub(crate) fn push(&mut self, token: OpToken) -> Result<(), BacklogError> {
        let index = token.index();
        let Some(link) = self.links.get(index) else {
            return Err(BacklogError::Capacity);
        };
        if let Some(existing) = link.entry {
            return if existing.token == token {
                Err(BacklogError::Duplicate)
            } else {
                Err(BacklogError::GenerationMismatch)
            };
        }
        if self.len == self.links.len() {
            return Err(BacklogError::Capacity);
        }

        let previous_tail = self.tail;
        self.links[index] = BacklogLink {
            entry: Some(BacklogEntry { token }),
            prev: previous_tail,
            next: None,
        };
        if let Some(previous_tail) = previous_tail {
            self.links[previous_tail].next = Some(index);
        } else {
            self.head = Some(index);
        }
        self.tail = Some(index);
        self.len += 1;
        self.peak_len = self.peak_len.max(self.len);
        Ok(())
    }

    pub(crate) fn front(&self) -> Option<BacklogEntry> {
        self.head.and_then(|index| self.links[index].entry)
    }

    pub(crate) fn pop_front(&mut self) -> Option<BacklogEntry> {
        self.head.map(|index| self.unlink(index))
    }

    pub(crate) fn remove(&mut self, token: OpToken) -> Result<BacklogEntry, BacklogError> {
        let Some(link) = self.links.get(token.index()) else {
            return Err(BacklogError::Capacity);
        };
        let Some(entry) = link.entry else {
            return Err(BacklogError::Missing);
        };
        if entry.token != token {
            return Err(BacklogError::GenerationMismatch);
        }
        Ok(self.unlink(token.index()))
    }

    pub(crate) fn contains(&self, token: OpToken) -> bool {
        self.links
            .get(token.index())
            .and_then(|link| link.entry)
            .is_some_and(|entry| entry.token == token)
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    #[cfg(test)]
    pub(crate) fn peak_len(&self) -> usize {
        self.peak_len
    }

    fn unlink(&mut self, index: usize) -> BacklogEntry {
        let link = self.links[index];
        let entry = link
            .entry
            .expect("linked backlog node must contain an entry");
        if let Some(previous) = link.prev {
            self.links[previous].next = link.next;
        } else {
            self.head = link.next;
        }
        if let Some(next) = link.next {
            self.links[next].prev = link.prev;
        } else {
            self.tail = link.prev;
        }
        self.links[index] = BacklogLink::default();
        self.len -= 1;
        entry
    }

    #[cfg(any(test, feature = "test-hooks"))]
    pub(crate) fn entries(&self) -> Vec<BacklogEntry> {
        let mut entries = Vec::with_capacity(self.len);
        let mut current = self.head;
        while let Some(index) = current {
            let link = self.links[index];
            if let Some(entry) = link.entry {
                entries.push(entry);
            }
            current = link.next;
        }
        entries
    }
}
