use alloc::vec::Vec;
use slotmap::{DefaultKey, Key};

use crate::error::TimerError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct DeadlineNode {
    pub(crate) key: DefaultKey,
    pub(crate) deadline_tick: u64,
}

pub(crate) struct DeadlineHeap {
    nodes: Vec<DeadlineNode>,
}

impl DeadlineHeap {
    pub(crate) fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    pub(crate) fn peek(&self) -> Option<DeadlineNode> {
        self.nodes.first().copied()
    }

    pub(crate) fn node(&self, index: usize) -> Option<DeadlineNode> {
        self.nodes.get(index).copied()
    }

    pub(crate) fn insert<F>(
        &mut self,
        key: DefaultKey,
        deadline_tick: u64,
        mut set_index: F,
    ) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        let index = self.nodes.len();
        self.nodes.push(DeadlineNode { key, deadline_tick });
        if let Err(error) = set_index(key, index) {
            self.nodes.pop();
            return Err(error);
        }
        self.sift_up(index, &mut set_index)
    }

    pub(crate) fn update<F>(
        &mut self,
        key: DefaultKey,
        index: usize,
        deadline_tick: u64,
        mut set_index: F,
    ) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        let node = self
            .nodes
            .get_mut(index)
            .ok_or(TimerError::InvariantViolation)?;
        if node.key != key {
            return Err(TimerError::InvariantViolation);
        }
        node.deadline_tick = deadline_tick;

        if index != 0 && self.less(index, (index - 1) / 2) {
            self.sift_up(index, &mut set_index)
        } else {
            self.sift_down(index, &mut set_index)
        }
    }

    pub(crate) fn remove<F>(
        &mut self,
        key: DefaultKey,
        index: usize,
        mut set_index: F,
    ) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        if index >= self.nodes.len() || self.nodes[index].key != key {
            return Err(TimerError::InvariantViolation);
        }

        let last = self.nodes.pop().ok_or(TimerError::InvariantViolation)?;
        if index == self.nodes.len() {
            return Ok(());
        }

        self.nodes[index] = last;
        set_index(last.key, index)?;
        if index != 0 && self.less(index, (index - 1) / 2) {
            self.sift_up(index, &mut set_index)
        } else {
            self.sift_down(index, &mut set_index)
        }
    }

    pub(crate) fn clear(&mut self) {
        self.nodes.clear();
    }

    fn sift_up<F>(&mut self, mut index: usize, set_index: &mut F) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        while index != 0 {
            let parent = (index - 1) / 2;
            if !self.less(index, parent) {
                break;
            }
            self.swap_nodes(index, parent, set_index)?;
            index = parent;
        }
        Ok(())
    }

    fn sift_down<F>(&mut self, mut index: usize, set_index: &mut F) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        loop {
            let left = index * 2 + 1;
            if left >= self.nodes.len() {
                break;
            }
            let right = left + 1;
            let child = if right < self.nodes.len() && self.less(right, left) {
                right
            } else {
                left
            };
            if !self.less(child, index) {
                break;
            }
            self.swap_nodes(index, child, set_index)?;
            index = child;
        }
        Ok(())
    }

    fn swap_nodes<F>(
        &mut self,
        left: usize,
        right: usize,
        set_index: &mut F,
    ) -> Result<(), TimerError>
    where
        F: FnMut(DefaultKey, usize) -> Result<(), TimerError>,
    {
        self.nodes.swap(left, right);
        set_index(self.nodes[left].key, left)?;
        set_index(self.nodes[right].key, right)?;
        Ok(())
    }

    fn less(&self, left: usize, right: usize) -> bool {
        let left = self.nodes[left];
        let right = self.nodes[right];
        (left.deadline_tick, left.key.data().as_ffi())
            < (right.deadline_tick, right.key.data().as_ffi())
    }
}
