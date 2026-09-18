use veloq::std::{
    collections::HashMap,
    time::{Duration, Instant},
    vec::Vec,
};
use veloq_wheel::{Expired, TimerId, Wheel};

use crate::{
    Config,
    error::Result,
    timer::{TimerCommand, TimerKind},
};

use super::ConnectionKey;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct TimerSlotKey {
    connection: ConnectionKey,
    kind: TimerKind,
    generation: u64,
}

impl TimerSlotKey {
    pub(super) fn connection(&self) -> ConnectionKey {
        self.connection
    }

    pub(super) fn kind(&self) -> TimerKind {
        self.kind
    }

    pub(super) fn generation(&self) -> u64 {
        self.generation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointTimer {
    slot: TimerSlotKey,
    deadline: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ExpiredTimer {
    slot: TimerSlotKey,
    delay: Duration,
}

impl ExpiredTimer {
    pub(super) fn slot(&self) -> TimerSlotKey {
        self.slot
    }

    pub(super) fn delay(&self) -> Duration {
        self.delay
    }
}

struct EndpointTimerRegistry {
    slots: HashMap<TimerSlotKey, TimerId>,
}

impl EndpointTimerRegistry {
    fn new() -> Self {
        Self {
            slots: HashMap::default(),
        }
    }

    fn arm(
        &mut self,
        wheel: &mut Wheel<EndpointTimer>,
        connection: ConnectionKey,
        kind: TimerKind,
        generation: u64,
        delay: Duration,
        now: Duration,
    ) -> Result<()> {
        let slot = TimerSlotKey {
            connection,
            kind,
            generation,
        };
        if let Some(timer_id) = self.slots.remove(&slot) {
            let _ = wheel.cancel(timer_id);
        }
        let deadline = now.checked_add(delay).unwrap_or(Duration::MAX);
        let timer_id = wheel.insert(EndpointTimer { slot, deadline }, delay)?;
        self.slots.insert(slot, timer_id);
        Ok(())
    }

    fn cancel(
        &mut self,
        wheel: &mut Wheel<EndpointTimer>,
        connection: ConnectionKey,
        kind: TimerKind,
        generation: u64,
    ) {
        let slot = TimerSlotKey {
            connection,
            kind,
            generation,
        };
        if let Some(timer_id) = self.slots.remove(&slot) {
            let _ = wheel.cancel(timer_id);
        }
    }

    fn cancel_connection(&mut self, wheel: &mut Wheel<EndpointTimer>, connection: ConnectionKey) {
        let slots: Vec<TimerSlotKey> = self
            .slots
            .keys()
            .copied()
            .filter(|slot| slot.connection == connection)
            .collect();
        for slot in slots {
            self.cancel(wheel, connection, slot.kind, slot.generation);
        }
    }

    fn take_expired(
        &mut self,
        expired: &Expired<EndpointTimer>,
    ) -> Option<(TimerSlotKey, Duration)> {
        if self.slots.get(&expired.item.slot) != Some(&expired.id) {
            return None;
        }
        self.slots.remove(&expired.item.slot);
        Some((expired.item.slot, expired.item.deadline))
    }

    fn clear(&mut self, wheel: &mut Wheel<EndpointTimer>) {
        let mut discarded = Vec::new();
        wheel.clear(&mut discarded);
        self.slots.clear();
    }
}

pub(super) struct EndpointClock {
    wheel: Wheel<EndpointTimer>,
    registry: EndpointTimerRegistry,
    logical_now: Duration,
    last_advanced: Instant,
}

impl EndpointClock {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            wheel: Wheel::new(config.wheel.clone()),
            registry: EndpointTimerRegistry::new(),
            logical_now: Duration::ZERO,
            last_advanced: Instant::now(),
        }
    }

    pub(super) fn now(&self) -> Duration {
        self.logical_now
    }

    pub(super) fn next_deadline(&self) -> Result<Option<Duration>> {
        Ok(self.wheel.next_deadline()?)
    }

    pub(super) fn advance(&mut self) -> Result<Vec<ExpiredTimer>> {
        let now = Instant::now();
        let elapsed = now.saturating_duration_since(self.last_advanced);
        self.last_advanced = now;
        self.logical_now = self.logical_now.saturating_add(elapsed);
        let mut expired = Vec::new();
        self.wheel.advance_by(elapsed, &mut expired)?;
        Ok(expired
            .into_iter()
            .filter_map(|timer| {
                let (slot, deadline) = self.registry.take_expired(&timer)?;
                Some(ExpiredTimer {
                    slot,
                    delay: self.logical_now.saturating_sub(deadline),
                })
            })
            .collect())
    }

    pub(super) fn apply(&mut self, connection: ConnectionKey, command: TimerCommand) -> Result<()> {
        match command {
            TimerCommand::Cancel { kind, generation } => {
                self.registry
                    .cancel(&mut self.wheel, connection, kind, generation);
                Ok(())
            }
            TimerCommand::Arm {
                kind,
                generation,
                delay,
            } => self.registry.arm(
                &mut self.wheel,
                connection,
                kind,
                generation,
                delay,
                self.logical_now,
            ),
        }
    }

    pub(super) fn cancel_connection(&mut self, connection: ConnectionKey) {
        self.registry.cancel_connection(&mut self.wheel, connection);
    }

    pub(super) fn clear(&mut self) {
        self.registry.clear(&mut self.wheel);
    }
}

#[cfg(test)]
mod tests {
    use veloq::std::{
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        time::Duration,
        vec::Vec,
    };

    use crate::{
        Config,
        packet::{ConnectionId, FrameSequence},
        timer::TimerKind,
    };

    use super::{ConnectionKey, EndpointTimerRegistry};
    use veloq_wheel::Wheel;

    fn key(port: u16, id: u64) -> ConnectionKey {
        ConnectionKey::new(
            SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port)),
            ConnectionId::new(id).expect("connection ID"),
        )
    }

    #[test]
    fn registry_replaces_and_cancels_a_logical_slot() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let connection = key(10_001, 1);
        let kind = TimerKind::Retransmit {
            sequence: FrameSequence::new(1).expect("sequence"),
        };
        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                1,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("first arm");
        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                1,
                Duration::from_millis(30),
                Duration::ZERO,
            )
            .expect("replacement arm");
        assert_eq!(wheel.len(), 1);
        registry.cancel(&mut wheel, connection, kind, 1);
        assert!(wheel.is_empty());
    }

    #[test]
    fn registry_rejects_stale_expiration_ids() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let connection = key(10_002, 2);
        let kind = TimerKind::AckDelay;
        registry
            .arm(
                &mut wheel,
                connection,
                kind,
                7,
                Duration::from_millis(20),
                Duration::ZERO,
            )
            .expect("arm");
        let mut expired = Vec::new();
        wheel
            .advance_by(Duration::from_millis(20), &mut expired)
            .expect("advance");
        assert!(registry.take_expired(&expired[0]).is_some());
        assert!(registry.take_expired(&expired[0]).is_none());
    }

    #[test]
    fn registry_cleanup_is_scoped_to_one_connection() {
        let mut wheel = Wheel::new(Config::default().wheel.clone());
        let mut registry = EndpointTimerRegistry::new();
        let first = key(10_003, 3);
        let second = key(10_004, 4);
        for (connection, id) in [(first, 1), (second, 2)] {
            registry
                .arm(
                    &mut wheel,
                    connection,
                    TimerKind::HandshakeRetry,
                    id,
                    Duration::from_millis(20),
                    Duration::ZERO,
                )
                .expect("arm");
        }
        registry.cancel_connection(&mut wheel, first);
        assert_eq!(wheel.len(), 1);
        registry.cancel_connection(&mut wheel, second);
        assert!(wheel.is_empty());
    }
}
