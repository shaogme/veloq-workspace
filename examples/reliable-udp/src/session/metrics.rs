use veloq::std::time::Duration;

use crate::config::Config;

use super::SessionStatsSnapshot;

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct SessionMetrics {
    packets_sent: u64,
    packets_received: u64,
    data_packets: u64,
    ack_packets: u64,
    retransmissions: u64,
    duplicate_packets: u64,
    dropped_packets: u64,
    receive_window_drops: u64,
    out_of_window_drops: u64,
    duplicate_acks: u64,
    rtt_samples: u64,
    latest_rtt: Option<Duration>,
    min_rtt: Option<Duration>,
    max_rtt: Option<Duration>,
    ack_delayed: u64,
    piggybacked_acks: u64,
    completed_messages: u64,
    duplicate_fragments: u64,
    message_ack_retries: u64,
    reassembly_timeouts: u64,
    rtt: RttEstimator,
}

#[derive(Debug, Clone, Copy, Default)]
pub(super) struct RttEstimator {
    srtt: Option<Duration>,
    rttvar: Option<Duration>,
    rto: Duration,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct OutboundView {
    send_window: usize,
    congestion_window: usize,
    slow_start_threshold: usize,
    peer_receive_window: usize,
}

impl OutboundView {
    pub(super) fn new(
        send_window: usize,
        congestion_window: usize,
        slow_start_threshold: usize,
        peer_receive_window: usize,
    ) -> Self {
        Self {
            send_window,
            congestion_window,
            slow_start_threshold,
            peer_receive_window,
        }
    }
}

impl RttEstimator {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            srtt: None,
            rttvar: None,
            rto: config.initial_rto,
        }
    }

    pub(super) fn sample(&mut self, sample: Duration, config: &Config) {
        match (self.srtt, self.rttvar) {
            (None, None) => {
                self.srtt = Some(sample);
                self.rttvar = Some(half(sample));
            }
            (Some(srtt), Some(rttvar)) => {
                let variation = sample.abs_diff(srtt);
                self.rttvar = Some(weighted_duration(rttvar, 3, variation, 1, 4));
                self.srtt = Some(weighted_duration(srtt, 7, sample, 1, 8));
            }
            _ => {
                self.srtt = Some(sample);
                self.rttvar = Some(half(sample));
            }
        }

        let srtt = self.srtt.expect("RTT sample sets srtt");
        let rttvar = self.rttvar.expect("RTT sample sets rttvar");
        let estimate = srtt
            .checked_add(rttvar.saturating_mul(4))
            .unwrap_or(config.max_rto);
        self.rto = estimate.clamp(config.min_rto, config.max_rto);
    }

    pub(super) fn srtt(&self) -> Option<Duration> {
        self.srtt
    }

    pub(super) fn rto(&self) -> Duration {
        self.rto
    }
}

impl SessionMetrics {
    pub(super) fn new(config: &Config) -> Self {
        Self {
            rtt: RttEstimator::new(config),
            ..Self::default()
        }
    }

    pub(super) fn rto(&self) -> Duration {
        self.rtt.rto()
    }

    pub(super) fn record_received(&mut self, data: bool, ack: bool) {
        self.packets_received = self.packets_received.saturating_add(1);
        if data {
            self.data_packets = self.data_packets.saturating_add(1);
        }
        if ack {
            self.ack_packets = self.ack_packets.saturating_add(1);
        }
    }

    pub(super) fn record_sent(&mut self) {
        self.packets_sent = self.packets_sent.saturating_add(1);
    }

    pub(super) fn record_data_sent(&mut self) {
        self.record_sent();
        self.data_packets = self.data_packets.saturating_add(1);
    }

    pub(super) fn record_ack_sent(&mut self) {
        self.record_sent();
        self.ack_packets = self.ack_packets.saturating_add(1);
    }

    pub(super) fn record_retransmission(&mut self) {
        self.retransmissions = self.retransmissions.saturating_add(1);
        self.record_data_sent();
    }

    pub(super) fn record_duplicate(&mut self) {
        self.duplicate_packets = self.duplicate_packets.saturating_add(1);
    }

    pub(super) fn record_drop(&mut self) {
        self.dropped_packets = self.dropped_packets.saturating_add(1);
    }

    pub(super) fn record_receive_window_drop(&mut self) {
        self.record_drop();
        self.receive_window_drops = self.receive_window_drops.saturating_add(1);
    }

    pub(super) fn record_duplicate_ack(&mut self) {
        self.duplicate_acks = self.duplicate_acks.saturating_add(1);
    }

    pub(super) fn record_ack_delayed(&mut self) {
        self.ack_delayed = self.ack_delayed.saturating_add(1);
    }

    pub(super) fn record_piggybacked_ack(&mut self) {
        self.piggybacked_acks = self.piggybacked_acks.saturating_add(1);
    }

    pub(super) fn record_duplicate_fragment(&mut self) {
        self.duplicate_fragments = self.duplicate_fragments.saturating_add(1);
    }

    pub(super) fn record_reassembly_limit(&mut self) {
        self.dropped_packets = self.dropped_packets.saturating_add(1);
    }

    pub(super) fn record_message_ack_retry(&mut self) {
        self.message_ack_retries = self.message_ack_retries.saturating_add(1);
    }

    pub(super) fn record_reassembly_timeout(&mut self) {
        self.reassembly_timeouts = self.reassembly_timeouts.saturating_add(1);
    }

    pub(super) fn record_completed_message(&mut self) {
        self.completed_messages = self.completed_messages.saturating_add(1);
    }

    pub(super) fn record_rtt(&mut self, sample: Duration, config: &Config) {
        self.rtt.sample(sample, config);
        self.rtt_samples = self.rtt_samples.saturating_add(1);
        self.latest_rtt = Some(sample);
        self.min_rtt = Some(self.min_rtt.map_or(sample, |current| current.min(sample)));
        self.max_rtt = Some(self.max_rtt.map_or(sample, |current| current.max(sample)));
    }

    pub(super) fn snapshot(
        &self,
        _config: &Config,
        outbound: OutboundView,
        receive_window: usize,
        reassembly_messages: usize,
        reassembly_bytes: usize,
    ) -> SessionStatsSnapshot {
        SessionStatsSnapshot {
            packets_sent: self.packets_sent,
            packets_received: self.packets_received,
            data_packets: self.data_packets,
            ack_packets: self.ack_packets,
            retransmissions: self.retransmissions,
            duplicate_packets: self.duplicate_packets,
            dropped_packets: self.dropped_packets,
            receive_window_drops: self.receive_window_drops,
            out_of_window_drops: self.out_of_window_drops,
            duplicate_acks: self.duplicate_acks,
            rtt_samples: self.rtt_samples,
            latest_rtt: self.latest_rtt,
            min_rtt: self.min_rtt,
            max_rtt: self.max_rtt,
            srtt: self.rtt.srtt(),
            rto: self.rtt.rto(),
            send_window: outbound.send_window,
            congestion_window: outbound.congestion_window,
            slow_start_threshold: outbound.slow_start_threshold,
            peer_receive_window: outbound.peer_receive_window,
            receive_window,
            ack_delayed: self.ack_delayed,
            piggybacked_acks: self.piggybacked_acks,
            reassembly_messages,
            reassembly_bytes,
            completed_messages: self.completed_messages,
            duplicate_fragments: self.duplicate_fragments,
            message_ack_retries: self.message_ack_retries,
            reassembly_timeouts: self.reassembly_timeouts,
        }
    }
}

pub(super) fn half(duration: Duration) -> Duration {
    duration / 2
}

pub(super) fn weighted_duration(
    first: Duration,
    first_weight: u32,
    second: Duration,
    second_weight: u32,
    divisor: u32,
) -> Duration {
    let first_nanos = first.as_nanos().saturating_mul(u128::from(first_weight));
    let second_nanos = second.as_nanos().saturating_mul(u128::from(second_weight));
    let nanos = first_nanos.saturating_add(second_nanos) / u128::from(divisor);
    let nanos = nanos.min(u128::from(u64::MAX) * 1_000_000_000 + 999_999_999);
    Duration::new(
        u64::try_from(nanos / 1_000_000_000).unwrap_or(u64::MAX),
        u32::try_from(nanos % 1_000_000_000).unwrap_or(999_999_999),
    )
}

#[cfg(test)]
mod tests {
    use veloq::std::time::Duration;

    use super::{half, weighted_duration};

    #[test]
    fn duration_helpers_preserve_fractional_nanos() {
        assert_eq!(half(Duration::from_nanos(3)), Duration::from_nanos(1));
        assert_eq!(
            weighted_duration(Duration::from_nanos(7), 7, Duration::from_nanos(15), 1, 8,),
            Duration::from_nanos(8),
        );
    }

    #[test]
    fn weighted_duration_saturates_at_duration_max() {
        assert_eq!(
            weighted_duration(Duration::MAX, 1, Duration::MAX, 1, 1),
            Duration::MAX,
        );
    }
}
