use veloq::std::{
    sync::atomic::{NativeAtomicU64, Ordering},
    time::Duration,
};

use crate::session::SessionStatsSnapshot;

use super::EndpointStatsSnapshot;

pub(super) struct EndpointStats {
    active_connections: NativeAtomicU64,
    total_connections: NativeAtomicU64,
    packets_sent: NativeAtomicU64,
    packets_received: NativeAtomicU64,
    data_packets: NativeAtomicU64,
    ack_packets: NativeAtomicU64,
    retransmissions: NativeAtomicU64,
    duplicate_packets: NativeAtomicU64,
    dropped_packets: NativeAtomicU64,
    malformed_packets: NativeAtomicU64,
    oversized_datagrams: NativeAtomicU64,
    unknown_connections: NativeAtomicU64,
    receive_window_drops: NativeAtomicU64,
    out_of_window_drops: NativeAtomicU64,
    duplicate_acks: NativeAtomicU64,
    rtt_samples: NativeAtomicU64,
    latest_rtt_nanos: NativeAtomicU64,
    min_rtt_nanos: NativeAtomicU64,
    max_rtt_nanos: NativeAtomicU64,
    rto_nanos: NativeAtomicU64,
    congestion_window: NativeAtomicU64,
    send_window: NativeAtomicU64,
    peer_receive_window: NativeAtomicU64,
    receive_window: NativeAtomicU64,
    ack_delayed: NativeAtomicU64,
    piggybacked_acks: NativeAtomicU64,
    reassembly_messages: NativeAtomicU64,
    reassembly_bytes: NativeAtomicU64,
    completed_messages: NativeAtomicU64,
    duplicate_fragments: NativeAtomicU64,
    message_ack_retries: NativeAtomicU64,
    reassembly_timeouts: NativeAtomicU64,
    timer_expirations: NativeAtomicU64,
    timer_delay_samples: NativeAtomicU64,
    timer_delay_total_nanos: NativeAtomicU64,
    timer_delay_max_nanos: NativeAtomicU64,
    inbound_dropped: NativeAtomicU64,
    outbound_dropped: NativeAtomicU64,
}

impl EndpointStats {
    pub(super) fn new() -> Self {
        Self {
            active_connections: NativeAtomicU64::new(0),
            total_connections: NativeAtomicU64::new(0),
            packets_sent: NativeAtomicU64::new(0),
            packets_received: NativeAtomicU64::new(0),
            data_packets: NativeAtomicU64::new(0),
            ack_packets: NativeAtomicU64::new(0),
            retransmissions: NativeAtomicU64::new(0),
            duplicate_packets: NativeAtomicU64::new(0),
            dropped_packets: NativeAtomicU64::new(0),
            malformed_packets: NativeAtomicU64::new(0),
            oversized_datagrams: NativeAtomicU64::new(0),
            unknown_connections: NativeAtomicU64::new(0),
            receive_window_drops: NativeAtomicU64::new(0),
            out_of_window_drops: NativeAtomicU64::new(0),
            duplicate_acks: NativeAtomicU64::new(0),
            rtt_samples: NativeAtomicU64::new(0),
            latest_rtt_nanos: NativeAtomicU64::new(0),
            min_rtt_nanos: NativeAtomicU64::new(u64::MAX),
            max_rtt_nanos: NativeAtomicU64::new(0),
            rto_nanos: NativeAtomicU64::new(0),
            congestion_window: NativeAtomicU64::new(0),
            send_window: NativeAtomicU64::new(0),
            peer_receive_window: NativeAtomicU64::new(0),
            receive_window: NativeAtomicU64::new(0),
            ack_delayed: NativeAtomicU64::new(0),
            piggybacked_acks: NativeAtomicU64::new(0),
            reassembly_messages: NativeAtomicU64::new(0),
            reassembly_bytes: NativeAtomicU64::new(0),
            completed_messages: NativeAtomicU64::new(0),
            duplicate_fragments: NativeAtomicU64::new(0),
            message_ack_retries: NativeAtomicU64::new(0),
            reassembly_timeouts: NativeAtomicU64::new(0),
            timer_expirations: NativeAtomicU64::new(0),
            timer_delay_samples: NativeAtomicU64::new(0),
            timer_delay_total_nanos: NativeAtomicU64::new(0),
            timer_delay_max_nanos: NativeAtomicU64::new(0),
            inbound_dropped: NativeAtomicU64::new(0),
            outbound_dropped: NativeAtomicU64::new(0),
        }
    }

    pub(super) fn snapshot(&self) -> EndpointStatsSnapshot {
        let samples = self.rtt_samples.load(Ordering::Relaxed);
        let min = self.min_rtt_nanos.load(Ordering::Relaxed);
        EndpointStatsSnapshot {
            active_connections: self.active_connections.load(Ordering::Relaxed),
            total_connections: self.total_connections.load(Ordering::Relaxed),
            packets_sent: self.packets_sent.load(Ordering::Relaxed),
            packets_received: self.packets_received.load(Ordering::Relaxed),
            data_packets: self.data_packets.load(Ordering::Relaxed),
            ack_packets: self.ack_packets.load(Ordering::Relaxed),
            retransmissions: self.retransmissions.load(Ordering::Relaxed),
            duplicate_packets: self.duplicate_packets.load(Ordering::Relaxed),
            dropped_packets: self.dropped_packets.load(Ordering::Relaxed),
            malformed_packets: self.malformed_packets.load(Ordering::Relaxed),
            oversized_datagrams: self.oversized_datagrams.load(Ordering::Relaxed),
            unknown_connections: self.unknown_connections.load(Ordering::Relaxed),
            receive_window_drops: self.receive_window_drops.load(Ordering::Relaxed),
            out_of_window_drops: self.out_of_window_drops.load(Ordering::Relaxed),
            duplicate_acks: self.duplicate_acks.load(Ordering::Relaxed),
            rtt_samples: samples,
            latest_rtt: (samples > 0)
                .then(|| duration_from_nanos(self.latest_rtt_nanos.load(Ordering::Relaxed))),
            min_rtt: (samples > 0 && min != u64::MAX).then(|| duration_from_nanos(min)),
            max_rtt: (samples > 0)
                .then(|| duration_from_nanos(self.max_rtt_nanos.load(Ordering::Relaxed))),
            rto: duration_from_nanos(self.rto_nanos.load(Ordering::Relaxed)),
            congestion_window: self.congestion_window.load(Ordering::Relaxed),
            send_window: self.send_window.load(Ordering::Relaxed),
            peer_receive_window: self.peer_receive_window.load(Ordering::Relaxed),
            receive_window: self.receive_window.load(Ordering::Relaxed),
            ack_delayed: self.ack_delayed.load(Ordering::Relaxed),
            piggybacked_acks: self.piggybacked_acks.load(Ordering::Relaxed),
            reassembly_messages: self.reassembly_messages.load(Ordering::Relaxed),
            reassembly_bytes: self.reassembly_bytes.load(Ordering::Relaxed),
            completed_messages: self.completed_messages.load(Ordering::Relaxed),
            duplicate_fragments: self.duplicate_fragments.load(Ordering::Relaxed),
            message_ack_retries: self.message_ack_retries.load(Ordering::Relaxed),
            reassembly_timeouts: self.reassembly_timeouts.load(Ordering::Relaxed),
            timer_expirations: self.timer_expirations.load(Ordering::Relaxed),
            timer_delay_samples: self.timer_delay_samples.load(Ordering::Relaxed),
            timer_delay_total: duration_from_nanos(
                self.timer_delay_total_nanos.load(Ordering::Relaxed),
            ),
            timer_delay_max: duration_from_nanos(
                self.timer_delay_max_nanos.load(Ordering::Relaxed),
            ),
            inbound_queue_drops: self.inbound_dropped.load(Ordering::Relaxed),
            outbound_queue_drops: self.outbound_dropped.load(Ordering::Relaxed),
        }
    }

    pub(super) fn inbound_dropped(&self) -> u64 {
        self.inbound_dropped.load(Ordering::Relaxed)
    }

    pub(super) fn outbound_dropped(&self) -> u64 {
        self.outbound_dropped.load(Ordering::Relaxed)
    }

    pub(super) fn connection_started(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        self.total_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn connection_finished(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    pub(super) fn record_protocol_drop(&self) {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_inbound_drop(&self) {
        self.inbound_dropped.fetch_add(1, Ordering::Relaxed);
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_outbound_drop(&self) {
        self.outbound_dropped.fetch_add(1, Ordering::Relaxed);
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_oversized(&self) {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
        self.oversized_datagrams.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_malformed(&self) {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
        self.malformed_packets.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_unknown(&self) {
        self.dropped_packets.fetch_add(1, Ordering::Relaxed);
        self.unknown_connections.fetch_add(1, Ordering::Relaxed);
    }

    pub(super) fn record_timer_expiration(&self, delay: Duration) {
        self.timer_expirations.fetch_add(1, Ordering::Relaxed);
        self.timer_delay_samples.fetch_add(1, Ordering::Relaxed);
        self.timer_delay_total_nanos
            .fetch_add(duration_nanos(delay), Ordering::Relaxed);
        self.timer_delay_max_nanos
            .fetch_max(duration_nanos(delay), Ordering::Relaxed);
    }

    pub(super) fn record_session_delta(
        &self,
        previous: SessionStatsSnapshot,
        current: SessionStatsSnapshot,
    ) {
        add_delta(
            &self.packets_sent,
            previous.packets_sent,
            current.packets_sent,
        );
        add_delta(
            &self.packets_received,
            previous.packets_received,
            current.packets_received,
        );
        add_delta(
            &self.data_packets,
            previous.data_packets,
            current.data_packets,
        );
        add_delta(&self.ack_packets, previous.ack_packets, current.ack_packets);
        add_delta(
            &self.retransmissions,
            previous.retransmissions,
            current.retransmissions,
        );
        add_delta(
            &self.duplicate_packets,
            previous.duplicate_packets,
            current.duplicate_packets,
        );
        add_delta(
            &self.dropped_packets,
            previous.dropped_packets,
            current.dropped_packets,
        );
        add_delta(
            &self.receive_window_drops,
            previous.receive_window_drops,
            current.receive_window_drops,
        );
        add_delta(
            &self.out_of_window_drops,
            previous.out_of_window_drops,
            current.out_of_window_drops,
        );
        add_delta(
            &self.duplicate_acks,
            previous.duplicate_acks,
            current.duplicate_acks,
        );
        add_delta(&self.rtt_samples, previous.rtt_samples, current.rtt_samples);
        add_delta(&self.ack_delayed, previous.ack_delayed, current.ack_delayed);
        add_delta(
            &self.piggybacked_acks,
            previous.piggybacked_acks,
            current.piggybacked_acks,
        );
        add_delta(
            &self.completed_messages,
            previous.completed_messages,
            current.completed_messages,
        );
        add_delta(
            &self.duplicate_fragments,
            previous.duplicate_fragments,
            current.duplicate_fragments,
        );
        add_delta(
            &self.message_ack_retries,
            previous.message_ack_retries,
            current.message_ack_retries,
        );
        add_delta(
            &self.reassembly_timeouts,
            previous.reassembly_timeouts,
            current.reassembly_timeouts,
        );
        if current.latest_rtt != previous.latest_rtt
            && let Some(rtt) = current.latest_rtt
        {
            self.latest_rtt_nanos
                .store(duration_nanos(rtt), Ordering::Relaxed);
        }
        if let Some(rtt) = current.min_rtt {
            self.min_rtt_nanos
                .fetch_min(duration_nanos(rtt), Ordering::Relaxed);
        }
        if let Some(rtt) = current.max_rtt {
            self.max_rtt_nanos
                .fetch_max(duration_nanos(rtt), Ordering::Relaxed);
        }
        adjust_gauge(
            &self.rto_nanos,
            duration_nanos(previous.rto),
            duration_nanos(current.rto),
        );
        adjust_gauge(
            &self.congestion_window,
            previous.congestion_window as u64,
            current.congestion_window as u64,
        );
        adjust_gauge(
            &self.send_window,
            previous.send_window as u64,
            current.send_window as u64,
        );
        adjust_gauge(
            &self.peer_receive_window,
            previous.peer_receive_window as u64,
            current.peer_receive_window as u64,
        );
        adjust_gauge(
            &self.receive_window,
            previous.receive_window as u64,
            current.receive_window as u64,
        );
        adjust_gauge(
            &self.reassembly_messages,
            previous.reassembly_messages as u64,
            current.reassembly_messages as u64,
        );
        adjust_gauge(
            &self.reassembly_bytes,
            previous.reassembly_bytes as u64,
            current.reassembly_bytes as u64,
        );
    }

    pub(super) fn remove_session_gauges(&self, snapshot: SessionStatsSnapshot) {
        adjust_gauge(&self.rto_nanos, duration_nanos(snapshot.rto), 0);
        adjust_gauge(
            &self.congestion_window,
            snapshot.congestion_window as u64,
            0,
        );
        adjust_gauge(&self.send_window, snapshot.send_window as u64, 0);
        adjust_gauge(
            &self.peer_receive_window,
            snapshot.peer_receive_window as u64,
            0,
        );
        adjust_gauge(&self.receive_window, snapshot.receive_window as u64, 0);
        adjust_gauge(
            &self.reassembly_messages,
            snapshot.reassembly_messages as u64,
            0,
        );
        adjust_gauge(&self.reassembly_bytes, snapshot.reassembly_bytes as u64, 0);
    }
}

pub(super) fn duration_nanos(duration: Duration) -> u64 {
    duration.as_nanos().try_into().unwrap_or(u64::MAX)
}

fn duration_from_nanos(nanos: u64) -> Duration {
    Duration::from_nanos(nanos)
}

fn add_delta(counter: &NativeAtomicU64, previous: u64, current: u64) {
    counter.fetch_add(current.saturating_sub(previous), Ordering::Relaxed);
}

fn adjust_gauge(counter: &NativeAtomicU64, previous: u64, current: u64) {
    if current >= previous {
        counter.fetch_add(current - previous, Ordering::Relaxed);
    } else {
        counter.fetch_sub(previous - current, Ordering::Relaxed);
    }
}
