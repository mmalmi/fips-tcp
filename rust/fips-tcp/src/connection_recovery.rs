impl<P: Clone> Connection<P> {
    fn retransmit_oldest(&mut self, now_ms: u64, timeout: bool) -> Option<Segment> {
        let window = self.available_window_u16();
        let ack = self.recv_nxt;
        let local_port = self.local_port;
        let remote_port = self.remote_port;
        let mss = self.mss as u16;
        let tracked = self.unacked.front_mut()?;
        // Peer-triggered retries share the timer retry budget. Suppressed
        // attempts must not postpone the last actual send's timeout.
        if tracked.transmissions >= self.max_transmissions {
            return None;
        }
        self.rtt.on_retransmit(self.send_nxt);
        tracked.sent_at_ms = now_ms;
        tracked.transmissions = tracked.transmissions.saturating_add(1);
        if timeout {
            self.duplicate_acks = 0;
        }
        Some(build_segment(
            SegmentHeader {
                local_port,
                remote_port,
                seq: tracked.seq,
                ack,
                window,
                mss,
                flags: tracked.flags,
            },
            tracked.payload.clone(),
        ))
    }

    fn repair_timed_out_flight(&mut self, now_ms: u64, config: &Config) -> Option<Segment> {
        // An advancing ACK exposes the next hole in the old timed-out flight.
        // Repair one segment per ACK, without another RTO backoff for each hole.
        // The caller has already applied this ACK's advertised receive window.
        self.rto_recovery_until?;
        let oldest = self.unacked.front()?;
        if self.remote_window == 0 || oldest.transmissions >= config.max_retransmissions {
            return None;
        }
        self.retransmit_oldest(now_ms, false)
    }
}
