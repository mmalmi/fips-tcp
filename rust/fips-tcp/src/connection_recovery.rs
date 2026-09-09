impl<P: Clone> Connection<P> {
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
