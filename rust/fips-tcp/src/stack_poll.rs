impl<P: Clone + Eq + Hash> Stack<P> {
    /// Earliest time at which `poll` can advance a protocol timer.
    ///
    /// Uses the same injected millisecond clock as `input`, `write` and `poll`.
    /// Recompute after each operation. `None` means the stack can sleep until
    /// input or application work; unread application bytes do not need polling.
    /// A returned time at or before `now_ms` should be polled immediately.
    pub fn next_poll_at(&self, now_ms: u64) -> Option<u64> {
        self.connections
            .values()
            .filter_map(|connection| connection.next_poll_at(now_ms, &self.config))
            .min()
    }
}

impl<P: Clone> Connection<P> {
    fn close_deadline(&self) -> Option<u64> {
        match self.state {
            State::FinWait2 => self.fin_wait_2_until_ms,
            State::TimeWait => self.time_wait_until_ms,
            _ => None,
        }
    }

    fn has_probe_payload(&self) -> bool {
        !self.send_queue.is_empty()
            || self
                .unacked
                .iter()
                .any(|segment| !segment.payload.is_empty())
    }

    fn next_poll_at(&self, now_ms: u64, config: &Config) -> Option<u64> {
        let payload = self.has_probe_payload();
        let transport = if self.remote_window == 0 && (self.close_requested || payload) {
            // A close alone cannot produce a zero-window probe. Wait for input
            // instead of repeatedly waking on a deadline that cannot advance.
            (payload || self.zero_window_probes >= config.max_retransmissions)
                .then_some(self.next_zero_window_probe_ms.unwrap_or(now_ms))
        } else {
            self.unacked
                .front()
                .map(|oldest| oldest.sent_at_ms.saturating_add(self.rtt.timeout_ms()))
        };
        self.close_deadline().into_iter().chain(transport).min()
    }

    fn poll(&mut self, now_ms: u64, config: &Config) -> Update {
        let close_expired = self
            .close_deadline()
            .is_some_and(|deadline| now_ms >= deadline);
        if close_expired {
            return Update {
                segments: Vec::new(),
                accepted: false,
                closed: true,
            };
        }
        let mut segments = Vec::new();
        let zero_window_work = self.close_requested || self.has_probe_payload();
        if self.remote_window == 0 && zero_window_work {
            let deadline = self
                .next_zero_window_probe_ms
                .get_or_insert(now_ms.saturating_add(config.initial_rto_ms));
            if now_ms >= *deadline {
                if self.zero_window_probes >= config.max_retransmissions {
                    return Update {
                        segments,
                        accepted: false,
                        closed: true,
                    };
                }
                if let Some(probe) = self.zero_window_probe(now_ms, config) {
                    segments.push(probe);
                    self.zero_window_probes = self.zero_window_probes.saturating_add(1);
                    let shift = u32::from(self.zero_window_probes.min(16));
                    let delay = config
                        .initial_rto_ms
                        .saturating_mul(1_u64 << shift)
                        .min(config.max_rto_ms);
                    self.next_zero_window_probe_ms = Some(now_ms.saturating_add(delay));
                }
            }
            return Update::open(segments);
        }
        if let Some(oldest) = self.unacked.front()
            && now_ms >= oldest.sent_at_ms.saturating_add(self.rtt.timeout_ms())
        {
            if oldest.transmissions >= config.max_retransmissions {
                return Update {
                    segments,
                    accepted: false,
                    closed: true,
                };
            }
            let in_flight = distance(self.send_una, self.send_nxt);
            self.rto_recovery_until.get_or_insert(self.send_nxt);
            self.reno.on_timeout(in_flight);
            self.rtt.on_timeout();
            if let Some(retransmit) = self.retransmit_oldest(now_ms, true) {
                segments.push(retransmit);
            }
        }
        segments.extend(self.flush_data(now_ms, config));
        Update::open(segments)
    }
}
