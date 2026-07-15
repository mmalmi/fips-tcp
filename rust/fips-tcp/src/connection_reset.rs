impl<P: Clone> Connection<P> {
    fn on_reset(&self, segment: &Segment) -> Update {
        if self.state == State::SynSent {
            let acceptable_ack = segment
                .ack
                .is_some_and(|ack| after(ack, self.send_una) && !after(ack, self.send_nxt));
            return Update {
                segments: Vec::new(),
                accepted: false,
                closed: acceptable_ack,
            };
        }
        if segment.seq == self.recv_nxt {
            return Update {
                segments: Vec::new(),
                accepted: false,
                closed: true,
            };
        }
        let receive_window = self.available_window();
        let in_window =
            receive_window > 0 && distance(self.recv_nxt, segment.seq) < receive_window;
        Update::open(if in_window {
            vec![self.ack_segment()]
        } else {
            Vec::new()
        })
    }
}
