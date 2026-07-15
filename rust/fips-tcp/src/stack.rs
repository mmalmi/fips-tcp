use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;

use crate::connection_types::{ReassemblySegment, TrackedSegment};
use crate::reno::Reno;
use crate::rtt::RttEstimator;
use crate::segment::{SegmentHeader, build_segment};
use crate::seq::{after, before, before_or_equal, distance, in_closed_interval};
use crate::types::{Config, ConnectionId, Outbound, StackError, State};
use crate::wire::{FIPS_VERSION, Flags, Segment};

include!("stack_types.rs");
include!("stack_abort.rs");

pub struct Stack<P> {
    config: Config,
    listeners: HashSet<u16>,
    accepts: HashMap<u16, VecDeque<ConnectionId>>,
    connections: HashMap<ConnectionId, Connection<P>>,
    lookup: HashMap<ConnectionKey<P>, ConnectionId>,
    outbound: Vec<Outbound<P>>,
    next_connection_id: u64,
    next_ephemeral_port: u16,
    isn_state: u64,
}

impl<P> Stack<P>
where
    P: Clone + Eq + Hash,
{
    pub fn new(config: Config, isn_seed: u64) -> Self {
        config.validate().expect("invalid TCP/FIPS configuration");
        Self {
            config,
            listeners: HashSet::new(),
            accepts: HashMap::new(),
            connections: HashMap::new(),
            lookup: HashMap::new(),
            outbound: Vec::new(),
            next_connection_id: 1,
            next_ephemeral_port: 49_152,
            isn_state: isn_seed.max(1),
        }
    }

    pub fn listen(&mut self, port: u16) -> Result<(), StackError> {
        if port == 0 {
            return Err(StackError::ZeroPort);
        }
        if !self.listeners.insert(port) {
            return Err(StackError::AlreadyListening(port));
        }
        self.accepts.entry(port).or_default();
        Ok(())
    }

    pub fn close_listener(&mut self, port: u16) {
        self.listeners.remove(&port);
        self.accepts.remove(&port);
    }

    pub fn accept(&mut self, port: u16) -> Option<ConnectionId> {
        let queue = self.accepts.get_mut(&port)?;
        while let Some(id) = queue.pop_front() {
            if self.connections.contains_key(&id) {
                return Some(id);
            }
        }
        None
    }

    pub fn connect(
        &mut self,
        peer: P,
        remote_port: u16,
        now_ms: u64,
    ) -> Result<ConnectionId, StackError> {
        let local_port = self.allocate_ephemeral_port(&peer, remote_port)?;
        let isn = self.next_isn();
        self.connect_from_with_isn(peer, local_port, remote_port, isn, now_ms)
    }

    pub fn connect_from_with_isn(
        &mut self,
        peer: P,
        local_port: u16,
        remote_port: u16,
        isn: u32,
        now_ms: u64,
    ) -> Result<ConnectionId, StackError> {
        if local_port == 0 || remote_port == 0 {
            return Err(StackError::ZeroPort);
        }
        self.ensure_connection_capacity(&peer)?;
        let key = ConnectionKey {
            peer: peer.clone(),
            local_port,
            remote_port,
        };
        if self.lookup.contains_key(&key) {
            return Err(StackError::ConnectionExists);
        }
        let id = self.allocate_connection_id();
        let (connection, segments) =
            Connection::client(peer, local_port, remote_port, isn, now_ms, &self.config);
        self.lookup.insert(key, id);
        self.connections.insert(id, connection);
        self.emit(id, segments)?;
        Ok(id)
    }

    pub fn input(&mut self, peer: P, bytes: &[u8], now_ms: u64) -> Result<(), StackError> {
        let segment = Segment::decode(bytes)?;
        let key = ConnectionKey {
            peer: peer.clone(),
            local_port: segment.dst_port,
            remote_port: segment.src_port,
        };
        let id = if let Some(id) = self.lookup.get(&key).copied() {
            id
        } else if segment.flags.contains(Flags::SYN)
            && !segment.flags.contains(Flags::ACK)
            && self.listeners.contains(&segment.dst_port)
        {
            if !segment.supports_fips_version(FIPS_VERSION) {
                self.emit_reset(peer, &segment)?;
                return Ok(());
            }
            self.ensure_connection_capacity(&peer)?;
            let id = self.allocate_connection_id();
            let isn = self.next_isn();
            let (connection, segments) =
                Connection::server(peer, &segment, isn, now_ms, &self.config);
            self.lookup.insert(key, id);
            self.connections.insert(id, connection);
            self.emit(id, segments)?;
            return Ok(());
        } else {
            if !segment.flags.contains(Flags::RST) {
                self.emit_reset(peer, &segment)?;
            }
            return Ok(());
        };

        let update = self
            .connections
            .get_mut(&id)
            .expect("lookup must reference a connection")
            .on_segment(&segment, now_ms, &self.config);
        if update.accepted {
            let port = self.connections[&id].local_port;
            self.accepts.entry(port).or_default().push_back(id);
        }
        self.emit(id, update.segments)?;
        if update.closed {
            self.remove_connection(id);
        }
        Ok(())
    }

    pub fn poll(&mut self, now_ms: u64) {
        let ids: Vec<_> = self.connections.keys().copied().collect();
        for id in ids {
            let Some(connection) = self.connections.get_mut(&id) else {
                continue;
            };
            let update = connection.poll(now_ms, &self.config);
            let _ = self.emit(id, update.segments);
            if update.closed {
                self.remove_connection(id);
            }
        }
    }

    pub fn write(
        &mut self,
        id: ConnectionId,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<usize, StackError> {
        let (accepted, segments) = self
            .connections
            .get_mut(&id)
            .ok_or(StackError::UnknownConnection)?
            .write(bytes, now_ms, &self.config)?;
        self.emit(id, segments)?;
        Ok(accepted)
    }

    pub fn read(
        &mut self,
        id: ConnectionId,
        max: usize,
        now_ms: u64,
    ) -> Result<Vec<u8>, StackError> {
        let (bytes, segments) = self
            .connections
            .get_mut(&id)
            .ok_or(StackError::UnknownConnection)?
            .read(max, now_ms);
        self.emit(id, segments)?;
        Ok(bytes)
    }

    pub fn close(&mut self, id: ConnectionId, now_ms: u64) -> Result<(), StackError> {
        let update = self
            .connections
            .get_mut(&id)
            .ok_or(StackError::UnknownConnection)?
            .close(now_ms, &self.config);
        self.emit(id, update.segments)?;
        if update.closed {
            self.remove_connection(id);
        }
        Ok(())
    }

    pub fn state(&self, id: ConnectionId) -> Option<State> {
        self.connections.get(&id).map(|connection| connection.state)
    }

    pub fn is_read_closed(&self, id: ConnectionId) -> bool {
        self.connections
            .get(&id)
            .is_none_or(|connection| connection.read_closed)
    }

    pub fn peer(&self, id: ConnectionId) -> Option<&P> {
        self.connections.get(&id).map(|connection| &connection.peer)
    }

    pub fn ports(&self, id: ConnectionId) -> Option<(u16, u16)> {
        self.connections
            .get(&id)
            .map(|connection| (connection.local_port, connection.remote_port))
    }

    pub fn drain_outbound(&mut self) -> Vec<Outbound<P>> {
        std::mem::take(&mut self.outbound)
    }

    fn emit(&mut self, id: ConnectionId, segments: Vec<Segment>) -> Result<(), StackError> {
        let Some(peer) = self
            .connections
            .get(&id)
            .map(|connection| connection.peer.clone())
        else {
            return Ok(());
        };
        for segment in segments {
            self.outbound.push(Outbound {
                peer: peer.clone(),
                bytes: segment.encode()?,
            });
        }
        Ok(())
    }

    fn emit_reset(&mut self, peer: P, incoming: &Segment) -> Result<(), StackError> {
        let mut reset = Segment::new(incoming.dst_port, incoming.src_port, 0);
        reset.window = 0;
        if let Some(ack) = incoming.ack {
            reset.seq = ack;
            reset.flags = Flags::RST;
        } else {
            reset.flags = Flags::RST | Flags::ACK;
            reset.ack = Some(incoming.seq.wrapping_add(incoming.sequence_len()));
        }
        self.outbound.push(Outbound {
            peer,
            bytes: reset.encode()?,
        });
        Ok(())
    }

    fn remove_connection(&mut self, id: ConnectionId) {
        if let Some(connection) = self.connections.remove(&id) {
            self.lookup.remove(&ConnectionKey {
                peer: connection.peer,
                local_port: connection.local_port,
                remote_port: connection.remote_port,
            });
        }
    }

    fn ensure_connection_capacity(&self, peer: &P) -> Result<(), StackError> {
        let peer_connections = self
            .connections
            .values()
            .filter(|connection| &connection.peer == peer)
            .count();
        if self.connections.len() >= self.config.max_connections
            || peer_connections >= self.config.max_connections_per_peer
        {
            Err(StackError::ConnectionLimit)
        } else {
            Ok(())
        }
    }

    fn allocate_connection_id(&mut self) -> ConnectionId {
        let id = ConnectionId(self.next_connection_id);
        self.next_connection_id = self.next_connection_id.wrapping_add(1).max(1);
        id
    }

    fn allocate_ephemeral_port(&mut self, peer: &P, remote_port: u16) -> Result<u16, StackError> {
        for _ in 0..16_384 {
            let port = self.next_ephemeral_port;
            self.next_ephemeral_port = if port == u16::MAX { 49_152 } else { port + 1 };
            let key = ConnectionKey {
                peer: peer.clone(),
                local_port: port,
                remote_port,
            };
            if !self.lookup.contains_key(&key) {
                return Ok(port);
            }
        }
        Err(StackError::NoEphemeralPort)
    }

    fn next_isn(&mut self) -> u32 {
        let mut value = self.isn_state;
        value ^= value << 13;
        value ^= value >> 7;
        value ^= value << 17;
        self.isn_state = value.max(1);
        (value ^ (value >> 32)) as u32
    }
}

impl<P: Clone> Connection<P> {
    fn client(
        peer: P,
        local_port: u16,
        remote_port: u16,
        isn: u32,
        now_ms: u64,
        config: &Config,
    ) -> (Self, Vec<Segment>) {
        let mut connection = Self::new(
            peer,
            local_port,
            remote_port,
            State::SynSent,
            isn,
            0,
            config,
        );
        let syn = connection.send_tracked(Flags::SYN, Vec::new(), now_ms, config);
        (connection, vec![syn])
    }

    fn server(
        peer: P,
        syn: &Segment,
        isn: u32,
        now_ms: u64,
        config: &Config,
    ) -> (Self, Vec<Segment>) {
        let mut connection = Self::new(
            peer,
            syn.dst_port,
            syn.src_port,
            State::SynReceived,
            isn,
            syn.seq.wrapping_add(1),
            config,
        );
        connection.update_remote_window(syn.window, now_ms);
        connection.negotiate_mss(syn, config);
        let syn_ack = connection.send_tracked(Flags::SYN | Flags::ACK, Vec::new(), now_ms, config);
        (connection, vec![syn_ack])
    }

    fn new(
        peer: P,
        local_port: u16,
        remote_port: u16,
        state: State,
        send_isn: u32,
        recv_nxt: u32,
        config: &Config,
    ) -> Self {
        let mss = usize::from(config.mss);
        Self {
            peer,
            local_port,
            remote_port,
            state,
            send_una: send_isn,
            send_nxt: send_isn,
            recv_nxt,
            remote_window: u16::MAX as usize,
            mss,
            receive_capacity: config.receive_buffer,
            send_queue: VecDeque::new(),
            recv_queue: VecDeque::new(),
            reassembly: Vec::new(),
            unacked: VecDeque::new(),
            rtt: RttEstimator::new(config.initial_rto_ms, config.min_rto_ms, config.max_rto_ms),
            reno: Reno::new(mss),
            duplicate_acks: 0,
            close_requested: false,
            next_zero_window_probe_ms: None,
            zero_window_probes: 0,
            read_closed: false,
            fin_wait_2_until_ms: None,
            time_wait_until_ms: None,
        }
    }

    fn on_segment(&mut self, segment: &Segment, now_ms: u64, config: &Config) -> Update {
        if segment.flags.contains(Flags::RST) {
            return Update {
                segments: Vec::new(),
                accepted: false,
                closed: true,
            };
        }

        if self.state == State::SynSent {
            if segment.flags.contains(Flags::SYN)
                && segment.flags.contains(Flags::ACK)
                && segment.ack == Some(self.send_nxt)
                && segment.supports_fips_version(FIPS_VERSION)
            {
                self.update_remote_window(segment.window, now_ms);
                self.negotiate_mss(segment, config);
                let _ = self.apply_ack(self.send_nxt, now_ms, false);
                self.recv_nxt = segment.seq.wrapping_add(1);
                self.state = State::Established;
                return Update::open(vec![self.ack_segment()]);
            }
            return Update::open(Vec::new());
        }

        if self.state == State::SynReceived {
            if segment.flags.contains(Flags::SYN)
                && !segment.flags.contains(Flags::ACK)
                && segment.seq.wrapping_add(1) == self.recv_nxt
            {
                let retransmit = self.retransmit_oldest(now_ms, false);
                return Update::open(retransmit.into_iter().collect());
            }
            if segment.ack != Some(self.send_nxt) {
                return Update::open(Vec::new());
            }
            let _ = self.apply_ack(self.send_nxt, now_ms, false);
            self.update_remote_window(segment.window, now_ms);
            self.state = State::Established;
            let mut update = Update::open(Vec::new());
            update.accepted = true;
            if !segment.payload.is_empty() || segment.flags.contains(Flags::FIN) {
                self.receive_stream_data(segment, now_ms, config, &mut update.segments);
            }
            update.segments.extend(self.flush_data(now_ms, config));
            return update;
        }

        let mut output = Vec::new();
        if let Some(ack) = segment.ack {
            let duplicate = ack == self.send_una && segment.payload.is_empty();
            let outcome = self.apply_ack(ack, now_ms, duplicate);
            if let Some(retransmit) = outcome.retransmit {
                output.push(retransmit);
            }
            if outcome.fin_acked {
                match self.state {
                    State::FinWait1 => {
                        self.state = State::FinWait2;
                        self.fin_wait_2_until_ms =
                            Some(now_ms.saturating_add(config.fin_wait_2_ms));
                    }
                    State::Closing => self.enter_time_wait(now_ms, config),
                    State::LastAck => {
                        return Update {
                            segments: output,
                            accepted: false,
                            closed: true,
                        };
                    }
                    _ => {}
                }
            }
            if in_closed_interval(ack, self.send_una, self.send_nxt) {
                self.update_remote_window(segment.window, now_ms);
            }
        }

        if !segment.payload.is_empty() || segment.flags.contains(Flags::FIN) {
            self.receive_stream_data(segment, now_ms, config, &mut output);
        }
        output.extend(self.flush_data(now_ms, config));
        Update::open(output)
    }

    fn write(
        &mut self,
        bytes: &[u8],
        now_ms: u64,
        config: &Config,
    ) -> Result<(usize, Vec<Segment>), StackError> {
        if !matches!(self.state, State::Established | State::CloseWait) || self.close_requested {
            return Err(StackError::InvalidState(self.state));
        }
        let buffered = self.send_queue.len()
            + self
                .unacked
                .iter()
                .map(|segment| segment.payload.len())
                .sum::<usize>();
        let accepted = bytes.len().min(config.send_buffer.saturating_sub(buffered));
        self.send_queue.extend(&bytes[..accepted]);
        Ok((accepted, self.flush_data(now_ms, config)))
    }

    fn read(&mut self, max: usize, now_ms: u64) -> (Vec<u8>, Vec<Segment>) {
        let previous_window = self.available_window();
        let count = max.min(self.recv_queue.len());
        let bytes = self.recv_queue.drain(..count).collect();
        let should_update = count > 0
            && self.available_window() > previous_window
            && !matches!(
                self.state,
                State::SynSent | State::SynReceived | State::TimeWait
            );
        let segments = should_update
            .then(|| self.ack_segment())
            .into_iter()
            .collect();
        let _ = now_ms;
        (bytes, segments)
    }

    fn close(&mut self, now_ms: u64, config: &Config) -> Update {
        match self.state {
            State::Established | State::CloseWait => {
                self.close_requested = true;
                Update::open(self.flush_data(now_ms, config))
            }
            State::SynSent | State::SynReceived => Update {
                segments: Vec::new(),
                accepted: false,
                closed: true,
            },
            _ => Update::open(Vec::new()),
        }
    }

    fn poll(&mut self, now_ms: u64, config: &Config) -> Update {
        let close_expired = match self.state {
            State::FinWait2 => self.fin_wait_2_until_ms,
            State::TimeWait => self.time_wait_until_ms,
            _ => None,
        }
        .is_some_and(|deadline| now_ms >= deadline);
        if close_expired {
            return Update {
                segments: Vec::new(),
                accepted: false,
                closed: true,
            };
        }
        let mut segments = Vec::new();
        let zero_window_work = self.close_requested
            || !self.send_queue.is_empty()
            || self
                .unacked
                .iter()
                .any(|segment| !segment.payload.is_empty());
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
            self.reno.on_timeout(in_flight);
            self.rtt.on_timeout();
            if let Some(retransmit) = self.retransmit_oldest(now_ms, true) {
                segments.push(retransmit);
            }
        }
        segments.extend(self.flush_data(now_ms, config));
        Update::open(segments)
    }

    fn apply_ack(&mut self, ack: u32, now_ms: u64, duplicate_candidate: bool) -> AckOutcome {
        if after(ack, self.send_nxt) || before(ack, self.send_una) {
            return AckOutcome {
                fin_acked: false,
                retransmit: None,
            };
        }
        if ack == self.send_una {
            if duplicate_candidate && !self.unacked.is_empty() {
                self.duplicate_acks = self.duplicate_acks.saturating_add(1);
                self.reno.on_duplicate_ack();
                if self.duplicate_acks == 3 {
                    self.reno
                        .on_fast_loss(distance(self.send_una, self.send_nxt));
                    return AckOutcome {
                        fin_acked: false,
                        retransmit: self.retransmit_oldest(now_ms, false),
                    };
                }
            }
            return AckOutcome {
                fin_acked: false,
                retransmit: None,
            };
        }

        self.duplicate_acks = 0;
        let mut acked_payload = 0;
        let mut fin_acked = false;
        let mut rtt_sample = None;
        while self
            .unacked
            .front()
            .is_some_and(|segment| before_or_equal(segment.end_seq(), ack))
        {
            let segment = self.unacked.pop_front().expect("front exists");
            acked_payload += segment.payload.len();
            fin_acked |= segment.flags.contains(Flags::FIN);
            if !segment.retransmitted {
                rtt_sample = Some(now_ms.saturating_sub(segment.sent_at_ms));
            }
        }
        if let Some(segment) = self.unacked.front_mut()
            && before(segment.seq, ack)
            && before(ack, segment.end_seq())
            && !segment.flags.contains(Flags::SYN)
            && !segment.flags.contains(Flags::FIN)
        {
            let count = distance(segment.seq, ack).min(segment.payload.len());
            segment.payload.drain(..count);
            segment.seq = ack;
            acked_payload += count;
        }
        self.send_una = ack;
        if let Some(sample) = rtt_sample {
            self.rtt.sample(sample);
        }
        self.reno.on_ack(acked_payload);
        AckOutcome {
            fin_acked,
            retransmit: None,
        }
    }

    fn receive_stream_data(
        &mut self,
        segment: &Segment,
        now_ms: u64,
        config: &Config,
        output: &mut Vec<Segment>,
    ) {
        let fin = segment.flags.contains(Flags::FIN);
        self.insert_received(segment.seq, &segment.payload, fin, config);
        self.drain_reassembly(now_ms, config);
        output.push(self.ack_segment());
    }

    fn insert_received(&mut self, seq: u32, payload: &[u8], fin: bool, config: &Config) {
        let original_end = seq.wrapping_add(payload.len() as u32);
        let mut start = seq;
        let mut data = payload;
        if before(start, self.recv_nxt) {
            let trim = distance(start, self.recv_nxt);
            if trim >= data.len() {
                data = &[];
                start = self.recv_nxt;
            } else {
                data = &data[trim..];
                start = self.recv_nxt;
            }
        }
        let window = self.available_window();
        let offset = distance(self.recv_nxt, start);
        if after(start, self.recv_nxt) && offset >= window {
            return;
        }
        let allowed = data.len().min(window.saturating_sub(offset));
        let kept_fin =
            fin && allowed == data.len() && original_end == start.wrapping_add(data.len() as u32);
        let chunk = ReassemblySegment {
            seq: start,
            payload: data[..allowed].to_vec(),
            fin: kept_fin,
        };
        if chunk.payload.is_empty() && !chunk.fin {
            return;
        }
        if self
            .reassembly
            .iter()
            .any(|existing| existing.seq == chunk.seq && existing.end_seq() == chunk.end_seq())
        {
            return;
        }
        if self.reassembly.len() < config.max_reassembly_segments {
            self.reassembly.push(chunk);
        }
    }

    fn drain_reassembly(&mut self, now_ms: u64, config: &Config) {
        loop {
            self.reassembly.retain(|segment| {
                !before_or_equal(segment.end_seq(), self.recv_nxt)
                    || (segment.fin && segment.end_seq() == self.recv_nxt.wrapping_add(1))
            });
            let next = self
                .reassembly
                .iter()
                .enumerate()
                .find_map(|(index, segment)| {
                    if segment.seq == self.recv_nxt || before(segment.seq, self.recv_nxt) {
                        Some(index)
                    } else {
                        None
                    }
                });
            let Some(index) = next else {
                break;
            };
            let mut segment = self.reassembly.remove(index);
            if before(segment.seq, self.recv_nxt) {
                let trim = distance(segment.seq, self.recv_nxt).min(segment.payload.len());
                segment.payload.drain(..trim);
                segment.seq = self.recv_nxt;
            }
            let capacity = config.receive_buffer.saturating_sub(self.recv_queue.len());
            let accepted = capacity.min(segment.payload.len());
            self.recv_queue.extend(segment.payload.drain(..accepted));
            self.recv_nxt = self.recv_nxt.wrapping_add(accepted as u32);
            if !segment.payload.is_empty() {
                segment.seq = self.recv_nxt;
                self.reassembly.push(segment);
                break;
            }
            if segment.fin {
                self.recv_nxt = self.recv_nxt.wrapping_add(1);
                self.on_remote_fin(now_ms, config);
            }
        }
    }

    fn on_remote_fin(&mut self, now_ms: u64, config: &Config) {
        self.read_closed = true;
        match self.state {
            State::Established => self.state = State::CloseWait,
            State::FinWait1 => self.state = State::Closing,
            State::FinWait2 => self.enter_time_wait(now_ms, config),
            _ => {}
        }
    }

    fn enter_time_wait(&mut self, now_ms: u64, config: &Config) {
        self.state = State::TimeWait;
        self.fin_wait_2_until_ms = None;
        self.time_wait_until_ms = Some(now_ms.saturating_add(config.time_wait_ms));
    }

    fn reset_segment(&self) -> Segment {
        build_segment(
            SegmentHeader {
                local_port: self.local_port,
                remote_port: self.remote_port,
                seq: self.send_nxt,
                ack: self.recv_nxt,
                window: 0,
                mss: self.mss as u16,
                flags: Flags::RST,
            },
            Vec::new(),
        )
    }

    fn send_tracked(
        &mut self,
        flags: Flags,
        payload: Vec<u8>,
        now_ms: u64,
        config: &Config,
    ) -> Segment {
        let seq = self.send_nxt;
        let tracked = TrackedSegment {
            seq,
            flags,
            payload,
            sent_at_ms: now_ms,
            retransmitted: false,
            transmissions: 1,
        };
        self.send_nxt = tracked.end_seq();
        let segment = self.segment_for(&tracked, config);
        self.unacked.push_back(tracked);
        segment
    }

    fn retransmit_oldest(&mut self, now_ms: u64, timeout: bool) -> Option<Segment> {
        let window = self.available_window_u16();
        let ack = self.recv_nxt;
        let local_port = self.local_port;
        let remote_port = self.remote_port;
        let mss = self.mss as u16;
        let tracked = self.unacked.front_mut()?;
        tracked.sent_at_ms = now_ms;
        tracked.retransmitted = true;
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

    fn flush_data(&mut self, now_ms: u64, config: &Config) -> Vec<Segment> {
        if !matches!(self.state, State::Established | State::CloseWait) {
            return Vec::new();
        }
        let mut output = Vec::new();
        loop {
            let in_flight = distance(self.send_una, self.send_nxt);
            let window = self.remote_window.min(self.reno.window());
            let available = window.saturating_sub(in_flight);
            if available == 0 || self.send_queue.is_empty() {
                break;
            }
            let count = available.min(self.mss).min(self.send_queue.len());
            let payload = self.send_queue.drain(..count).collect();
            output.push(self.send_tracked(Flags::ACK | Flags::PSH, payload, now_ms, config));
        }
        if self.close_requested && self.send_queue.is_empty() {
            let in_flight = distance(self.send_una, self.send_nxt);
            let available = self
                .remote_window
                .min(self.reno.window())
                .saturating_sub(in_flight);
            if available > 0 {
                self.close_requested = false;
                self.state = if self.state == State::Established {
                    State::FinWait1
                } else {
                    State::LastAck
                };
                output.push(self.send_tracked(Flags::FIN | Flags::ACK, Vec::new(), now_ms, config));
            }
        }
        output
    }

    fn zero_window_probe(&mut self, now_ms: u64, config: &Config) -> Option<Segment> {
        if let Some(segment) = self
            .unacked
            .iter()
            .find(|segment| !segment.payload.is_empty())
        {
            return Some(build_segment(
                SegmentHeader {
                    local_port: self.local_port,
                    remote_port: self.remote_port,
                    seq: segment.seq,
                    ack: self.recv_nxt,
                    window: self.available_window_u16(),
                    mss: self.mss as u16,
                    flags: Flags::ACK | Flags::PSH,
                },
                vec![segment.payload[0]],
            ));
        }
        let byte = self.send_queue.pop_front()?;
        Some(self.send_tracked(Flags::ACK | Flags::PSH, vec![byte], now_ms, config))
    }

    fn update_remote_window(&mut self, window: u16, now_ms: u64) {
        self.remote_window = usize::from(window);
        if window == 0 {
            self.next_zero_window_probe_ms
                .get_or_insert(now_ms.saturating_add(self.rtt.timeout_ms()));
        } else {
            self.next_zero_window_probe_ms = None;
            self.zero_window_probes = 0;
        }
    }

    fn ack_segment(&self) -> Segment {
        build_segment(
            SegmentHeader {
                local_port: self.local_port,
                remote_port: self.remote_port,
                seq: self.send_nxt,
                ack: self.recv_nxt,
                window: self.available_window_u16(),
                mss: self.mss as u16,
                flags: Flags::ACK,
            },
            Vec::new(),
        )
    }

    fn segment_for(&self, tracked: &TrackedSegment, config: &Config) -> Segment {
        let _ = config;
        build_segment(
            SegmentHeader {
                local_port: self.local_port,
                remote_port: self.remote_port,
                seq: tracked.seq,
                ack: self.recv_nxt,
                window: self.available_window_u16(),
                mss: self.mss as u16,
                flags: tracked.flags,
            },
            tracked.payload.clone(),
        )
    }

    fn negotiate_mss(&mut self, segment: &Segment, config: &Config) {
        self.mss = usize::from(segment.max_segment_size().unwrap_or(1024).min(config.mss)).max(1);
        self.reno.set_mss(self.mss);
    }

    fn available_window(&self) -> usize {
        let reassembly_bytes: usize = self
            .reassembly
            .iter()
            .map(|segment| segment.payload.len())
            .sum();
        self.receive_capacity
            .saturating_sub(self.recv_queue.len().saturating_add(reassembly_bytes))
    }

    fn available_window_u16(&self) -> u16 {
        self.available_window().min(u16::MAX as usize) as u16
    }
}
