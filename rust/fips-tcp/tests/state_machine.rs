use fips_tcp::{Config, ConnectionId, Stack, State};

struct Pair {
    a: Stack<String>,
    b: Stack<String>,
    now: u64,
}

impl Pair {
    fn new(config: Config) -> Self {
        Self {
            a: Stack::new(config.clone(), 0x1111_2222_3333_4444),
            b: Stack::new(config, 0xaaaa_bbbb_cccc_dddd),
            now: 0,
        }
    }

    fn step_with<F>(&mut self, mut transform: F) -> usize
    where
        F: FnMut(bool, Vec<u8>) -> Vec<Vec<u8>>,
    {
        self.a.poll(self.now);
        self.b.poll(self.now);
        let from_a = self.a.drain_outbound();
        let from_b = self.b.drain_outbound();
        let mut delivered = 0;
        for outbound in from_a {
            assert_eq!(outbound.peer, "b");
            for bytes in transform(true, outbound.bytes) {
                self.b.input("a".to_string(), &bytes, self.now).unwrap();
                delivered += 1;
            }
        }
        for outbound in from_b {
            assert_eq!(outbound.peer, "a");
            for bytes in transform(false, outbound.bytes) {
                self.a.input("b".to_string(), &bytes, self.now).unwrap();
                delivered += 1;
            }
        }
        delivered
    }

    fn settle(&mut self) {
        for _ in 0..256 {
            if self.step_with(|_, bytes| vec![bytes]) == 0 {
                return;
            }
        }
        panic!("pair did not settle");
    }

    fn advance(&mut self, millis: u64) {
        self.now += millis;
    }

    fn connect(&mut self) -> (ConnectionId, ConnectionId) {
        self.b.listen(443).unwrap();
        let client = self.a.connect("b".to_string(), 443, self.now).unwrap();
        self.settle();
        assert_eq!(self.a.state(client), Some(State::Established));
        let server = self.b.accept(443).expect("server should accept");
        assert_eq!(self.b.state(server), Some(State::Established));
        (client, server)
    }
}

#[test]
fn handshake_bidirectional_stream_and_orderly_close() {
    let mut pair = Pair::new(Config::default());
    let (client, server) = pair.connect();

    assert_eq!(
        pair.a.write(client, b"hello from rust", pair.now).unwrap(),
        15
    );
    assert_eq!(pair.b.write(server, b"hello back", pair.now).unwrap(), 10);
    pair.settle();
    assert_eq!(
        pair.b.read(server, 1024, pair.now).unwrap(),
        b"hello from rust"
    );
    assert_eq!(pair.a.read(client, 1024, pair.now).unwrap(), b"hello back");

    pair.a.close(client, pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.b.state(server), Some(State::CloseWait));
    assert!(pair.b.is_read_closed(server));
    pair.b.close(server, pair.now).unwrap();
    pair.settle();
    pair.advance(60_000);
    pair.settle();
    assert_eq!(pair.a.state(client), None);
    assert_eq!(pair.b.state(server), None);
}

#[test]
fn lost_syn_and_first_payload_recover_via_rto() {
    let mut pair = Pair::new(Config::default());
    pair.b.listen(443).unwrap();
    let client = pair.a.connect("b".to_string(), 443, pair.now).unwrap();

    let mut dropped_syn = false;
    pair.step_with(|from_a, bytes| {
        if from_a && !dropped_syn {
            dropped_syn = true;
            Vec::new()
        } else {
            vec![bytes]
        }
    });
    assert_eq!(pair.a.state(client), Some(State::SynSent));
    pair.advance(1_000);
    pair.settle();
    let server = pair
        .b
        .accept(443)
        .expect("retransmitted SYN should connect");

    let payload = vec![0x5a; 4096];
    assert_eq!(
        pair.a.write(client, &payload, pair.now).unwrap(),
        payload.len()
    );
    let mut dropped_data = false;
    pair.step_with(|from_a, bytes| {
        let segment = fips_tcp::wire::Segment::decode(&bytes).unwrap();
        if from_a && !segment.payload.is_empty() && !dropped_data {
            dropped_data = true;
            Vec::new()
        } else {
            vec![bytes]
        }
    });
    pair.settle();
    assert!(pair.b.read(server, payload.len(), pair.now).unwrap().len() < payload.len());
    pair.advance(1_000);
    pair.settle();
    let mut received = Vec::new();
    while received.len() < payload.len() {
        received.extend(pair.b.read(server, payload.len(), pair.now).unwrap());
        if received.len() < payload.len() {
            pair.advance(1_000);
            pair.settle();
        }
    }
    assert_eq!(received, payload);
}

#[test]
fn reverse_order_and_duplicate_segments_reassemble_once() {
    let config = Config {
        mss: 256,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let (client, server) = pair.connect();
    let payload: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
    pair.a.write(client, &payload, pair.now).unwrap();

    pair.a.poll(pair.now);
    let mut packets = pair.a.drain_outbound();
    packets.reverse();
    for outbound in packets {
        pair.b
            .input("a".to_string(), &outbound.bytes, pair.now)
            .unwrap();
        pair.b
            .input("a".to_string(), &outbound.bytes, pair.now)
            .unwrap();
    }
    pair.settle();
    assert_eq!(
        pair.b.read(server, payload.len(), pair.now).unwrap(),
        payload
    );
}

#[test]
fn receive_window_reopens_after_application_reads() {
    let config = Config {
        mss: 8,
        receive_buffer: 16,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let (client, server) = pair.connect();
    let payload: Vec<u8> = (0..64).collect();
    pair.a.write(client, &payload, pair.now).unwrap();
    pair.settle();

    let first = pair.b.read(server, 16, pair.now).unwrap();
    assert_eq!(first, payload[..16]);
    pair.settle();
    let mut received = first;
    for _ in 0..16 {
        let part = pair.b.read(server, 16, pair.now).unwrap();
        received.extend(part);
        pair.settle();
        if received.len() == payload.len() {
            break;
        }
    }
    assert_eq!(received, payload);
}

#[test]
fn byte_sequence_wraparound_is_ordered_correctly() {
    let mut pair = Pair::new(Config::default());
    pair.b.listen(443).unwrap();
    let client = pair
        .a
        .connect_from_with_isn("b".to_string(), 50_000, 443, u32::MAX - 8, pair.now)
        .unwrap();
    pair.settle();
    let server = pair.b.accept(443).unwrap();
    let payload = b"crosses the sequence wrap";
    pair.a.write(client, payload, pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.b.read(server, 1024, pair.now).unwrap(), payload);
}

#[test]
fn close_waits_for_flow_controlled_bytes_before_fin() {
    let config = Config {
        mss: 8,
        receive_buffer: 16,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let (client, server) = pair.connect();
    let payload: Vec<u8> = (0..64).collect();
    pair.a.write(client, &payload, pair.now).unwrap();
    pair.a.close(client, pair.now).unwrap();
    pair.settle();

    let mut received = Vec::new();
    for _ in 0..8 {
        received.extend(pair.b.read(server, 16, pair.now).unwrap());
        pair.settle();
        if pair.b.is_read_closed(server) {
            break;
        }
    }
    assert_eq!(received, payload);
    assert!(pair.b.is_read_closed(server));
    assert_eq!(pair.b.state(server), Some(State::CloseWait));
}

#[test]
fn zero_window_probe_recovers_a_lost_window_update() {
    let config = Config {
        mss: 8,
        receive_buffer: 16,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let (client, server) = pair.connect();
    let payload: Vec<u8> = (0..64).collect();
    pair.a.write(client, &payload, pair.now).unwrap();
    pair.settle();

    let mut received = pair.b.read(server, 16, pair.now).unwrap();
    let mut dropped_update = false;
    pair.step_with(|from_a, bytes| {
        if !from_a && !dropped_update {
            dropped_update = true;
            Vec::new()
        } else {
            vec![bytes]
        }
    });
    assert!(
        dropped_update,
        "receiver should advertise its reopened window"
    );
    pair.advance(1_000);
    pair.settle();

    for _ in 0..8 {
        received.extend(pair.b.read(server, 16, pair.now).unwrap());
        pair.settle();
        if received.len() == payload.len() {
            break;
        }
    }
    assert_eq!(received, payload);
}

#[test]
fn closed_port_rst_and_retry_limit_remove_connections() {
    let config = Config {
        initial_rto_ms: 200,
        max_retransmissions: 1,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let client = pair.a.connect("b".to_string(), 443, pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.a.state(client), None, "closed port should reset SYN");

    let client = pair.a.connect("b".to_string(), 444, pair.now).unwrap();
    pair.a.drain_outbound();
    pair.advance(200);
    pair.a.poll(pair.now);
    assert_eq!(
        pair.a.state(client),
        None,
        "retry limit should close connection"
    );
}

#[test]
fn triple_duplicate_ack_fast_retransmits_without_waiting_for_rto() {
    let config = Config {
        mss: 128,
        ..Config::default()
    };
    let mut pair = Pair::new(config);
    let (client, server) = pair.connect();
    pair.a.write(client, &[0x11; 2048], pair.now).unwrap();
    pair.settle();
    pair.b.read(server, 4096, pair.now).unwrap();
    pair.settle();

    let payload: Vec<u8> = (0..2048).map(|index| (index % 251) as u8).collect();
    pair.a.write(client, &payload, pair.now).unwrap();
    let packets = pair.a.drain_outbound();
    assert!(
        packets.len() >= 4,
        "warm connection should have a larger cwnd"
    );
    let first = fips_tcp::wire::Segment::decode(&packets[0].bytes).unwrap();
    for outbound in packets.into_iter().skip(1) {
        pair.b
            .input("a".to_string(), &outbound.bytes, pair.now)
            .unwrap();
    }
    for outbound in pair.b.drain_outbound() {
        pair.a
            .input("b".to_string(), &outbound.bytes, pair.now)
            .unwrap();
    }
    let retransmits = pair.a.drain_outbound();
    assert!(retransmits.iter().any(|outbound| {
        let segment = fips_tcp::wire::Segment::decode(&outbound.bytes).unwrap();
        segment.seq == first.seq && segment.payload == first.payload
    }));
    for outbound in retransmits {
        pair.b
            .input("a".to_string(), &outbound.bytes, pair.now)
            .unwrap();
    }
    pair.settle();
    assert_eq!(
        pair.b.read(server, payload.len(), pair.now).unwrap(),
        payload
    );
}

#[test]
fn lost_fin_is_retransmitted() {
    let mut pair = Pair::new(Config::default());
    let (client, server) = pair.connect();
    pair.a.close(client, pair.now).unwrap();
    let mut dropped = false;
    pair.step_with(|from_a, bytes| {
        let segment = fips_tcp::wire::Segment::decode(&bytes).unwrap();
        if from_a && segment.flags.contains(fips_tcp::wire::Flags::FIN) && !dropped {
            dropped = true;
            Vec::new()
        } else {
            vec![bytes]
        }
    });
    assert!(dropped);
    assert_eq!(pair.b.state(server), Some(State::Established));
    pair.advance(1_000);
    pair.settle();
    assert_eq!(pair.b.state(server), Some(State::CloseWait));
}
