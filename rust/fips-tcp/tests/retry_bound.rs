use fips_tcp::wire::{Flags, Segment};
use fips_tcp::{Config, ConnectionId, Stack, StackError, State};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    name: String,
    initial_sequence: u32,
    max_transmissions: u8,
    reset: String,
}

fn config(limit: u8) -> Config {
    Config {
        mss: 16,
        receive_buffer: 128,
        max_connections: 1,
        max_retransmissions: limit,
        initial_rto_ms: 1000,
        min_rto_ms: 1000,
        max_rto_ms: 1000,
        ..Config::default()
    }
}

fn sender(vector: &Vector) -> (Stack<String>, ConnectionId, Segment, Segment) {
    let mut a = Stack::<String>::new(config(vector.max_transmissions), 1);
    let mut b = Stack::<String>::new(config(vector.max_transmissions), 2);
    b.listen(443).unwrap();
    let id = a
        .connect_from_with_isn("b".into(), 50_000, 443, vector.initial_sequence, 0)
        .unwrap();
    b.input("a".into(), &a.drain_outbound()[0].bytes, 0)
        .unwrap();
    let syn_ack = b.drain_outbound().remove(0);
    let decoded = Segment::decode(&syn_ack.bytes).unwrap();
    a.input("b".into(), &syn_ack.bytes, 0).unwrap();
    b.input("a".into(), &a.drain_outbound()[0].bytes, 0)
        .unwrap();
    assert_eq!(a.state(id), Some(State::Established));
    let mut ack = Segment::new(443, 50_000, decoded.seq.wrapping_add(1));
    ack.flags = Flags::ACK;
    ack.ack = Some(vector.initial_sequence.wrapping_add(1));
    ack.window = 64;
    assert_eq!(a.write(id, &[7; 16], 0).unwrap(), 16);
    let original = Segment::decode(&a.drain_outbound().remove(0).bytes).unwrap();
    (a, id, ack, original)
}

fn exhaust(vector: &Vector, a: &mut Stack<String>, ack: &mut Segment, original: &Segment) {
    let mut transmissions = 1;
    for cycle in 0..usize::from(vector.max_transmissions) + 3 {
        let now = (cycle as u64 + 1) * 10;
        for _ in 0..3 {
            a.input("b".into(), &ack.encode().unwrap(), now).unwrap();
            for packet in a.drain_outbound() {
                let segment = Segment::decode(&packet.bytes).unwrap();
                if segment.payload.is_empty() {
                    continue;
                }
                assert_eq!(segment.seq, original.seq, "{}", vector.name);
                assert_eq!(segment.payload, original.payload, "{}", vector.name);
                transmissions += 1;
            }
        }
        assert_eq!(
            transmissions,
            (cycle + 2).min(usize::from(vector.max_transmissions)),
            "{}: peer ACKs bypassed the transmission budget",
            vector.name
        );
        let mut reset = ack.clone();
        if vector.reset == "window" {
            reset.window += 1;
            ack.window = reset.window;
        } else {
            reset.flags = Flags::ACK | Flags::PSH;
            reset.payload = vec![9];
            ack.seq = ack.seq.wrapping_add(1);
        }
        a.input("b".into(), &reset.encode().unwrap(), now).unwrap();
        assert!(
            a.drain_outbound()
                .iter()
                .all(|packet| { Segment::decode(&packet.bytes).unwrap().payload.is_empty() })
        );
    }
}

#[test]
fn shared_vectors_bound_rearmed_fast_retransmits_without_extending_deadline() {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/retry-bound-vectors.json")).unwrap();
    for vector in vectors {
        let (mut a, id, mut ack, original) = sender(&vector);
        exhaust(&vector, &mut a, &mut ack, &original);
        let deadline = u64::from(vector.max_transmissions - 1) * 10 + 1000;
        a.poll(deadline - 1);
        assert_eq!(a.state(id), Some(State::Established), "{}", vector.name);
        assert!(a.drain_outbound().is_empty());
        a.poll(deadline);
        assert_eq!(
            a.state(id),
            None,
            "{}: retries postponed timeout",
            vector.name
        );
        assert!(a.drain_outbound().is_empty());
    }
}

#[test]
fn final_permitted_retry_can_be_acked_and_fresh_data_gets_a_new_budget() {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/retry-bound-vectors.json")).unwrap();
    for vector in vectors {
        let (mut a, id, mut ack, original) = sender(&vector);
        exhaust(&vector, &mut a, &mut ack, &original);
        let deadline = u64::from(vector.max_transmissions - 1) * 10 + 1000;
        ack.ack = Some(original.seq.wrapping_add(16));
        a.input("b".into(), &ack.encode().unwrap(), deadline - 1)
            .unwrap();
        a.poll(deadline);
        assert_eq!(a.state(id), Some(State::Established));
        assert!(a.drain_outbound().is_empty());
        assert_eq!(a.write(id, &[8; 16], deadline).unwrap(), 16);
        let fresh = a.drain_outbound();
        assert_eq!(fresh.len(), 1);
        assert_eq!(
            Segment::decode(&fresh[0].bytes).unwrap().seq,
            ack.ack.unwrap()
        );
        for _ in 0..3 {
            a.input("b".into(), &ack.encode().unwrap(), deadline + 10)
                .unwrap();
        }
        let repairs = a.drain_outbound();
        assert_eq!(repairs.len(), usize::from(vector.max_transmissions > 1));
        if let Some(repair) = repairs.first() {
            assert_eq!(Segment::decode(&repair.bytes).unwrap().payload, vec![8; 16]);
        }
    }
}

#[test]
fn duplicate_syns_share_the_retry_budget_without_extending_handshake_deadline() {
    for (limit, acknowledge) in [1, 3, 8]
        .into_iter()
        .flat_map(|limit| [false, true].map(|ack| (limit, ack)))
    {
        let mut a = Stack::<String>::new(config(limit), 1);
        let mut b = Stack::<String>::new(config(limit), 2);
        b.listen(443).unwrap();
        a.connect("b".into(), 443, 0).unwrap();
        let syn = a.drain_outbound().remove(0);
        b.input("a".into(), &syn.bytes, 0).unwrap();
        let original = b.drain_outbound().remove(0);
        for cycle in 0..limit + 3 {
            b.input("a".into(), &syn.bytes, u64::from(cycle + 1) * 10)
                .unwrap();
            let repairs = b.drain_outbound();
            assert_eq!(repairs.len(), usize::from(cycle < limit - 1));
            for repair in repairs {
                assert_eq!(repair.bytes, original.bytes);
            }
        }
        let deadline = u64::from(limit - 1) * 10 + 1000;
        if !acknowledge {
            let mut fresh = Segment::decode(&syn.bytes).unwrap();
            fresh.src_port += 1;
            let fresh = fresh.encode().unwrap();
            b.poll(deadline - 1);
            assert!(matches!(
                b.input("a".into(), &fresh, deadline - 1),
                Err(StackError::ConnectionLimit)
            ));
            b.poll(deadline);
            assert!(b.drain_outbound().is_empty());
            b.input("a".into(), &fresh, deadline).unwrap();
            assert_eq!(b.drain_outbound().len(), 1);
            continue;
        }
        // A valid ACK of the last permitted SYN-ACK still establishes the stream.
        a.input("b".into(), &original.bytes, deadline - 1).unwrap();
        b.input("a".into(), &a.drain_outbound()[0].bytes, deadline - 1)
            .unwrap();
        let id = b.accept(443).unwrap();
        b.poll(deadline);
        assert_eq!(b.state(id), Some(State::Established));
    }
}
