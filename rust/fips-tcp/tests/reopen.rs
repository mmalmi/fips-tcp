mod support;

use fips_tcp::{Config, Stack, State};
use support::Pair;

#[test]
fn reopened_client_connects_while_peer_retains_its_previous_stream() {
    let mut pair = Pair::new(Config::default());
    pair.b.listen(443).unwrap();
    let old = pair.a.connect("b".into(), 443, pair.now).unwrap();
    pair.settle();
    let retained = pair.b.accept(443).unwrap();
    assert_eq!(pair.a.state(old), Some(State::Established));

    // A crash or lost reset leaves the authenticated peer's old tuple alive.
    pair.advance(1);
    pair.a = Stack::new(Config::default(), 0x1111_2222_3333_5555);
    let fresh = pair.a.connect("b".into(), 443, pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.b.state(retained), None);
    pair.advance(Config::default().initial_rto_ms);
    pair.settle();
    assert_eq!(pair.a.state(fresh), Some(State::Established));
    let accepted = pair.b.accept(443).unwrap();
    assert_ne!(accepted, retained);
    assert!(
        pair.now <= 2001,
        "reopening must not wait for the old idle deadline"
    );
    pair.a.write(fresh, b"new request", pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.b.read(accepted, 64, pair.now).unwrap(), b"new request");
    pair.b.write(accepted, b"new response", pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.a.read(fresh, 64, pair.now).unwrap(), b"new response");
}

#[test]
fn shared_handshake_vectors_reset_unacceptable_acks_without_a_reset_loop() {
    use fips_tcp::wire::{Flags, Segment};
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vector {
        name: String,
        isn: u32,
        ack: u32,
        reset_seq: Option<u32>,
        #[serde(default)]
        rst: bool,
        #[serde(default)]
        syn: bool,
        #[serde(default)]
        closed: bool,
    }
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/handshake-recovery-vectors.json")).unwrap();
    for v in vectors {
        let mut stack = Stack::new(Config::default(), 1);
        let id = stack
            .connect_from_with_isn("peer", 50000, 443, v.isn, 0)
            .unwrap();
        stack.drain_outbound();
        let mut incoming = Segment::new(443, 50000, 7);
        incoming.flags = Flags::ACK;
        incoming.ack = Some(v.ack);
        if v.rst {
            incoming.flags = incoming.flags | Flags::RST;
        }
        if v.syn {
            incoming.flags = incoming.flags | Flags::SYN;
        }
        stack.input("peer", &incoming.encode().unwrap(), 0).unwrap();
        let output = stack.drain_outbound();
        assert_eq!(
            output.len(),
            usize::from(v.reset_seq.is_some()),
            "{}",
            v.name
        );
        if let Some(sequence) = v.reset_seq {
            let reset = Segment::decode(&output[0].bytes).unwrap();
            assert_eq!((reset.src_port, reset.dst_port), (50000, 443));
            assert_eq!(reset.flags, Flags::RST);
            assert_eq!(reset.seq, sequence);
            assert_eq!(reset.ack, None);
        }
        assert_eq!(
            stack.state(id),
            (!v.closed).then_some(State::SynSent),
            "{}",
            v.name
        );
    }
}
