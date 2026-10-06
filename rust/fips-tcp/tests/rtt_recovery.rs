use fips_tcp::Config;
mod support;
use support::Pair;

#[test]
fn cumulative_recovery_ack_keeps_backoff_until_fresh_data() {
    #[derive(serde::Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct Vector {
        initial_sequence: u32,
        chunk_bytes: usize,
        dropped_poll_deltas_ms: Vec<u64>,
        recovery_poll_delta_ms: u64,
        expected_backoff_ms: u64,
        fresh_rtt_ms: u64,
        expected_fresh_rto_ms: u64,
    }
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/rtt-recovery-vectors.json")).unwrap();
    for v in vectors {
        let mut pair = Pair::new(Config::default());
        pair.b.listen(443).unwrap();
        let client = pair
            .a
            .connect_from_with_isn("b".into(), 50_000, 443, v.initial_sequence, 0)
            .unwrap();
        pair.settle();
        let server = pair.b.accept(443).unwrap();
        let bytes = vec![0x73; v.chunk_bytes];
        for _ in 0..2 {
            pair.a.write(client, &bytes, pair.now).unwrap();
        }
        let flight = pair.a.drain_outbound();
        assert_eq!(flight.len(), 2);
        // Retain the original tail beyond a missing, repeatedly retransmitted head.
        pair.b
            .input("a".into(), &flight[1].bytes, pair.now)
            .unwrap();
        pair.b.drain_outbound();
        for delta in v.dropped_poll_deltas_ms {
            pair.advance(delta);
            pair.a.poll(pair.now);
            assert_eq!(pair.a.drain_outbound().len(), 1);
        }
        pair.advance(v.recovery_poll_delta_ms);
        pair.a.poll(pair.now);
        let repair = pair.a.drain_outbound();
        assert_eq!(repair.len(), 1);
        pair.b
            .input("a".into(), &repair[0].bytes, pair.now)
            .unwrap();
        let ack = pair.b.drain_outbound();
        assert_eq!(ack.len(), 1);
        pair.a.input("b".into(), &ack[0].bytes, pair.now).unwrap();
        assert_eq!(
            pair.b
                .read(server, 2 * v.chunk_bytes, pair.now)
                .unwrap()
                .len(),
            2 * v.chunk_bytes
        );
        pair.settle();
        pair.a.write(client, &bytes, pair.now).unwrap();
        assert_eq!(pair.a.drain_outbound().len(), 1);
        pair.advance(v.expected_backoff_ms);
        pair.a.poll(pair.now);
        let repair = pair.a.drain_outbound();
        assert_eq!(
            repair.len(),
            1,
            "old buffered data cannot replace the backed-off RTT estimate"
        );
        pair.b
            .input("a".into(), &repair[0].bytes, pair.now)
            .unwrap();
        pair.settle();
        pair.a.write(client, &bytes, pair.now).unwrap();
        let fresh = pair.a.drain_outbound();
        assert_eq!(fresh.len(), 1);
        pair.advance(v.fresh_rtt_ms);
        pair.b.input("a".into(), &fresh[0].bytes, pair.now).unwrap();
        pair.settle();
        pair.a.write(client, &bytes, pair.now).unwrap();
        pair.a.drain_outbound();
        pair.advance(v.expected_fresh_rto_ms);
        pair.a.poll(pair.now);
        assert_eq!(
            pair.a.drain_outbound().len(),
            1,
            "fresh original data must restore RTT sampling"
        );
    }
}
