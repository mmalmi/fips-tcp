use fips_tcp::wire::{Flags, Segment};
use fips_tcp::{Config, Stack};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    name: String,
    initial_sequence: u32,
    read_chunks: Vec<usize>,
    #[serde(default)]
    duplicate_acks_before_read: usize,
}

fn pump(a: &mut Stack<String>, b: &mut Stack<String>) {
    for _ in 0..32 {
        let left = a.drain_outbound();
        let right = b.drain_outbound();
        if left.is_empty() && right.is_empty() {
            return;
        }
        for packet in left {
            b.input("a".into(), &packet.bytes, 0).unwrap();
        }
        for packet in right {
            a.input("b".into(), &packet.bytes, 0).unwrap();
        }
    }
    panic!("wire did not settle");
}

#[test]
fn shared_window_update_vectors_do_not_signal_loss_but_duplicate_acks_do() {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/window-update-vectors.json")).unwrap();
    for vector in vectors {
        let config = Config {
            mss: 16,
            receive_buffer: 128,
            ..Config::default()
        };
        let mut a = Stack::new(config.clone(), 1);
        let mut b = Stack::new(config, 2);
        b.listen(443).unwrap();
        let client = a
            .connect_from_with_isn("b".into(), 50_000, 443, vector.initial_sequence, 0)
            .unwrap();
        pump(&mut a, &mut b);
        let server = b.accept(443).unwrap();
        a.write(client, &[1; 16], 0).unwrap();
        a.write(client, &[2; 16], 0).unwrap();
        let flight = a.drain_outbound();
        assert_eq!(flight.len(), 2);
        b.input("a".into(), &flight[0].bytes, 0).unwrap();
        let ack = b.drain_outbound().pop().unwrap();
        let mut previous = Segment::decode(&ack.bytes).unwrap();
        a.input("b".into(), &ack.bytes, 0).unwrap();
        assert!(a.drain_outbound().is_empty());
        let mut last_update = Vec::new();
        for size in &vector.read_chunks {
            for _ in 0..vector.duplicate_acks_before_read {
                a.input("b".into(), &previous.encode().unwrap(), 0).unwrap();
                assert!(a.drain_outbound().is_empty(), "{}", vector.name);
            }
            assert_eq!(b.read(server, *size, 0).unwrap(), vec![1; *size]);
            let updates = b.drain_outbound();
            assert_eq!(updates.len(), 1);
            last_update = updates[0].bytes.clone();
            let update = Segment::decode(&last_update).unwrap();
            assert_eq!(update.ack, previous.ack);
            assert!(update.window > previous.window);
            previous = update;
            a.input("b".into(), &last_update, 0).unwrap();
            assert!(
                a.drain_outbound().is_empty(),
                "{}: a window update retransmitted data",
                vector.name
            );
        }
        // Data and connection-control ACKs also interrupt duplicate ACK runs.
        for flag in [Flags::PSH, Flags::SYN, Flags::FIN] {
            for _ in 0..vector.duplicate_acks_before_read {
                a.input("b".into(), &last_update, 0).unwrap();
                assert!(a.drain_outbound().is_empty(), "{}", vector.name);
            }
            let mut control = Segment::decode(&last_update).unwrap();
            control.flags = Flags::ACK | flag;
            if flag == Flags::PSH {
                control.payload = vec![9];
            }
            for _ in 0..3 {
                a.input("b".into(), &control.encode().unwrap(), 0).unwrap();
                assert!(
                    a.drain_outbound()
                        .iter()
                        .all(|packet| Segment::decode(&packet.bytes).unwrap().payload.is_empty())
                );
            }
        }
        for _ in 0..2 {
            a.input("b".into(), &last_update, 0).unwrap();
            assert!(a.drain_outbound().is_empty());
        }
        a.input("b".into(), &last_update, 0).unwrap();
        let repair = a.drain_outbound();
        assert_eq!(
            repair.len(),
            1,
            "three true duplicate ACKs must still repair loss"
        );
        let expected = Segment::decode(&flight[1].bytes).unwrap();
        let actual = Segment::decode(&repair[0].bytes).unwrap();
        assert_eq!(actual.seq, expected.seq);
        assert_eq!(actual.payload, expected.payload);
        b.input("a".into(), &repair[0].bytes, 0).unwrap();
        pump(&mut a, &mut b);
        let mut expected = vec![1; 16 - vector.read_chunks.iter().sum::<usize>()];
        expected.extend([2; 16]);
        assert_eq!(b.read(server, 64, 0).unwrap(), expected);
    }
}
