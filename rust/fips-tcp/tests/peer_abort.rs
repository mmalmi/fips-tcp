use fips_tcp::wire::{FIPS_VERSION, Flags, Segment, TcpOption};
use fips_tcp::{Config, Stack, State};

#[test]
fn abort_peer_reclaims_half_open_capacity_without_disturbing_other_peers() {
    let mut stack = Stack::<String>::new(
        Config {
            max_connections: 3,
            max_connections_per_peer: 2,
            ..Config::default()
        },
        7,
    );
    stack.listen(39017).unwrap();
    let keeper = stack.connect("keeper".into(), 39017, 0).unwrap();
    stack.drain_outbound();
    for round in 0..40 {
        let peer = format!("client-{round}");
        for port in [50000, 50001] {
            let mut syn = Segment::new(port, 39017, 7);
            syn.flags = Flags::SYN;
            syn.options.push(TcpOption::FipsVersion {
                version: FIPS_VERSION,
                reserved: 0,
            });
            stack
                .input(peer.clone(), &syn.encode().unwrap(), 0)
                .unwrap();
        }
        assert!(
            stack.accept(39017).is_none(),
            "SYN states are not accepted streams"
        );
        assert_eq!(stack.abort_peer(&peer).unwrap(), 2);
        let output = stack.drain_outbound();
        assert_eq!(output.len(), 2);
        assert!(output.iter().all(|packet| {
            packet.peer == peer
                && Segment::decode(&packet.bytes)
                    .unwrap()
                    .flags
                    .contains(Flags::RST)
        }));
        assert_eq!(stack.state(keeper), Some(State::SynSent));
        assert_eq!(stack.abort_peer(&peer).unwrap(), 0);
        assert!(stack.drain_outbound().is_empty());
    }
}
