use fips_tcp::wire::{FIPS_VERSION, Flags, Segment, TcpOption};
use fips_tcp::{Config, Stack, StackError};

#[test]
fn unsupported_version_is_reset_and_connection_count_is_bounded() {
    let mut server = Stack::new(Config::default(), 1);
    server.listen(443).unwrap();
    let mut syn = Segment::new(50_000, 443, 1234);
    syn.flags = Flags::SYN;
    syn.options = vec![
        TcpOption::MaxSegmentSize(1024),
        TcpOption::FipsVersion {
            version: FIPS_VERSION + 1,
            reserved: 0,
        },
    ];
    server
        .input("peer".to_string(), &syn.encode().unwrap(), 0)
        .unwrap();
    let reset = server.drain_outbound();
    assert_eq!(reset.len(), 1);
    assert!(
        Segment::decode(&reset[0].bytes)
            .unwrap()
            .flags
            .contains(Flags::RST)
    );

    syn.options[1] = TcpOption::FipsVersion {
        version: FIPS_VERSION,
        reserved: 1,
    };
    server
        .input("other".to_string(), &syn.encode().unwrap(), 0)
        .unwrap();
    assert!(
        Segment::decode(&server.drain_outbound()[0].bytes)
            .unwrap()
            .flags
            .contains(Flags::RST)
    );

    let config = Config {
        max_connections: 1,
        ..Config::default()
    };
    let mut client = Stack::new(config, 2);
    client.connect("a".to_string(), 443, 0).unwrap();
    assert!(matches!(
        client.connect("b".to_string(), 443, 0),
        Err(StackError::ConnectionLimit)
    ));
}

#[test]
fn send_buffer_acceptance_is_bounded() {
    let config = Config {
        send_buffer: 10,
        ..Config::default()
    };
    let mut client = Stack::new(config.clone(), 1);
    let mut server = Stack::new(config, 2);
    server.listen(443).unwrap();
    let id = client.connect("server".to_string(), 443, 0).unwrap();
    for _ in 0..4 {
        for packet in client.drain_outbound() {
            server
                .input("client".to_string(), &packet.bytes, 0)
                .unwrap();
        }
        for packet in server.drain_outbound() {
            client
                .input("server".to_string(), &packet.bytes, 0)
                .unwrap();
        }
    }
    assert_eq!(client.write(id, &[7; 100], 0).unwrap(), 10);
}

#[test]
fn one_peer_syn_flood_cannot_consume_another_peers_capacity() {
    let mut server = Stack::new(
        Config {
            max_connections: 2,
            max_connections_per_peer: 1,
            ..Config::default()
        },
        3,
    );
    server.listen(443).unwrap();

    server
        .input("flooder".to_string(), &syn(50_000), 0)
        .unwrap();
    assert_eq!(server.drain_outbound().len(), 1);
    for source_port in 50_001..50_020 {
        assert!(matches!(
            server.input("flooder".to_string(), &syn(source_port), 0),
            Err(StackError::ConnectionLimit)
        ));
        assert!(server.drain_outbound().is_empty());
    }

    server
        .input("healthy".to_string(), &syn(51_000), 0)
        .expect("a second peer must retain its global table slot");
    assert_eq!(server.drain_outbound().len(), 1);
}

#[test]
#[should_panic(expected = "FIN-WAIT-2 duration must be non-zero")]
fn fin_wait_2_retention_must_be_bounded() {
    let _ = Stack::<String>::new(
        Config {
            fin_wait_2_ms: 0,
            ..Config::default()
        },
        4,
    );
}

fn syn(source_port: u16) -> Vec<u8> {
    let mut syn = Segment::new(source_port, 443, u32::from(source_port));
    syn.flags = Flags::SYN;
    syn.options = vec![
        TcpOption::MaxSegmentSize(1024),
        TcpOption::FipsVersion {
            version: FIPS_VERSION,
            reserved: 0,
        },
    ];
    syn.encode().unwrap()
}
