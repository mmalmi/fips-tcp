use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket};
use std::sync::Arc;
use std::time::Duration;

use fips_core::discovery::local::{LocalInstanceCapability, select_capability_provider};
use fips_core::{Config as FipsConfig, FipsEndpoint, FipsEndpointError, PeerIdentity};
use fips_tcp::wire::{FIPS_VERSION, Flags, Segment, TcpOption};
use fips_tcp::{Config, MarkerStatus, State};
use fips_tcp_endpoint::{AdapterError, FipsTcpEndpoint};

const FSP_SERVICE_PORT: u16 = 39_017;
const CAPABILITY_SERVICE_PORT: u16 = 39_018;
const TCP_CAPABILITY: &str = "test.tcp/1";

#[tokio::test]
async fn tcp_stream_runs_through_real_fips_endpoint_service_datagrams() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind_with_capability(
        endpoint.clone(),
        LocalInstanceCapability::service(TCP_CAPABILITY, FSP_SERVICE_PORT),
        Config::default(),
        0x1234_5678,
    )
    .await
    .expect("bind standalone TCP service");
    assert!(
        endpoint
            .local_instance_advertisements()
            .expect("standalone capability snapshot")
            .is_empty(),
        "capability registration must not require or enable local rendezvous"
    );

    let client = tcp.connect(local, 0).await.expect("connect");
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(2), tcp.receive(0))
            .await
            .expect("handshake datagram timed out")
            .expect("receive handshake datagram");
    }
    let server = tcp.accept().expect("accept loopback connection");
    assert_eq!(tcp.state(client), Some(State::Established));
    assert_eq!(tcp.state(server), Some(State::Established));
    assert_eq!(tcp.peer(client), Some(local));
    assert_eq!(tcp.peer(server), Some(local));
    assert_eq!(tcp.ports(client).expect("client ports").1, FSP_SERVICE_PORT);
    assert_eq!(tcp.ports(server).expect("server ports").0, FSP_SERVICE_PORT);

    let (accepted, marker) = tcp
        .write_with_marker(client, b"actual FIPS service datagram", 10)
        .await
        .expect("write client stream");
    assert_eq!(accepted, 28);
    assert_eq!(tcp.marker_status(&marker), MarkerStatus::Pending);
    tcp.receive(10).await.expect("receive stream segment");
    tcp.receive(10).await.expect("receive acknowledgment");
    assert_eq!(tcp.marker_status(&marker), MarkerStatus::Acked);
    assert_eq!(
        tcp.read(server, 1024, 10)
            .await
            .expect("read server stream"),
        b"actual FIPS service datagram"
    );

    tcp.write(server, b"reply", 20)
        .await
        .expect("write server stream");
    tcp.receive(20).await.expect("receive reply segment");
    tcp.receive(20).await.expect("receive reply acknowledgment");
    assert_eq!(
        tcp.read(client, 1024, 20)
            .await
            .expect("read client stream"),
        b"reply"
    );

    endpoint.shutdown().await.expect("shutdown endpoint");
}

#[tokio::test]
async fn endpoint_abort_removes_both_loopback_stream_halves() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config::default(),
        0x7654_3210,
    )
    .await
    .expect("bind TCP service");

    let client = tcp.connect(local, 0).await.expect("connect");
    for _ in 0..3 {
        tcp.receive(0).await.expect("receive handshake datagram");
    }
    let server = tcp.accept().expect("accept loopback connection");
    tcp.abort(client).await.expect("abort client stream");
    assert_eq!(tcp.state(client), None);
    tcp.receive(1).await.expect("receive active reset");
    assert_eq!(tcp.state(server), None);
    assert!(matches!(
        tcp.abort(client).await,
        Err(AdapterError::Tcp(fips_tcp::StackError::UnknownConnection))
    ));

    endpoint.shutdown().await.expect("shutdown endpoint");
}

#[tokio::test]
async fn failed_initial_flush_releases_connection_capacity_and_preserves_the_fips_error() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let remote_endpoint = FipsEndpoint::builder()
        .without_system_tun()
        .bind()
        .await
        .expect("bind remote endpoint identity");
    let remote = PeerIdentity::from_npub(remote_endpoint.npub()).expect("parse remote identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config {
            max_connections: 1,
            ..Config::default()
        },
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");
    endpoint.shutdown().await.expect("shutdown endpoint");
    remote_endpoint
        .shutdown()
        .await
        .expect("shutdown remote endpoint");

    for attempt in 0..3 {
        let error = tcp
            .connect(remote, attempt)
            .await
            .expect_err("closed FIPS endpoint must reject the initial SYN");
        assert!(
            matches!(error, AdapterError::Fips(FipsEndpointError::Closed)),
            "attempt {attempt} returned {error}; failed connects must preserve the FIPS send error instead of retaining a hidden SYN until the connection limit"
        );
    }
}

#[tokio::test]
async fn capability_bind_is_announced_after_registration_and_withdrawn_on_drop() {
    let rendezvous_addr = reserve_rendezvous_addr();
    let provider = bind_local_endpoint(rendezvous_addr).await;
    let provider_npub = provider.npub().to_string();
    let ordinary = FipsTcpEndpoint::bind(
        provider.clone(),
        CAPABILITY_SERVICE_PORT,
        Config::default(),
        1,
    )
    .await
    .expect("bind ordinary TCP service");

    let capability = LocalInstanceCapability::service(TCP_CAPABILITY, CAPABILITY_SERVICE_PORT);
    assert!(
        FipsTcpEndpoint::bind_with_capability(
            provider.clone(),
            capability.clone(),
            Config::default(),
            2,
        )
        .await
        .is_err(),
        "the already-registered FSP port must reject a capability owner"
    );
    assert!(
        select_capability_provider(
            &provider
                .local_instance_advertisements()
                .expect("provider capability snapshot"),
            TCP_CAPABILITY,
        )
        .is_none(),
        "a failed service registration must not announce a capability"
    );

    drop(ordinary);
    let advertised = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match FipsTcpEndpoint::bind_with_capability(
                provider.clone(),
                capability.clone(),
                Config::default(),
                3,
            )
            .await
            {
                Ok(adapter) => break adapter,
                Err(_) => tokio::time::sleep(Duration::from_millis(20)).await,
            }
        }
    })
    .await
    .expect("dropped receiver must release its FSP port");
    let adverts = provider
        .local_instance_advertisements()
        .expect("provider capability snapshot");
    let selected = select_capability_provider(&adverts, TCP_CAPABILITY)
        .expect("successful registration must announce its capability");
    assert_eq!(selected.npub, provider_npub);
    assert_eq!(selected.capabilities, vec![capability.clone()]);

    assert!(
        FipsTcpEndpoint::bind_with_capability(provider.clone(), capability, Config::default(), 4,)
            .await
            .is_err(),
        "the live advertised service must have one FSP owner"
    );

    drop(advertised);
    wait_for_capability_removal(&provider).await;
    let _replacement = FipsTcpEndpoint::bind_with_capability(
        provider.clone(),
        LocalInstanceCapability::service(TCP_CAPABILITY, CAPABILITY_SERVICE_PORT),
        Config::default(),
        5,
    )
    .await
    .expect("withdrawn capability port must be reusable");

    provider.shutdown().await.expect("shutdown provider");
}

#[tokio::test]
async fn malformed_datagram_does_not_drop_later_valid_datagram_from_the_batch() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config::default(),
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");
    send_loopback(&endpoint, local, vec![1, 2, 3]).await;
    send_loopback(&endpoint, local, rst(50_000)).await;

    let report = tcp.receive_report(0).await.expect("receive mixed batch");
    assert_eq!(report.datagrams, 2);
    assert_eq!(report.processed, 1);
    assert_eq!(report.malformed, 1);
    assert_eq!(report.connection_limited, 0);
    assert_eq!(report.other_errors, 0);
    assert_eq!(report.rejected(), 1);

    endpoint.shutdown().await.expect("shutdown endpoint");
}

#[tokio::test]
async fn full_table_error_does_not_drop_later_valid_datagram_from_the_batch() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config {
            max_connections: 1,
            max_connections_per_peer: 1,
            ..Config::default()
        },
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");
    send_loopback(&endpoint, local, syn(50_000)).await;
    send_loopback(&endpoint, local, syn(50_001)).await;
    send_loopback(&endpoint, local, rst(50_002)).await;

    let report = tcp
        .receive_report(0)
        .await
        .expect("receive full-table batch");
    assert_eq!(report.datagrams, 3);
    assert_eq!(report.processed, 2);
    assert_eq!(report.malformed, 0);
    assert_eq!(report.connection_limited, 1);
    assert_eq!(report.other_errors, 0);
    assert_eq!(report.rejected(), 1);

    endpoint.shutdown().await.expect("shutdown endpoint");
}

#[tokio::test]
async fn rejected_peer_cannot_emit_syn_ack_or_consume_connection_capacity() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config {
            max_connections: 2,
            max_connections_per_peer: 2,
            ..Config::default()
        },
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");
    send_loopback(&endpoint, local, syn(50_000)).await;

    let mut checked = Vec::new();
    let report = tcp
        .receive_report_filtered(0, |peer| {
            checked.push(peer.npub());
            false
        })
        .await
        .expect("reject unadmitted peer");
    assert_eq!(checked, vec![local.npub()]);
    assert_eq!(report.datagrams, 1);
    assert_eq!(report.processed, 0);
    assert_eq!(report.connection_limited, 1);
    assert_eq!(report.rejected(), 1);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), tcp.receive_report(0))
            .await
            .is_err(),
        "rejected SYN must not emit a loopback SYN-ACK"
    );

    // Admitting this identity now must leave room for both stream halves.
    let client = tcp.connect(local, 0).await.expect("connect admitted peer");
    for _ in 0..3 {
        let report = tokio::time::timeout(
            Duration::from_secs(2),
            tcp.receive_report_filtered(0, |peer| peer.npub() == local.npub()),
        )
        .await
        .expect("admitted handshake datagram timed out")
        .expect("receive admitted handshake datagram");
        assert_eq!(report.rejected(), 0);
    }
    let server = tcp.accept().expect("accept admitted peer");
    assert_eq!(tcp.state(client), Some(State::Established));
    assert_eq!(tcp.state(server), Some(State::Established));

    endpoint.shutdown().await.expect("shutdown endpoint");
}

#[tokio::test]
async fn filtered_receive_preserves_batch_bound_and_isolates_rejected_and_malformed_datagrams() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .expect("bind embedded endpoint"),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).expect("parse local identity");
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config::default(),
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");
    send_loopback(&endpoint, local, syn(50_000)).await;
    send_loopback(&endpoint, local, vec![1, 2, 3]).await;
    for _ in 0..64 {
        send_loopback(&endpoint, local, rst(50_001)).await;
    }

    let mut checked = 0;
    let report = tcp
        .receive_report_filtered(0, |peer| {
            assert_eq!(peer.npub(), local.npub());
            checked += 1;
            checked > 1
        })
        .await
        .expect("receive mixed filtered batch");
    assert_eq!(checked, 64);
    assert_eq!(report.datagrams, 64);
    assert_eq!(report.processed, 62);
    assert_eq!(report.malformed, 1);
    assert_eq!(report.connection_limited, 1);
    assert_eq!(report.other_errors, 0);
    assert_eq!(report.rejected(), 2);

    let remainder = tcp
        .receive_report(0)
        .await
        .expect("receive remaining batch");
    assert_eq!(remainder.datagrams, 2);
    assert_eq!(remainder.processed, 2);
    assert_eq!(remainder.rejected(), 0);

    endpoint.shutdown().await.expect("shutdown endpoint");
}

async fn send_loopback(endpoint: &FipsEndpoint, local: PeerIdentity, bytes: Vec<u8>) {
    endpoint
        .send_datagram(local, FSP_SERVICE_PORT, FSP_SERVICE_PORT, bytes)
        .await
        .expect("send loopback service datagram");
}

fn syn(source_port: u16) -> Vec<u8> {
    let mut segment = Segment::new(source_port, FSP_SERVICE_PORT, u32::from(source_port));
    segment.flags = Flags::SYN;
    segment.options = vec![
        TcpOption::MaxSegmentSize(1024),
        TcpOption::FipsVersion {
            version: FIPS_VERSION,
            reserved: 0,
        },
    ];
    segment.encode().expect("encode SYN")
}

fn rst(source_port: u16) -> Vec<u8> {
    let mut segment = Segment::new(source_port, FSP_SERVICE_PORT, u32::from(source_port));
    segment.flags = Flags::RST;
    segment.encode().expect("encode RST")
}

fn reserve_rendezvous_addr() -> SocketAddrV4 {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).expect("reserve rendezvous address");
    let SocketAddr::V4(addr) = socket.local_addr().expect("reserved rendezvous address") else {
        panic!("IPv4 loopback bind returned an IPv6 address");
    };
    addr
}

async fn bind_local_endpoint(rendezvous_addr: SocketAddrV4) -> Arc<FipsEndpoint> {
    let mut config = FipsConfig::new();
    config.node.discovery.local.rendezvous_addr = rendezvous_addr;
    config.node.discovery.local.retry_interval_ms = 20;
    Arc::new(
        FipsEndpoint::builder()
            .config(config)
            .local_rendezvous()
            .without_system_tun()
            .bind()
            .await
            .expect("bind local FIPS endpoint"),
    )
}

async fn wait_for_capability_removal(endpoint: &FipsEndpoint) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let adverts = endpoint
                .local_instance_advertisements()
                .expect("capability snapshot");
            if select_capability_provider(&adverts, TCP_CAPABILITY).is_none() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("capability was not withdrawn");
}

#[tokio::test]
async fn datagram_filter_sees_authenticated_bytes_before_tcp_admission() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .unwrap(),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).unwrap();
    let mut tcp = FipsTcpEndpoint::bind(endpoint.clone(), FSP_SERVICE_PORT, Config::default(), 7)
        .await
        .unwrap();
    let rejected = syn(50_000);
    let admitted = syn(50_001);
    send_loopback(&endpoint, local, rejected.clone()).await;
    send_loopback(&endpoint, local, vec![1, 2, 3]).await;
    send_loopback(&endpoint, local, admitted.clone()).await;
    let mut seen = Vec::new();
    let report = tcp
        .receive_report_filtered_datagrams(0, |peer, bytes| {
            assert_eq!(peer.npub(), local.npub());
            seen.push(bytes.to_vec());
            bytes == admitted
        })
        .await
        .unwrap();
    assert_eq!(seen, vec![rejected, vec![1, 2, 3], admitted]);
    assert_eq!(report.processed, 1);
    assert_eq!(report.connection_limited, 2);
    assert_eq!(report.malformed, 0, "filtered bytes must never enter TCP");
    endpoint.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_peer_reclaims_accepted_and_hidden_half_open_streams() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .unwrap(),
    );
    let local = PeerIdentity::from_npub(endpoint.npub()).unwrap();
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config {
            max_connections: 3,
            max_connections_per_peer: 3,
            ..Config::default()
        },
        7,
    )
    .await
    .unwrap();
    let client = tcp.connect(local, 0).await.unwrap();
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(2), tcp.receive(0))
            .await
            .unwrap()
            .unwrap();
    }
    let server = tcp.accept().unwrap();
    send_loopback(&endpoint, local, syn(50000)).await;
    tokio::time::timeout(Duration::from_secs(2), tcp.receive(0))
        .await
        .unwrap()
        .unwrap();
    assert!(tcp.accept().is_none());
    assert_eq!(tcp.abort_peer(local).await.unwrap(), 3);
    assert_eq!(tcp.state(client), None);
    assert_eq!(tcp.state(server), None);
    assert!(tcp.accept().is_none());
    assert_eq!(tcp.abort_peer(local).await.unwrap(), 0);
    assert!(tcp.connect(local, 0).await.is_ok());
    endpoint.shutdown().await.unwrap();
}
