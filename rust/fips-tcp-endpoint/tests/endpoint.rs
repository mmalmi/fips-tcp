use std::sync::Arc;
use std::time::Duration;

use fips_core::{FipsEndpoint, FipsEndpointError, PeerIdentity};
use fips_tcp::wire::{FIPS_VERSION, Flags, Segment, TcpOption};
use fips_tcp::{Config, MarkerStatus, State};
use fips_tcp_endpoint::{AdapterError, FipsTcpEndpoint};

const FSP_SERVICE_PORT: u16 = 39_017;

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
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        FSP_SERVICE_PORT,
        Config::default(),
        0x1234_5678,
    )
    .await
    .expect("bind TCP service");

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
