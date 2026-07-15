use std::sync::Arc;
use std::time::Duration;

use fips_core::{FipsEndpoint, FipsEndpointError, PeerIdentity};
use fips_tcp::{Config, State};
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

    tcp.write(client, b"actual FIPS service datagram", 10)
        .await
        .expect("write client stream");
    tcp.receive(10).await.expect("receive stream segment");
    tcp.receive(10).await.expect("receive acknowledgment");
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
