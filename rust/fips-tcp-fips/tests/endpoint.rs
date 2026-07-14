use std::sync::Arc;
use std::time::Duration;

use fips_core::{FipsEndpoint, PeerIdentity};
use fips_tcp::{Config, State};
use fips_tcp_fips::FipsTcpEndpoint;

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
    tcp.listen(443).expect("listen");

    let client = tcp.connect(local, 443, 0).await.expect("connect");
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(2), tcp.receive(0))
            .await
            .expect("handshake datagram timed out")
            .expect("receive handshake datagram");
    }
    let server = tcp.accept(443).expect("accept loopback connection");
    assert_eq!(tcp.state(client), Some(State::Established));
    assert_eq!(tcp.state(server), Some(State::Established));

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
