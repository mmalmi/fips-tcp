use fips_tcp::{Config, ConnectionId, Stack, State};

mod support;
use support::Pair;

fn connected(config: Config) -> (Pair, ConnectionId, ConnectionId) {
    let mut pair = Pair::new(config);
    pair.b.listen(443).unwrap();
    let client = pair.a.connect("b".into(), 443, pair.now).unwrap();
    pair.settle();
    let server = pair.b.accept(443).unwrap();
    (pair, client, server)
}

#[test]
fn idle_streams_sleep_and_lost_data_wakes_at_the_existing_retry_deadline() {
    let (mut pair, client, server) = connected(Config::default());
    assert_eq!(pair.a.next_poll_at(pair.now), None);
    assert_eq!(pair.b.next_poll_at(pair.now), None);
    pair.advance(30_000);
    pair.a
        .write(client, b"lost then recovered", pair.now)
        .unwrap();
    let original = pair.a.drain_outbound().remove(0);
    let due = pair.a.next_poll_at(pair.now).unwrap();
    assert!(due > pair.now);
    pair.a.poll(due - 1);
    assert!(pair.a.drain_outbound().is_empty());
    assert_eq!(pair.a.next_poll_at(due - 1), Some(due));
    pair.now = due;
    pair.a.poll(due);
    let retry = pair.a.drain_outbound().remove(0);
    assert_eq!(retry.bytes, original.bytes);
    assert!(pair.a.next_poll_at(due).unwrap() > due);
    pair.b.input("a".into(), &retry.bytes, due).unwrap();
    pair.settle();
    assert_eq!(pair.a.next_poll_at(due), None);
    // Buffered application bytes alone need reads, not TCP timer polling.
    assert_eq!(pair.b.next_poll_at(due), None);
    assert_eq!(
        pair.b.read(server, 100, due).unwrap(),
        b"lost then recovered"
    );
}

#[test]
fn a_closed_receive_window_sleeps_until_payload_needs_a_probe() {
    let (mut pair, client, server) = connected(Config {
        mss: 4,
        receive_buffer: 4,
        ..Config::default()
    });
    pair.a.write(client, b"full", pair.now).unwrap();
    pair.settle();
    assert_eq!(pair.a.next_poll_at(pair.now), None);
    pair.a.write(client, b"next", pair.now).unwrap();
    assert!(pair.a.drain_outbound().is_empty());
    let due = pair.a.next_poll_at(pair.now).unwrap();
    pair.a.poll(due - 1);
    assert!(pair.a.drain_outbound().is_empty());
    pair.now = due;
    pair.a.poll(due);
    assert_eq!(pair.a.drain_outbound().len(), 1);
    assert!(pair.a.next_poll_at(due).unwrap() > due);
    assert_eq!(pair.b.read(server, 4, due).unwrap(), b"full");
    pair.settle();
    // The lost probe is recovered by the existing retransmission deadline.
    if let Some(due) = pair.a.next_poll_at(pair.now) {
        pair.now = due;
        pair.settle();
    }
    assert_eq!(pair.b.read(server, 4, pair.now).unwrap(), b"next");
    pair.settle();
    assert_eq!(pair.a.next_poll_at(pair.now), None);
}

#[test]
fn closing_retention_expires_without_periodic_idle_polls() {
    for finish_both in [false, true] {
        let (mut pair, client, server) = connected(Config {
            fin_wait_2_ms: 50,
            time_wait_ms: 70,
            ..Config::default()
        });
        pair.a.close(client, pair.now).unwrap();
        pair.settle();
        if finish_both {
            pair.b.close(server, pair.now).unwrap();
            pair.settle();
        }
        let expected = if finish_both { 70 } else { 50 };
        assert_eq!(pair.a.next_poll_at(pair.now), Some(expected));
        pair.a.poll(expected - 1);
        assert!(pair.a.state(client).is_some());
        pair.a.poll(expected);
        assert_eq!(pair.a.state(client), None);
        assert_eq!(pair.a.next_poll_at(expected), None);
    }
}

#[test]
fn a_close_waiting_for_receive_capacity_does_not_create_a_busy_timer() {
    let (mut pair, client, server) = connected(Config {
        mss: 4,
        receive_buffer: 4,
        ..Config::default()
    });
    pair.a.write(client, b"full", 0).unwrap();
    pair.settle();
    pair.a.close(client, 0).unwrap();
    assert_eq!(pair.a.next_poll_at(10_000), None);
    assert_eq!(pair.b.read(server, 4, 10_000).unwrap(), b"full");
    pair.now = 10_000;
    pair.settle();
    assert_eq!(pair.a.state(client), Some(State::FinWait2));
    assert!(pair.a.next_poll_at(pair.now).unwrap() > pair.now);
}

#[test]
fn deadline_only_handshake_polling_preserves_the_transmission_bound() {
    let config = Config {
        max_retransmissions: 3,
        ..Config::default()
    };
    let mut stack = Stack::new(config, 1);
    let id = stack.connect("absent", 443, 0).unwrap();
    let mut transmissions = stack.drain_outbound().len();
    let mut now = 0;
    for _ in 0..4 {
        let Some(due) = stack.next_poll_at(now) else {
            break;
        };
        assert!(due > now);
        stack.poll(due - 1);
        assert!(stack.drain_outbound().is_empty());
        now = due;
        stack.poll(now);
        transmissions += stack.drain_outbound().len();
    }
    assert_eq!(transmissions, 3);
    assert_eq!(stack.state(id), None);
    assert_eq!(stack.next_poll_at(now), None);
}
