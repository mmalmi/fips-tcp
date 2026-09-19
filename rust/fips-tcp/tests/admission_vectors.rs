use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use fips_tcp::wire::{FIPS_VERSION, Flags, Segment, TcpOption};
use fips_tcp::{Config, ConnectionId, Stack, StackError};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vector {
    name: String,
    max_connections: usize,
    max_connections_per_peer: usize,
    max_retransmissions: Option<u8>,
    reserved: usize,
    eligible: Vec<String>,
    steps: Vec<Step>,
}

#[derive(Deserialize)]
struct Step {
    #[serde(flatten)]
    action: Action,
    #[serde(default)]
    now: u64,
    ok: bool,
    outbound: usize,
    connections: usize,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Action {
    Syn { peer: String, port: u16 },
    Connect { peer: String, port: u16 },
    Reset { peer: String, port: u16 },
    Abort { id: u64 },
    Eligible { peers: Vec<String> },
    Reserve { slots: usize },
    Poll,
}

#[test]
fn shared_admission_vectors_preserve_bounded_local_reservations() {
    let vectors: Vec<Vector> =
        serde_json::from_str(include_str!("../protocol/admission-vectors.json")).unwrap();
    for vector in vectors {
        let mut stack = Stack::new(
            Config {
                max_connections: vector.max_connections,
                max_connections_per_peer: vector.max_connections_per_peer,
                max_retransmissions: vector.max_retransmissions.unwrap_or(8),
                ..Config::default()
            },
            1,
        );
        stack.listen(443).unwrap();
        let eligible = Arc::new(RwLock::new(
            vector.eligible.into_iter().collect::<HashSet<_>>(),
        ));
        reserve(&mut stack, vector.reserved, &eligible).unwrap();
        for (index, step) in vector.steps.iter().enumerate() {
            let outcome = match &step.action {
                Action::Syn { peer, port } => stack.input(peer.clone(), &syn(*port), step.now),
                Action::Connect { peer, port } => stack
                    .connect_from_with_isn(peer.clone(), *port, 443, u32::from(*port), step.now)
                    .map(|_| ()),
                Action::Reset { peer, port } => {
                    let mut reset = Segment::new(*port, 443, u32::from(*port) + 1);
                    reset.flags = Flags::RST;
                    stack.input(peer.clone(), &reset.encode().unwrap(), step.now)
                }
                Action::Abort { id } => stack.abort(ConnectionId::from_raw(*id)),
                Action::Eligible { peers } => {
                    *eligible.write().unwrap() = peers.iter().cloned().collect();
                    Ok(())
                }
                Action::Reserve { slots } => reserve(&mut stack, *slots, &eligible),
                Action::Poll => {
                    stack.poll(step.now);
                    Ok(())
                }
            };
            assert_eq!(
                outcome.is_ok(),
                step.ok,
                "{} step {index}: {outcome:?}",
                vector.name
            );
            assert_eq!(
                stack.drain_outbound().len(),
                step.outbound,
                "{} step {index}",
                vector.name
            );
            let count = (1..=vector.steps.len() as u64)
                .filter(|id| stack.state(ConnectionId::from_raw(*id)).is_some())
                .count();
            assert_eq!(count, step.connections, "{} step {index}", vector.name);
            assert!(count <= vector.max_connections);
        }
    }
}

fn reserve(
    stack: &mut Stack<String>,
    slots: usize,
    eligible: &Arc<RwLock<HashSet<String>>>,
) -> Result<(), StackError> {
    let eligible = eligible.clone();
    stack.set_connection_reservation(
        slots,
        Arc::new(move |peer| eligible.read().unwrap().contains(peer)),
    )
}

fn syn(port: u16) -> Vec<u8> {
    let mut syn = Segment::new(port, 443, u32::from(port));
    syn.flags = Flags::SYN;
    syn.options = vec![TcpOption::FipsVersion {
        version: FIPS_VERSION,
        reserved: 0,
    }];
    syn.encode().unwrap()
}
