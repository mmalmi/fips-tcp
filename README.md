# TCP/FIPS

TCP/FIPS provides reliable ordered byte streams directly over authenticated
FIPS service datagrams. It has no IP layer, IP addresses, TUN device, TLS, or
second encryption layer.

The application explicitly chooses the outer FSP service port when binding the
adapter. Inside that authenticated peer-to-peer channel, the normal TCP header
supplies connection ports, byte sequence numbers, cumulative acknowledgments,
receive windows, retransmission, Reno congestion control, orderly close, and
reset behavior. A connection is identified by
`(remote FIPS identity, local TCP port, remote TCP port)`.

Applications configure only that one FSP service number. The standard endpoint
adapters automatically listen on the numerically matching internal TCP port;
client-side ephemeral TCP ports and all TCP multiplexing stay private to the
stack. The low-level sans-I/O cores retain explicit TCP ports for advanced
embeddings and standard-stack interoperability tests.

## Repository layout

- `protocol`: the TCP/FIPS v1 contract and shared byte-exact wire vectors.
- `rust/fips-tcp`: dependency-free Rust sans-I/O state machine.
- `rust/fips-tcp-endpoint`: async adapter for `fips_core::FipsEndpoint`.
- `rust/interop-driver`: JSON-lines test driver used by TypeScript interop tests.
- `rust/smoltcp-oracle`: unpublished test-only synthetic-IP bridge to the
  pinned smoltcp state machine.
- `ts`: `@fips/tcp`, including the TypeScript sans-I/O state machine and a
  structural `FipsNode` adapter.
- `SMOLTCP_REFERENCE.md`: the pinned smoltcp reference revision and the
  behavior mapped from it.

The briefly published `fips-tcp-fips` 0.1.0 package is yanked and superseded
by the clearer `fips-tcp-endpoint` name. New Rust consumers should use the
latter.

The Rust and TypeScript implementations use the same wire encoding and expose
the same deterministic clock-driven operations. Neither implementation calls
the other at runtime.

## Which layer should an application use?

Use TCP/FIPS when an application needs a connected reliable byte stream over a
FIPS peer relationship. HTTP or a REST API can run above that stream if the
application actually benefits from HTTP semantics, but HTTP is not required
for delivery and does not belong between TCP/FIPS and FIPS.

A TCP ACK proves that the remote stream stack accepted bytes. It does not prove
that an application validated or durably committed a message. Applications
such as chat, drive, git, audio, or pubsub should frame stable record IDs above
the stream and send a separate committed receipt after durable processing when
that stronger guarantee matters. Offline delivery still needs an outbox and a
store-and-forward service.

## Minimal APIs

The sans-I/O cores emit complete TCP/FIPS segment bodies. An embedding can
carry them over any FIPS transport or tunnel because only the endpoint service
datagram API is visible to TCP/FIPS. The FSP service port passed to the adapter
remains a separate field from the TCP ports inside each segment, but the
standard adapter mirrors its value as the hidden TCP listening port.

Rust applications using the standard endpoint adapter create a
`FipsTcpEndpoint`, connect or accept, and call `receive(now_ms)` from their event
loop:

```rust,no_run
use std::sync::Arc;
use fips_core::{FipsEndpoint, PeerIdentity};
use fips_tcp::Config;
use fips_tcp_endpoint::FipsTcpEndpoint;

# async fn example(peer: PeerIdentity) -> Result<(), Box<dyn std::error::Error>> {
let endpoint = Arc::new(FipsEndpoint::builder().without_system_tun().bind().await?);
let mut tcp = FipsTcpEndpoint::bind(endpoint, 39_017, Config::default(), 0x1234).await?;
let stream = tcp.connect(peer, 0).await?;
tcp.write(stream, b"record", 1).await?;
# Ok(())
# }
```

Services that permit only one stream per authenticated peer set Rust
`Config::max_connections_per_peer` or TypeScript `maxConnectionsPerPeer` to
`1`. Pending handshakes, established streams, FIN-WAIT-2, other closing
streams, and TIME-WAIT all count toward the cap, so distinct source ports
cannot bypass admission. FIN-WAIT-2 expires after a configurable 60-second
default if a peer acknowledges our FIN but never sends its own FIN. The generic
per-peer default remains effectively unlimited and preserves applications that
intentionally multiplex several streams per peer.

Rust `FipsTcpEndpoint::receive_report` isolates malformed and over-capacity
segments within each bounded FIPS receive batch. Its aggregate reports counts
only; it neither retains attacker-controlled error strings nor logs each bad
datagram. The compatibility `receive` method returns the total batch count.

Graceful `close` remains flow-controlled. Protocol owners such as pubsub should
give it a bounded application shutdown deadline, then call `abort` only when
that deadline expires. Abort emits one TCP reset and immediately releases the
retained tuple; it is not a replacement for normal orderly close.

The TypeScript adapter accepts the public `FipsNode` service shape without a
runtime package dependency:

```ts
import { FipsTcpEndpoint } from "@fips/tcp";

const tcp = new FipsTcpEndpoint(fipsNode, 39_017);
const stream = await tcp.connect(remotePubkeyHex);
await tcp.write(stream, new TextEncoder().encode("record"));
```

Both standard adapters expose `peer(stream)` so servers can bind accepted
streams to the authenticated FIPS identity, plus `ports(stream)` for advanced
diagnostics. TypeScript distribution files are tracked, so consumers can pin
the `ts` package at an immutable public Git revision without a local build or
sibling checkout.

## Payload ACK markers

`write_with_marker` in Rust and `writeWithMarker` in TypeScript preserve the
ordinary `write` behavior while also returning an opaque marker at the payload
boundary accepted into that connection's local send buffer. Query it with
`marker_status` or `markerStatus`:

```rust,no_run
# use fips_tcp::{ConnectionId, MarkerStatus, Stack};
# fn example(stack: &mut Stack<String>, stream: ConnectionId) -> Result<(), Box<dyn std::error::Error>> {
let (accepted, marker) = stack.write_with_marker(stream, b"record", 10)?;
assert_eq!(accepted, 6);
match stack.marker_status(&marker) {
    MarkerStatus::Acked => { /* the peer TCP stack cumulatively ACKed the bytes */ }
    MarkerStatus::Pending => { /* accepted locally, but not cumulatively ACKed yet */ }
    MarkerStatus::ConnectionGone => { /* the original connection no longer exists */ }
}
# Ok(())
# }
```

Markers count payload bytes only: SYN and FIN sequence space, retransmissions,
and duplicate ACKs do not advance them. A zero-length write returns a barrier
at the connection's existing accepted-payload boundary, so it is immediately
`Acked` only when all previously accepted payload is already acknowledged.
Markers remain bound to their original connection incarnation and become
`ConnectionGone` once that connection is removed by orderly close, abort,
timeout, or reset. Reusing its tuple or numeric ID cannot revive them.

A marker still proves only receipt by the peer's TCP/FIPS stream stack. Pubsub
and other durable protocols must retain their application commit receipts
above this API.

## Verification

```sh
cargo test --manifest-path rust/Cargo.toml --workspace
pnpm --dir ts install
pnpm --dir ts check
pnpm --dir ts build
pnpm --dir ts test
```

The TypeScript endpoint tests include a self-contained structural FIPS service
endpoint and two real `FipsNode` instances over an in-memory test transport. The
unpublished test-only `@fips/core` package is pinned to an exact public
[`mmalmi/fips-ts`](https://github.com/mmalmi/fips-ts) commit. The Rust endpoint
test uses the published `fips-core` release selected in `rust/Cargo.lock` and
its real loopback service-datagram API.

The test matrix covers byte-exact shared vectors, malformed input, both
same-language stacks, Rust↔TypeScript in both client/server directions, SYN,
data, and FIN loss, reversal, duplication, sequence wrap, bounded buffers and
connections, flow control, lost window updates, zero-window probes, RTO
backoff, fast retransmit, stale/out-of-window RST rejection, challenge ACKs,
TIME-WAIT, structural TypeScript FIPS endpoint carriage, explicit abort/reset,
payload ACK markers, real TypeScript/Rust FIPS endpoint carriage, and
bidirectional smoltcp interoperability through SYN, data, ACK loss,
retransmission, and FIN.
