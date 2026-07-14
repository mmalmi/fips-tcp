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

## Repository layout

- `protocol`: the TCP/FIPS v1 contract and shared byte-exact wire vectors.
- `rust/fips-tcp`: dependency-free Rust sans-I/O state machine.
- `rust/fips-tcp-fips`: async adapter for `fips_core::FipsEndpoint`.
- `rust/interop-driver`: JSON-lines test driver used by TypeScript interop tests.
- `ts`: `@fips/tcp`, including the TypeScript sans-I/O state machine and a
  structural `FipsNode` adapter.
- `SMOLTCP_REFERENCE.md`: the pinned smoltcp reference revision and the
  behavior mapped from it.

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
is separate from the TCP listener and connection ports inside each segment.

Rust applications using the standard endpoint adapter create a
`FipsTcpEndpoint`, listen or connect, and call `receive(now_ms)` from their
event loop:

```rust,no_run
use std::sync::Arc;
use fips_core::{FipsEndpoint, PeerIdentity};
use fips_tcp::Config;
use fips_tcp_fips::FipsTcpEndpoint;

# async fn example(peer: PeerIdentity) -> Result<(), Box<dyn std::error::Error>> {
let endpoint = Arc::new(FipsEndpoint::builder().without_system_tun().bind().await?);
let mut tcp = FipsTcpEndpoint::bind(endpoint, 39_017, Config::default(), 0x1234).await?;
let stream = tcp.connect(peer, 443, 0).await?;
tcp.write(stream, b"record", 1).await?;
# Ok(())
# }
```

The TypeScript adapter accepts the public `FipsNode` service shape without a
runtime package dependency:

```ts
import { FipsTcpEndpoint } from "@fips/tcp";

const tcp = new FipsTcpEndpoint(fipsNode, 39_017);
await tcp.listen(443);
const stream = await tcp.connect(remotePubkeyHex, 443);
await tcp.write(stream, new TextEncoder().encode("record"));
```

## Verification

```sh
cargo test --manifest-path rust/Cargo.toml --workspace
pnpm --dir ts install
pnpm --dir ts check
pnpm --dir ts build
pnpm --dir ts test
```

The TypeScript endpoint integration test links the sibling `fips-ts` checkout
at `../fips-ts`; those packages are not published to npm as of 2026-07-14. The
test runs two real `FipsNode` instances over their memory transport. The Rust
endpoint test uses published `fips-core` 0.3.96 and its real loopback service
datagram API.

The test matrix covers byte-exact shared vectors, malformed input, both
same-language stacks, Rust↔TypeScript in both client/server directions, SYN,
data, and FIN loss, reversal, duplication, sequence wrap, bounded buffers and
connections, flow control, lost window updates, zero-window probes, RTO
backoff, fast retransmit, RST, TIME-WAIT, and real FIPS endpoint carriage.
