# smoltcp reference oracle

TCP/FIPS is an independent implementation. smoltcp is used as a readable,
well-tested behavior reference, not as a dependency and not as an IP layer.

The pinned reference revision is:

```text
repository: https://github.com/smoltcp-rs/smoltcp.git
commit:     4d32d47d6905dc41e8baa2f9c29ab7f0bc81639d
describe:   v0.13.1-68-g4d32d47
```

Reference mapping:

| TCP/FIPS behavior | smoltcp reference |
| --- | --- |
| wrapping 32-bit sequence comparisons | `src/wire/tcp.rs`, `SeqNumber` |
| RTT sampling, Karn suppression, RTO bounds/backoff | `src/socket/tcp.rs`, `RttEstimator` |
| retransmit and zero-window-probe timers | `src/socket/tcp.rs`, `Timer` |
| Reno slow start, congestion avoidance, fast recovery | `src/socket/tcp/congestion/reno.rs` |
| state transitions, ACK validation, FIN/TIME-WAIT | `src/socket/tcp.rs`, `Socket` |
| loss, partial ACK, wrap, zero-window, and close cases | tests embedded in `src/socket/tcp.rs` |

The Rust and TypeScript state machines are carrier-independent sans-I/O cores:
they accept `(peer, segment bytes, time)` and emit `(peer, segment bytes)`.
FIPS is one adapter for that contract, and its explicitly configured FSP
service port remains a separate wire field from the TCP ports encoded in every
segment. For a simpler application API, the standard adapter mirrors the FSP
service number as its hidden TCP listening port and manages ephemeral client
ports internally.

Carrier independence alone does not make TCP/FIPS wire-compatible with a
standard TCP stack. smoltcp expects TCP inside IP and calculates an IP
pseudo-header checksum, while TCP/FIPS deliberately has no IP, requires a zero
checksum under FSP authentication, and requires its version option during the
handshake. A non-public smoltcp oracle bridges these boundaries in
`rust/smoltcp-oracle`:

1. wrap emitted segments in a synthetic IP envelope and fill the checksum;
2. verify and clear the checksum before passing replies to TCP/FIPS; and
3. inject the TCP/FIPS option before end-of-options padding on
   smoltcp-generated SYN and SYN-ACK segments, because smoltcp accepts the
   unknown option but does not echo it.

The production TCP/FIPS state machine still requires the version option on
every handshake; there is no permissive or standard-handshake mode. Synthetic
IP types and smoltcp are confined to an unpublished test-only crate, so neither
public library gains IP APIs or runtime dependencies. iperf3 remains a later
system test rather than a direct oracle: it also requires an OS-visible socket
bridge and implementation of iperf3's application-level control flow.

Features intentionally deferred from TCP/FIPS v1 include SACK, window scaling,
timestamps, ECN, urgent data, and delayed ACK. Adding one requires a protocol
revision, matching Rust and TypeScript behavior, shared vectors, and live
cross-language tests.
