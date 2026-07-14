# smoltcp reference oracle

TCP/FIPS is an independent implementation. smoltcp is used as a readable,
well-tested behavior reference, not as a dependency and not as an IP layer.

The pinned local reference checkout is:

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
handshake. A non-public smoltcp oracle can bridge these boundaries in tests:

1. wrap emitted segments in a synthetic IP envelope and fill the checksum;
2. verify and clear the checksum before passing replies to TCP/FIPS; and
3. run the TCP/FIPS state machine in an explicit standard-handshake test
   profile because smoltcp ignores but does not echo the TCP/FIPS option.

That bridge is moderate test-harness work and does not require IP in either
public library. iperf3 is a later system test, not a direct oracle: it also
requires an OS-visible socket/TUN bridge and implementation of iperf3's
application-level control flow. smoltcp state-machine interoperability should
come first.

Features intentionally deferred from TCP/FIPS v1 include SACK, window scaling,
timestamps, ECN, urgent data, and delayed ACK. Adding one requires a protocol
revision, matching Rust and TypeScript behavior, shared vectors, and live
cross-language tests.
