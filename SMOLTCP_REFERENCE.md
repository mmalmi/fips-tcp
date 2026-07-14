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

The local Rust and TypeScript tests reproduce the relevant observable
behaviors with an injectable datagram network. Byte-for-byte interoperability
with smoltcp itself is intentionally not claimed: smoltcp expects TCP inside IP
and calculates an IP pseudo-header checksum, while TCP/FIPS deliberately has no
IP and requires the checksum field to be zero under FSP authentication.

Features intentionally deferred from TCP/FIPS v1 include SACK, window scaling,
timestamps, ECN, urgent data, and delayed ACK. Adding one requires a protocol
revision, matching Rust and TypeScript behavior, shared vectors, and live
cross-language tests.
