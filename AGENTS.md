# Agent Instructions

- This repository implements TCP semantics directly over authenticated FIPS
  service datagrams. Do not add IP, IPv4, IPv6, TUN, TLS, or a second encryption
  layer to the protocol.
- Keep the Rust and TypeScript implementations behaviorally and byte-for-byte
  compatible. Every wire or state-machine change requires shared vectors plus
  bidirectional live interop coverage.
- Treat the smoltcp revision pinned in `SMOLTCP_REFERENCE.md` as a reference and
  test oracle, not a runtime dependency of the public TCP/FIPS libraries.
- Prefer deterministic sans-I/O state machines. Clocks, randomness, packet
  delivery, loss, duplication, and reordering must remain injectable in tests.
- Bound send queues, receive queues, reassembly, retransmission attempts, and
  connection counts. Never turn peer input into unbounded allocation.
- Keep Rust source files at or below 1000 lines and TypeScript source files at
  or below 500 lines. The default test suites enforce these limits.
- TCP acknowledgments prove stream receipt only. Application-level durable
  commit receipts remain above this library.
