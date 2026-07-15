# Changelog

## 0.2.0 - 2026-07-15

- Harden TCP connection admission, reset validation, retransmission, flow
  control, bounded close lifecycles, and failed-connect rollback.
- Add cumulative payload-acknowledgement markers without changing TCP/FIPS v1
  wire bytes.
- Add bidirectional smoltcp interoperability coverage and matching Rust and
  TypeScript lifecycle tests.
- Let the Rust endpoint adapter advertise a generic same-host FIPS capability
  only after it owns the FSP service port, then withdraw it with the receiver.
  Ordinary binds remain standalone and do not require local rendezvous.
- Require `fips-core 0.4.0` for the fixed-UDP authenticated capability API.

## 0.1.0 - 2026-07-14

- First public Rust TCP/FIPS state-machine and endpoint-adapter releases.
