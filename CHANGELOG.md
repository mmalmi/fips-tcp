# Changelog

## 0.2.9 - 2026-09-04

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.73 so reliable
  streams inherit the routing, transport cleanup, and BLE identity hardening.
- TCP/FIPS v1 wire bytes, runtime behavior, and the dependency-free
  `nvpn-fips-tcp` 0.2.1 state machine are unchanged.

## 0.2.8 - 2026-08-31

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.72 so release
  verification uses the corrected configured-peer cache fixtures.
- TCP/FIPS v1 wire bytes, runtime behavior, and the dependency-free
  `nvpn-fips-tcp` 0.2.1 state machine are unchanged.

## 0.2.7 - 2026-08-31

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.71 so reliable
  FIPS streams reuse validated configured-peer identities during liveness
  recovery.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.6 - 2026-08-31

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.70 so reliable
  FIPS streams retry recovered direct paths promptly across repeated outages.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.5 - 2026-08-31

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.69 so reliable
  FIPS streams retain direct recovery across consecutive outages and stale
  receiver-report feedback.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.4 - 2026-08-28

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.68 so reliable
  FIPS streams share repeated roaming, rekey, and live-rebind recovery fixes.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.3 - 2026-08-28

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.67 so reliable
  FIPS streams retain prompt direct-path recovery across live network rebinds.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.2 - 2026-08-28

- Update `nvpn-fips-tcp-endpoint` to `nvpn-fips-core` 0.4.66 so reliable
  FIPS streams share the app's exact roaming-recovery implementation.
- TCP/FIPS v1 wire bytes and the dependency-free `nvpn-fips-tcp` 0.2.1
  state machine are unchanged.

## 0.2.1 - 2026-08-25

- Publish the Rust crates as `nvpn-fips-tcp` and
  `nvpn-fips-tcp-endpoint`, while preserving the existing `fips_tcp` and
  `fips_tcp_endpoint` Rust import names.
- Target `nvpn-fips-core 0.4.65` from the
  [Nostr VPN FIPS implementation](https://git.iris.to/#/npub1xdhnr9mrv47kkrn95k6cwecearydeh8e895990n3acntwvmgk2dsdeeycm/fips),
  which descends from the
  [original FIPS implementation](https://github.com/jmcorgan/fips).

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
