# TCP/FIPS Wire Protocol v1

## Carrier and Addressing

TCP/FIPS segments are FSP service datagram bodies. The embedding application
chooses the non-zero FIPS service port and both peers must agree on it; the port
is adapter configuration, not part of this wire protocol. The authenticated
FIPS source identity replaces the source network address. The destination is
the peer selected by the FIPS endpoint send API.

A connection is identified locally by:

```text
(remote FIPS identity, local TCP port, remote TCP port)
```

Ephemeral ports, random initial sequence numbers, and TIME-WAIT prevent stale
segments from being accepted by a later connection using the same tuple.

## Segment Encoding

The body is the standard big-endian TCP header, options, and payload:

```text
0               4               8              12
+---------------+---------------+---------------+
| src port      | dst port      | sequence number              |
+---------------+---------------+---------------+
| acknowledgment number                        |off| flags     |
+---------------+---------------+---------------+
| receive window| checksum=0    | urgent=0      | options ...  |
+---------------+---------------+---------------+---------------+
| payload ...                                                   |
+---------------------------------------------------------------+
```

- The checksum field MUST be zero. FSP already authenticates the complete
  segment and there is no IP pseudo-header.
- The urgent pointer MUST be zero. URG is unsupported.
- SYN and SYN-ACK MUST carry an MSS option and the four-byte TCP/FIPS option
  `fe 04 01 00` (`kind=254`, `length=4`, `version=1`, reserved byte zero).
- Unknown well-formed options are ignored. Unsupported protocol versions reject
  the handshake with RST.
- MSS defaults to `1024` if a peer omits it. Implementations may configure a
  smaller MSS to fit a constrained FIPS route.
- SYN and FIN each consume one byte of sequence space. ACKs are cumulative.
- v1 does not negotiate window scaling, timestamps, ECN, or urgent data.

## Required Behavior

Implementations follow the TCP state model through CLOSED, LISTEN, SYN-SENT,
SYN-RECEIVED, ESTABLISHED, FIN-WAIT-1, FIN-WAIT-2, CLOSE-WAIT, CLOSING,
LAST-ACK, and TIME-WAIT.

Required loss behavior includes RFC 6298-style RTT/RTO estimation with Karn's
rule, exponential RTO backoff, Reno slow start and congestion avoidance,
triple-duplicate-ACK fast retransmit, bounded out-of-order reassembly,
duplicate suppression, receive-window backpressure, zero-window probing, SYN
and FIN retransmission, and a bounded retry/user timeout.

TCP/FIPS provides a reliable stream while both endpoints retain connection
state. Durable application delivery, offline store-and-forward, and idempotent
effects are intentionally outside this protocol.
