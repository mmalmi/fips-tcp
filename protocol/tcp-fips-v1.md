# TCP/FIPS Wire Protocol v1

## Carrier and Addressing

TCP/FIPS segments are FSP service datagram bodies. The embedding application
chooses the non-zero FIPS service port and both peers must agree on it; the port
is adapter configuration, not part of this wire protocol. The authenticated
FIPS source identity replaces the source network address. The destination is
the peer selected by the FIPS endpoint send API.

The standard per-service adapters use the selected FSP service port as the
server-side TCP destination port as well. This is an API convention that gives
applications one service number to configure; the TCP header still carries its
own destination port and an ephemeral client source port. Low-level embeddings
may use other TCP port mappings without changing the segment encoding.

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

Implementations must bound retained connections globally and may apply a
stricter authenticated-peer cap. That cap counts pending, active, FIN-WAIT-2,
other closing, and TIME-WAIT state. FIN-WAIT-2 retention must have a positive,
finite local deadline so an ACK-without-FIN peer cannot hold capacity forever.
Rejecting a new tuple at either cap must not allocate connection state or
disturb valid later segments from the same bounded carrier batch.

An application may abort a retained tuple after its own bounded graceful-close
deadline. Following RFC 9293, the implementation sends one reset using
`<SEQ=SND.NXT><CTL=RST>`, flushes all other queued transmissions, and removes
local connection state immediately. Aborting an unknown tuple emits nothing
and cannot disturb another tuple.

Incoming resets follow RFC 9293 and RFC 5961 state and sequence validation.
SYN-SENT accepts a reset only when its ACK acknowledges the outstanding SYN.
For SYN-RECEIVED and synchronized states, an RST at `RCV.NXT` closes the tuple,
an in-window non-exact RST elicits one challenge ACK without closing, and an
out-of-window RST is silently dropped. With a zero receive window, only the
exact `RCV.NXT` reset is accepted. These checks apply after the authenticated
carrier identity and TCP port tuple have selected the retained connection.
