import { FIPS_VERSION, FlagSet, Flags, Segment, TcpOptionKind } from "./wire.js";

export function buildSegment(
  localPort: number,
  remotePort: number,
  seq: number,
  ack: number,
  window: number,
  mss: number,
  flags: FlagSet,
  payload: Uint8Array,
): Segment {
  return new Segment({
    srcPort: localPort,
    dstPort: remotePort,
    seq,
    ...(flags.has(Flags.Ack) ? { ack } : {}),
    flags,
    window,
    options: flags.has(Flags.Syn)
      ? [
          { kind: TcpOptionKind.MaxSegmentSize, value: mss },
          { kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION, reserved: 0 },
        ]
      : [],
    payload,
  });
}
