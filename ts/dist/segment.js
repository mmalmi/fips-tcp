import { FIPS_VERSION, Flags, Segment, TcpOptionKind } from "./wire.js";
export function buildSegment(localPort, remotePort, seq, ack, window, mss, flags, payload) {
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
