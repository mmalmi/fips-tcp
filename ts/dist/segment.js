import { u32 } from "./seq.js";
import { FIPS_VERSION, FlagSet, Flags, Segment, TcpOptionKind } from "./wire.js";
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
export function resetResponse(incoming) {
    const hasAck = incoming.ack !== undefined;
    return new Segment({
        srcPort: incoming.dstPort,
        dstPort: incoming.srcPort,
        seq: incoming.ack ?? 0,
        ...(hasAck ? {} : { ack: u32(incoming.seq + incoming.sequenceLength()) }),
        flags: new FlagSet(hasAck ? Flags.Rst : Flags.Rst | Flags.Ack),
        window: 0,
    });
}
