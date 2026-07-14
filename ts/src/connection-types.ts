import { u32 } from "./seq.js";
import { FlagSet, Flags, Segment } from "./wire.js";

export interface TrackedSegment {
  seq: number;
  flags: FlagSet;
  payload: Uint8Array;
  sentAtMs: number;
  retransmitted: boolean;
  transmissions: number;
}

export interface ReassemblySegment {
  seq: number;
  payload: Uint8Array;
  fin: boolean;
}

export interface ConnectionUpdate {
  segments: Segment[];
  accepted: boolean;
  closed: boolean;
}

export interface AckOutcome {
  finAcked: boolean;
  retransmit?: Segment;
}

export const openUpdate = (segments: Segment[] = []): ConnectionUpdate => ({
  segments,
  accepted: false,
  closed: false,
});

export const trackedEnd = (segment: TrackedSegment): number =>
  u32(
    segment.seq +
      segment.payload.length +
      Number(segment.flags.has(Flags.Syn)) +
      Number(segment.flags.has(Flags.Fin)),
  );

export const reassemblyEnd = (segment: ReassemblySegment): number =>
  u32(segment.seq + segment.payload.length + Number(segment.fin));
