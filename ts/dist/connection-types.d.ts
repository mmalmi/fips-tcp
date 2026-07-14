import { FlagSet, Segment } from "./wire.js";
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
export declare const openUpdate: (segments?: Segment[]) => ConnectionUpdate;
export declare const trackedEnd: (segment: TrackedSegment) => number;
export declare const reassemblyEnd: (segment: ReassemblySegment) => number;
