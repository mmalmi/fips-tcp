import { FlagSet, Segment } from "./wire.js";
export declare function buildSegment(localPort: number, remotePort: number, seq: number, ack: number, window: number, mss: number, flags: FlagSet, payload: Uint8Array): Segment;
