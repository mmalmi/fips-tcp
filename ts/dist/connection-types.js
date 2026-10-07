import { u32 } from "./seq.js";
import { Flags } from "./wire.js";
// Reopening receive capacity is not a loss signal. Existing FIPS peers reduce
// their window while buffering out-of-order data; keep counting those ACKs.
// The caller must pass the window before applying this segment's new value.
export const isDuplicateAckCandidate = (segment, sendUna, remoteWindow) => segment.ack === sendUna && segment.payload.length === 0 &&
    !segment.flags.has(Flags.Syn) && !segment.flags.has(Flags.Fin) &&
    segment.window <= remoteWindow;
export const openUpdate = (segments = []) => ({
    segments,
    accepted: false,
    closed: false,
});
export const trackedEnd = (segment) => u32(segment.seq +
    segment.payload.length +
    Number(segment.flags.has(Flags.Syn)) +
    Number(segment.flags.has(Flags.Fin)));
export const reassemblyEnd = (segment) => u32(segment.seq + segment.payload.length + Number(segment.fin));
