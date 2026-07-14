import { u32 } from "./seq.js";
import { Flags } from "./wire.js";
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
