import { readFileSync } from "node:fs";
import { Config, FlagSet, Flags, Segment } from "../src/index.js";

export interface RetryBoundVector {
  name: string;
  initialSequence: number;
  maxTransmissions: number;
  reset: "window" | "payload";
}

export const retryBoundVectors = JSON.parse(readFileSync(
  new URL("../../rust/fips-tcp/protocol/retry-bound-vectors.json", import.meta.url), "utf8",
)) as RetryBoundVector[];

export const retryConfig = (maxTransmissions: number): Partial<Config> => ({
  maxRetransmissions: maxTransmissions,
  initialRtoMs: 1000,
  minRtoMs: 1000,
  maxRtoMs: 1000,
});

export const reverseAck = (data: Segment): Segment => new Segment({
  srcPort: data.dstPort,
  dstPort: data.srcPort,
  seq: data.ack!,
  ack: data.seq,
  flags: new FlagSet(Flags.Ack),
  window: 64,
});

export const interruptDuplicates = (ack: Segment, reset: RetryBoundVector["reset"]): Segment =>
  new Segment({ ...ack, ack: ack.ack!,
    window: reset === "window" ? ack.window + 1 : ack.window,
    payload: reset === "payload" ? Uint8Array.of(9) : new Uint8Array(),
  });

export const afterInterruption = (reset: Segment): Segment => new Segment({
  ...reset, ack: reset.ack!, seq: (reset.seq + reset.payload.length) >>> 0,
  payload: new Uint8Array(),
});
