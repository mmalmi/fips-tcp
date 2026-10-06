import { readFileSync } from "node:fs";
import { expect, test } from "vitest";
import { FlagSet, Flags, Segment, Stack, State } from "../src/index.js";

interface Vector { name: string; isn: number; ack: number; resetSeq: number | null; rst?: boolean; syn?: boolean; closed?: boolean }
const vectors = JSON.parse(readFileSync(
  new URL("../../rust/fips-tcp/protocol/handshake-recovery-vectors.json", import.meta.url), "utf8",
)) as Vector[];

test.each(vectors)("handshake recovery: $name", (v) => {
  const stack = new Stack({}, 1n);
  const id = stack.connectFromWithIsn("peer", 50000, 443, v.isn, 0);
  stack.drainOutbound();
  const incoming = new Segment({ srcPort: 443, dstPort: 50000, seq: 7, ack: v.ack,
    flags: new FlagSet(Flags.Ack | (v.rst ? Flags.Rst : 0) | (v.syn ? Flags.Syn : 0)) });
  stack.input("peer", incoming.encode(), 0);
  const output = stack.drainOutbound();
  expect(output).toHaveLength(v.resetSeq === null ? 0 : 1);
  if (v.resetSeq !== null) {
    const reset = Segment.decode(output[0]!.bytes);
    expect([reset.srcPort, reset.dstPort]).toEqual([50000, 443]);
    expect(reset.flags).toEqual(new FlagSet(Flags.Rst));
    expect(reset.seq).toBe(v.resetSeq);
    expect(reset.ack).toBeUndefined();
  }
  expect(stack.state(id)).toBe(v.closed ? undefined : State.SynSent);
});
