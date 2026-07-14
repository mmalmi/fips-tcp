import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import { Flags, Segment, TcpOptionKind } from "../src/index.js";

interface Vector {
  name: string;
  hex: string;
  srcPort: number;
  dstPort: number;
  seq: number;
  ack: number | null;
  flags: string[];
  window: number;
  mss: number | null;
  version: number | null;
  payloadHex: string;
}

const vectors = JSON.parse(
  readFileSync(
    fileURLToPath(new URL("../../protocol/wire-vectors.json", import.meta.url)),
    "utf8",
  ),
) as Vector[];

const fromHex = (hex: string): Uint8Array =>
  Uint8Array.from(hex.match(/.{2}/g)?.map((byte) => Number.parseInt(byte, 16)) ?? []);

describe("shared TCP/FIPS wire vectors", () => {
  it("decodes and re-encodes every vector exactly", () => {
    for (const vector of vectors) {
      const bytes = fromHex(vector.hex);
      const segment = Segment.decode(bytes);
      expect(segment.srcPort, vector.name).toBe(vector.srcPort);
      expect(segment.dstPort, vector.name).toBe(vector.dstPort);
      expect(segment.seq, vector.name).toBe(vector.seq);
      expect(segment.ack ?? null, vector.name).toBe(vector.ack);
      expect(segment.window, vector.name).toBe(vector.window);
      expect(segment.payload, vector.name).toEqual(fromHex(vector.payloadHex));
      expect(segment.flags.has(Flags.Syn), vector.name).toBe(vector.flags.includes("syn"));
      expect(segment.flags.has(Flags.Ack), vector.name).toBe(vector.flags.includes("ack"));
      expect(
        segment.options.find((option) => option.kind === TcpOptionKind.MaxSegmentSize)?.value,
        vector.name,
      ).toBe(vector.mss ?? undefined);
      expect(segment.fipsVersion(), vector.name).toBe(vector.version ?? undefined);
      expect(segment.encode(), vector.name).toEqual(bytes);
    }
  });

  it("rejects malformed headers and non-zero checksums", () => {
    expect(() => Segment.decode(new Uint8Array(19))).toThrow();
    const bad = fromHex("01bbc0005566778811223345501080000001000068656c6c6f");
    expect(() => Segment.decode(bad)).toThrow(/checksum/i);
  });
});
