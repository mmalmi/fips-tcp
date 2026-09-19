import { readFileSync } from "node:fs";
import { expect, test } from "vitest";
import { FIPS_VERSION, FlagSet, Flags, Segment, Stack, TcpOptionKind } from "../src/index.js";

type Action =
  | { op: "syn" | "connect" | "reset"; peer: string; port: number }
  | { op: "abort"; id: number }
  | { op: "eligible"; peers: string[] }
  | { op: "reserve"; slots: number }
  | { op: "poll" };
interface Vector {
  name: string;
  maxConnections: number;
  maxConnectionsPerPeer: number;
  maxRetransmissions?: number;
  reserved: number;
  eligible: string[];
  steps: (Action & { now?: number; ok: boolean; outbound: number; connections: number })[];
}
const vectors = JSON.parse(readFileSync(
  new URL("../../rust/fips-tcp/protocol/admission-vectors.json", import.meta.url), "utf8",
)) as Vector[];

test.each(vectors)("shared admission vector: $name", (vector) => {
  const stack = new Stack({
    maxConnections: vector.maxConnections,
    maxConnectionsPerPeer: vector.maxConnectionsPerPeer,
    maxRetransmissions: vector.maxRetransmissions ?? 8,
  }, 1);
  stack.listen(443);
  let eligible = new Set(vector.eligible);
  const reserve = (slots: number): void => stack.setConnectionReservation(slots, (peer) => eligible.has(peer));
  reserve(vector.reserved);
  for (const [index, step] of vector.steps.entries()) {
    const run = (): void => {
      const now = step.now ?? 0;
      switch (step.op) {
        case "syn": stack.input(step.peer, syn(step.port), now); break;
        case "connect": stack.connectFromWithIsn(step.peer, step.port, 443, step.port, now); break;
        case "reset": stack.input(step.peer, new Segment({ srcPort: step.port, dstPort: 443,
          seq: step.port + 1, flags: new FlagSet(Flags.Rst) }).encode(), now); break;
        case "abort": stack.abort(step.id); break;
        case "eligible": eligible = new Set(step.peers); break;
        case "reserve": reserve(step.slots); break;
        case "poll": stack.poll(now); break;
      }
    };
    if (step.ok) expect(run, `step ${index}`).not.toThrow();
    else expect(run, `step ${index}`).toThrow(/connection|capacity/i);
    expect(stack.drainOutbound(), `step ${index}`).toHaveLength(step.outbound);
    const retained = Array.from({ length: vector.steps.length }, (_, n) => n + 1)
      .filter((id) => stack.state(id) !== undefined).length;
    expect(retained, `step ${index}`).toBe(step.connections);
    expect(retained).toBeLessThanOrEqual(vector.maxConnections);
  }
});

test("invalid numeric reservations do not replace the prior policy", () => {
  const stack = new Stack({ maxConnections: 2 });
  stack.listen(443);
  stack.setConnectionReservation(1, () => false);
  stack.input("first", syn(50_000), 0);
  for (const slots of [-1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1, 2]) {
    expect(() => stack.setConnectionReservation(slots, () => true)).toThrow(/reserved/i);
    expect(() => stack.input("second", syn(50_001), 0)).toThrow(/connection limit/i);
  }
});

function syn(port: number): Uint8Array {
  return new Segment({ srcPort: port, dstPort: 443, seq: port, flags: new FlagSet(Flags.Syn),
    options: [{ kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION, reserved: 0 }],
  }).encode();
}
