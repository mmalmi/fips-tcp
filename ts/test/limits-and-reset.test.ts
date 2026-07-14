import { expect, test } from "vitest";

import {
  FIPS_VERSION,
  FlagSet,
  Flags,
  Segment,
  Stack,
  TcpOptionKind,
} from "../src/index.js";

test("unsupported version is reset and connection count is bounded", () => {
  const server = new Stack({}, 1);
  server.listen(443);
  const syn = new Segment({
    srcPort: 50_000,
    dstPort: 443,
    seq: 1234,
    flags: new FlagSet(Flags.Syn),
    options: [
      { kind: TcpOptionKind.MaxSegmentSize, value: 1024 },
      { kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION + 1, reserved: 0 },
    ],
  });
  server.input("peer", syn.encode(), 0);
  const reset = server.drainOutbound();
  expect(reset).toHaveLength(1);
  expect(Segment.decode(reset[0]!.bytes).flags.has(Flags.Rst)).toBe(true);

  const reserved = new Segment({
    srcPort: 50_001,
    dstPort: 443,
    seq: 5678,
    flags: new FlagSet(Flags.Syn),
    options: [{ kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION, reserved: 1 }],
  });
  server.input("other", reserved.encode(), 0);
  expect(Segment.decode(server.drainOutbound()[0]!.bytes).flags.has(Flags.Rst)).toBe(true);

  const client = new Stack({ maxConnections: 1 }, 2);
  client.connect("a", 443, 0);
  expect(() => client.connect("b", 443, 0)).toThrow(/connection limit/i);
});

test("send-buffer acceptance is bounded", () => {
  const client = new Stack({ sendBuffer: 10 }, 1);
  const server = new Stack({ sendBuffer: 10 }, 2);
  server.listen(443);
  const id = client.connect("server", 443, 0);
  for (let step = 0; step < 4; step += 1) {
    for (const packet of client.drainOutbound()) server.input("client", packet.bytes, 0);
    for (const packet of server.drainOutbound()) client.input("server", packet.bytes, 0);
  }
  expect(client.write(id, new Uint8Array(100).fill(7), 0)).toBe(10);
});
