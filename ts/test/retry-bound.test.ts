import { expect, test } from "vitest";
import { Segment, State } from "../src/index.js";
import { Pair } from "./pair.js";
import { afterInterruption, interruptDuplicates, retryBoundVectors, retryConfig, reverseAck } from "./retry-bound-vectors.js";

test.each(retryBoundVectors)("fast retransmit remains bounded: $name", (vector) => {
  const pair = new Pair(retryConfig(vector.maxTransmissions));
  pair.b.listen(443);
  const client = pair.a.connectFromWithIsn("b", 50_000, 443, vector.initialSequence, 0);
  pair.settle();
  pair.a.write(client, new Uint8Array(16).fill(7), 0);
  const initial = pair.a.drainOutbound();
  expect(initial).toHaveLength(1);
  const data = Segment.decode(initial[0]!.bytes);
  let ack = reverseAck(data);
  let transmissions = 1;
  let lastSend = 0;
  for (let cycle = 0; cycle < vector.maxTransmissions + 3; cycle += 1) {
    pair.now = (cycle + 1) * 10;
    for (let duplicate = 0; duplicate < 3; duplicate += 1) {
      pair.a.input("b", ack.encode(), pair.now);
      for (const packet of pair.a.drainOutbound()) {
        const repair = Segment.decode(packet.bytes);
        expect(repair.seq).toBe(data.seq);
        expect(repair.payload).toEqual(data.payload);
        transmissions += 1;
        lastSend = pair.now;
      }
    }
    expect(transmissions).toBe(Math.min(cycle + 2, vector.maxTransmissions));
    const reset = interruptDuplicates(ack, vector.reset);
    pair.a.input("b", reset.encode(), pair.now);
    expect(pair.a.drainOutbound().every((p) => Segment.decode(p.bytes).payload.length === 0)).toBe(true);
    ack = afterInterruption(reset);
  }
  expect(lastSend).toBe((vector.maxTransmissions - 1) * 10);
  pair.a.poll(lastSend + 999);
  expect(pair.a.state(client)).toBe(State.Established);
  expect(pair.a.drainOutbound()).toHaveLength(0);
  pair.a.poll(lastSend + 1000);
  expect(pair.a.state(client)).toBeUndefined();
  expect(pair.a.drainOutbound()).toHaveLength(0);
});

test.each([1, 3, 8])("duplicate SYN retransmit respects cap %i and expiry", (maxTransmissions) => {
  const pair = new Pair({ ...retryConfig(maxTransmissions), maxConnections: 1 });
  pair.b.listen(443);
  pair.a.connectFromWithIsn("b", 50_000, 443, 1000, 0);
  const syn = pair.a.drainOutbound()[0]!.bytes;
  pair.b.input("a", syn, 0);
  const original = Segment.decode(pair.b.drainOutbound()[0]!.bytes);
  let transmissions = 1;
  for (let cycle = 0; cycle < maxTransmissions + 3; cycle += 1) {
    pair.b.input("a", syn, (cycle + 1) * 10);
    const replies = pair.b.drainOutbound();
    for (const reply of replies) expect(Segment.decode(reply.bytes)).toEqual(original);
    transmissions += replies.length;
    expect(transmissions).toBe(Math.min(cycle + 2, maxTransmissions));
  }
  const decoded = Segment.decode(syn);
  const fresh = new Segment({ srcPort: 50_001, dstPort: decoded.dstPort, seq: 2000,
    flags: decoded.flags, window: decoded.window, options: decoded.options }).encode();
  const deadline = (maxTransmissions - 1) * 10 + 1000;
  pair.b.poll(deadline - 1);
  expect(() => pair.b.input("a", fresh, deadline - 1)).toThrow(/connection limit/i);
  pair.b.poll(deadline);
  expect(pair.b.drainOutbound()).toHaveLength(0);
  pair.b.input("a", fresh, deadline);
  expect(pair.b.drainOutbound()).toHaveLength(1);
});

test("ACK of the final permitted send preserves the stream and fresh retry budget", () => {
  const pair = new Pair(retryConfig(3));
  const [client, server] = pair.connect();
  for (const round of [0, 1]) {
    const start = round * 2000;
    const payload = new Uint8Array(16).fill(7 + round);
    pair.a.write(client, payload, start);
    let lastData = pair.a.drainOutbound()[0]!.bytes;
    let ack = reverseAck(Segment.decode(lastData));
    for (let cycle = 0; cycle < 3; cycle += 1) {
      const now = start + (cycle + 1) * 10;
      for (let duplicate = 0; duplicate < 3; duplicate += 1) pair.a.input("b", ack.encode(), now);
      const repairs = pair.a.drainOutbound();
      expect(repairs).toHaveLength(cycle < 2 ? 1 : 0);
      if (repairs.length > 0) lastData = repairs[0]!.bytes;
      const reset = interruptDuplicates(ack, "window");
      pair.a.input("b", reset.encode(), now);
      expect(pair.a.drainOutbound()).toHaveLength(0);
      ack = afterInterruption(reset);
    }
    const beforeExpiry = start + 1019;
    pair.b.input("a", lastData, beforeExpiry);
    for (const packet of pair.b.drainOutbound()) pair.a.input("b", packet.bytes, beforeExpiry);
    expect(pair.b.read(server, 16, beforeExpiry)).toEqual(payload);
    for (const packet of pair.b.drainOutbound()) pair.a.input("b", packet.bytes, beforeExpiry);
    pair.a.poll(start + 1020);
    expect(pair.a.state(client)).toBe(State.Established);
    expect(pair.a.drainOutbound()).toHaveLength(0);
  }
});

test("final permitted SYN ACK can still complete its handshake", () => {
  const pair = new Pair(retryConfig(3));
  pair.b.listen(443);
  const client = pair.a.connectFromWithIsn("b", 50_000, 443, 1000, 0);
  const syn = pair.a.drainOutbound()[0]!.bytes;
  let lastReply = new Uint8Array();
  for (let attempt = 0; attempt < 6; attempt += 1) {
    pair.b.input("a", syn, attempt * 10);
    const replies = pair.b.drainOutbound();
    expect(replies).toHaveLength(attempt < 3 ? 1 : 0);
    if (replies.length > 0) lastReply = Uint8Array.from(replies[0]!.bytes);
  }
  pair.a.input("b", lastReply, 1019);
  for (const packet of pair.a.drainOutbound()) pair.b.input("a", packet.bytes, 1019);
  const server = pair.b.accept(443);
  expect(server).toBeDefined();
  pair.b.poll(1020);
  expect(pair.a.state(client)).toBe(State.Established);
  expect(pair.b.state(server!)).toBe(State.Established);
});
