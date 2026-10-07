import { expect, test } from "vitest";
import { FlagSet, Flags, Segment } from "../src/index.js";
import { Pair } from "./pair.js";
import { windowUpdateVectors } from "./window-update-vectors.js";

test.each(windowUpdateVectors)("$name", (vector) => {
  const pair = new Pair({ mss: 16, receiveBuffer: 128 });
  pair.b.listen(443);
  const client = pair.a.connectFromWithIsn("b", 50_000, 443, vector.initialSequence, pair.now);
  pair.settle();
  const server = pair.b.accept(443)!;
  pair.a.write(client, new Uint8Array(16).fill(1), pair.now);
  pair.a.write(client, new Uint8Array(16).fill(2), pair.now);
  const flight = pair.a.drainOutbound();
  expect(flight).toHaveLength(2);
  pair.b.input("a", flight[0]!.bytes, pair.now);
  const ack = pair.b.drainOutbound()[0]!;
  let previous = Segment.decode(ack.bytes);
  pair.a.input("b", ack.bytes, pair.now);
  expect(pair.a.drainOutbound()).toHaveLength(0);
  let lastUpdate = new Uint8Array();
  for (const size of vector.readChunks) {
    expect(pair.b.read(server, size, pair.now)).toEqual(new Uint8Array(size).fill(1));
    const updates = pair.b.drainOutbound();
    expect(updates).toHaveLength(1);
    lastUpdate = Uint8Array.from(updates[0]!.bytes);
    const update = Segment.decode(lastUpdate);
    expect(update.ack).toBe(previous.ack);
    expect(update.window).toBeGreaterThan(previous.window);
    previous = update;
    pair.a.input("b", lastUpdate, pair.now);
    expect(pair.a.drainOutbound(), "window updates must not retransmit data").toHaveLength(0);
  }
  for (const flag of [Flags.Syn, Flags.Fin]) {
    const decoded = Segment.decode(lastUpdate);
    const control = new Segment({ ...decoded, ack: decoded.ack!, flags: new FlagSet(Flags.Ack | flag) });
    for (let i = 0; i < 3; i += 1) {
      pair.a.input("b", control.encode(), pair.now);
      expect(pair.a.drainOutbound().every((packet) => Segment.decode(packet.bytes).payload.length === 0)).toBe(true);
    }
  }
  for (let i = 0; i < 2; i += 1) {
    pair.a.input("b", lastUpdate, pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(0);
  }
  pair.a.input("b", lastUpdate, pair.now);
  const repair = pair.a.drainOutbound();
  expect(repair).toHaveLength(1);
  expect(Segment.decode(repair[0]!.bytes).seq).toBe(Segment.decode(flight[1]!.bytes).seq);
  expect(Segment.decode(repair[0]!.bytes).payload).toEqual(Segment.decode(flight[1]!.bytes).payload);
  pair.b.input("a", repair[0]!.bytes, pair.now);
  pair.settle();
  const remaining = 16 - vector.readChunks.reduce((a, b) => a + b, 0);
  expect(pair.b.read(server, 64, pair.now)).toEqual(new Uint8Array([
    ...new Uint8Array(remaining).fill(1), ...new Uint8Array(16).fill(2),
  ]));
});
