import { describe, expect, test } from "vitest";
import { FlagSet, Flags, MarkerStatus, Segment } from "../src/index.js";
import { Pair } from "./pair.js";
import { recoveryVectors } from "./recovery-vectors.js";

describe("timed-out flight recovery", () => {
  test.each([0, 0xffff])("a partial ACK cannot exceed the retry bound with window %i", (window) => {
    const pair = new Pair({ maxRetransmissions: 2 });
    const [client] = pair.connect();
    pair.a.write(client, new TextEncoder().encode("first"), pair.now);
    pair.a.write(client, new TextEncoder().encode("second"), pair.now);
    const first = Segment.decode(pair.a.drainOutbound()[0]!.bytes);
    pair.advance(200);
    pair.a.poll(pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(1);
    const partial = new Segment({ srcPort: first.dstPort, dstPort: first.srcPort,
      seq: 0, ack: (first.seq + 1) >>> 0, flags: new FlagSet(Flags.Ack), window });
    pair.a.input("b", partial.encode(), pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(0);
  });

  test("non-advancing ACKs and newly closed windows cannot drive repair", () => {
    const pair = new Pair();
    const [client, server] = pair.connect();
    pair.a.write(client, new TextEncoder().encode("first"), pair.now);
    pair.a.write(client, new TextEncoder().encode("second"), pair.now);
    const first = Segment.decode(pair.a.drainOutbound()[0]!.bytes);
    pair.advance(200);
    pair.a.poll(pair.now);
    const repair = pair.a.drainOutbound();
    expect(repair).toHaveLength(1);
    for (const ack of [first.seq, (first.seq + 100_000) >>> 0]) {
      pair.a.input("b", new Segment({ srcPort: first.dstPort, dstPort: first.srcPort,
        seq: 0, ack, flags: new FlagSet(Flags.Ack) }).encode(), pair.now);
      expect(pair.a.drainOutbound()).toHaveLength(0);
    }
    pair.b.input("a", repair[0]!.bytes, pair.now);
    const acknowledgment = Segment.decode(pair.b.drainOutbound()[0]!.bytes);
    pair.a.input("b", new Segment({ ...acknowledgment, ack: acknowledgment.ack!, window: 0 }).encode(), pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(0);
    expect(pair.b.read(server, 5, pair.now)).toEqual(new TextEncoder().encode("first"));
    for (const reopened of pair.b.drainOutbound()) pair.a.input("b", reopened.bytes, pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(0);
    pair.advance(400);
    pair.settle();
    expect(pair.b.read(server, 6, pair.now)).toEqual(new TextEncoder().encode("second"));
  });

  test.each(recoveryVectors)("$name", (vector) => {
    const pair = new Pair();
    pair.b.listen(443);
    const client = pair.a.connectFromWithIsn("b", 50_000, 443, vector.initialSequence, pair.now);
    pair.settle();
    const server = pair.b.accept(443)!;
    const warmup = new Uint8Array(vector.warmupBytes).fill(0x31);
    expect(pair.a.write(client, warmup, pair.now)).toBe(warmup.length);
    pair.settle();
    expect(pair.b.read(server, warmup.length, pair.now)).toEqual(warmup);
    pair.settle();

    const payload = new Uint8Array(vector.expectedPayloadBytes).fill(0x73);
    const markers = [];
    for (let index = 0; index < vector.chunkCount; index += 1) {
      const write = pair.a.writeWithMarker(client,
        payload.subarray(index * vector.chunkBytes, (index + 1) * vector.chunkBytes), pair.now);
      expect(write.accepted).toBe(vector.chunkBytes);
      markers.push(write.marker);
    }
    const lost = pair.a.drainOutbound().map((packet) => Segment.decode(packet.bytes));
    expect(lost).toHaveLength(vector.chunkCount);
    expect(lost.every((segment) => segment.payload.length === vector.chunkBytes)).toBe(true);
    expect(lost[0]!.seq).toBeGreaterThan(lost.at(-1)!.seq);
    for (const delta of vector.droppedPollDeltasMs) {
      pair.advance(delta);
      pair.a.poll(pair.now);
      expect(pair.a.drainOutbound()).toHaveLength(1);
    }
    expect(pair.b.read(server, payload.length, pair.now)).toHaveLength(0);
    expect(pair.a.markerStatus(markers.at(-1)!)).toBe(MarkerStatus.Pending);

    pair.advance(vector.recoveryPollDeltaMs);
    const recoveryTime = pair.now;
    pair.settle(vector.maxRecoveryPumpSteps);
    expect(pair.b.read(server, payload.length, pair.now)).toEqual(payload);
    expect(markers.every((marker) => pair.a.markerStatus(marker) === MarkerStatus.Acked)).toBe(true);
    expect(pair.now).toBe(recoveryTime);

    // A normal ACK in a subsequent flight must not spuriously repair its next segment.
    pair.settle();
    expect(pair.a.write(client, new Uint8Array([1, 2, 3]), pair.now)).toBe(3);
    expect(pair.a.write(client, new Uint8Array([4, 5, 6]), pair.now)).toBe(3);
    const nextFlight = pair.a.drainOutbound();
    expect(nextFlight).toHaveLength(2);
    pair.b.input("a", nextFlight[0]!.bytes, pair.now);
    for (const ack of pair.b.drainOutbound()) pair.a.input("b", ack.bytes, pair.now);
    expect(pair.a.drainOutbound()).toHaveLength(0);
    pair.b.input("a", nextFlight[1]!.bytes, pair.now);
    pair.settle();
    expect(pair.b.read(server, 6, pair.now)).toEqual(new Uint8Array([1, 2, 3, 4, 5, 6]));
    pair.settle();
    expect(pair.stepWith((_fromA, bytes) => [bytes])).toBe(0);
  });
});
