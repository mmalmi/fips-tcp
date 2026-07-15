import { expect, test } from "vitest";

import {
  Config,
  ConnectionId,
  FlagSet,
  Flags,
  MarkerStatus,
  Segment,
  Stack,
  State,
} from "../src/index.js";

class Pair {
  readonly a: Stack;
  readonly b: Stack;
  now = 0;

  constructor(config: Partial<Config> = {}) {
    this.a = new Stack(config, 0x1111_2222_3333_4444n);
    this.b = new Stack(config, 0xaaaa_bbbb_cccc_ddddn);
  }

  settle(): void {
    for (let attempt = 0; attempt < 256; attempt += 1) {
      this.a.poll(this.now);
      this.b.poll(this.now);
      const fromA = this.a.drainOutbound();
      const fromB = this.b.drainOutbound();
      for (const outbound of fromA) this.b.input("a", outbound.bytes, this.now);
      for (const outbound of fromB) this.a.input("b", outbound.bytes, this.now);
      if (fromA.length + fromB.length === 0) return;
    }
    throw new Error("pair did not settle");
  }

  advance(milliseconds: number): void {
    this.now += milliseconds;
  }

  connect(): [ConnectionId, ConnectionId] {
    this.b.listen(443);
    const client = this.a.connect("b", 443, this.now);
    this.settle();
    const server = this.b.accept(443)!;
    expect(this.a.state(client)).toBe(State.Established);
    expect(this.b.state(server)).toBe(State.Established);
    return [client, server];
  }
}

test("marker waits for zero-window buffered payload and zero-length is a barrier", () => {
  const pair = new Pair({ mss: 8, receiveBuffer: 8 });
  const [client, server] = pair.connect();
  pair.a.write(client, new Uint8Array(8).fill(0x11), pair.now);
  pair.settle();

  const { accepted, marker } = pair.a.writeWithMarker(
    client, new Uint8Array(8).fill(0x22), pair.now,
  );
  expect(accepted).toBe(8);
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Pending);
  expect(pair.a.drainOutbound()).toHaveLength(0);
  const barrier = pair.a.writeWithMarker(client, new Uint8Array(), pair.now);
  expect(barrier.accepted).toBe(0);
  expect(pair.a.markerStatus(barrier.marker)).toBe(MarkerStatus.Pending);

  expect(pair.b.read(server, 8, pair.now)).toEqual(new Uint8Array(8).fill(0x11));
  pair.settle();
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Acked);
  expect(pair.a.markerStatus(barrier.marker)).toBe(MarkerStatus.Acked);
  const emptyAfterAck = pair.a.writeWithMarker(client, new Uint8Array(), pair.now);
  expect(emptyAfterAck.accepted).toBe(0);
  expect(pair.a.markerStatus(emptyAfterAck.marker)).toBe(MarkerStatus.Acked);
});

test("marker tracks partial wraparound ACKs without retransmit or other-stream noise", () => {
  const pair = new Pair({ mss: 8, initialRtoMs: 200 });
  pair.b.listen(443);
  const initial = 0xffff_ffff - 8;
  const client = pair.a.connectFromWithIsn("b", 50_000, 443, initial, pair.now);
  pair.settle();
  pair.b.accept(443);
  const other = pair.a.connectFromWithIsn("b", 50_001, 443, 100, pair.now);
  pair.settle();
  pair.b.accept(443);

  const { accepted, marker } = pair.a.writeWithMarker(
    client, new Uint8Array(12).fill(0x33), pair.now,
  );
  expect(accepted).toBe(12);
  pair.a.drainOutbound();
  for (let byte = 0; byte < 4; byte += 1) {
    pair.a.write(other, new Uint8Array(4).fill(byte), pair.now);
    pair.settle();
    expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Pending);
  }

  pair.advance(200);
  pair.a.poll(pair.now);
  expect(pair.a.drainOutbound().length).toBeGreaterThan(0);
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Pending);
  const payloadStart = (initial + 1) >>> 0;
  const partial = ack(443, 50_000, (payloadStart + 5) >>> 0);
  pair.a.input("b", partial, pair.now);
  for (let duplicate = 0; duplicate < 3; duplicate += 1) pair.a.input("b", partial, pair.now);
  pair.a.drainOutbound();
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Pending);

  pair.a.input("b", ack(443, 50_000, (payloadStart + 12) >>> 0), pair.now);
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Acked);
  const later = pair.a.writeWithMarker(client, new Uint8Array(4).fill(0x44), pair.now);
  pair.a.drainOutbound();
  expect(pair.a.markerStatus(marker)).toBe(MarkerStatus.Acked);
  expect(pair.a.markerStatus(later.marker)).toBe(MarkerStatus.Pending);
  pair.a.input("b", ack(443, 50_000, (payloadStart + 16) >>> 0), pair.now);
  expect(pair.a.markerStatus(later.marker)).toBe(MarkerStatus.Acked);
});

test("marker is gone after abort and tuple reuse cannot revive it", () => {
  const pair = new Pair();
  pair.b.listen(443);
  const oldClient = pair.a.connectFromWithIsn("b", 50_000, 443, 100, pair.now);
  pair.settle();
  const oldServer = pair.b.accept(443)!;
  const oldMarker = pair.a.writeWithMarker(oldClient, new Uint8Array(4).fill(0x55), pair.now).marker;
  expect(pair.b.markerStatus(oldMarker)).toBe(MarkerStatus.ConnectionGone);
  pair.a.abort(oldClient);
  pair.a.drainOutbound();
  pair.b.abort(oldServer);
  pair.b.drainOutbound();
  expect(pair.a.markerStatus(oldMarker)).toBe(MarkerStatus.ConnectionGone);

  const newClient = pair.a.connectFromWithIsn("b", 50_000, 443, 1_000_000, pair.now);
  pair.settle();
  pair.b.accept(443);
  const newMarker = pair.a.writeWithMarker(
    newClient, new Uint8Array(16).fill(0x66), pair.now,
  ).marker;
  pair.settle();
  expect(pair.a.markerStatus(newMarker)).toBe(MarkerStatus.Acked);
  expect(pair.a.markerStatus(oldMarker)).toBe(MarkerStatus.ConnectionGone);
});

const ack = (sourcePort: number, destinationPort: number, acknowledgment: number): Uint8Array =>
  new Segment({
    srcPort: sourcePort,
    dstPort: destinationPort,
    seq: 0,
    ack: acknowledgment,
    flags: new FlagSet(Flags.Ack),
  }).encode();
