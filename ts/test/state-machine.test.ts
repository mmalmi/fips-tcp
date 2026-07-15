import { describe, expect, test } from "vitest";

import { Config, ConnectionId, Flags, Segment, Stack, State } from "../src/index.js";

type Transform = (fromA: boolean, bytes: Uint8Array) => Uint8Array[];

class Pair {
  readonly a: Stack;
  readonly b: Stack;
  now = 0;

  constructor(config: Partial<Config> = {}) {
    this.a = new Stack(config, 0x1111_2222_3333_4444n);
    this.b = new Stack(config, 0xaaaa_bbbb_cccc_ddddn);
  }

  stepWith(transform: Transform): number {
    this.a.poll(this.now);
    this.b.poll(this.now);
    const fromA = this.a.drainOutbound();
    const fromB = this.b.drainOutbound();
    let delivered = 0;
    for (const outbound of fromA) {
      expect(outbound.peer).toBe("b");
      for (const bytes of transform(true, outbound.bytes)) {
        this.b.input("a", bytes, this.now);
        delivered += 1;
      }
    }
    for (const outbound of fromB) {
      expect(outbound.peer).toBe("a");
      for (const bytes of transform(false, outbound.bytes)) {
        this.a.input("b", bytes, this.now);
        delivered += 1;
      }
    }
    return delivered;
  }

  settle(): void {
    for (let attempt = 0; attempt < 256; attempt += 1) {
      if (this.stepWith((_fromA, bytes) => [bytes]) === 0) return;
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
    expect(this.a.state(client)).toBe(State.Established);
    const server = this.b.accept(443);
    expect(server).toBeDefined();
    expect(this.b.state(server!)).toBe(State.Established);
    return [client, server!];
  }
}

describe("TCP/FIPS TypeScript state machine", () => {
  test("handshake, bidirectional stream, and orderly close", () => {
    const pair = new Pair();
    const [client, server] = pair.connect();

    expect(pair.a.write(client, new TextEncoder().encode("hello from ts"), pair.now)).toBe(13);
    expect(pair.b.write(server, new TextEncoder().encode("hello back"), pair.now)).toBe(10);
    pair.settle();
    expect(new TextDecoder().decode(pair.b.read(server, 1024, pair.now))).toBe("hello from ts");
    expect(new TextDecoder().decode(pair.a.read(client, 1024, pair.now))).toBe("hello back");

    pair.a.close(client, pair.now);
    pair.settle();
    expect(pair.b.state(server)).toBe(State.CloseWait);
    expect(pair.b.isReadClosed(server)).toBe(true);
    pair.b.close(server, pair.now);
    pair.settle();
    pair.advance(60_000);
    pair.settle();
    expect(pair.a.state(client)).toBeUndefined();
    expect(pair.b.state(server)).toBeUndefined();
  });

  test("per-peer limit counts active and TIME-WAIT state until expiry", () => {
    const pair = new Pair({ maxConnectionsPerPeer: 1, timeWaitMs: 50 });
    const [client, server] = pair.connect();
    expect(() => pair.a.connect("b", 443, pair.now)).toThrow(/connection limit/i);
    pair.a.close(client, pair.now);
    pair.settle();
    pair.b.close(server, pair.now);
    pair.settle();
    expect(pair.a.state(client)).toBe(State.TimeWait);
    expect(() => pair.a.connect("b", 443, pair.now)).toThrow(/connection limit/i);

    pair.advance(50);
    pair.settle();
    expect(() => pair.a.connect("b", 443, pair.now)).not.toThrow();
  });

  test("lost SYN and first payload recover via RTO", () => {
    const pair = new Pair();
    pair.b.listen(443);
    const client = pair.a.connect("b", 443, pair.now);
    let droppedSyn = false;
    pair.stepWith((fromA, bytes) => {
      if (fromA && !droppedSyn) {
        droppedSyn = true;
        return [];
      }
      return [bytes];
    });
    expect(pair.a.state(client)).toBe(State.SynSent);
    pair.advance(1000);
    pair.settle();
    const server = pair.b.accept(443)!;

    const payload = new Uint8Array(4096).fill(0x5a);
    expect(pair.a.write(client, payload, pair.now)).toBe(payload.length);
    let droppedData = false;
    pair.stepWith((fromA, bytes) => {
      if (fromA && Segment.decode(bytes).payload.length > 0 && !droppedData) {
        droppedData = true;
        return [];
      }
      return [bytes];
    });
    pair.settle();
    const received = [...pair.b.read(server, payload.length, pair.now)];
    pair.advance(1000);
    pair.settle();
    while (received.length < payload.length) {
      received.push(...pair.b.read(server, payload.length, pair.now));
      if (received.length < payload.length) {
        pair.advance(1000);
        pair.settle();
      }
    }
    expect(Uint8Array.from(received)).toEqual(payload);
  });

  test("reverse-order duplicates reassemble exactly once", () => {
    const pair = new Pair({ mss: 256 });
    const [client, server] = pair.connect();
    const payload = Uint8Array.from({ length: 2048 }, (_, index) => index % 251);
    pair.a.write(client, payload, pair.now);
    pair.a.poll(pair.now);
    for (const outbound of pair.a.drainOutbound().reverse()) {
      pair.b.input("a", outbound.bytes, pair.now);
      pair.b.input("a", outbound.bytes, pair.now);
    }
    pair.settle();
    expect(pair.b.read(server, payload.length, pair.now)).toEqual(payload);
  });

  test("receive window reopens after application reads", () => {
    const pair = new Pair({ mss: 8, receiveBuffer: 16 });
    const [client, server] = pair.connect();
    const payload = Uint8Array.from({ length: 64 }, (_, index) => index);
    pair.a.write(client, payload, pair.now);
    pair.settle();
    const received = [...pair.b.read(server, 16, pair.now)];
    for (let attempt = 0; received.length < payload.length && attempt < 16; attempt += 1) {
      pair.settle();
      received.push(...pair.b.read(server, 16, pair.now));
    }
    expect(Uint8Array.from(received)).toEqual(payload);
  });

  test("byte sequence wraparound remains ordered", () => {
    const pair = new Pair();
    pair.b.listen(443);
    const client = pair.a.connectFromWithIsn("b", 50_000, 443, 0xffff_ffff - 8, pair.now);
    pair.settle();
    const server = pair.b.accept(443)!;
    const payload = new TextEncoder().encode("crosses the sequence wrap");
    pair.a.write(client, payload, pair.now);
    pair.settle();
    expect(pair.b.read(server, 1024, pair.now)).toEqual(payload);
  });

  test("close waits for flow-controlled bytes before FIN", () => {
    const pair = new Pair({ mss: 8, receiveBuffer: 16 });
    const [client, server] = pair.connect();
    const payload = Uint8Array.from({ length: 64 }, (_, index) => index);
    pair.a.write(client, payload, pair.now);
    pair.a.close(client, pair.now);
    pair.settle();

    const received: number[] = [];
    for (let attempt = 0; attempt < 8; attempt += 1) {
      received.push(...pair.b.read(server, 16, pair.now));
      pair.settle();
      if (pair.b.isReadClosed(server)) break;
    }
    expect(Uint8Array.from(received)).toEqual(payload);
    expect(pair.b.isReadClosed(server)).toBe(true);
    expect(pair.b.state(server)).toBe(State.CloseWait);
  });

  test("zero-window probe recovers a lost window update", () => {
    const pair = new Pair({ mss: 8, receiveBuffer: 16 });
    const [client, server] = pair.connect();
    const payload = Uint8Array.from({ length: 64 }, (_, index) => index);
    pair.a.write(client, payload, pair.now);
    pair.settle();

    const received = [...pair.b.read(server, 16, pair.now)];
    let droppedUpdate = false;
    pair.stepWith((fromA, bytes) => {
      if (!fromA && !droppedUpdate) {
        droppedUpdate = true;
        return [];
      }
      return [bytes];
    });
    expect(droppedUpdate).toBe(true);
    pair.advance(1000);
    pair.settle();
    for (let attempt = 0; received.length < payload.length && attempt < 8; attempt += 1) {
      received.push(...pair.b.read(server, 16, pair.now));
      pair.settle();
    }
    expect(Uint8Array.from(received)).toEqual(payload);
  });

  test("closed-port RST and retry limit remove connections", () => {
    const pair = new Pair({ initialRtoMs: 200, maxRetransmissions: 1 });
    const reset = pair.a.connect("b", 443, pair.now);
    pair.settle();
    expect(pair.a.state(reset)).toBeUndefined();

    const timedOut = pair.a.connect("b", 444, pair.now);
    pair.a.drainOutbound();
    pair.advance(200);
    pair.a.poll(pair.now);
    expect(pair.a.state(timedOut)).toBeUndefined();
  });

  test("triple duplicate ACK fast-retransmits without waiting for RTO", () => {
    const pair = new Pair({ mss: 128 });
    const [client, server] = pair.connect();
    pair.a.write(client, new Uint8Array(2048).fill(0x11), pair.now);
    pair.settle();
    pair.b.read(server, 4096, pair.now);
    pair.settle();

    const payload = Uint8Array.from({ length: 2048 }, (_, index) => index % 251);
    pair.a.write(client, payload, pair.now);
    const packets = pair.a.drainOutbound();
    expect(packets.length).toBeGreaterThanOrEqual(4);
    const first = Segment.decode(packets[0]!.bytes);
    for (const outbound of packets.slice(1)) pair.b.input("a", outbound.bytes, pair.now);
    for (const outbound of pair.b.drainOutbound()) pair.a.input("b", outbound.bytes, pair.now);
    const retransmits = pair.a.drainOutbound();
    expect(
      retransmits.some((outbound) => {
        const segment = Segment.decode(outbound.bytes);
        return segment.seq === first.seq && toHex(segment.payload) === toHex(first.payload);
      }),
    ).toBe(true);
    for (const outbound of retransmits) pair.b.input("a", outbound.bytes, pair.now);
    pair.settle();
    expect(pair.b.read(server, payload.length, pair.now)).toEqual(payload);
  });

  test("lost FIN is retransmitted", () => {
    const pair = new Pair();
    const [client, server] = pair.connect();
    pair.a.close(client, pair.now);
    let dropped = false;
    pair.stepWith((fromA, bytes) => {
      if (fromA && Segment.decode(bytes).flags.has(Flags.Fin) && !dropped) {
        dropped = true;
        return [];
      }
      return [bytes];
    });
    expect(dropped).toBe(true);
    expect(pair.b.state(server)).toBe(State.Established);
    pair.advance(1000);
    pair.settle();
    expect(pair.b.state(server)).toBe(State.CloseWait);
  });
});

const toHex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");
