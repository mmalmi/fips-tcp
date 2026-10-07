import { afterEach, describe, expect, test } from "vitest";

import { ConnectionId, FIPS_VERSION, FlagSet, Flags, MarkerStatus, Segment,
  Stack, State, TcpOptionKind } from "../src/index.js";
import { recoveryVectors } from "./recovery-vectors.js";
import { windowUpdateVectors } from "./window-update-vectors.js";

import { CrossPair, fromHex, toHex, type NetworkTransform } from "./retry-bound-pair.js";

interface DriverMarkerWrite {
  accepted: number;
  marker: number;
}

const dropFirstAndDuplicateReverse = (): NetworkTransform => {
  let droppedTs = false;
  let droppedRust = false;
  const mutate = (packets: Uint8Array[], direction: "ts" | "rust"): Uint8Array[] => {
    const output: Uint8Array[] = [];
    for (const bytes of packets) {
      const data = Segment.decode(bytes).payload.length > 0;
      if (data && direction === "ts" && !droppedTs) {
        droppedTs = true;
        continue;
      }
      if (data && direction === "rust" && !droppedRust) {
        droppedRust = true;
        continue;
      }
      output.push(bytes, bytes.slice());
    }
    return output.reverse();
  };
  return (fromTs, fromRust) => [mutate(fromTs, "ts"), mutate(fromRust, "rust")];
};

const pairs: CrossPair[] = [];
afterEach(async () => {
  await Promise.all(pairs.splice(0).map((pair) => pair.rust.close()));
});

describe("live Rust/TypeScript TCP/FIPS interoperability", () => {
  test.each(["TypeScript", "Rust"])("%s reconnects the same tuple after its reset is lost", async (initiator) => {
    const pair = new CrossPair();
    pairs.push(pair);
    const tsClient = initiator === "TypeScript";
    const connect = async (): Promise<number> => tsClient
      ? pair.ts.connect("rust", 443, pair.now)
      : Number(await pair.rustCommand({ op: "autoConnect", peer: "ts", remotePort: 443, now: pair.now }));
    const accept = async (): Promise<number> => tsClient
      ? Number(await pair.rustCommand({ op: "accept", port: 443 }))
      : pair.ts.accept(443)!;
    const state = async (id: number): Promise<unknown> => tsClient
      ? pair.rustCommand({ op: "state", id }) : pair.ts.state(id);
    if (tsClient) await pair.rustCommand({ op: "listen", port: 443 });
    else pair.ts.listen(443);
    await connect();
    await pair.settle();
    const retained = await accept();
    expect(await state(retained)).toBe(tsClient ? "established" : State.Established);

    // Recreate only the client; the peer never sees its previous reset.
    pair.advance(1);
    if (tsClient) pair.ts = new Stack({}, 2n);
    else await pair.rustCommand({ op: "configure", maxConnections: 8, maxConnectionsPerPeer: 4, isnSeed: 2 });
    const fresh = await connect();
    await pair.settle();
    expect(await state(retained)).toBe(tsClient ? null : undefined);
    pair.advance(pair.ts.config.initialRtoMs);
    await pair.settle();
    const accepted = await accept();
    expect(accepted).not.toBe(retained);
    expect(await state(accepted)).toBe(tsClient ? "established" : State.Established);
    expect(pair.now).toBeLessThanOrEqual(2001);
    const request = Buffer.from("new request");
    const response = Buffer.from("new response");
    const rustClient = tsClient ? accepted : fresh;
    const tsId = tsClient ? fresh : accepted;
    pair.ts.write(tsId, tsClient ? request : response, pair.now);
    await pair.rustCommand({ op: "write", id: rustClient, bytes: toHex(tsClient ? response : request), now: pair.now });
    await pair.settle();
    expect(pair.ts.read(tsId, 64, pair.now)).toEqual(Uint8Array.from(tsClient ? response : request));
    expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustClient, max: 64, now: pair.now }))))
      .toEqual(Uint8Array.from(tsClient ? request : response));
  }, 30_000);

  test.each(["TypeScript", "Rust"])("%s establishes a reserved stream during a half-open flood", async (initiator) => {
    const pair = new CrossPair({ maxConnections: 4, maxConnectionsPerPeer: 2 });
    pairs.push(pair);
    await pair.rustCommand({ op: "configure", maxConnections: 4, maxConnectionsPerPeer: 2 });
    pair.ts.listen(443);
    await pair.rustCommand({ op: "listen", port: 443 });
    pair.ts.setConnectionReservation(2, (peer) => peer === "rust");
    await pair.rustCommand({ op: "reserve", slots: 2, eligible: ["ts"] });
    for (let n = 0; n < 2; n += 1) {
      const peer = `attacker-${n}`;
      const bytes = admissionSyn(51_000 + n);
      pair.ts.input(peer, bytes, pair.now);
      expect(pair.ts.drainOutbound()).toHaveLength(1);
      const response = await pair.rust.command({ op: "input", peer, bytes: toHex(bytes), now: pair.now });
      expect(response.outbound).toHaveLength(1);
      // Retain both SYN-RECEIVED tuples by withholding their final ACKs.
    }
    expect(() => pair.ts.input("extra", admissionSyn(51_002), pair.now)).toThrow(/connection limit/i);
    await expect(pair.rust.command({ op: "input", peer: "extra",
      bytes: toHex(admissionSyn(51_002)), now: pair.now })).rejects.toThrow(/connection limit/i);
    let tsId: number;
    let rustId: number;
    if (initiator === "TypeScript") {
      tsId = pair.ts.connectFromWithIsn("rust", 50_000, 443, 1234, pair.now);
      await pair.settle();
      rustId = Number(await pair.rustCommand({ op: "accept", port: 443 }));
    } else {
      rustId = Number(await pair.rustCommand({ op: "connect", peer: "ts",
        localPort: 50_000, remotePort: 443, isn: 1234, now: pair.now }));
      await pair.settle();
      tsId = pair.ts.accept(443)!;
    }
    expect(pair.ts.state(tsId)).toBe(State.Established);
    expect(await pair.rustCommand({ op: "state", id: rustId })).toBe("established");
    // Withdrawal prevents new reserved allocations without aborting this stream.
    pair.ts.setConnectionReservation(2, () => false);
    await pair.rustCommand({ op: "reserve", slots: 2, eligible: [] });
    const payload = new TextEncoder().encode("payment-sized application record");
    expect(pair.ts.write(tsId, payload, pair.now)).toBe(payload.length);
    expect(await pair.rustCommand({ op: "write", id: rustId, bytes: toHex(payload), now: pair.now })).toBe(payload.length);
    await pair.settle();
    expect(pair.ts.read(tsId, payload.length, pair.now)).toEqual(payload);
    expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustId,
      max: payload.length, now: pair.now })))).toEqual(payload);
    pair.ts.close(tsId, pair.now);
    await pair.settle();
    await pair.rustCommand({ op: "close", id: rustId, now: pair.now });
    await pair.settle();
    expect(pair.ts.state(tsId)).toBe(State.TimeWait);
    expect(await pair.rustCommand({ op: "state", id: rustId })).toBeNull();
  }, 30_000);

  test.each(windowUpdateVectors)("bidirectional window updates: $name", async (vector) => {
    const pair = new CrossPair();
    pairs.push(pair);
    await pair.rustCommand({ op: "listen", port: 443 });
    const tsId = pair.ts.connectFromWithIsn("rust", 50_000, 443, vector.initialSequence, pair.now);
    await pair.settle();
    const rustId = Number(await pair.rustCommand({ op: "accept", port: 443 }));
    for (const value of [1, 2]) {
      const payload = new Uint8Array(16).fill(value);
      pair.ts.write(tsId, payload, pair.now);
      await pair.rustCommand({ op: "write", id: rustId, bytes: toHex(payload), now: pair.now });
    }
    // Withhold one data segment in each direction, retaining outstanding data
    // while both applications independently drain their receive buffers.
    await pair.step((left, right) => {
      expect(left).toHaveLength(2);
      expect(right).toHaveLength(2);
      return [left.slice(0, 1), right.slice(0, 1)];
    });
    let leftUpdate = new Uint8Array();
    let rightUpdate = new Uint8Array();
    await pair.step((left, right) => {
      expect(left).toHaveLength(1);
      expect(right).toHaveLength(1);
      leftUpdate = Uint8Array.from(left[0]!);
      rightUpdate = Uint8Array.from(right[0]!);
      return [left, right];
    });
    expect(await pair.step()).toBe(0);
    for (const size of vector.readChunks) {
      for (let i = 0; i < (vector.duplicateAcksBeforeRead ?? 0); i += 1) {
        pair.ts.input("rust", rightUpdate, pair.now);
        await pair.rustCommand({ op: "input", peer: "ts", bytes: toHex(leftUpdate), now: pair.now });
        expect(await pair.step()).toBe(0);
      }
      expect(pair.ts.read(tsId, size, pair.now)).toEqual(new Uint8Array(size).fill(1));
      expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustId,
        max: size, now: pair.now })))).toEqual(new Uint8Array(size).fill(1));
      await pair.step((left, right) => {
        expect(left).toHaveLength(1);
        expect(right).toHaveLength(1);
        leftUpdate = Uint8Array.from(left[0]!);
        rightUpdate = Uint8Array.from(right[0]!);
        return [left, right];
      });
      expect(await pair.step()).toBe(0);
    }
    // True duplicate ACKs still recover the withheld data immediately in both
    // directions, without a timeout or changing the advertised window.
    for (let i = 0; i < 3; i += 1) {
      pair.ts.input("rust", rightUpdate, pair.now);
      await pair.rustCommand({ op: "input", peer: "ts", bytes: toHex(leftUpdate), now: pair.now });
    }
    await pair.settle();
    const remaining = 16 - vector.readChunks.reduce((a, b) => a + b, 0);
    const expected = new Uint8Array([...new Uint8Array(remaining).fill(1), ...new Uint8Array(16).fill(2)]);
    expect(pair.ts.read(tsId, 64, pair.now)).toEqual(expected);
    expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustId,
      max: 64, now: pair.now })))).toEqual(expected);
  }, 30_000);

  test.each(["TypeScript", "Rust"])("%s initiates a bidirectional shared outage recovery vector", async (initiator) => {
    for (const vector of recoveryVectors) {
      const pair = new CrossPair();
      pairs.push(pair);
      let tsId: ConnectionId;
      let rustId: number;
      if (initiator === "TypeScript") {
        await pair.rustCommand({ op: "listen", port: 443 });
        tsId = pair.ts.connectFromWithIsn("rust", 50_000, 443, vector.initialSequence, pair.now);
        await pair.settle();
        rustId = Number(await pair.rustCommand({ op: "accept", port: 443 }));
      } else {
        pair.ts.listen(443);
        rustId = Number(await pair.rustCommand({ op: "connect", peer: "ts", localPort: 50_000,
          remotePort: 443, isn: vector.initialSequence, now: pair.now }));
        await pair.settle();
        tsId = pair.ts.accept(443)!;
      }
      const warmup = new Uint8Array(vector.warmupBytes).fill(0x31);
      expect(pair.ts.write(tsId, warmup, pair.now)).toBe(warmup.length);
      expect(await pair.rustCommand({ op: "write", id: rustId, bytes: toHex(warmup), now: pair.now })).toBe(warmup.length);
      await pair.settle();
      expect(pair.ts.read(tsId, warmup.length, pair.now)).toEqual(warmup);
      expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustId,
        max: warmup.length, now: pair.now })))).toEqual(warmup);
      await pair.settle();

      const toRust = new Uint8Array(vector.expectedPayloadBytes).fill(0x73);
      const toTs = new Uint8Array(vector.expectedPayloadBytes).fill(0xa7);
      const tsMarkers = [];
      const rustMarkers = [];
      for (let index = 0; index < vector.chunkCount; index += 1) {
        const start = index * vector.chunkBytes;
        const tsWrite = pair.ts.writeWithMarker(tsId, toRust.subarray(start, start + vector.chunkBytes), pair.now);
        const rustWrite = await pair.rustCommand({ op: "writeWithMarker", id: rustId,
          bytes: toHex(toTs.subarray(start, start + vector.chunkBytes)), now: pair.now }) as DriverMarkerWrite;
        expect(tsWrite.accepted).toBe(vector.chunkBytes);
        expect(rustWrite.accepted).toBe(vector.chunkBytes);
        tsMarkers.push(tsWrite.marker);
        rustMarkers.push(rustWrite.marker);
      }
      await pair.step((fromTs, fromRust) => {
        for (const packets of [fromTs, fromRust]) {
          expect(packets).toHaveLength(vector.chunkCount);
          expect(packets.every((bytes) => Segment.decode(bytes).payload.length === vector.chunkBytes)).toBe(true);
        }
        const wrapped = (initiator === "TypeScript" ? fromTs : fromRust).map((bytes) => Segment.decode(bytes));
        expect(wrapped[0]!.seq).toBeGreaterThan(wrapped.at(-1)!.seq);
        return [[], []];
      });
      for (const delta of vector.droppedPollDeltasMs) {
        pair.advance(delta);
        await pair.step((fromTs, fromRust) => {
          expect(fromTs).toHaveLength(1);
          expect(fromRust).toHaveLength(1);
          return [[], []];
        });
      }
      pair.advance(vector.recoveryPollDeltaMs);
      const restoredTime = pair.now;
      await pair.settle(vector.maxRecoveryPumpSteps);
      expect(pair.ts.read(tsId, toTs.length, pair.now)).toEqual(toTs);
      expect(fromHex(String(await pair.rustCommand({ op: "read", id: rustId,
        max: toRust.length, now: pair.now })))).toEqual(toRust);
      for (const marker of tsMarkers) expect(pair.ts.markerStatus(marker)).toBe(MarkerStatus.Acked);
      for (const marker of rustMarkers) {
        expect(await pair.rustCommand({ op: "markerStatus", marker })).toBe("acked");
      }
      expect(pair.now).toBe(restoredTime);
      await pair.settle();
      expect(await pair.step()).toBe(0);
    }
  }, 30_000);

  test("send markers cross the hostile Rust/TypeScript wire schedule exactly", async () => {
    const pair = new CrossPair();
    pairs.push(pair);
    await pair.rustCommand({ op: "listen", port: 443 });
    const client = pair.ts.connect("rust", 443, pair.now);
    await pair.settle();
    const server = Number(await pair.rustCommand({ op: "accept", port: 443 }));

    const toRust = new Uint8Array(2048).fill(0x5a);
    const toTs = new Uint8Array(2048).fill(0xa5);
    const tsWrite = pair.ts.writeWithMarker(client, toRust, pair.now);
    const rustWrite = await pair.rustCommand({
      op: "writeWithMarker",
      id: server,
      bytes: toHex(toTs),
      now: pair.now,
    }) as DriverMarkerWrite;
    expect(tsWrite.accepted).toBe(toRust.length);
    expect(rustWrite.accepted).toBe(toTs.length);
    expect(pair.ts.markerStatus(tsWrite.marker)).toBe(MarkerStatus.Pending);
    expect(await pair.rustCommand({ op: "markerStatus", marker: rustWrite.marker })).toBe("pending");

    await pair.step(dropFirstAndDuplicateReverse());
    await pair.settle();
    expect(pair.ts.markerStatus(tsWrite.marker)).toBe(MarkerStatus.Pending);
    expect(await pair.rustCommand({ op: "markerStatus", marker: rustWrite.marker })).toBe("pending");
    pair.advance(2000);
    await pair.settle();
    expect(pair.ts.markerStatus(tsWrite.marker)).toBe(MarkerStatus.Acked);
    expect(await pair.rustCommand({ op: "markerStatus", marker: rustWrite.marker })).toBe("acked");
  }, 30_000);

  test("TypeScript client and Rust server survive loss, reversal, and duplication", async () => {
    const pair = new CrossPair();
    pairs.push(pair);
    await pair.rustCommand({ op: "listen", port: 443 });
    const client = pair.ts.connect("rust", 443, pair.now);

    let lostSyn = false;
    await pair.step((fromTs, fromRust) => {
      const kept = lostSyn ? fromTs : fromTs.slice(1);
      lostSyn ||= fromTs.length > 0;
      return [kept, fromRust];
    });
    expect(pair.ts.state(client)).toBe(State.SynSent);
    pair.advance(2000);
    await pair.settle();
    const server = Number(await pair.rustCommand({ op: "accept", port: 443 }));
    expect(await pair.rustCommand({ op: "state", id: server })).toBe("established");

    const toRust = Uint8Array.from({ length: 6144 }, (_, index) => index % 251);
    const toTs = Uint8Array.from({ length: 5120 }, (_, index) => 255 - (index % 251));
    expect(pair.ts.write(client, toRust, pair.now)).toBe(toRust.length);
    expect(
      await pair.rustCommand({ op: "write", id: server, bytes: toHex(toTs), now: pair.now }),
    ).toBe(toTs.length);
    await pair.step(dropFirstAndDuplicateReverse());
    await pair.settle();
    pair.advance(2000);
    await pair.settle();

    expect(
      fromHex(String(await pair.rustCommand({ op: "read", id: server, max: 10_000, now: pair.now }))),
    ).toEqual(toRust);
    expect(pair.ts.read(client, 10_000, pair.now)).toEqual(toTs);

    pair.ts.close(client, pair.now);
    await pair.settle();
    expect(await pair.rustCommand({ op: "state", id: server })).toBe("close-wait");
    await pair.rustCommand({ op: "close", id: server, now: pair.now });
    await pair.settle();
    pair.advance(60_000);
    await pair.settle();
    expect(pair.ts.state(client)).toBeUndefined();
    expect(await pair.rustCommand({ op: "state", id: server })).toBeNull();
  }, 30_000);

  test("Rust client and TypeScript server survive the same hostile schedule", async () => {
    const pair = new CrossPair();
    pairs.push(pair);
    pair.ts.listen(443);
    const client = Number(
      await pair.rustCommand({
        op: "connect",
        peer: "ts",
        localPort: 50_000,
        remotePort: 443,
        isn: 0xffff_fff8,
        now: pair.now,
      }),
    );

    await pair.step((fromTs, fromRust) => [fromTs, fromRust.slice(1)]);
    expect(await pair.rustCommand({ op: "state", id: client })).toBe("syn-sent");
    pair.advance(2000);
    await pair.settle();
    const server = pair.ts.accept(443)!;

    const toTs = Uint8Array.from({ length: 4096 }, (_, index) => index % 239);
    const toRust = Uint8Array.from({ length: 3072 }, (_, index) => index % 197);
    expect(
      await pair.rustCommand({ op: "write", id: client, bytes: toHex(toTs), now: pair.now }),
    ).toBe(toTs.length);
    expect(pair.ts.write(server, toRust, pair.now)).toBe(toRust.length);
    await pair.step(dropFirstAndDuplicateReverse());
    await pair.settle();
    pair.advance(2000);
    await pair.settle();

    expect(pair.ts.read(server, 10_000, pair.now)).toEqual(toTs);
    expect(
      fromHex(String(await pair.rustCommand({ op: "read", id: client, max: 10_000, now: pair.now }))),
    ).toEqual(toRust);
  }, 30_000);
});

function admissionSyn(port: number): Uint8Array {
  return new Segment({ srcPort: port, dstPort: 443, seq: port, flags: new FlagSet(Flags.Syn),
    options: [{ kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION, reserved: 0 }],
  }).encode();
}
