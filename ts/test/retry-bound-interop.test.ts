import { afterEach, expect, test } from "vitest";
import { Segment, State } from "../src/index.js";
import { CrossPair, fromHex, toHex } from "./retry-bound-pair.js";
import { afterInterruption, interruptDuplicates, retryBoundVectors, retryConfig, reverseAck } from "./retry-bound-vectors.js";

const pairs: CrossPair[] = [];
afterEach(async () => {
  await Promise.all(pairs.splice(0).map((pair) => pair.rust.close()));
});

const cases = retryBoundVectors.flatMap((vector) =>
  ["TypeScript", "Rust"].map((sender) => ({ ...vector, sender })));

test.each(cases)("$sender bounds live retry rearming: $name", async (vector) => {
  const pair = new CrossPair(retryConfig(vector.maxTransmissions));
  pairs.push(pair);
  await pair.rustCommand({ op: "configure", maxConnections: 8, maxConnectionsPerPeer: 4,
    ...retryConfig(vector.maxTransmissions) });
  const senderTs = vector.sender === "TypeScript";
  let senderId: number;
  if (senderTs) {
    await pair.rustCommand({ op: "listen", port: 443 });
    senderId = pair.ts.connectFromWithIsn("rust", 50_000, 443, vector.initialSequence, 0);
  } else {
    pair.ts.listen(443);
    senderId = Number(await pair.rustCommand({ op: "connect", peer: "ts", localPort: 50_000,
      remotePort: 443, isn: vector.initialSequence, now: 0 }));
  }
  await pair.settle();
  if (senderTs) expect(await pair.rustCommand({ op: "accept", port: 443 })).not.toBeNull();
  else expect(pair.ts.accept(443)).toBeDefined();
  const payload = new Uint8Array(16).fill(7);
  if (senderTs) pair.ts.write(senderId, payload, 0);
  else await pair.rustCommand({ op: "write", id: senderId, bytes: toHex(payload), now: 0 });
  let emitted: Segment[] = [];
  const collect = async (): Promise<void> => {
    emitted = [];
    await pair.step((left, right) => {
      expect(senderTs ? right : left).toHaveLength(0);
      emitted = (senderTs ? left : right).map((bytes) => Segment.decode(bytes));
      return [[], []];
    });
  };
  const input = async (packet: Segment): Promise<void> => {
    if (senderTs) pair.ts.input("rust", packet.encode(), pair.now);
    else await pair.rustCommand({ op: "input", peer: "ts", bytes: toHex(packet.encode()), now: pair.now });
    await collect();
  };
  const state = async (): Promise<unknown> => senderTs ? pair.ts.state(senderId)
    : pair.rustCommand({ op: "state", id: senderId });
  await collect();
  expect(emitted).toHaveLength(1);
  const data = emitted[0]!;
  let ack = reverseAck(data);
  let transmissions = 1;
  for (let cycle = 0; cycle < vector.maxTransmissions + 3; cycle += 1) {
    pair.now = (cycle + 1) * 10;
    for (let duplicate = 0; duplicate < 3; duplicate += 1) {
      await input(ack);
      for (const repair of emitted) {
        expect(repair.seq).toBe(data.seq);
        expect(repair.payload).toEqual(payload);
      }
      transmissions += emitted.length;
    }
    expect(transmissions).toBe(Math.min(cycle + 2, vector.maxTransmissions));
    const reset = interruptDuplicates(ack, vector.reset);
    await input(reset);
    expect(emitted.every((packet) => packet.payload.length === 0)).toBe(true);
    ack = afterInterruption(reset);
  }
  const deadline = (vector.maxTransmissions - 1) * 10 + 1000;
  pair.now = deadline - 1;
  await collect();
  expect(emitted).toHaveLength(0);
  expect(await state()).toBe(senderTs ? State.Established : "established");
  pair.now = deadline;
  await collect();
  expect(emitted).toHaveLength(0);
  expect(await state()).toBe(senderTs ? undefined : null);
}, 30_000);

test.each(["TypeScript", "Rust"])("%s final permitted retry recovers through its real peer", async (sender) => {
  const pair = new CrossPair(retryConfig(3));
  pairs.push(pair);
  await pair.rustCommand({ op: "configure", maxConnections: 8, maxConnectionsPerPeer: 4,
    ...retryConfig(3) });
  const senderTs = sender === "TypeScript";
  let tsId: number;
  let rustId: number;
  if (senderTs) {
    await pair.rustCommand({ op: "listen", port: 443 });
    tsId = pair.ts.connectFromWithIsn("rust", 50_000, 443, 1000, 0);
    await pair.settle();
    rustId = Number(await pair.rustCommand({ op: "accept", port: 443 }));
  } else {
    pair.ts.listen(443);
    rustId = Number(await pair.rustCommand({ op: "connect", peer: "ts", localPort: 50_000,
      remotePort: 443, isn: 1000, now: 0 }));
    await pair.settle();
    tsId = pair.ts.accept(443)!;
  }
  const write = async (payload: Uint8Array): Promise<void> => {
    if (senderTs) pair.ts.write(tsId, payload, pair.now);
    else await pair.rustCommand({ op: "write", id: rustId, bytes: toHex(payload), now: pair.now });
  };
  const read = async (): Promise<Uint8Array> => senderTs
    ? fromHex(String(await pair.rustCommand({ op: "read", id: rustId, max: 64, now: pair.now })))
    : pair.ts.read(tsId, 64, pair.now);
  const collect = async (): Promise<Segment[]> => {
    let packets: Segment[] = [];
    await pair.step((left, right) => {
      expect(senderTs ? right : left).toHaveLength(0);
      packets = (senderTs ? left : right).map((bytes) => Segment.decode(bytes));
      return [[], []];
    });
    return packets;
  };
  const input = async (packet: Segment): Promise<void> => {
    if (senderTs) pair.ts.input("rust", packet.encode(), pair.now);
    else await pair.rustCommand({ op: "input", peer: "ts", bytes: toHex(packet.encode()), now: pair.now });
  };
  const payload = new Uint8Array(16).fill(7);
  await write(payload);
  const initial = await collect();
  expect(initial).toHaveLength(1);
  let lastData = initial[0]!;
  let ack = reverseAck(lastData);
  for (let cycle = 0; cycle < 3; cycle += 1) {
    pair.now = (cycle + 1) * 10;
    for (let duplicate = 0; duplicate < 3; duplicate += 1) await input(ack);
    const repairs = await collect();
    expect(repairs).toHaveLength(cycle < 2 ? 1 : 0);
    if (repairs.length > 0) {
      expect(repairs[0]!.seq).toBe(lastData.seq);
      expect(repairs[0]!.payload).toEqual(payload);
      lastData = repairs[0]!;
    }
    const reset = interruptDuplicates(ack, "window");
    await input(reset);
    expect(await collect()).toHaveLength(0);
    ack = afterInterruption(reset);
  }
  pair.now = 1019;
  if (senderTs) await pair.rustCommand({ op: "input", peer: "ts", bytes: toHex(lastData.encode()), now: pair.now });
  else pair.ts.input("rust", lastData.encode(), pair.now);
  await pair.settle();
  expect(await read()).toEqual(payload);
  expect(await read()).toHaveLength(0);
  await pair.settle();
  pair.now = 1021;
  await pair.settle();
  const fresh = Uint8Array.of(8, 9, 10);
  await write(fresh);
  await pair.settle();
  expect(await read()).toEqual(fresh);
  expect(await read()).toHaveLength(0);
  expect(pair.ts.state(tsId)).toBe(State.Established);
  expect(await pair.rustCommand({ op: "state", id: rustId })).toBe("established");
}, 30_000);
