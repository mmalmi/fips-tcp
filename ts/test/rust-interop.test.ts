import { ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { createInterface, Interface } from "node:readline";
import { afterEach, describe, expect, test } from "vitest";

import { ConnectionId, MarkerStatus, Segment, Stack, State } from "../src/index.js";

interface WireOutbound {
  peer: string;
  bytes: string;
}

interface DriverResponse {
  ok: boolean;
  result: unknown;
  outbound: WireOutbound[];
  error?: string;
}

interface DriverMarkerWrite {
  accepted: number;
  marker: number;
}

type Command = Record<string, unknown> & { op: string };
type NetworkTransform = (
  fromTs: Uint8Array[],
  fromRust: Uint8Array[],
) => [Uint8Array[], Uint8Array[]];

const toHex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");
const fromHex = (value: string): Uint8Array => Uint8Array.from(Buffer.from(value, "hex"));

class RustDriver {
  private readonly child: ChildProcessWithoutNullStreams;
  private readonly lines: Interface;
  private readonly iterator: AsyncIterableIterator<string>;
  private stderr = "";

  constructor() {
    this.child = spawn("cargo", ["run", "--quiet", "-p", "fips-tcp-interop-driver"], {
      cwd: fileURLToPath(new URL("../../rust", import.meta.url)),
      stdio: "pipe",
    });
    this.child.stderr.setEncoding("utf8");
    this.child.stderr.on("data", (chunk: string) => (this.stderr += chunk));
    this.lines = createInterface({ input: this.child.stdout });
    this.iterator = this.lines[Symbol.asyncIterator]();
  }

  async command(command: Command): Promise<DriverResponse> {
    this.child.stdin.write(`${JSON.stringify(command)}\n`);
    const line = await this.iterator.next();
    if (line.done) throw new Error(`Rust driver exited early: ${this.stderr}`);
    const response = JSON.parse(line.value) as DriverResponse;
    if (!response.ok) throw new Error(response.error ?? "Rust driver command failed");
    return response;
  }

  async close(): Promise<void> {
    this.child.stdin.end();
    if (this.child.exitCode === null) await once(this.child, "exit");
    this.lines.close();
  }
}

class CrossPair {
  readonly ts = new Stack({}, 0x1234_5678_9abc_def0n);
  readonly rust = new RustDriver();
  now = 0;
  private rustOutbound: Uint8Array[] = [];

  async rustCommand(command: Command): Promise<unknown> {
    const response = await this.rust.command(command);
    this.rustOutbound.push(...response.outbound.map((item) => fromHex(item.bytes)));
    return response.result;
  }

  async step(transform: NetworkTransform = identity): Promise<number> {
    this.ts.poll(this.now);
    await this.rustCommand({ op: "poll", now: this.now });
    const fromTs = this.ts.drainOutbound().map((item) => item.bytes);
    const fromRust = this.rustOutbound.splice(0);
    const [deliverTs, deliverRust] = transform(fromTs, fromRust);
    for (const bytes of deliverTs) {
      await this.rustCommand({ op: "input", peer: "ts", bytes: toHex(bytes), now: this.now });
    }
    for (const bytes of deliverRust) this.ts.input("rust", bytes, this.now);
    return deliverTs.length + deliverRust.length;
  }

  async settle(): Promise<void> {
    for (let attempt = 0; attempt < 256; attempt += 1) {
      if ((await this.step()) === 0) return;
    }
    throw new Error("cross-language pair did not settle");
  }

  advance(milliseconds: number): void {
    this.now += milliseconds;
  }
}

const identity: NetworkTransform = (fromTs, fromRust) => [fromTs, fromRust];

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
