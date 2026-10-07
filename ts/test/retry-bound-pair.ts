import { ChildProcessWithoutNullStreams, spawn } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import { createInterface, Interface } from "node:readline";
import { Config, Stack } from "../src/index.js";

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

type Command = Record<string, unknown> & { op: string };
export type NetworkTransform = (
  fromTs: Uint8Array[],
  fromRust: Uint8Array[],
) => [Uint8Array[], Uint8Array[]];

export const toHex = (bytes: Uint8Array): string => Buffer.from(bytes).toString("hex");
export const fromHex = (value: string): Uint8Array => Uint8Array.from(Buffer.from(value, "hex"));

export class RustDriver {
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

export class CrossPair {
  ts: Stack;
  readonly rust = new RustDriver();
  now = 0;
  private rustOutbound: Uint8Array[] = [];

  constructor(config: Partial<Config> = {}) {
    this.ts = new Stack(config, 0x1234_5678_9abc_def0n);
  }

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

  async settle(maxSteps = 256): Promise<void> {
    for (let attempt = 0; attempt < maxSteps; attempt += 1) {
      if ((await this.step()) === 0) return;
    }
    throw new Error("cross-language pair did not settle");
  }

  advance(milliseconds: number): void {
    this.now += milliseconds;
  }
}

const identity: NetworkTransform = (fromTs, fromRust) => [fromTs, fromRust];

