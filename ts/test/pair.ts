import { expect } from "vitest";
import { Config, ConnectionId, Stack, State } from "../src/index.js";

type Transform = (fromA: boolean, bytes: Uint8Array) => Uint8Array[];

export class Pair {
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

  settle(maxSteps = 256): void {
    for (let attempt = 0; attempt < maxSteps; attempt += 1) {
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

