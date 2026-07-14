import {
  FipsNode,
  generateIdentity,
  toHex,
  type Transport,
  type TransportAddress,
  type TransportContext,
} from "@fips/core";
import { afterEach, expect, test } from "vitest";

import { FipsTcpEndpoint } from "../src/index.js";

interface RunningPair {
  aNode: FipsNode;
  bNode: FipsNode;
  aTcp: FipsTcpEndpoint;
  bTcp: FipsTcpEndpoint;
}

const running: RunningPair[] = [];
const fspServicePort = 39_017;

class TestMemoryHub {
  readonly peers = new Map<string, TestMemoryTransport>();
}

class TestMemoryTransport implements Transport {
  readonly type = "memory";
  readonly mtu = 65_535;
  private context: TransportContext | undefined;
  private publicKey = "";

  constructor(private readonly hub: TestMemoryHub) {}

  async start(context: TransportContext): Promise<void> {
    this.context = context;
    this.publicKey = toHex(context.localIdentity.publicKey);
    this.hub.peers.set(this.publicKey, this);
  }

  async stop(): Promise<void> {
    this.hub.peers.delete(this.publicKey);
    this.context = undefined;
  }

  async connect(address: TransportAddress): Promise<void> {
    const remote = this.requirePeer(address.addr);
    this.context?.onConnectionState?.({ remoteAddr: address, state: "connected" });
    remote.context?.onConnectionState?.({
      remoteAddr: { transport: this.type, addr: this.publicKey },
      state: "connected",
    });
  }

  async send(address: TransportAddress, packet: Uint8Array): Promise<void> {
    const remote = this.requirePeer(address.addr);
    const remoteAddress = { transport: this.type, addr: this.publicKey };
    queueMicrotask(() =>
      remote.context?.onPacket({
        transportType: this.type,
        remoteAddr: remoteAddress,
        data: packet.slice(),
        receivedAtMs: Date.now(),
      }),
    );
  }

  private requirePeer(publicKey: string): TestMemoryTransport {
    const peer = this.hub.peers.get(publicKey);
    if (peer === undefined || peer.context === undefined) throw new Error("memory peer offline");
    return peer;
  }
}

afterEach(async () => {
  await Promise.all(
    running.splice(0).map(async ({ aNode, bNode, aTcp, bTcp }) => {
      await Promise.all([aTcp.dispose(), bTcp.dispose()]);
      await Promise.all([aNode.stop(), bNode.stop()]);
    }),
  );
});

test("TCP stream runs through two real FipsNode service endpoints", async () => {
  const [aIdentity, bIdentity] = await Promise.all([generateIdentity(), generateIdentity()]);
  const hub = new TestMemoryHub();
  const aNode = new FipsNode({
    identity: aIdentity,
    transports: [new TestMemoryTransport(hub)],
  });
  const bNode = new FipsNode({
    identity: bIdentity,
    transports: [new TestMemoryTransport(hub)],
  });
  const aTcp = new FipsTcpEndpoint(aNode, fspServicePort, {}, 0x1111n);
  const bTcp = new FipsTcpEndpoint(bNode, fspServicePort, {}, 0x2222n);
  running.push({ aNode, bNode, aTcp, bTcp });
  await Promise.all([aNode.start(), bNode.start()]);
  await aNode.connect({ transport: "memory", addr: toHex(bIdentity.publicKey) });

  const client = await aTcp.connect(toHex(bIdentity.publicKey));
  const server = await eventually(async () => bTcp.accept());

  const request = Uint8Array.from({ length: 8192 }, (_, index) => index % 251);
  expect(await aTcp.write(client, request)).toBe(request.length);
  expect(await collect(bTcp, server, request.length)).toEqual(request);

  const response = new TextEncoder().encode("reply over authenticated FIPS service datagrams");
  expect(await bTcp.write(server, response)).toBe(response.length);
  expect(await collect(aTcp, client, response.length)).toEqual(response);
}, 15_000);

async function collect(
  endpoint: FipsTcpEndpoint,
  id: number,
  expected: number,
): Promise<Uint8Array> {
  const received: number[] = [];
  await eventually(async () => {
    received.push(...(await endpoint.read(id, expected - received.length)));
    return received.length === expected ? true : undefined;
  });
  return Uint8Array.from(received);
}

async function eventually<T>(work: () => Promise<T | undefined>): Promise<T> {
  const deadline = Date.now() + 5000;
  for (;;) {
    const value = await work();
    if (value !== undefined) return value;
    if (Date.now() >= deadline) throw new Error("timed out waiting for FIPS/TCP progress");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
