import { expect, test } from "vitest";

import {
  FipsDatagramEndpoint,
  FipsServiceContext,
  FipsTcpEndpoint,
  FIPS_VERSION,
  FlagSet,
  Flags,
  MarkerStatus,
  Segment,
  State,
  TcpOptionKind,
} from "../src/index.js";

type ServiceHandler = (context: FipsServiceContext) => Promise<void> | void;

class MemoryFipsEndpoint implements FipsDatagramEndpoint {
  private readonly services = new Map<number, ServiceHandler>();
  private remote?: MemoryFipsEndpoint;

  constructor(private readonly identity: string) {}

  connect(remote: MemoryFipsEndpoint): void {
    this.remote = remote;
  }

  registerService(port: number, handler: ServiceHandler): () => void {
    if (this.services.has(port)) throw new Error(`service ${port} is already registered`);
    this.services.set(port, handler);
    return () => this.services.delete(port);
  }

  async sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    const remote = this.remote;
    if (remote === undefined || remote.identity !== args.dst) throw new Error("unknown peer");
    const handler = remote.services.get(args.dstPort);
    if (handler === undefined) throw new Error(`service ${args.dstPort} is not registered`);
    const context: FipsServiceContext = {
      src: this.identity,
      srcPort: args.srcPort ?? 0,
      dstPort: args.dstPort,
      payload: args.payload.slice(),
    };
    queueMicrotask(() => void handler(context));
  }
}

const fspServicePort = 39_017;

test("TCP stream runs through the structural FIPS service endpoint API", async () => {
  const aNode = new MemoryFipsEndpoint("peer-a");
  const bNode = new MemoryFipsEndpoint("peer-b");
  aNode.connect(bNode);
  bNode.connect(aNode);
  const aTcp = new FipsTcpEndpoint(aNode, fspServicePort, {}, 0x1111n);
  const bTcp = new FipsTcpEndpoint(bNode, fspServicePort, {}, 0x2222n);

  try {
    const client = await aTcp.connect("peer-b");
    const server = await eventually(async () => bTcp.accept());
    expect(await aTcp.peer(client)).toBe("peer-b");
    expect(await bTcp.peer(server)).toBe("peer-a");
    expect((await aTcp.ports(client))?.[1]).toBe(fspServicePort);
    expect((await bTcp.ports(server))?.[0]).toBe(fspServicePort);

    const request = Uint8Array.from({ length: 8192 }, (_, index) => index % 251);
    const { accepted, marker } = await aTcp.writeWithMarker(client, request);
    expect(accepted).toBe(request.length);
    expect(await aTcp.markerStatus(marker)).toBe(MarkerStatus.Pending);
    expect(await collect(bTcp, server, request.length)).toEqual(request);
    await eventually(async () =>
      (await aTcp.markerStatus(marker)) === MarkerStatus.Acked ? true : undefined,
    );

    const response = new TextEncoder().encode("reply over FIPS service datagrams");
    expect(await bTcp.write(server, response)).toBe(response.length);
    expect(await collect(aTcp, client, response.length)).toEqual(response);
  } finally {
    await Promise.all([aTcp.dispose(), bTcp.dispose()]);
  }
}, 15_000);

test("failed initial sends release capacity and preserve the endpoint error", async () => {
  const node = new MemoryFipsEndpoint("peer-a");
  const tcp = new FipsTcpEndpoint(
    node,
    fspServicePort,
    { maxConnections: 1, maxConnectionsPerPeer: 1 },
    0x3333n,
  );
  try {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      await expect(tcp.connect("offline-peer", attempt)).rejects.toThrow("unknown peer");
    }
  } finally {
    await tcp.dispose();
  }
});

test("endpoint abort removes local and remote stream state", async () => {
  const aNode = new MemoryFipsEndpoint("peer-a");
  const bNode = new MemoryFipsEndpoint("peer-b");
  aNode.connect(bNode);
  bNode.connect(aNode);
  const aTcp = new FipsTcpEndpoint(aNode, fspServicePort, {}, 0x4444n);
  const bTcp = new FipsTcpEndpoint(bNode, fspServicePort, {}, 0x5555n);

  try {
    const client = await aTcp.connect("peer-b");
    const server = await eventually(async () => bTcp.accept());
    await aTcp.abort(client);
    expect(await aTcp.state(client)).toBeUndefined();
    await eventually(async () =>
      (await bTcp.state(server)) === undefined ? true : undefined,
    );
    await expect(aTcp.abort(client)).rejects.toThrow(/unknown connection/i);
    expect(await bTcp.state(server)).toBeUndefined();
  } finally {
    await Promise.all([aTcp.dispose(), bTcp.dispose()]);
  }
});

test("reservation rejects only new tuples and preserves later carrier operations", async () => {
  let handler: ServiceHandler | undefined;
  const sent: Uint8Array[] = [];
  const node: FipsDatagramEndpoint = {
    registerService(_port, callback) { handler = callback; return () => { handler = undefined; }; },
    async sendDatagram(args) { sent.push(args.payload); },
  };
  const tcp = new FipsTcpEndpoint(node, fspServicePort, { maxConnections: 2, maxConnectionsPerPeer: 1 });
  const inject = async (peer: string, segment: Segment): Promise<void> => {
    await handler!({ src: peer, srcPort: fspServicePort, dstPort: fspServicePort, payload: segment.encode() });
  };
  const syn = (port: number): Segment => new Segment({
    srcPort: port, dstPort: fspServicePort, seq: port, flags: new FlagSet(Flags.Syn),
    options: [{ kind: TcpOptionKind.FipsVersion, version: FIPS_VERSION, reserved: 0 }],
  });
  try {
    await tcp.setConnectionReservation(1, (peer) => peer === "paid");
    await inject("ordinary", syn(50_000));
    expect(sent.splice(0)).toHaveLength(1);
    await expect(inject("unknown", syn(50_001))).rejects.toThrow(/connection limit/i);
    expect(sent).toHaveLength(0);
    await inject("paid", syn(50_002));
    expect(sent).toHaveLength(1);
    const reply = Segment.decode(sent.shift()!);
    await tcp.setConnectionReservation(1, () => false);
    await inject("paid", new Segment({ srcPort: 50_002, dstPort: fspServicePort,
      seq: 50_003, ack: (reply.seq + 1) >>> 0, flags: new FlagSet(Flags.Ack), window: 65535,
      payload: new TextEncoder().encode("record"),
    }));
    const accepted = await tcp.accept();
    expect(accepted).toBeDefined();
    expect(await tcp.state(accepted!)).toBe(State.Established);
    expect(await tcp.peer(accepted!)).toBe("paid");
    expect(new TextDecoder().decode(await tcp.read(accepted!, 16))).toBe("record");
  } finally {
    await tcp.dispose();
  }
});

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
