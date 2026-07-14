import { Stack } from "./stack.js";
import { Config, ConnectionId, State } from "./types.js";

export interface FipsServiceContext {
  src: string;
  srcPort: number;
  dstPort: number;
  payload: Uint8Array;
}

export interface FipsDatagramEndpoint {
  registerService(
    port: number,
    handler: (context: FipsServiceContext) => Promise<void> | void,
  ): () => void;
  sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void>;
}

/** One FSP service with an automatically matching internal TCP listener. */
export class FipsTcpEndpoint {
  private readonly stack: Stack;
  private readonly unregister: () => void;
  private operation: Promise<void> = Promise.resolve();

  constructor(
    private readonly endpoint: FipsDatagramEndpoint,
    private readonly fspServicePort: number,
    config: Partial<Config> = {},
    isnSeed: bigint | number = 1n,
  ) {
    checkFspServicePort(fspServicePort);
    this.stack = new Stack(config, isnSeed);
    this.stack.listen(fspServicePort);
    this.unregister = endpoint.registerService(fspServicePort, (context) =>
      this.enqueue(async () => {
        this.stack.input(context.src, context.payload, Date.now());
        await this.flush();
      }),
    );
  }

  async accept(): Promise<ConnectionId | undefined> {
    return this.enqueue(() => this.stack.accept(this.fspServicePort));
  }

  async connect(peer: string, nowMs = Date.now()): Promise<ConnectionId> {
    return this.enqueue(async () => {
      const id = this.stack.connect(peer, this.fspServicePort, nowMs);
      await this.flush();
      return id;
    });
  }

  async write(id: ConnectionId, bytes: Uint8Array, nowMs = Date.now()): Promise<number> {
    return this.enqueue(async () => {
      const accepted = this.stack.write(id, bytes, nowMs);
      await this.flush();
      return accepted;
    });
  }

  async read(id: ConnectionId, max: number, nowMs = Date.now()): Promise<Uint8Array> {
    return this.enqueue(async () => {
      const bytes = this.stack.read(id, max, nowMs);
      await this.flush();
      return bytes;
    });
  }

  async close(id: ConnectionId, nowMs = Date.now()): Promise<void> {
    await this.enqueue(async () => {
      this.stack.close(id, nowMs);
      await this.flush();
    });
  }

  async poll(nowMs = Date.now()): Promise<void> {
    await this.enqueue(async () => {
      this.stack.poll(nowMs);
      await this.flush();
    });
  }

  async state(id: ConnectionId): Promise<State | undefined> {
    return this.enqueue(() => this.stack.state(id));
  }

  async isReadClosed(id: ConnectionId): Promise<boolean> {
    return this.enqueue(() => this.stack.isReadClosed(id));
  }

  /** Return the authenticated FIPS identity bound to this stream. */
  async peer(id: ConnectionId): Promise<string | undefined> {
    return this.enqueue(() => this.stack.peer(id));
  }

  /** Return the stream's internal `[local, remote]` TCP ports. */
  async ports(id: ConnectionId): Promise<readonly [number, number] | undefined> {
    return this.enqueue(() => this.stack.ports(id));
  }

  async dispose(): Promise<void> {
    await this.operation;
    this.unregister();
  }

  private async flush(): Promise<void> {
    for (const outbound of this.stack.drainOutbound()) {
      await this.endpoint.sendDatagram({
        dst: outbound.peer,
        srcPort: this.fspServicePort,
        dstPort: this.fspServicePort,
        payload: outbound.bytes,
      });
    }
  }

  private enqueue<T>(work: () => T | Promise<T>): Promise<T> {
    const result = this.operation.then(work, work);
    this.operation = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

function checkFspServicePort(port: number): void {
  if (!Number.isInteger(port) || port <= 0 || port > 0xffff) {
    throw new Error("FIPS service port must be an integer between 1 and 65535");
  }
}
