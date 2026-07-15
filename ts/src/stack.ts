import { Connection } from "./connection.js";
import { ConnectionUpdate } from "./connection-types.js";
import { u32 } from "./seq.js";
import { Config, ConnectionId, makeConfig, Outbound, State } from "./types.js";
import { FIPS_VERSION, FlagSet, Flags, Segment } from "./wire.js";
import { markerConnectionId, MarkerStatus, SendMarker, WriteWithMarkerResult } from "./marker.js";

const connectionKey = (peer: string, localPort: number, remotePort: number): string =>
  `${peer.length}:${peer}:${localPort}:${remotePort}`;

export class Stack {
  readonly config: Config;
  private readonly listeners = new Set<number>();
  private readonly accepts = new Map<number, ConnectionId[]>();
  private readonly connections = new Map<ConnectionId, Connection>();
  private readonly lookup = new Map<string, ConnectionId>();
  private outbound: Outbound[] = [];
  private nextConnectionId = 1;
  private nextEphemeralPort = 49_152;
  private isnState: bigint;

  constructor(config: Partial<Config> = {}, isnSeed: bigint | number = 1n) {
    this.config = makeConfig(config);
    const seed = typeof isnSeed === "bigint" ? isnSeed : BigInt(isnSeed);
    this.isnState = seed > 0n ? seed : 1n;
  }

  listen(port: number): void {
    checkPort(port);
    if (this.listeners.has(port)) throw new Error(`already listening on port ${port}`);
    this.listeners.add(port);
    this.accepts.set(port, []);
  }

  closeListener(port: number): void {
    this.listeners.delete(port);
    this.accepts.delete(port);
  }

  accept(port: number): ConnectionId | undefined {
    const queue = this.accepts.get(port);
    while (queue !== undefined && queue.length > 0) {
      const id = queue.shift()!;
      if (this.connections.has(id)) return id;
    }
    return undefined;
  }

  connect(peer: string, remotePort: number, nowMs: number): ConnectionId {
    const localPort = this.allocateEphemeralPort(peer, remotePort);
    return this.connectFromWithIsn(peer, localPort, remotePort, this.nextIsn(), nowMs);
  }

  connectFromWithIsn(
    peer: string,
    localPort: number,
    remotePort: number,
    isn: number,
    nowMs: number,
  ): ConnectionId {
    checkPeer(peer);
    checkPort(localPort);
    checkPort(remotePort);
    checkNow(nowMs);
    if (!Number.isInteger(isn) || isn < 0 || isn > 0xffff_ffff) throw new Error("ISN must be a u32");
    this.ensureConnectionCapacity(peer);
    const key = connectionKey(peer, localPort, remotePort);
    if (this.lookup.has(key)) throw new Error("connection already exists");
    const id = this.allocateConnectionId();
    const [connection, segments] = Connection.client(
      peer,
      localPort,
      remotePort,
      isn,
      nowMs,
      this.config,
    );
    this.lookup.set(key, id);
    this.connections.set(id, connection);
    this.emit(id, segments);
    return id;
  }

  input(peer: string, bytes: Uint8Array, nowMs: number): void {
    checkPeer(peer);
    checkNow(nowMs);
    const segment = Segment.decode(bytes);
    const key = connectionKey(peer, segment.dstPort, segment.srcPort);
    let id = this.lookup.get(key);
    if (id === undefined) {
      if (
        segment.flags.has(Flags.Syn) &&
        !segment.flags.has(Flags.Ack) &&
        this.listeners.has(segment.dstPort)
      ) {
        if (!segment.supportsFipsVersion(FIPS_VERSION)) {
          this.emitReset(peer, segment);
          return;
        }
        this.ensureConnectionCapacity(peer);
        id = this.allocateConnectionId();
        const [connection, segments] = Connection.server(
          peer,
          segment,
          this.nextIsn(),
          nowMs,
          this.config,
        );
        this.lookup.set(key, id);
        this.connections.set(id, connection);
        this.emit(id, segments);
        return;
      }
      if (!segment.flags.has(Flags.Rst)) this.emitReset(peer, segment);
      return;
    }

    const connection = this.connections.get(id);
    if (connection === undefined) throw new Error("connection lookup is inconsistent");
    const update = connection.onSegment(segment, nowMs, this.config);
    if (update.accepted) {
      const queue = this.accepts.get(connection.localPort) ?? [];
      queue.push(id);
      this.accepts.set(connection.localPort, queue);
    }
    this.finishUpdate(id, update);
  }

  poll(nowMs: number): void {
    checkNow(nowMs);
    for (const [id, connection] of [...this.connections.entries()]) {
      this.finishUpdate(id, connection.poll(nowMs, this.config));
    }
  }

  write(id: ConnectionId, bytes: Uint8Array, nowMs: number): number {
    return this.writeWithMarker(id, bytes, nowMs).accepted;
  }

  /** Accept payload and return its ACK boundary; an empty payload is a barrier. */
  writeWithMarker(id: ConnectionId, bytes: Uint8Array, nowMs: number): WriteWithMarkerResult {
    checkNow(nowMs);
    const connection = this.requireConnection(id);
    const [accepted, segments] = connection.write(bytes, nowMs, this.config);
    const marker = connection.sendProgress.marker(id);
    this.emit(id, segments);
    return { accepted, marker };
  }

  markerStatus(marker: SendMarker): MarkerStatus {
    const id = markerConnectionId(marker);
    const connection = id === undefined ? undefined : this.connections.get(id);
    return connection?.sendProgress.status(marker) ?? MarkerStatus.ConnectionGone;
  }

  read(id: ConnectionId, max: number, nowMs: number): Uint8Array {
    checkNow(nowMs);
    const [bytes, segments] = this.requireConnection(id).read(max);
    this.emit(id, segments);
    return bytes;
  }

  close(id: ConnectionId, nowMs: number): void {
    checkNow(nowMs);
    this.finishUpdate(id, this.requireConnection(id).close(nowMs, this.config));
  }

  /** Abort one retained tuple, emit one active reset, and release it immediately. */
  abort(id: ConnectionId): void {
    const connection = this.requireConnection(id);
    this.outbound = this.outbound.filter((outbound) => {
      if (outbound.peer !== connection.peer) return true;
      const segment = Segment.decode(outbound.bytes);
      return segment.srcPort !== connection.localPort || segment.dstPort !== connection.remotePort;
    });
    this.emit(id, [connection.resetSegment()]);
    this.removeConnection(id);
  }

  state(id: ConnectionId): State | undefined {
    return this.connections.get(id)?.state;
  }

  isReadClosed(id: ConnectionId): boolean {
    return this.connections.get(id)?.readClosed ?? true;
  }

  peer(id: ConnectionId): string | undefined {
    return this.connections.get(id)?.peer;
  }

  ports(id: ConnectionId): readonly [number, number] | undefined {
    const connection = this.connections.get(id);
    return connection === undefined ? undefined : [connection.localPort, connection.remotePort];
  }

  drainOutbound(): Outbound[] {
    const outbound = this.outbound;
    this.outbound = [];
    return outbound;
  }

  private finishUpdate(id: ConnectionId, update: ConnectionUpdate): void {
    this.emit(id, update.segments);
    if (update.closed) this.removeConnection(id);
  }

  private emit(id: ConnectionId, segments: Segment[]): void {
    const peer = this.connections.get(id)?.peer;
    if (peer === undefined) return;
    for (const segment of segments) this.outbound.push({ peer, bytes: segment.encode() });
  }

  private emitReset(peer: string, incoming: Segment): void {
    const hasAck = incoming.ack !== undefined;
    const flags = new FlagSet(hasAck ? Flags.Rst : Flags.Rst | Flags.Ack);
    const reset = new Segment({
      srcPort: incoming.dstPort,
      dstPort: incoming.srcPort,
      seq: incoming.ack ?? 0,
      ...(hasAck ? {} : { ack: u32(incoming.seq + incoming.sequenceLength()) }),
      flags,
      window: 0,
    });
    this.outbound.push({ peer, bytes: reset.encode() });
  }

  private removeConnection(id: ConnectionId): void {
    const connection = this.connections.get(id);
    if (connection === undefined) return;
    this.connections.delete(id);
    this.lookup.delete(connectionKey(connection.peer, connection.localPort, connection.remotePort));
  }

  private requireConnection(id: ConnectionId): Connection {
    const connection = this.connections.get(id);
    if (connection === undefined) throw new Error("unknown connection");
    return connection;
  }

  private ensureConnectionCapacity(peer: string): void {
    let peerConnections = 0;
    for (const connection of this.connections.values()) {
      if (connection.peer === peer) peerConnections += 1;
    }
    if (
      this.connections.size >= this.config.maxConnections ||
      peerConnections >= this.config.maxConnectionsPerPeer
    ) {
      throw new Error("connection limit reached");
    }
  }

  private allocateConnectionId(): ConnectionId {
    const id = this.nextConnectionId;
    this.nextConnectionId += 1;
    if (!Number.isSafeInteger(this.nextConnectionId)) this.nextConnectionId = 1;
    return id;
  }

  private allocateEphemeralPort(peer: string, remotePort: number): number {
    checkPeer(peer);
    checkPort(remotePort);
    for (let attempt = 0; attempt < 16_384; attempt += 1) {
      const port = this.nextEphemeralPort;
      this.nextEphemeralPort = port === 0xffff ? 49_152 : port + 1;
      if (!this.lookup.has(connectionKey(peer, port, remotePort))) return port;
    }
    throw new Error("no ephemeral port available");
  }

  private nextIsn(): number {
    const mask = 0xffff_ffff_ffff_ffffn;
    let value = this.isnState;
    value ^= (value << 13n) & mask;
    value ^= value >> 7n;
    value ^= (value << 17n) & mask;
    value &= mask;
    this.isnState = value > 0n ? value : 1n;
    return Number((value ^ (value >> 32n)) & 0xffff_ffffn);
  }
}

const checkPort = (port: number): void => {
  if (!Number.isInteger(port) || port <= 0 || port > 0xffff) {
    throw new Error("TCP/FIPS ports must be non-zero u16 values");
  }
};

const checkPeer = (peer: string): void => {
  if (peer.length === 0) throw new Error("FIPS peer identity must be non-empty");
};

const checkNow = (nowMs: number): void => {
  if (!Number.isSafeInteger(nowMs) || nowMs < 0) throw new Error("clock must be non-negative milliseconds");
};
