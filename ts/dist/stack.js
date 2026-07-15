import { Connection } from "./connection.js";
import { u32 } from "./seq.js";
import { makeConfig } from "./types.js";
import { FIPS_VERSION, FlagSet, Flags, Segment } from "./wire.js";
const connectionKey = (peer, localPort, remotePort) => `${peer.length}:${peer}:${localPort}:${remotePort}`;
export class Stack {
    config;
    listeners = new Set();
    accepts = new Map();
    connections = new Map();
    lookup = new Map();
    outbound = [];
    nextConnectionId = 1;
    nextEphemeralPort = 49_152;
    isnState;
    constructor(config = {}, isnSeed = 1n) {
        this.config = makeConfig(config);
        const seed = typeof isnSeed === "bigint" ? isnSeed : BigInt(isnSeed);
        this.isnState = seed > 0n ? seed : 1n;
    }
    listen(port) {
        checkPort(port);
        if (this.listeners.has(port))
            throw new Error(`already listening on port ${port}`);
        this.listeners.add(port);
        this.accepts.set(port, []);
    }
    closeListener(port) {
        this.listeners.delete(port);
        this.accepts.delete(port);
    }
    accept(port) {
        const queue = this.accepts.get(port);
        while (queue !== undefined && queue.length > 0) {
            const id = queue.shift();
            if (this.connections.has(id))
                return id;
        }
        return undefined;
    }
    connect(peer, remotePort, nowMs) {
        const localPort = this.allocateEphemeralPort(peer, remotePort);
        return this.connectFromWithIsn(peer, localPort, remotePort, this.nextIsn(), nowMs);
    }
    connectFromWithIsn(peer, localPort, remotePort, isn, nowMs) {
        checkPeer(peer);
        checkPort(localPort);
        checkPort(remotePort);
        checkNow(nowMs);
        if (!Number.isInteger(isn) || isn < 0 || isn > 0xffff_ffff)
            throw new Error("ISN must be a u32");
        this.ensureConnectionCapacity(peer);
        const key = connectionKey(peer, localPort, remotePort);
        if (this.lookup.has(key))
            throw new Error("connection already exists");
        const id = this.allocateConnectionId();
        const [connection, segments] = Connection.client(peer, localPort, remotePort, isn, nowMs, this.config);
        this.lookup.set(key, id);
        this.connections.set(id, connection);
        this.emit(id, segments);
        return id;
    }
    input(peer, bytes, nowMs) {
        checkPeer(peer);
        checkNow(nowMs);
        const segment = Segment.decode(bytes);
        const key = connectionKey(peer, segment.dstPort, segment.srcPort);
        let id = this.lookup.get(key);
        if (id === undefined) {
            if (segment.flags.has(Flags.Syn) &&
                !segment.flags.has(Flags.Ack) &&
                this.listeners.has(segment.dstPort)) {
                if (!segment.supportsFipsVersion(FIPS_VERSION)) {
                    this.emitReset(peer, segment);
                    return;
                }
                this.ensureConnectionCapacity(peer);
                id = this.allocateConnectionId();
                const [connection, segments] = Connection.server(peer, segment, this.nextIsn(), nowMs, this.config);
                this.lookup.set(key, id);
                this.connections.set(id, connection);
                this.emit(id, segments);
                return;
            }
            if (!segment.flags.has(Flags.Rst))
                this.emitReset(peer, segment);
            return;
        }
        const connection = this.connections.get(id);
        if (connection === undefined)
            throw new Error("connection lookup is inconsistent");
        const update = connection.onSegment(segment, nowMs, this.config);
        if (update.accepted) {
            const queue = this.accepts.get(connection.localPort) ?? [];
            queue.push(id);
            this.accepts.set(connection.localPort, queue);
        }
        this.finishUpdate(id, update);
    }
    poll(nowMs) {
        checkNow(nowMs);
        for (const [id, connection] of [...this.connections.entries()]) {
            this.finishUpdate(id, connection.poll(nowMs, this.config));
        }
    }
    write(id, bytes, nowMs) {
        checkNow(nowMs);
        const [accepted, segments] = this.requireConnection(id).write(bytes, nowMs, this.config);
        this.emit(id, segments);
        return accepted;
    }
    read(id, max, nowMs) {
        checkNow(nowMs);
        const [bytes, segments] = this.requireConnection(id).read(max);
        this.emit(id, segments);
        return bytes;
    }
    close(id, nowMs) {
        checkNow(nowMs);
        this.finishUpdate(id, this.requireConnection(id).close(nowMs, this.config));
    }
    /** Abort one retained tuple, emit one active reset, and release it immediately. */
    abort(id) {
        const connection = this.requireConnection(id);
        this.outbound = this.outbound.filter((outbound) => {
            if (outbound.peer !== connection.peer)
                return true;
            const segment = Segment.decode(outbound.bytes);
            return segment.srcPort !== connection.localPort || segment.dstPort !== connection.remotePort;
        });
        this.emit(id, [connection.resetSegment()]);
        this.removeConnection(id);
    }
    state(id) {
        return this.connections.get(id)?.state;
    }
    isReadClosed(id) {
        return this.connections.get(id)?.readClosed ?? true;
    }
    peer(id) {
        return this.connections.get(id)?.peer;
    }
    ports(id) {
        const connection = this.connections.get(id);
        return connection === undefined ? undefined : [connection.localPort, connection.remotePort];
    }
    drainOutbound() {
        const outbound = this.outbound;
        this.outbound = [];
        return outbound;
    }
    finishUpdate(id, update) {
        this.emit(id, update.segments);
        if (update.closed)
            this.removeConnection(id);
    }
    emit(id, segments) {
        const peer = this.connections.get(id)?.peer;
        if (peer === undefined)
            return;
        for (const segment of segments)
            this.outbound.push({ peer, bytes: segment.encode() });
    }
    emitReset(peer, incoming) {
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
    removeConnection(id) {
        const connection = this.connections.get(id);
        if (connection === undefined)
            return;
        this.connections.delete(id);
        this.lookup.delete(connectionKey(connection.peer, connection.localPort, connection.remotePort));
    }
    requireConnection(id) {
        const connection = this.connections.get(id);
        if (connection === undefined)
            throw new Error("unknown connection");
        return connection;
    }
    ensureConnectionCapacity(peer) {
        let peerConnections = 0;
        for (const connection of this.connections.values()) {
            if (connection.peer === peer)
                peerConnections += 1;
        }
        if (this.connections.size >= this.config.maxConnections ||
            peerConnections >= this.config.maxConnectionsPerPeer) {
            throw new Error("connection limit reached");
        }
    }
    allocateConnectionId() {
        const id = this.nextConnectionId;
        this.nextConnectionId += 1;
        if (!Number.isSafeInteger(this.nextConnectionId))
            this.nextConnectionId = 1;
        return id;
    }
    allocateEphemeralPort(peer, remotePort) {
        checkPeer(peer);
        checkPort(remotePort);
        for (let attempt = 0; attempt < 16_384; attempt += 1) {
            const port = this.nextEphemeralPort;
            this.nextEphemeralPort = port === 0xffff ? 49_152 : port + 1;
            if (!this.lookup.has(connectionKey(peer, port, remotePort)))
                return port;
        }
        throw new Error("no ephemeral port available");
    }
    nextIsn() {
        const mask = 0xffffffffffffffffn;
        let value = this.isnState;
        value ^= (value << 13n) & mask;
        value ^= value >> 7n;
        value ^= (value << 17n) & mask;
        value &= mask;
        this.isnState = value > 0n ? value : 1n;
        return Number((value ^ (value >> 32n)) & 0xffffffffn);
    }
}
const checkPort = (port) => {
    if (!Number.isInteger(port) || port <= 0 || port > 0xffff) {
        throw new Error("TCP/FIPS ports must be non-zero u16 values");
    }
};
const checkPeer = (peer) => {
    if (peer.length === 0)
        throw new Error("FIPS peer identity must be non-empty");
};
const checkNow = (nowMs) => {
    if (!Number.isSafeInteger(nowMs) || nowMs < 0)
        throw new Error("clock must be non-negative milliseconds");
};
