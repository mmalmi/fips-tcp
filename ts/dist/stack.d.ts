import { Config, ConnectionId, Outbound, State } from "./types.js";
import { MarkerStatus, SendMarker, WriteWithMarkerResult } from "./marker.js";
export declare class Stack {
    readonly config: Config;
    private readonly listeners;
    private readonly accepts;
    private readonly connections;
    private readonly lookup;
    private outbound;
    private nextConnectionId;
    private nextEphemeralPort;
    private isnState;
    constructor(config?: Partial<Config>, isnSeed?: bigint | number);
    listen(port: number): void;
    closeListener(port: number): void;
    accept(port: number): ConnectionId | undefined;
    connect(peer: string, remotePort: number, nowMs: number): ConnectionId;
    connectFromWithIsn(peer: string, localPort: number, remotePort: number, isn: number, nowMs: number): ConnectionId;
    input(peer: string, bytes: Uint8Array, nowMs: number): void;
    poll(nowMs: number): void;
    write(id: ConnectionId, bytes: Uint8Array, nowMs: number): number;
    /** Accept payload and return its ACK boundary; an empty payload is a barrier. */
    writeWithMarker(id: ConnectionId, bytes: Uint8Array, nowMs: number): WriteWithMarkerResult;
    markerStatus(marker: SendMarker): MarkerStatus;
    read(id: ConnectionId, max: number, nowMs: number): Uint8Array;
    close(id: ConnectionId, nowMs: number): void;
    /** Abort one retained tuple, emit one active reset, and release it immediately. */
    abort(id: ConnectionId): void;
    state(id: ConnectionId): State | undefined;
    isReadClosed(id: ConnectionId): boolean;
    peer(id: ConnectionId): string | undefined;
    ports(id: ConnectionId): readonly [number, number] | undefined;
    drainOutbound(): Outbound[];
    private finishUpdate;
    private emit;
    private emitReset;
    private removeConnection;
    private requireConnection;
    private ensureConnectionCapacity;
    private allocateConnectionId;
    private allocateEphemeralPort;
    private nextIsn;
}
