import { Config, ConnectionId, State } from "./types.js";
export interface FipsServiceContext {
    src: string;
    srcPort: number;
    dstPort: number;
    payload: Uint8Array;
}
export interface FipsDatagramEndpoint {
    registerService(port: number, handler: (context: FipsServiceContext) => Promise<void> | void): () => void;
    sendDatagram(args: {
        dst: string;
        srcPort?: number;
        dstPort: number;
        payload: Uint8Array;
    }): Promise<void>;
}
/** One FSP service with an automatically matching internal TCP listener. */
export declare class FipsTcpEndpoint {
    private readonly endpoint;
    private readonly fspServicePort;
    private readonly stack;
    private readonly unregister;
    private operation;
    constructor(endpoint: FipsDatagramEndpoint, fspServicePort: number, config?: Partial<Config>, isnSeed?: bigint | number);
    accept(): Promise<ConnectionId | undefined>;
    connect(peer: string, nowMs?: number): Promise<ConnectionId>;
    write(id: ConnectionId, bytes: Uint8Array, nowMs?: number): Promise<number>;
    read(id: ConnectionId, max: number, nowMs?: number): Promise<Uint8Array>;
    close(id: ConnectionId, nowMs?: number): Promise<void>;
    poll(nowMs?: number): Promise<void>;
    state(id: ConnectionId): Promise<State | undefined>;
    isReadClosed(id: ConnectionId): Promise<boolean>;
    /** Return the authenticated FIPS identity bound to this stream. */
    peer(id: ConnectionId): Promise<string | undefined>;
    /** Return the stream's internal `[local, remote]` TCP ports. */
    ports(id: ConnectionId): Promise<readonly [number, number] | undefined>;
    dispose(): Promise<void>;
    private flush;
    private enqueue;
}
