export interface Config {
    mss: number;
    receiveBuffer: number;
    sendBuffer: number;
    maxConnections: number;
    /** Retained connections allowed for one authenticated carrier peer. */
    maxConnectionsPerPeer: number;
    maxReassemblySegments: number;
    initialRtoMs: number;
    minRtoMs: number;
    maxRtoMs: number;
    maxRetransmissions: number;
    timeWaitMs: number;
}
export declare const DEFAULT_CONFIG: Readonly<Config>;
export declare enum State {
    SynSent = "syn-sent",
    SynReceived = "syn-received",
    Established = "established",
    FinWait1 = "fin-wait-1",
    FinWait2 = "fin-wait-2",
    CloseWait = "close-wait",
    Closing = "closing",
    LastAck = "last-ack",
    TimeWait = "time-wait"
}
export type ConnectionId = number;
export interface Outbound {
    peer: string;
    bytes: Uint8Array;
}
export declare function makeConfig(overrides?: Partial<Config>): Config;
