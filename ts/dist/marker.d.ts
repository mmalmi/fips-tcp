import type { ConnectionId } from "./types.js";
/** Opaque boundary after bytes accepted by one connection's local send buffer. */
export declare class SendMarker {
    private constructor();
}
export declare enum MarkerStatus {
    Pending = "pending",
    Acked = "acked",
    ConnectionGone = "connection-gone"
}
export interface WriteWithMarkerResult {
    accepted: number;
    marker: SendMarker;
}
export declare class SendProgress {
    private readonly connectionToken;
    private acceptedPayloadBytes;
    private ackedPayloadBytes;
    accept(bytes: number): void;
    acknowledge(bytes: number): void;
    marker(connectionId: ConnectionId): SendMarker;
    status(marker: SendMarker): MarkerStatus;
}
export declare const markerConnectionId: (marker: SendMarker) => ConnectionId | undefined;
