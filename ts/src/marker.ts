import type { ConnectionId } from "./types.js";

interface MarkerData {
  connectionId: ConnectionId;
  connectionToken: bigint;
  acceptedPayloadBytes: bigint;
}

const markerData = new WeakMap<SendMarker, MarkerData>();
let nextConnectionToken = 1n;

/** Opaque boundary after bytes accepted by one connection's local send buffer. */
export class SendMarker {
  private constructor() {}
}

export enum MarkerStatus {
  Pending = "pending",
  Acked = "acked",
  ConnectionGone = "connection-gone",
}

export interface WriteWithMarkerResult {
  accepted: number;
  marker: SendMarker;
}

export class SendProgress {
  private readonly connectionToken = nextConnectionToken++;
  private acceptedPayloadBytes = 0n;
  private ackedPayloadBytes = 0n;

  accept(bytes: number): void {
    this.acceptedPayloadBytes += BigInt(bytes);
  }

  acknowledge(bytes: number): void {
    this.ackedPayloadBytes += BigInt(bytes);
  }

  marker(connectionId: ConnectionId): SendMarker {
    const marker = Object.create(SendMarker.prototype) as SendMarker;
    markerData.set(marker, {
      connectionId,
      connectionToken: this.connectionToken,
      acceptedPayloadBytes: this.acceptedPayloadBytes,
    });
    return Object.freeze(marker);
  }

  status(marker: SendMarker): MarkerStatus {
    const data = markerData.get(marker);
    if (data?.connectionToken !== this.connectionToken) return MarkerStatus.ConnectionGone;
    return this.ackedPayloadBytes >= data.acceptedPayloadBytes
      ? MarkerStatus.Acked
      : MarkerStatus.Pending;
  }
}

export const markerConnectionId = (marker: SendMarker): ConnectionId | undefined =>
  markerData.get(marker)?.connectionId;
