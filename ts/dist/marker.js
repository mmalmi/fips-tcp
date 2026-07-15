const markerData = new WeakMap();
let nextConnectionToken = 1n;
/** Opaque boundary after bytes accepted by one connection's local send buffer. */
export class SendMarker {
    constructor() { }
}
export var MarkerStatus;
(function (MarkerStatus) {
    MarkerStatus["Pending"] = "pending";
    MarkerStatus["Acked"] = "acked";
    MarkerStatus["ConnectionGone"] = "connection-gone";
})(MarkerStatus || (MarkerStatus = {}));
export class SendProgress {
    connectionToken = nextConnectionToken++;
    acceptedPayloadBytes = 0n;
    ackedPayloadBytes = 0n;
    accept(bytes) {
        this.acceptedPayloadBytes += BigInt(bytes);
    }
    acknowledge(bytes) {
        this.ackedPayloadBytes += BigInt(bytes);
    }
    marker(connectionId) {
        const marker = Object.create(SendMarker.prototype);
        markerData.set(marker, {
            connectionId,
            connectionToken: this.connectionToken,
            acceptedPayloadBytes: this.acceptedPayloadBytes,
        });
        return Object.freeze(marker);
    }
    status(marker) {
        const data = markerData.get(marker);
        if (data?.connectionToken !== this.connectionToken)
            return MarkerStatus.ConnectionGone;
        return this.ackedPayloadBytes >= data.acceptedPayloadBytes
            ? MarkerStatus.Acked
            : MarkerStatus.Pending;
    }
}
export const markerConnectionId = (marker) => markerData.get(marker)?.connectionId;
