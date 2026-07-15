export const DEFAULT_CONFIG = Object.freeze({
    mss: 1024,
    receiveBuffer: 0xffff,
    sendBuffer: 1024 * 1024,
    maxConnections: 1024,
    maxConnectionsPerPeer: Number.MAX_SAFE_INTEGER,
    maxReassemblySegments: 128,
    initialRtoMs: 1000,
    minRtoMs: 200,
    maxRtoMs: 60_000,
    maxRetransmissions: 8,
    finWait2Ms: 60_000,
    timeWaitMs: 30_000,
});
export var State;
(function (State) {
    State["SynSent"] = "syn-sent";
    State["SynReceived"] = "syn-received";
    State["Established"] = "established";
    State["FinWait1"] = "fin-wait-1";
    State["FinWait2"] = "fin-wait-2";
    State["CloseWait"] = "close-wait";
    State["Closing"] = "closing";
    State["LastAck"] = "last-ack";
    State["TimeWait"] = "time-wait";
})(State || (State = {}));
const positiveInteger = (value, name) => {
    if (!Number.isSafeInteger(value) || value <= 0)
        throw new Error(`${name} must be a positive integer`);
};
export function makeConfig(overrides = {}) {
    const config = { ...DEFAULT_CONFIG, ...overrides };
    positiveInteger(config.mss, "MSS");
    if (config.mss > 0xffff)
        throw new Error("MSS must fit in a u16");
    positiveInteger(config.receiveBuffer, "receive buffer");
    if (config.receiveBuffer > 0xffff)
        throw new Error("receive buffer must be at most 65535 bytes");
    positiveInteger(config.sendBuffer, "send buffer");
    positiveInteger(config.maxConnections, "connection limit");
    positiveInteger(config.maxConnectionsPerPeer, "per-peer connection limit");
    positiveInteger(config.maxReassemblySegments, "reassembly segment limit");
    positiveInteger(config.initialRtoMs, "initial RTO");
    positiveInteger(config.minRtoMs, "minimum RTO");
    positiveInteger(config.maxRtoMs, "maximum RTO");
    positiveInteger(config.maxRetransmissions, "retransmission limit");
    if (config.maxRetransmissions > 0xff)
        throw new Error("retransmission limit must fit in a u8");
    positiveInteger(config.finWait2Ms, "FIN-WAIT-2 duration");
    if (config.initialRtoMs < config.minRtoMs || config.maxRtoMs < config.initialRtoMs) {
        throw new Error("invalid retransmission timeout bounds");
    }
    if (!Number.isSafeInteger(config.timeWaitMs) || config.timeWaitMs < 0) {
        throw new Error("TIME-WAIT duration must be a non-negative integer");
    }
    return config;
}
