export interface Config {
  mss: number;
  receiveBuffer: number;
  sendBuffer: number;
  maxConnections: number;
  maxReassemblySegments: number;
  initialRtoMs: number;
  minRtoMs: number;
  maxRtoMs: number;
  maxRetransmissions: number;
  timeWaitMs: number;
}

export const DEFAULT_CONFIG: Readonly<Config> = Object.freeze({
  mss: 1024,
  receiveBuffer: 0xffff,
  sendBuffer: 1024 * 1024,
  maxConnections: 1024,
  maxReassemblySegments: 128,
  initialRtoMs: 1000,
  minRtoMs: 200,
  maxRtoMs: 60_000,
  maxRetransmissions: 8,
  timeWaitMs: 30_000,
});

export enum State {
  SynSent = "syn-sent",
  SynReceived = "syn-received",
  Established = "established",
  FinWait1 = "fin-wait-1",
  FinWait2 = "fin-wait-2",
  CloseWait = "close-wait",
  Closing = "closing",
  LastAck = "last-ack",
  TimeWait = "time-wait",
}

export type ConnectionId = number;

export interface Outbound {
  peer: string;
  bytes: Uint8Array;
}

const positiveInteger = (value: number, name: string): void => {
  if (!Number.isSafeInteger(value) || value <= 0) throw new Error(`${name} must be a positive integer`);
};

export function makeConfig(overrides: Partial<Config> = {}): Config {
  const config = { ...DEFAULT_CONFIG, ...overrides };
  positiveInteger(config.mss, "MSS");
  if (config.mss > 0xffff) throw new Error("MSS must fit in a u16");
  positiveInteger(config.receiveBuffer, "receive buffer");
  if (config.receiveBuffer > 0xffff) throw new Error("receive buffer must be at most 65535 bytes");
  positiveInteger(config.sendBuffer, "send buffer");
  positiveInteger(config.maxConnections, "connection limit");
  positiveInteger(config.maxReassemblySegments, "reassembly segment limit");
  positiveInteger(config.initialRtoMs, "initial RTO");
  positiveInteger(config.minRtoMs, "minimum RTO");
  positiveInteger(config.maxRtoMs, "maximum RTO");
  positiveInteger(config.maxRetransmissions, "retransmission limit");
  if (config.maxRetransmissions > 0xff) throw new Error("retransmission limit must fit in a u8");
  if (config.initialRtoMs < config.minRtoMs || config.maxRtoMs < config.initialRtoMs) {
    throw new Error("invalid retransmission timeout bounds");
  }
  if (!Number.isSafeInteger(config.timeWaitMs) || config.timeWaitMs < 0) {
    throw new Error("TIME-WAIT duration must be a non-negative integer");
  }
  return config;
}
