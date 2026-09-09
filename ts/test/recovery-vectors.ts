import { readFileSync } from "node:fs";

export interface RecoveryVector {
  name: string;
  initialSequence: number;
  warmupBytes: number;
  chunkBytes: number;
  chunkCount: number;
  droppedPollDeltasMs: number[];
  recoveryPollDeltaMs: number;
  maxRecoveryPumpSteps: number;
  expectedPayloadBytes: number;
}

export const recoveryVectors = JSON.parse(readFileSync(
  new URL("../../rust/fips-tcp/protocol/recovery-vectors.json", import.meta.url), "utf8",
)) as RecoveryVector[];
