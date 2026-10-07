import { readFileSync } from "node:fs";

export interface WindowUpdateVector {
  name: string;
  initialSequence: number;
  readChunks: number[];
  duplicateAcksBeforeRead?: number;
}

export const windowUpdateVectors = JSON.parse(readFileSync(
  new URL("../../rust/fips-tcp/protocol/window-update-vectors.json", import.meta.url), "utf8",
)) as WindowUpdateVector[];
