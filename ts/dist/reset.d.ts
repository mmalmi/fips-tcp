import { State } from "./types.js";
export type ResetAction = "close" | "challenge" | "drop";
export declare function resetAction(state: State, sequence: number, acknowledgment: number | undefined, sendUna: number, sendNxt: number, recvNxt: number, recvWindow: number): ResetAction;
