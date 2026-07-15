import { after, distance } from "./seq.js";
import { State } from "./types.js";

export type ResetAction = "close" | "challenge" | "drop";

export function resetAction(
  state: State,
  sequence: number,
  acknowledgment: number | undefined,
  sendUna: number,
  sendNxt: number,
  recvNxt: number,
  recvWindow: number,
): ResetAction {
  if (state === State.SynSent) {
    return acknowledgment !== undefined && after(acknowledgment, sendUna) && !after(acknowledgment, sendNxt)
      ? "close"
      : "drop";
  }
  if (sequence === recvNxt) return "close";
  return recvWindow > 0 && distance(recvNxt, sequence) < recvWindow ? "challenge" : "drop";
}
