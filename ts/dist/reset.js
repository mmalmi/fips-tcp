import { after, distance } from "./seq.js";
import { State } from "./types.js";
export function resetAction(state, sequence, acknowledgment, sendUna, sendNxt, recvNxt, recvWindow) {
    if (state === State.SynSent) {
        return acknowledgment !== undefined && after(acknowledgment, sendUna) && !after(acknowledgment, sendNxt)
            ? "close"
            : "drop";
    }
    if (sequence === recvNxt)
        return "close";
    return recvWindow > 0 && distance(recvNxt, sequence) < recvWindow ? "challenge" : "drop";
}
