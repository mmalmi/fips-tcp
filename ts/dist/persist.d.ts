import { Config } from "./types.js";
export declare class PersistTimer {
    private nextMs;
    private probes;
    update(window: number, nowMs: number, rtoMs: number): void;
    action(nowMs: number, config: Config): "wait" | "probe" | "abort";
    onProbe(nowMs: number, config: Config): void;
}
