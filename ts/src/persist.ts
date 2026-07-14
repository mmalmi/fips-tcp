import { Config } from "./types.js";

export class PersistTimer {
  private nextMs: number | undefined;
  private probes = 0;

  update(window: number, nowMs: number, rtoMs: number): void {
    if (window === 0) this.nextMs ??= nowMs + rtoMs;
    else {
      this.nextMs = undefined;
      this.probes = 0;
    }
  }

  action(nowMs: number, config: Config): "wait" | "probe" | "abort" {
    this.nextMs ??= nowMs + config.initialRtoMs;
    if (nowMs < this.nextMs) return "wait";
    return this.probes >= config.maxRetransmissions ? "abort" : "probe";
  }

  onProbe(nowMs: number, config: Config): void {
    this.probes += 1;
    const delay = Math.min(
      config.maxRtoMs,
      config.initialRtoMs * 2 ** Math.min(16, this.probes),
    );
    this.nextMs = nowMs + delay;
  }
}
