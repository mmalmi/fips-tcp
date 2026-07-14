export class PersistTimer {
    nextMs;
    probes = 0;
    update(window, nowMs, rtoMs) {
        if (window === 0)
            this.nextMs ??= nowMs + rtoMs;
        else {
            this.nextMs = undefined;
            this.probes = 0;
        }
    }
    action(nowMs, config) {
        this.nextMs ??= nowMs + config.initialRtoMs;
        if (nowMs < this.nextMs)
            return "wait";
        return this.probes >= config.maxRetransmissions ? "abort" : "probe";
    }
    onProbe(nowMs, config) {
        this.probes += 1;
        const delay = Math.min(config.maxRtoMs, config.initialRtoMs * 2 ** Math.min(16, this.probes));
        this.nextMs = nowMs + delay;
    }
}
