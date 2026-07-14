export class RttEstimator {
    minRtoMs;
    maxRtoMs;
    haveMeasurement = false;
    srttMs = 0;
    rttvarMs = 0;
    rtoMs;
    consecutiveRtos = 0;
    constructor(initialRtoMs, minRtoMs, maxRtoMs) {
        this.minRtoMs = minRtoMs;
        this.maxRtoMs = maxRtoMs;
        this.rtoMs = Math.min(maxRtoMs, Math.max(minRtoMs, initialRtoMs));
    }
    timeoutMs() {
        return this.rtoMs;
    }
    sample(sampleMs) {
        const sample = Math.max(1, sampleMs);
        if (this.haveMeasurement) {
            const difference = Math.abs(this.srttMs - sample);
            this.rttvarMs = Math.ceil((this.rttvarMs * 3 + difference) / 4);
            this.srttMs = Math.ceil((this.srttMs * 7 + sample) / 8);
        }
        else {
            this.haveMeasurement = true;
            this.srttMs = sample;
            this.rttvarMs = Math.floor(sample / 2);
        }
        this.rtoMs = Math.min(this.maxRtoMs, Math.max(this.minRtoMs, this.srttMs + Math.max(5, this.rttvarMs * 4)));
        this.consecutiveRtos = 0;
    }
    onTimeout() {
        this.rtoMs = Math.min(this.maxRtoMs, this.rtoMs * 2);
        this.consecutiveRtos += 1;
        if (this.consecutiveRtos >= 3) {
            this.haveMeasurement = false;
            this.consecutiveRtos = 0;
        }
    }
}
