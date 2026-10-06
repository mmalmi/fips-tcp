import { after } from "./seq.js";
export class RttEstimator {
    minRtoMs;
    maxRtoMs;
    haveMeasurement = false;
    srttMs = 0;
    rttvarMs = 0;
    rtoMs;
    consecutiveRtos = 0;
    sampleAfter;
    constructor(initialRtoMs, minRtoMs, maxRtoMs) {
        this.minRtoMs = minRtoMs;
        this.maxRtoMs = maxRtoMs;
        this.rtoMs = Math.min(maxRtoMs, Math.max(minRtoMs, initialRtoMs));
    }
    timeoutMs() {
        return this.rtoMs;
    }
    canSample(endSeq) {
        return this.sampleAfter === undefined || after(endSeq, this.sampleAfter);
    }
    onRetransmit(endSeq) {
        // An old-flight cumulative ACK cannot provide a fresh RTT sample.
        this.sampleAfter = endSeq;
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
        this.sampleAfter = undefined;
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
