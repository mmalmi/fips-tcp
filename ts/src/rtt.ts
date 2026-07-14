export class RttEstimator {
  private haveMeasurement = false;
  private srttMs = 0;
  private rttvarMs = 0;
  private rtoMs: number;
  private consecutiveRtos = 0;

  constructor(
    initialRtoMs: number,
    private readonly minRtoMs: number,
    private readonly maxRtoMs: number,
  ) {
    this.rtoMs = Math.min(maxRtoMs, Math.max(minRtoMs, initialRtoMs));
  }

  timeoutMs(): number {
    return this.rtoMs;
  }

  sample(sampleMs: number): void {
    const sample = Math.max(1, sampleMs);
    if (this.haveMeasurement) {
      const difference = Math.abs(this.srttMs - sample);
      this.rttvarMs = Math.ceil((this.rttvarMs * 3 + difference) / 4);
      this.srttMs = Math.ceil((this.srttMs * 7 + sample) / 8);
    } else {
      this.haveMeasurement = true;
      this.srttMs = sample;
      this.rttvarMs = Math.floor(sample / 2);
    }
    this.rtoMs = Math.min(
      this.maxRtoMs,
      Math.max(this.minRtoMs, this.srttMs + Math.max(5, this.rttvarMs * 4)),
    );
    this.consecutiveRtos = 0;
  }

  onTimeout(): void {
    this.rtoMs = Math.min(this.maxRtoMs, this.rtoMs * 2);
    this.consecutiveRtos += 1;
    if (this.consecutiveRtos >= 3) {
      this.haveMeasurement = false;
      this.consecutiveRtos = 0;
    }
  }
}
