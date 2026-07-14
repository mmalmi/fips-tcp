export declare class RttEstimator {
    private readonly minRtoMs;
    private readonly maxRtoMs;
    private haveMeasurement;
    private srttMs;
    private rttvarMs;
    private rtoMs;
    private consecutiveRtos;
    constructor(initialRtoMs: number, minRtoMs: number, maxRtoMs: number);
    timeoutMs(): number;
    sample(sampleMs: number): void;
    onTimeout(): void;
}
