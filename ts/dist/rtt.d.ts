export declare class RttEstimator {
    private readonly minRtoMs;
    private readonly maxRtoMs;
    private haveMeasurement;
    private srttMs;
    private rttvarMs;
    private rtoMs;
    private consecutiveRtos;
    private sampleAfter;
    constructor(initialRtoMs: number, minRtoMs: number, maxRtoMs: number);
    timeoutMs(): number;
    canSample(endSeq: number): boolean;
    onRetransmit(endSeq: number): void;
    sample(sampleMs: number): void;
    onTimeout(): void;
}
