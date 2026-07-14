export declare class Reno {
    private mss;
    private cwnd;
    private ssthresh;
    private inFastRecovery;
    private inRtoRecovery;
    constructor(mss: number);
    window(): number;
    setMss(mss: number): void;
    onAck(acked: number): void;
    onDuplicateAck(): void;
    onFastLoss(inFlight: number): void;
    onTimeout(inFlight: number): void;
}
