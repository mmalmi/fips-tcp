export class Reno {
    mss;
    cwnd;
    ssthresh = Number.MAX_SAFE_INTEGER;
    inFastRecovery = false;
    inRtoRecovery = false;
    constructor(mss) {
        this.mss = mss;
        this.cwnd = mss * 2;
    }
    window() {
        return Math.max(this.cwnd, this.mss);
    }
    setMss(mss) {
        this.mss = Math.max(1, mss);
        this.cwnd = Math.max(this.cwnd, this.mss);
    }
    onAck(acked) {
        if (acked === 0)
            return;
        this.inRtoRecovery = false;
        if (this.inFastRecovery) {
            this.inFastRecovery = false;
            this.cwnd = this.ssthresh;
            return;
        }
        const increment = this.cwnd < this.ssthresh
            ? Math.min(acked, this.mss)
            : Math.max(1, Math.floor((this.mss * this.mss) / this.cwnd));
        this.cwnd = Math.max(this.mss, this.cwnd + increment);
    }
    onDuplicateAck() {
        if (this.inFastRecovery)
            this.cwnd += this.mss;
    }
    onFastLoss(inFlight) {
        if (this.inFastRecovery)
            return;
        this.ssthresh = Math.max(Math.floor(inFlight / 2), this.mss * 2);
        this.cwnd = this.ssthresh + this.mss * 3;
        this.inFastRecovery = true;
    }
    onTimeout(inFlight) {
        if (!this.inRtoRecovery) {
            this.ssthresh = Math.max(Math.floor(inFlight / 2), this.mss * 2);
            this.inRtoRecovery = true;
        }
        this.cwnd = this.mss;
        this.inFastRecovery = false;
    }
}
