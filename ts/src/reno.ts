export class Reno {
  private cwnd: number;
  private ssthresh = Number.MAX_SAFE_INTEGER;
  private inFastRecovery = false;
  private inRtoRecovery = false;

  constructor(private mss: number) {
    this.cwnd = mss * 2;
  }

  window(): number {
    return Math.max(this.cwnd, this.mss);
  }

  setMss(mss: number): void {
    this.mss = Math.max(1, mss);
    this.cwnd = Math.max(this.cwnd, this.mss);
  }

  onAck(acked: number): void {
    if (acked === 0) return;
    this.inRtoRecovery = false;
    if (this.inFastRecovery) {
      this.inFastRecovery = false;
      this.cwnd = this.ssthresh;
      return;
    }
    const increment =
      this.cwnd < this.ssthresh
        ? Math.min(acked, this.mss)
        : Math.max(1, Math.floor((this.mss * this.mss) / this.cwnd));
    this.cwnd = Math.max(this.mss, this.cwnd + increment);
  }

  onDuplicateAck(): void {
    if (this.inFastRecovery) this.cwnd += this.mss;
  }

  onFastLoss(inFlight: number): void {
    if (this.inFastRecovery) return;
    this.ssthresh = Math.max(Math.floor(inFlight / 2), this.mss * 2);
    this.cwnd = this.ssthresh + this.mss * 3;
    this.inFastRecovery = true;
  }

  onTimeout(inFlight: number): void {
    if (!this.inRtoRecovery) {
      this.ssthresh = Math.max(Math.floor(inFlight / 2), this.mss * 2);
      this.inRtoRecovery = true;
    }
    this.cwnd = this.mss;
    this.inFastRecovery = false;
  }
}
