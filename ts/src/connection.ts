import { Reno } from "./reno.js";
import { RttEstimator } from "./rtt.js";
import { buildSegment } from "./segment.js";
import { after, before, beforeOrEqual, distance, inClosedInterval, u32 } from "./seq.js";
import { Config, State } from "./types.js";
import { FIPS_VERSION, FlagSet, Flags, Segment } from "./wire.js";
import { AckOutcome, ConnectionUpdate, openUpdate, ReassemblySegment, reassemblyEnd, TrackedSegment, trackedEnd } from "./connection-types.js";
import { PersistTimer } from "./persist.js";
import { resetAction } from "./reset.js";
import { SendProgress } from "./marker.js";

export class Connection {
  state: State;
  readonly peer: string;
  readonly localPort: number;
  readonly remotePort: number;
  readClosed = false;

  private sendUna: number;
  private sendNxt: number;
  private recvNxt: number;
  private remoteWindow = 0xffff;
  private mss: number;
  private readonly receiveCapacity: number;
  private readonly sendQueue: number[] = [];
  private readonly recvQueue: number[] = [];
  private readonly reassembly: ReassemblySegment[] = [];
  private readonly unacked: TrackedSegment[] = [];
  private readonly rtt: RttEstimator;
  private readonly reno: Reno;
  private duplicateAcks = 0;
  private closeRequested = false;
  private readonly persist = new PersistTimer();
  readonly sendProgress = new SendProgress();
  private finWait2UntilMs: number | undefined;
  private timeWaitUntilMs: number | undefined;

  private constructor(
    peer: string,
    localPort: number,
    remotePort: number,
    state: State,
    sendIsn: number,
    recvNxt: number,
    config: Config,
  ) {
    this.peer = peer;
    this.localPort = localPort;
    this.remotePort = remotePort;
    this.state = state;
    this.sendUna = sendIsn;
    this.sendNxt = sendIsn;
    this.recvNxt = recvNxt;
    this.mss = config.mss;
    this.receiveCapacity = config.receiveBuffer;
    this.rtt = new RttEstimator(config.initialRtoMs, config.minRtoMs, config.maxRtoMs);
    this.reno = new Reno(this.mss);
  }

  static client(
    peer: string,
    localPort: number,
    remotePort: number,
    isn: number,
    nowMs: number,
    config: Config,
  ): [Connection, Segment[]] {
    const connection = new Connection(peer, localPort, remotePort, State.SynSent, isn, 0, config);
    return [connection, [connection.sendTracked(new FlagSet(Flags.Syn), new Uint8Array(), nowMs)]];
  }

  static server(
    peer: string,
    syn: Segment,
    isn: number,
    nowMs: number,
    config: Config,
  ): [Connection, Segment[]] {
    const connection = new Connection(peer, syn.dstPort, syn.srcPort, State.SynReceived, isn, u32(syn.seq + 1), config);
    connection.updateRemoteWindow(syn.window, nowMs);
    connection.negotiateMss(syn, config);
    return [
      connection,
      [connection.sendTracked(new FlagSet(Flags.Syn | Flags.Ack), new Uint8Array(), nowMs)],
    ];
  }

  onSegment(segment: Segment, nowMs: number, config: Config): ConnectionUpdate {
    if (segment.flags.has(Flags.Rst)) {
      const action = resetAction(
        this.state, segment.seq, segment.ack, this.sendUna, this.sendNxt, this.recvNxt, this.availableWindow(),
      );
      if (action === "close") return { segments: [], accepted: false, closed: true };
      return openUpdate(action === "challenge" ? [this.ackSegment()] : []);
    }

    if (this.state === State.SynSent) {
      if (
        segment.flags.has(Flags.Syn) &&
        segment.flags.has(Flags.Ack) &&
        segment.ack === this.sendNxt &&
        segment.supportsFipsVersion(FIPS_VERSION)
      ) {
        this.updateRemoteWindow(segment.window, nowMs);
        this.negotiateMss(segment, config);
        this.applyAck(this.sendNxt, nowMs, false);
        this.recvNxt = u32(segment.seq + 1);
        this.state = State.Established;
        return openUpdate([this.ackSegment()]);
      }
      return openUpdate();
    }

    if (this.state === State.SynReceived) {
      if (segment.flags.has(Flags.Syn) && !segment.flags.has(Flags.Ack) && u32(segment.seq + 1) === this.recvNxt) {
        const retransmit = this.retransmitOldest(nowMs, false);
        return openUpdate(retransmit === undefined ? [] : [retransmit]);
      }
      if (segment.ack !== this.sendNxt) return openUpdate();
      this.applyAck(this.sendNxt, nowMs, false);
      this.updateRemoteWindow(segment.window, nowMs);
      this.state = State.Established;
      const update = openUpdate();
      update.accepted = true;
      if (segment.payload.length > 0 || segment.flags.has(Flags.Fin)) {
        this.receiveStreamData(segment, nowMs, config, update.segments);
      }
      update.segments.push(...this.flushData(nowMs));
      return update;
    }

    const output: Segment[] = [];
    if (segment.ack !== undefined) {
      const duplicate = segment.ack === this.sendUna && segment.payload.length === 0;
      const outcome = this.applyAck(segment.ack, nowMs, duplicate);
      if (outcome.retransmit !== undefined) output.push(outcome.retransmit);
      if (outcome.finAcked) {
        if (this.state === State.FinWait1) {
          this.state = State.FinWait2;
          this.finWait2UntilMs = deadlineAfter(nowMs, config.finWait2Ms);
        } else if (this.state === State.Closing) this.enterTimeWait(nowMs, config);
        else if (this.state === State.LastAck) {
          return { segments: output, accepted: false, closed: true };
        }
      }
      if (inClosedInterval(segment.ack, this.sendUna, this.sendNxt)) {
        this.updateRemoteWindow(segment.window, nowMs);
      }
    }

    if (segment.payload.length > 0 || segment.flags.has(Flags.Fin)) {
      this.receiveStreamData(segment, nowMs, config, output);
    }
    output.push(...this.flushData(nowMs));
    return openUpdate(output);
  }

  write(bytes: Uint8Array, nowMs: number, config: Config): [number, Segment[]] {
    if (
      (this.state !== State.Established && this.state !== State.CloseWait) ||
      this.closeRequested
    ) {
      throw new Error(`write is invalid in ${this.state}`);
    }
    const buffered =
      this.sendQueue.length + this.unacked.reduce((sum, segment) => sum + segment.payload.length, 0);
    const accepted = Math.min(bytes.length, Math.max(0, config.sendBuffer - buffered));
    for (const byte of bytes.subarray(0, accepted)) this.sendQueue.push(byte);
    this.sendProgress.accept(accepted);
    return [accepted, this.flushData(nowMs)];
  }

  read(max: number): [Uint8Array, Segment[]] {
    if (!Number.isSafeInteger(max) || max < 0) throw new Error("read maximum must be non-negative");
    const previousWindow = this.availableWindow();
    const count = Math.min(max, this.recvQueue.length);
    const bytes = Uint8Array.from(this.recvQueue.splice(0, count));
    const shouldUpdate =
      count > 0 &&
      this.availableWindow() > previousWindow &&
      this.state !== State.SynSent &&
      this.state !== State.SynReceived &&
      this.state !== State.TimeWait;
    return [bytes, shouldUpdate ? [this.ackSegment()] : []];
  }

  close(nowMs: number, config: Config): ConnectionUpdate {
    if (this.state === State.Established || this.state === State.CloseWait) {
      this.closeRequested = true;
      return openUpdate(this.flushData(nowMs));
    }
    if (this.state === State.SynSent || this.state === State.SynReceived) {
      return { segments: [], accepted: false, closed: true };
    }
    void config;
    return openUpdate();
  }

  poll(nowMs: number, config: Config): ConnectionUpdate {
    const closeDeadline = this.state === State.FinWait2
      ? this.finWait2UntilMs
      : this.state === State.TimeWait ? this.timeWaitUntilMs : undefined;
    if (closeDeadline !== undefined && nowMs >= closeDeadline) {
      return { segments: [], accepted: false, closed: true };
    }
    const segments: Segment[] = [];
    if (this.remoteWindow === 0 && this.hasZeroWindowWork()) {
      const action = this.persist.action(nowMs, config);
      if (action === "abort") return { segments, accepted: false, closed: true };
      if (action === "probe") {
        const probe = this.zeroWindowProbe(nowMs);
        if (probe !== undefined) {
          segments.push(probe);
          this.persist.onProbe(nowMs, config);
        }
      }
      return openUpdate(segments);
    }
    const oldest = this.unacked[0];
    if (oldest !== undefined && nowMs >= oldest.sentAtMs + this.rtt.timeoutMs()) {
      if (oldest.transmissions >= config.maxRetransmissions) {
        return { segments, accepted: false, closed: true };
      }
      this.reno.onTimeout(distance(this.sendUna, this.sendNxt));
      this.rtt.onTimeout();
      const retransmit = this.retransmitOldest(nowMs, true);
      if (retransmit !== undefined) segments.push(retransmit);
    }
    segments.push(...this.flushData(nowMs));
    return openUpdate(segments);
  }

  private applyAck(ack: number, nowMs: number, duplicateCandidate: boolean): AckOutcome {
    if (after(ack, this.sendNxt) || before(ack, this.sendUna)) return { finAcked: false };
    if (ack === this.sendUna) {
      if (duplicateCandidate && this.unacked.length > 0) {
        this.duplicateAcks = Math.min(0xff, this.duplicateAcks + 1);
        this.reno.onDuplicateAck();
        if (this.duplicateAcks === 3) {
          this.reno.onFastLoss(distance(this.sendUna, this.sendNxt));
          const retransmit = this.retransmitOldest(nowMs, false);
          return retransmit === undefined ? { finAcked: false } : { finAcked: false, retransmit };
        }
      }
      return { finAcked: false };
    }

    this.duplicateAcks = 0;
    let ackedPayload = 0;
    let finAcked = false;
    let rttSample: number | undefined;
    while (this.unacked[0] !== undefined && beforeOrEqual(trackedEnd(this.unacked[0]), ack)) {
      const tracked = this.unacked.shift()!;
      ackedPayload += tracked.payload.length;
      finAcked ||= tracked.flags.has(Flags.Fin);
      if (!tracked.retransmitted) rttSample = Math.max(0, nowMs - tracked.sentAtMs);
    }
    const first = this.unacked[0];
    if (
      first !== undefined &&
      before(first.seq, ack) &&
      before(ack, trackedEnd(first)) &&
      !first.flags.has(Flags.Syn) &&
      !first.flags.has(Flags.Fin)
    ) {
      const count = Math.min(distance(first.seq, ack), first.payload.length);
      first.payload = first.payload.slice(count);
      first.seq = ack;
      ackedPayload += count;
    }
    this.sendUna = ack;
    if (rttSample !== undefined) this.rtt.sample(rttSample);
    this.reno.onAck(ackedPayload);
    this.sendProgress.acknowledge(ackedPayload);
    return { finAcked };
  }

  private receiveStreamData(segment: Segment, nowMs: number, config: Config, output: Segment[]): void {
    this.insertReceived(segment.seq, segment.payload, segment.flags.has(Flags.Fin), config);
    this.drainReassembly(nowMs, config);
    output.push(this.ackSegment());
  }

  private insertReceived(seq: number, payload: Uint8Array, fin: boolean, config: Config): void {
    const originalEnd = u32(seq + payload.length);
    let start = seq;
    let data = payload;
    if (before(start, this.recvNxt)) {
      const trim = distance(start, this.recvNxt);
      if (trim >= data.length) {
        data = new Uint8Array();
        start = this.recvNxt;
      } else {
        data = data.subarray(trim);
        start = this.recvNxt;
      }
    }
    const window = this.availableWindow();
    const offset = distance(this.recvNxt, start);
    if (after(start, this.recvNxt) && offset >= window) return;
    const allowed = Math.min(data.length, Math.max(0, window - offset));
    const keptFin = fin && allowed === data.length && originalEnd === u32(start + data.length);
    const chunk: ReassemblySegment = { seq: start, payload: data.slice(0, allowed), fin: keptFin };
    if (chunk.payload.length === 0 && !chunk.fin) return;
    if (this.reassembly.some((existing) => existing.seq === chunk.seq && reassemblyEnd(existing) === reassemblyEnd(chunk))) {
      return;
    }
    if (this.reassembly.length < config.maxReassemblySegments) this.reassembly.push(chunk);
  }

  private drainReassembly(nowMs: number, config: Config): void {
    for (;;) {
      for (let index = this.reassembly.length - 1; index >= 0; index -= 1) {
        const segment = this.reassembly[index]!;
        if (
          beforeOrEqual(reassemblyEnd(segment), this.recvNxt) &&
          !(segment.fin && reassemblyEnd(segment) === u32(this.recvNxt + 1))
        ) {
          this.reassembly.splice(index, 1);
        }
      }
      const index = this.reassembly.findIndex(
        (segment) => segment.seq === this.recvNxt || before(segment.seq, this.recvNxt),
      );
      if (index < 0) break;
      const segment = this.reassembly.splice(index, 1)[0]!;
      if (before(segment.seq, this.recvNxt)) {
        const trim = Math.min(distance(segment.seq, this.recvNxt), segment.payload.length);
        segment.payload = segment.payload.slice(trim);
        segment.seq = this.recvNxt;
      }
      const accepted = Math.min(config.receiveBuffer - this.recvQueue.length, segment.payload.length);
      for (const byte of segment.payload.subarray(0, accepted)) this.recvQueue.push(byte);
      this.recvNxt = u32(this.recvNxt + accepted);
      segment.payload = segment.payload.slice(accepted);
      if (segment.payload.length > 0) {
        segment.seq = this.recvNxt;
        this.reassembly.push(segment);
        break;
      }
      if (segment.fin) {
        this.recvNxt = u32(this.recvNxt + 1);
        this.onRemoteFin(nowMs, config);
      }
    }
  }

  private onRemoteFin(nowMs: number, config: Config): void {
    this.readClosed = true;
    if (this.state === State.Established) this.state = State.CloseWait;
    else if (this.state === State.FinWait1) this.state = State.Closing;
    else if (this.state === State.FinWait2) this.enterTimeWait(nowMs, config);
  }

  private enterTimeWait(nowMs: number, config: Config): void {
    this.state = State.TimeWait;
    this.finWait2UntilMs = undefined;
    this.timeWaitUntilMs = deadlineAfter(nowMs, config.timeWaitMs);
  }

  resetSegment(): Segment {
    return buildSegment(this.localPort, this.remotePort, this.sendNxt, this.recvNxt, 0, this.mss, new FlagSet(Flags.Rst), new Uint8Array());
  }

  private sendTracked(flags: FlagSet, payload: Uint8Array, nowMs: number): Segment {
    const tracked: TrackedSegment = {
      seq: this.sendNxt,
      flags,
      payload: payload.slice(),
      sentAtMs: nowMs,
      retransmitted: false,
      transmissions: 1,
    };
    this.sendNxt = trackedEnd(tracked);
    const segment = this.segmentFor(tracked);
    this.unacked.push(tracked);
    return segment;
  }

  private retransmitOldest(nowMs: number, timeout: boolean): Segment | undefined {
    const tracked = this.unacked[0];
    if (tracked === undefined) return undefined;
    tracked.sentAtMs = nowMs;
    tracked.retransmitted = true;
    tracked.transmissions = Math.min(0xff, tracked.transmissions + 1);
    if (timeout) this.duplicateAcks = 0;
    return buildSegment(
      this.localPort,
      this.remotePort,
      tracked.seq,
      this.recvNxt,
      this.availableWindowU16(),
      this.mss,
      tracked.flags,
      tracked.payload,
    );
  }

  private flushData(nowMs: number): Segment[] {
    if (this.state !== State.Established && this.state !== State.CloseWait) return [];
    const output: Segment[] = [];
    for (;;) {
      const inFlight = distance(this.sendUna, this.sendNxt);
      const window = Math.min(this.remoteWindow, this.reno.window());
      const available = Math.max(0, window - inFlight);
      if (available === 0 || this.sendQueue.length === 0) break;
      const count = Math.min(available, this.mss, this.sendQueue.length);
      const payload = Uint8Array.from(this.sendQueue.splice(0, count));
      output.push(this.sendTracked(new FlagSet(Flags.Ack | Flags.Psh), payload, nowMs));
    }
    if (this.closeRequested && this.sendQueue.length === 0) {
      const inFlight = distance(this.sendUna, this.sendNxt);
      const available = Math.max(0, Math.min(this.remoteWindow, this.reno.window()) - inFlight);
      if (available > 0) {
        this.closeRequested = false;
        this.state = this.state === State.Established ? State.FinWait1 : State.LastAck;
        output.push(
          this.sendTracked(new FlagSet(Flags.Fin | Flags.Ack), new Uint8Array(), nowMs),
        );
      }
    }
    return output;
  }

  private hasZeroWindowWork(): boolean {
    return (
      this.closeRequested ||
      this.sendQueue.length > 0 ||
      this.unacked.some((segment) => segment.payload.length > 0)
    );
  }

  private zeroWindowProbe(nowMs: number): Segment | undefined {
    const unacked = this.unacked.find((segment) => segment.payload.length > 0);
    if (unacked !== undefined) {
      return buildSegment(
        this.localPort,
        this.remotePort,
        unacked.seq,
        this.recvNxt,
        this.availableWindowU16(),
        this.mss,
        new FlagSet(Flags.Ack | Flags.Psh),
        unacked.payload.slice(0, 1),
      );
    }
    const byte = this.sendQueue.shift();
    return byte === undefined
      ? undefined
      : this.sendTracked(new FlagSet(Flags.Ack | Flags.Psh), Uint8Array.of(byte), nowMs);
  }

  private updateRemoteWindow(window: number, nowMs: number): void {
    this.remoteWindow = window;
    this.persist.update(window, nowMs, this.rtt.timeoutMs());
  }

  private ackSegment(): Segment {
    return buildSegment(
      this.localPort,
      this.remotePort,
      this.sendNxt,
      this.recvNxt,
      this.availableWindowU16(),
      this.mss,
      new FlagSet(Flags.Ack),
      new Uint8Array(),
    );
  }

  private segmentFor(tracked: TrackedSegment): Segment {
    return buildSegment(
      this.localPort,
      this.remotePort,
      tracked.seq,
      this.recvNxt,
      this.availableWindowU16(),
      this.mss,
      tracked.flags,
      tracked.payload,
    );
  }

  private negotiateMss(segment: Segment, config: Config): void {
    this.mss = Math.max(1, Math.min(segment.maxSegmentSize() ?? 1024, config.mss));
    this.reno.setMss(this.mss);
  }

  private availableWindow(): number {
    const reassemblyBytes = this.reassembly.reduce((sum, segment) => sum + segment.payload.length, 0);
    return Math.max(0, this.receiveCapacity - this.recvQueue.length - reassemblyBytes);
  }

  private availableWindowU16(): number {
    return Math.min(0xffff, this.availableWindow());
  }
}

const deadlineAfter = (nowMs: number, durationMs: number): number => Math.min(Number.MAX_SAFE_INTEGER, nowMs + durationMs);
