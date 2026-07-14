export const HEADER_LEN = 20;
export const FIPS_OPTION_KIND = 254;
export const FIPS_VERSION = 1;

export enum Flags {
  Fin = 0x001,
  Syn = 0x002,
  Rst = 0x004,
  Psh = 0x008,
  Ack = 0x010,
  Ece = 0x040,
  Cwr = 0x080,
}

const SUPPORTED_FLAGS =
  Flags.Fin | Flags.Syn | Flags.Rst | Flags.Psh | Flags.Ack | Flags.Ece | Flags.Cwr;

export class FlagSet {
  constructor(readonly bits = 0) {
    if ((bits & ~SUPPORTED_FLAGS) !== 0) throw new Error(`unsupported TCP flags: ${bits}`);
  }

  has(flag: Flags): boolean {
    return (this.bits & flag) === flag;
  }

  with(flag: Flags): FlagSet {
    return new FlagSet(this.bits | flag);
  }
}

export enum TcpOptionKind {
  EndOfList = "end",
  NoOperation = "nop",
  MaxSegmentSize = "mss",
  FipsVersion = "fips-version",
  Unknown = "unknown",
}

export type TcpOption =
  | { kind: TcpOptionKind.EndOfList }
  | { kind: TcpOptionKind.NoOperation }
  | { kind: TcpOptionKind.MaxSegmentSize; value: number }
  | { kind: TcpOptionKind.FipsVersion; version: number; reserved: number }
  | { kind: TcpOptionKind.Unknown; wireKind: number; data: Uint8Array };

export interface SegmentInit {
  srcPort: number;
  dstPort: number;
  seq: number;
  ack?: number;
  flags?: FlagSet;
  window?: number;
  options?: TcpOption[];
  payload?: Uint8Array;
}

const checkU16 = (value: number, name: string): void => {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff) {
    throw new Error(`${name} must be a u16`);
  }
};

const checkU32 = (value: number, name: string): void => {
  if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new Error(`${name} must be a u32`);
  }
};

export class Segment {
  readonly srcPort: number;
  readonly dstPort: number;
  readonly seq: number;
  readonly ack: number | undefined;
  readonly flags: FlagSet;
  readonly window: number;
  readonly options: TcpOption[];
  readonly payload: Uint8Array;

  constructor(init: SegmentInit) {
    checkU16(init.srcPort, "source port");
    checkU16(init.dstPort, "destination port");
    checkU32(init.seq, "sequence number");
    if (init.ack !== undefined) checkU32(init.ack, "acknowledgment number");
    this.srcPort = init.srcPort;
    this.dstPort = init.dstPort;
    this.seq = init.seq >>> 0;
    this.ack = init.ack;
    this.flags = init.flags ?? new FlagSet();
    this.window = init.window ?? 0xffff;
    this.options = init.options?.map(cloneOption) ?? [];
    this.payload = init.payload?.slice() ?? new Uint8Array();
  }

  fipsVersion(): number | undefined {
    return this.options.find((option) => option.kind === TcpOptionKind.FipsVersion)?.version;
  }

  supportsFipsVersion(expected: number): boolean {
    return this.options.some(
      (option) =>
        option.kind === TcpOptionKind.FipsVersion &&
        option.version === expected &&
        option.reserved === 0,
    );
  }

  maxSegmentSize(): number | undefined {
    return this.options.find((option) => option.kind === TcpOptionKind.MaxSegmentSize)?.value;
  }

  sequenceLength(): number {
    return this.payload.length + Number(this.flags.has(Flags.Syn)) + Number(this.flags.has(Flags.Fin));
  }

  encode(): Uint8Array {
    if (this.srcPort === 0 || this.dstPort === 0) throw new Error("TCP/FIPS ports must be non-zero");
    if (this.flags.has(Flags.Ack) !== (this.ack !== undefined)) {
      throw new Error("ACK flag and ACK number disagree");
    }
    const encodedOptions = this.options.map(encodeOption);
    const optionsLength = encodedOptions.reduce((sum, value) => sum + value.length, 0);
    const paddedOptionsLength = Math.ceil(optionsLength / 4) * 4;
    const headerLength = HEADER_LEN + paddedOptionsLength;
    if (headerLength > 60) throw new Error("TCP/FIPS header exceeds 60 bytes");
    const output = new Uint8Array(headerLength + this.payload.length);
    const view = new DataView(output.buffer);
    view.setUint16(0, this.srcPort);
    view.setUint16(2, this.dstPort);
    view.setUint32(4, this.seq);
    view.setUint32(8, this.ack ?? 0);
    view.setUint16(12, ((headerLength / 4) << 12) | this.flags.bits);
    view.setUint16(14, this.window);
    let offset = HEADER_LEN;
    for (const option of encodedOptions) {
      output.set(option, offset);
      offset += option.length;
    }
    output.set(this.payload, headerLength);
    return output;
  }

  static decode(bytes: Uint8Array): Segment {
    if (bytes.length < HEADER_LEN) throw new Error("truncated TCP/FIPS segment");
    const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const srcPort = view.getUint16(0);
    const dstPort = view.getUint16(2);
    if (srcPort === 0 || dstPort === 0) throw new Error("TCP/FIPS ports must be non-zero");
    const offsetAndFlags = view.getUint16(12);
    const headerLength = (offsetAndFlags >>> 12) * 4;
    if (headerLength < HEADER_LEN || headerLength > 60 || headerLength > bytes.length) {
      throw new Error("invalid TCP/FIPS header length");
    }
    if (view.getUint16(16) !== 0) throw new Error("TCP/FIPS checksum must be zero");
    if (view.getUint16(18) !== 0) throw new Error("TCP/FIPS urgent data is unsupported");
    const flags = new FlagSet(offsetAndFlags & 0x1ff);
    const options = decodeOptions(bytes.subarray(HEADER_LEN, headerLength));
    return new Segment({
      srcPort,
      dstPort,
      seq: view.getUint32(4),
      ...(flags.has(Flags.Ack) ? { ack: view.getUint32(8) } : {}),
      flags,
      window: view.getUint16(14),
      options,
      payload: bytes.subarray(headerLength),
    });
  }
}

const cloneOption = (option: TcpOption): TcpOption =>
  option.kind === TcpOptionKind.Unknown ? { ...option, data: option.data.slice() } : { ...option };

function encodeOption(option: TcpOption): Uint8Array {
  switch (option.kind) {
    case TcpOptionKind.EndOfList:
      return Uint8Array.of(0);
    case TcpOptionKind.NoOperation:
      return Uint8Array.of(1);
    case TcpOptionKind.MaxSegmentSize: {
      checkU16(option.value, "MSS");
      return Uint8Array.of(2, 4, option.value >>> 8, option.value & 0xff);
    }
    case TcpOptionKind.FipsVersion:
      return Uint8Array.of(FIPS_OPTION_KIND, 4, option.version, option.reserved);
    case TcpOptionKind.Unknown: {
      if (option.wireKind <= 1 || option.wireKind > 255 || option.data.length > 253) {
        throw new Error("malformed TCP option");
      }
      const output = new Uint8Array(option.data.length + 2);
      output.set([option.wireKind, output.length]);
      output.set(option.data, 2);
      return output;
    }
  }
}

function decodeOptions(bytes: Uint8Array): TcpOption[] {
  const options: TcpOption[] = [];
  for (let offset = 0; offset < bytes.length; ) {
    const kind = bytes[offset];
    if (kind === undefined || kind === 0) break;
    if (kind === 1) {
      options.push({ kind: TcpOptionKind.NoOperation });
      offset += 1;
      continue;
    }
    const length = bytes[offset + 1];
    if (length === undefined || length < 2 || offset + length > bytes.length) {
      throw new Error("malformed TCP option");
    }
    const data = bytes.subarray(offset + 2, offset + length);
    if (kind === 2) {
      if (length !== 4) throw new Error("malformed MSS option");
      options.push({ kind: TcpOptionKind.MaxSegmentSize, value: (data[0]! << 8) | data[1]! });
    } else if (kind === FIPS_OPTION_KIND) {
      if (length !== 4) throw new Error("malformed TCP/FIPS version option");
      options.push({ kind: TcpOptionKind.FipsVersion, version: data[0]!, reserved: data[1]! });
    } else {
      options.push({ kind: TcpOptionKind.Unknown, wireKind: kind, data: data.slice() });
    }
    offset += length;
  }
  return options;
}
