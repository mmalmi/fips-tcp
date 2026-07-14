export declare const HEADER_LEN = 20;
export declare const FIPS_OPTION_KIND = 254;
export declare const FIPS_VERSION = 1;
export declare enum Flags {
    Fin = 1,
    Syn = 2,
    Rst = 4,
    Psh = 8,
    Ack = 16,
    Ece = 64,
    Cwr = 128
}
export declare class FlagSet {
    readonly bits: number;
    constructor(bits?: number);
    has(flag: Flags): boolean;
    with(flag: Flags): FlagSet;
}
export declare enum TcpOptionKind {
    EndOfList = "end",
    NoOperation = "nop",
    MaxSegmentSize = "mss",
    FipsVersion = "fips-version",
    Unknown = "unknown"
}
export type TcpOption = {
    kind: TcpOptionKind.EndOfList;
} | {
    kind: TcpOptionKind.NoOperation;
} | {
    kind: TcpOptionKind.MaxSegmentSize;
    value: number;
} | {
    kind: TcpOptionKind.FipsVersion;
    version: number;
    reserved: number;
} | {
    kind: TcpOptionKind.Unknown;
    wireKind: number;
    data: Uint8Array;
};
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
export declare class Segment {
    readonly srcPort: number;
    readonly dstPort: number;
    readonly seq: number;
    readonly ack: number | undefined;
    readonly flags: FlagSet;
    readonly window: number;
    readonly options: TcpOption[];
    readonly payload: Uint8Array;
    constructor(init: SegmentInit);
    fipsVersion(): number | undefined;
    supportsFipsVersion(expected: number): boolean;
    maxSegmentSize(): number | undefined;
    sequenceLength(): number;
    encode(): Uint8Array;
    static decode(bytes: Uint8Array): Segment;
}
