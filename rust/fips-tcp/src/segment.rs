use crate::wire::{FIPS_VERSION, Flags, Segment, TcpOption};

pub(crate) struct SegmentHeader {
    pub(crate) local_port: u16,
    pub(crate) remote_port: u16,
    pub(crate) seq: u32,
    pub(crate) ack: u32,
    pub(crate) window: u16,
    pub(crate) mss: u16,
    pub(crate) flags: Flags,
}

pub(crate) fn build_segment(header: SegmentHeader, payload: Vec<u8>) -> Segment {
    let mut segment = Segment::new(header.local_port, header.remote_port, header.seq);
    segment.flags = header.flags;
    segment.window = header.window;
    segment.payload = payload;
    if header.flags.contains(Flags::ACK) {
        segment.ack = Some(header.ack);
    }
    if header.flags.contains(Flags::SYN) {
        segment.options = vec![
            TcpOption::MaxSegmentSize(header.mss),
            TcpOption::FipsVersion {
                version: FIPS_VERSION,
                reserved: 0,
            },
        ];
    }
    segment
}
