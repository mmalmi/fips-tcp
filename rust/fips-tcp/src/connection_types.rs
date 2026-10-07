use crate::wire::{Flags, Segment};

// Reopening receive capacity is flow control, not evidence of loss. Existing
// FIPS receivers shrink their window while buffering out-of-order data, so
// retain those ACKs as loss signals. Compare before updating the remote window.
pub(crate) fn is_duplicate_ack_candidate(
    segment: &Segment,
    send_una: u32,
    remote_window: usize,
) -> bool {
    segment.ack == Some(send_una)
        && segment.payload.is_empty()
        && !segment.flags.contains(Flags::SYN)
        && !segment.flags.contains(Flags::FIN)
        && usize::from(segment.window) <= remote_window
}

#[derive(Clone, Debug)]
pub(crate) struct TrackedSegment {
    pub(crate) seq: u32,
    pub(crate) flags: Flags,
    pub(crate) payload: Vec<u8>,
    pub(crate) sent_at_ms: u64,
    pub(crate) transmissions: u8,
}

impl TrackedSegment {
    pub(crate) fn end_seq(&self) -> u32 {
        self.seq
            .wrapping_add(self.payload.len() as u32)
            .wrapping_add(u32::from(self.flags.contains(Flags::SYN)))
            .wrapping_add(u32::from(self.flags.contains(Flags::FIN)))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ReassemblySegment {
    pub(crate) seq: u32,
    pub(crate) payload: Vec<u8>,
    pub(crate) fin: bool,
}

impl ReassemblySegment {
    pub(crate) fn end_seq(&self) -> u32 {
        self.seq
            .wrapping_add(self.payload.len() as u32)
            .wrapping_add(u32::from(self.fin))
    }
}
