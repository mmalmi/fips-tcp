use crate::wire::Flags;

#[derive(Clone, Debug)]
pub(crate) struct TrackedSegment {
    pub(crate) seq: u32,
    pub(crate) flags: Flags,
    pub(crate) payload: Vec<u8>,
    pub(crate) sent_at_ms: u64,
    pub(crate) retransmitted: bool,
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
