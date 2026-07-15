#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ConnectionKey<P> {
    peer: P,
    local_port: u16,
    remote_port: u16,
}

struct Connection<P> {
    peer: P,
    local_port: u16,
    remote_port: u16,
    state: State,
    send_una: u32,
    send_nxt: u32,
    recv_nxt: u32,
    remote_window: usize,
    mss: usize,
    receive_capacity: usize,
    send_queue: VecDeque<u8>,
    recv_queue: VecDeque<u8>,
    reassembly: Vec<ReassemblySegment>,
    unacked: VecDeque<TrackedSegment>,
    rtt: RttEstimator,
    reno: Reno,
    duplicate_acks: u8,
    close_requested: bool,
    next_zero_window_probe_ms: Option<u64>,
    zero_window_probes: u8,
    read_closed: bool,
    fin_wait_2_until_ms: Option<u64>,
    time_wait_until_ms: Option<u64>,
}

struct Update {
    segments: Vec<Segment>,
    accepted: bool,
    closed: bool,
}

impl Update {
    fn open(segments: Vec<Segment>) -> Self {
        Self {
            segments,
            accepted: false,
            closed: false,
        }
    }
}

struct AckOutcome {
    fin_acked: bool,
    retransmit: Option<Segment>,
}
