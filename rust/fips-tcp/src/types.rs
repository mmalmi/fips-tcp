use std::fmt;

use crate::wire::WireError;

#[derive(Clone, Debug)]
pub struct Config {
    pub mss: u16,
    pub receive_buffer: usize,
    pub send_buffer: usize,
    pub max_connections: usize,
    /// Retained connections allowed for one authenticated carrier peer.
    pub max_connections_per_peer: usize,
    pub max_reassembly_segments: usize,
    pub initial_rto_ms: u64,
    pub min_rto_ms: u64,
    pub max_rto_ms: u64,
    pub max_retransmissions: u8,
    /// Maximum retention after the peer acknowledges our FIN without sending its FIN.
    pub fin_wait_2_ms: u64,
    pub time_wait_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mss: 1024,
            receive_buffer: u16::MAX as usize,
            send_buffer: 1024 * 1024,
            max_connections: 1024,
            max_connections_per_peer: usize::MAX,
            max_reassembly_segments: 128,
            initial_rto_ms: 1000,
            min_rto_ms: 200,
            max_rto_ms: 60_000,
            max_retransmissions: 8,
            fin_wait_2_ms: 60_000,
            time_wait_ms: 30_000,
        }
    }
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), StackError> {
        if self.mss == 0 {
            return Err(StackError::InvalidConfig("MSS must be non-zero"));
        }
        if self.receive_buffer == 0 || self.receive_buffer > u16::MAX as usize {
            return Err(StackError::InvalidConfig(
                "receive buffer must be between 1 and 65535 bytes",
            ));
        }
        if self.send_buffer == 0 {
            return Err(StackError::InvalidConfig("send buffer must be non-zero"));
        }
        if self.max_connections == 0
            || self.max_connections_per_peer == 0
            || self.max_reassembly_segments == 0
        {
            return Err(StackError::InvalidConfig(
                "connection limits must be non-zero",
            ));
        }
        if self.min_rto_ms == 0
            || self.initial_rto_ms < self.min_rto_ms
            || self.max_rto_ms < self.initial_rto_ms
        {
            return Err(StackError::InvalidConfig(
                "invalid retransmission timeout bounds",
            ));
        }
        if self.fin_wait_2_ms == 0 {
            return Err(StackError::InvalidConfig(
                "FIN-WAIT-2 duration must be non-zero",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ConnectionId(pub(crate) u64);

impl ConnectionId {
    pub fn get(self) -> u64 {
        self.0
    }

    /// Reconstruct an ID previously returned by the same stack.
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    SynSent,
    SynReceived,
    Established,
    FinWait1,
    FinWait2,
    CloseWait,
    Closing,
    LastAck,
    TimeWait,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outbound<P> {
    pub peer: P,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub enum StackError {
    Wire(WireError),
    InvalidConfig(&'static str),
    ZeroPort,
    AlreadyListening(u16),
    ConnectionExists,
    ConnectionLimit,
    NoEphemeralPort,
    UnknownConnection,
    InvalidState(State),
}

impl fmt::Display for StackError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Wire(error) => write!(formatter, "{error}"),
            Self::InvalidConfig(message) => write!(formatter, "invalid config: {message}"),
            Self::ZeroPort => formatter.write_str("TCP/FIPS ports must be non-zero"),
            Self::AlreadyListening(port) => write!(formatter, "already listening on port {port}"),
            Self::ConnectionExists => formatter.write_str("connection already exists"),
            Self::ConnectionLimit => formatter.write_str("connection limit reached"),
            Self::NoEphemeralPort => formatter.write_str("no ephemeral port available"),
            Self::UnknownConnection => formatter.write_str("unknown connection"),
            Self::InvalidState(state) => write!(formatter, "operation invalid in {state:?}"),
        }
    }
}

impl std::error::Error for StackError {}

impl From<WireError> for StackError {
    fn from(error: WireError) -> Self {
        Self::Wire(error)
    }
}
