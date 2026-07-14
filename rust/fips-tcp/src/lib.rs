#![forbid(unsafe_code)]

//! TCP reliable byte streams carried directly by authenticated FIPS datagrams.

mod connection_types;
mod reno;
mod rtt;
mod segment;
mod seq;
mod stack;
mod types;
pub mod wire;

pub use stack::Stack;
pub use types::{Config, ConnectionId, Outbound, StackError, State};

/// FSP service port reserved for TCP/FIPS segments.
pub const FIPS_TCP_SERVICE_PORT: u16 = 6;
