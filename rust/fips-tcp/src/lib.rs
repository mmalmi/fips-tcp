#![forbid(unsafe_code)]

//! TCP reliable byte streams carried directly by authenticated FIPS datagrams.

mod connection_types;
mod marker;
mod reno;
mod rtt;
mod segment;
mod seq;
mod stack;
mod types;
pub mod wire;

pub use marker::{MarkerStatus, SendMarker};
pub use stack::Stack;
pub use types::{Config, ConnectionId, Outbound, StackError, State};
