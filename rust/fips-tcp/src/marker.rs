use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::ConnectionId;

static NEXT_CONNECTION_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Opaque boundary after bytes accepted by one connection's local send buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct SendMarker {
    connection_id: ConnectionId,
    connection_token: u64,
    accepted_payload_bytes: u128,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MarkerStatus {
    Pending,
    Acked,
    ConnectionGone,
}

pub(crate) struct SendProgress {
    connection_token: u64,
    accepted_payload_bytes: u128,
    acked_payload_bytes: u128,
}

impl SendProgress {
    pub(crate) fn new() -> Self {
        let connection_token = NEXT_CONNECTION_TOKEN
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                value.checked_add(1)
            })
            .expect("TCP/FIPS connection token space exhausted");
        Self {
            connection_token,
            accepted_payload_bytes: 0,
            acked_payload_bytes: 0,
        }
    }

    pub(crate) fn accept(&mut self, bytes: usize) {
        self.accepted_payload_bytes = self
            .accepted_payload_bytes
            .checked_add(bytes as u128)
            .expect("TCP/FIPS accepted payload counter exhausted");
    }

    pub(crate) fn acknowledge(&mut self, bytes: usize) {
        self.acked_payload_bytes = self
            .acked_payload_bytes
            .checked_add(bytes as u128)
            .expect("TCP/FIPS acknowledged payload counter exhausted");
        debug_assert!(self.acked_payload_bytes <= self.accepted_payload_bytes);
    }

    pub(crate) fn marker(&self, connection_id: ConnectionId) -> SendMarker {
        SendMarker {
            connection_id,
            connection_token: self.connection_token,
            accepted_payload_bytes: self.accepted_payload_bytes,
        }
    }

    pub(crate) fn status(&self, marker: &SendMarker) -> MarkerStatus {
        if marker.connection_token != self.connection_token {
            MarkerStatus::ConnectionGone
        } else if self.acked_payload_bytes >= marker.accepted_payload_bytes {
            MarkerStatus::Acked
        } else {
            MarkerStatus::Pending
        }
    }
}

impl SendMarker {
    pub(crate) fn connection_id(self) -> ConnectionId {
        self.connection_id
    }
}
