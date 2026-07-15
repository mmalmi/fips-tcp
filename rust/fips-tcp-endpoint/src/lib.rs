#![forbid(unsafe_code)]

//! Drive TCP/FIPS segments through an embedded [`fips_core::FipsEndpoint`].

use std::error::Error;
use std::fmt;
use std::sync::Arc;

use fips_core::{
    FipsEndpoint, FipsEndpointError, FipsEndpointServiceDatagram, FipsEndpointServiceReceiver,
    IdentityError, PeerIdentity,
};
use fips_tcp::{Config, ConnectionId, Stack, StackError, State};

/// Bounded aggregate of one received FIPS service batch.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReceiveReport {
    /// Total datagrams drained from the bounded FIPS receiver batch.
    pub datagrams: usize,
    /// Datagrams accepted or intentionally ignored by the TCP state machine.
    pub processed: usize,
    /// Datagrams rejected by bounded TCP wire decoding.
    pub malformed: usize,
    /// Valid new tuples rejected by global or authenticated-peer admission.
    pub connection_limited: usize,
    /// Other isolated TCP state-machine errors.
    pub other_errors: usize,
}

impl ReceiveReport {
    /// Total isolated errors in this batch.
    pub fn rejected(self) -> usize {
        self.malformed + self.connection_limited + self.other_errors
    }
}

pub struct FipsTcpEndpoint {
    endpoint: Arc<FipsEndpoint>,
    receiver: FipsEndpointServiceReceiver,
    fsp_service_port: u16,
    stack: Stack<String>,
    receive_batch: Vec<FipsEndpointServiceDatagram>,
}

impl FipsTcpEndpoint {
    /// Bind one FSP service and open its numerically matching TCP listener.
    pub async fn bind(
        endpoint: Arc<FipsEndpoint>,
        fsp_service_port: u16,
        config: Config,
        isn_seed: u64,
    ) -> Result<Self, AdapterError> {
        if fsp_service_port == 0 {
            return Err(AdapterError::InvalidServicePort);
        }
        let mut stack = Stack::new(config, isn_seed);
        stack.listen(fsp_service_port)?;
        let receiver = endpoint.register_service_receiver(fsp_service_port).await?;
        Ok(Self {
            endpoint,
            receiver,
            fsp_service_port,
            stack,
            receive_batch: Vec::new(),
        })
    }

    pub fn accept(&mut self) -> Option<ConnectionId> {
        self.stack.accept(self.fsp_service_port)
    }

    pub async fn connect(
        &mut self,
        peer: PeerIdentity,
        now_ms: u64,
    ) -> Result<ConnectionId, AdapterError> {
        let id = self
            .stack
            .connect(peer.npub(), self.fsp_service_port, now_ms)?;
        if let Err(error) = self.flush().await {
            // `connect` retained a SYN-SENT entry before emitting its initial
            // segment. If FIPS rejects that segment, the caller never receives
            // the ID and therefore cannot release the hidden connection. A
            // SYN-SENT close removes it immediately; preserve the send error
            // even if rollback unexpectedly fails.
            let _ = self.stack.close(id, now_ms);
            return Err(error);
        }
        Ok(id)
    }

    pub async fn write(
        &mut self,
        id: ConnectionId,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<usize, AdapterError> {
        let accepted = self.stack.write(id, bytes, now_ms)?;
        self.flush().await?;
        Ok(accepted)
    }

    pub async fn read(
        &mut self,
        id: ConnectionId,
        max: usize,
        now_ms: u64,
    ) -> Result<Vec<u8>, AdapterError> {
        let bytes = self.stack.read(id, max, now_ms)?;
        self.flush().await?;
        Ok(bytes)
    }

    pub async fn close(&mut self, id: ConnectionId, now_ms: u64) -> Result<(), AdapterError> {
        self.stack.close(id, now_ms)?;
        self.flush().await
    }

    pub async fn poll(&mut self, now_ms: u64) -> Result<(), AdapterError> {
        self.stack.poll(now_ms);
        self.flush().await
    }

    /// Await one FIPS service batch and return its datagram count.
    ///
    /// Individual invalid or over-capacity segments are isolated within the
    /// batch. Use [`Self::receive_report`] to observe their bounded aggregate.
    pub async fn receive(&mut self, now_ms: u64) -> Result<usize, AdapterError> {
        Ok(self.receive_report(now_ms).await?.datagrams)
    }

    /// Feed one complete bounded FIPS batch into TCP and report isolated errors.
    pub async fn receive_report(&mut self, now_ms: u64) -> Result<ReceiveReport, AdapterError> {
        let count = self
            .receiver
            .recv_batch_into(&mut self.receive_batch, 64)
            .await
            .ok_or(AdapterError::Closed)?;
        let mut report = ReceiveReport {
            datagrams: count,
            ..ReceiveReport::default()
        };
        for datagram in self.receive_batch.drain(..) {
            debug_assert_eq!(datagram.destination_port, self.fsp_service_port);
            match self.stack.input(
                datagram.source_peer.npub(),
                datagram.data.as_slice(),
                now_ms,
            ) {
                Ok(()) => report.processed += 1,
                Err(StackError::Wire(_)) => report.malformed += 1,
                Err(StackError::ConnectionLimit) => report.connection_limited += 1,
                Err(_) => report.other_errors += 1,
            }
        }
        debug_assert_eq!(report.datagrams, report.processed + report.rejected());
        self.flush().await?;
        Ok(report)
    }

    pub fn state(&self, id: ConnectionId) -> Option<State> {
        self.stack.state(id)
    }

    pub fn is_read_closed(&self, id: ConnectionId) -> bool {
        self.stack.is_read_closed(id)
    }

    /// Return the authenticated FIPS identity bound to this stream.
    pub fn peer(&self, id: ConnectionId) -> Option<PeerIdentity> {
        self.stack
            .peer(id)
            .and_then(|npub| PeerIdentity::from_npub(npub).ok())
    }

    /// Return the stream's internal `(local, remote)` TCP ports.
    pub fn ports(&self, id: ConnectionId) -> Option<(u16, u16)> {
        self.stack.ports(id)
    }

    async fn flush(&mut self) -> Result<(), AdapterError> {
        for outbound in self.stack.drain_outbound() {
            let peer = PeerIdentity::from_npub(&outbound.peer)?;
            self.endpoint
                .send_datagram(
                    peer,
                    self.fsp_service_port,
                    self.fsp_service_port,
                    outbound.bytes,
                )
                .await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum AdapterError {
    Closed,
    InvalidServicePort,
    Fips(FipsEndpointError),
    Identity(IdentityError),
    Tcp(StackError),
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("FIPS endpoint service receiver closed"),
            Self::InvalidServicePort => formatter.write_str("FIPS service port must be non-zero"),
            Self::Fips(error) => write!(formatter, "FIPS endpoint error: {error}"),
            Self::Identity(error) => write!(formatter, "FIPS identity error: {error}"),
            Self::Tcp(error) => write!(formatter, "TCP/FIPS error: {error}"),
        }
    }
}

impl Error for AdapterError {}

impl From<FipsEndpointError> for AdapterError {
    fn from(error: FipsEndpointError) -> Self {
        Self::Fips(error)
    }
}

impl From<StackError> for AdapterError {
    fn from(error: StackError) -> Self {
        Self::Tcp(error)
    }
}

impl From<IdentityError> for AdapterError {
    fn from(error: IdentityError) -> Self {
        Self::Identity(error)
    }
}
