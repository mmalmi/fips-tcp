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
        self.flush().await?;
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

    /// Await one FIPS service batch, feed every segment into TCP, and flush replies.
    pub async fn receive(&mut self, now_ms: u64) -> Result<usize, AdapterError> {
        let count = self
            .receiver
            .recv_batch_into(&mut self.receive_batch, 64)
            .await
            .ok_or(AdapterError::Closed)?;
        for datagram in self.receive_batch.drain(..) {
            debug_assert_eq!(datagram.destination_port, self.fsp_service_port);
            self.stack.input(
                datagram.source_peer.npub(),
                datagram.data.as_slice(),
                now_ms,
            )?;
        }
        self.flush().await?;
        Ok(count)
    }

    pub fn state(&self, id: ConnectionId) -> Option<State> {
        self.stack.state(id)
    }

    pub fn is_read_closed(&self, id: ConnectionId) -> bool {
        self.stack.is_read_closed(id)
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
