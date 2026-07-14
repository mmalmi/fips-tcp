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

pub use fips_tcp::FIPS_TCP_SERVICE_PORT;

pub struct FipsTcpEndpoint {
    endpoint: Arc<FipsEndpoint>,
    receiver: FipsEndpointServiceReceiver,
    stack: Stack<String>,
    receive_batch: Vec<FipsEndpointServiceDatagram>,
}

impl FipsTcpEndpoint {
    pub async fn bind(
        endpoint: Arc<FipsEndpoint>,
        config: Config,
        isn_seed: u64,
    ) -> Result<Self, AdapterError> {
        let receiver = endpoint
            .register_service_receiver(FIPS_TCP_SERVICE_PORT)
            .await?;
        Ok(Self {
            endpoint,
            receiver,
            stack: Stack::new(config, isn_seed),
            receive_batch: Vec::new(),
        })
    }

    pub fn listen(&mut self, port: u16) -> Result<(), StackError> {
        self.stack.listen(port)
    }

    pub fn accept(&mut self, port: u16) -> Option<ConnectionId> {
        self.stack.accept(port)
    }

    pub async fn connect(
        &mut self,
        peer: PeerIdentity,
        remote_port: u16,
        now_ms: u64,
    ) -> Result<ConnectionId, AdapterError> {
        let id = self.stack.connect(peer.npub(), remote_port, now_ms)?;
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
            debug_assert_eq!(datagram.destination_port, FIPS_TCP_SERVICE_PORT);
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
                    FIPS_TCP_SERVICE_PORT,
                    FIPS_TCP_SERVICE_PORT,
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
    Fips(FipsEndpointError),
    Identity(IdentityError),
    Tcp(StackError),
}

impl fmt::Display for AdapterError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("FIPS endpoint service receiver closed"),
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
