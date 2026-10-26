#![forbid(unsafe_code)]
//! Recipient-approved file transfer above TCP/FIPS. Derived from Iris Chat's
//! MIT-licensed direct-file engine. The transport and FIPS wire are unchanged.
use fips_core::{FipsEndpoint, PeerIdentity};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use tokio::task::JoinHandle;

mod disk;
mod runtime;
pub mod wire;

pub const PORT: u16 = 39512;
pub const CHUNK: usize = 32 * 1024;
pub const MAX_TRANSFERS: usize = 16;
pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ManifestFile {
    pub filename: String,
    pub size_bytes: u64,
    pub sha256: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferFile {
    pub filename: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub path: PathBuf,
}
impl TransferFile {
    pub fn manifest(&self) -> ManifestFile {
        ManifestFile {
            filename: self.filename.clone(),
            size_bytes: self.size_bytes,
            sha256: self.sha256.clone(),
        }
    }
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TransferStatus {
    Offered,
    Connecting,
    Transferring,
    Paused,
    Completed,
    Declined,
    Cancelled,
    Failed,
}
impl TransferStatus {
    pub fn terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Declined | Self::Cancelled | Self::Failed
        )
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TransferSnapshot {
    pub id: String,
    /// Authenticated peer's lowercase x-only public-key hex; empty before claim for external offers; selected recipient for in-band offers.
    pub peer: String,
    pub files: Vec<ManifestFile>,
    pub status: TransferStatus,
    pub is_sender: bool,
    pub transferred_bytes: u64,
    pub total_bytes: u64,
    pub local_paths: Vec<String>,
    pub error: Option<String>,
}
#[derive(Clone, Debug)]
pub enum TransferEvent {
    Offered {
        transfer_id: String,
        peer: String,
        files: Vec<ManifestFile>,
    },
    Accepted {
        transfer_id: String,
        peer: String,
    },
    Progress {
        transfer_id: String,
        peer: String,
        transferred_bytes: u64,
        total_bytes: u64,
    },
    Completed {
        transfer_id: String,
        peer: String,
        local_paths: Vec<String>,
    },
    Declined {
        transfer_id: String,
        peer: String,
    },
    Cancelled {
        transfer_id: String,
        peer: String,
    },
    Paused {
        transfer_id: String,
        peer: String,
        error: String,
    },
    Failed {
        transfer_id: String,
        peer: String,
        error: String,
    },
}
/// Application policy for opening source files, checked again on every read.
pub trait Source: Send + Sync {
    fn open(&self, path: &Path) -> Result<std::fs::File>;
}

/// Export verified bytes to a user-selected destination. Never truncate existing
/// files; abort removes only outputs created by this acceptance.
pub trait Destination: Send + Sync {
    fn prepare(&self, transfer_id: String, files: Vec<ManifestFile>) -> Result<()>;
    fn write(&self, file_index: u32, bytes: Vec<u8>) -> Result<()>;
    fn finish_file(&self, file_index: u32) -> Result<()>;
    fn commit(&self) -> Result<Vec<String>>;
    fn abort(&self);
}
#[derive(Default)]
struct Policy {
    allowed: HashSet<String>,
    restrictions: HashMap<String, HashSet<String>>,
}
#[derive(Clone)]
pub struct TransferSender {
    tx: flume::Sender<Command>,
    policy: Arc<RwLock<Policy>>,
    snapshots: Arc<RwLock<Vec<TransferSnapshot>>>,
    source: Arc<RwLock<Arc<dyn Source>>>,
}
enum Command {
    Offer {
        id: String,
        token: String,
        peers: Vec<PeerIdentity>,
        files: Vec<TransferFile>,
        in_band: bool,
    },
    Receive {
        id: String,
        token: String,
        peer: PeerIdentity,
        files: Vec<TransferFile>,
        destination: Arc<dyn Destination>,
        resumable: bool,
    },
    Resume {
        id: String,
        destination: Arc<dyn Destination>,
    },
    Decline {
        id: String,
        token: String,
        peer: PeerIdentity,
    },
    ResumeOffer(String),
    DeclineIncoming(String),
    Cancel(String),
}
impl TransferSender {
    pub fn set_source_opener(&self, source: Arc<dyn Source>) -> Result<()> {
        *self.source.write().map_err(|_| "File source unavailable")? = source;
        Ok(())
    }
    /// In-band incoming offers are denied by default. The caller supplies current
    /// authenticated membership; this set is consulted again during transfers.
    pub fn set_allowed_peers(&self, peers: Vec<PeerIdentity>) -> Result<()> {
        self.policy
            .write()
            .map_err(|_| "File authorization unavailable")?
            .allowed = peers.into_iter().map(peer_hex).collect();
        Ok(())
    }
    pub fn restrict_offer(&self, id: &str, peers: Vec<PeerIdentity>) -> Result<()> {
        self.policy
            .write()
            .map_err(|_| "File authorization unavailable")?
            .restrictions
            .insert(id.into(), peers.into_iter().map(peer_hex).collect());
        Ok(())
    }
    pub fn snapshots(&self) -> Vec<TransferSnapshot> {
        self.snapshots.read().map(|v| v.clone()).unwrap_or_default()
    }
    pub fn register_offer(
        &self,
        id: String,
        token: String,
        peers: Vec<PeerIdentity>,
        files: Vec<TransferFile>,
    ) -> Result<()> {
        self.offer(id, token, peers, files, false)
    }
    pub fn send_offer(
        &self,
        id: String,
        token: String,
        peer: PeerIdentity,
        files: Vec<TransferFile>,
    ) -> Result<()> {
        self.offer(id, token, vec![peer], files, true)
    }
    fn offer(
        &self,
        id: String,
        token: String,
        peers: Vec<PeerIdentity>,
        files: Vec<TransferFile>,
        in_band: bool,
    ) -> Result<()> {
        wire::validate_claim(&id, &token)?;
        wire::validate_manifest(&files.iter().map(TransferFile::manifest).collect::<Vec<_>>())?;
        if peers.is_empty() || peers.len() > 128 {
            return Err("No receiving device is available".into());
        }
        self.send(Command::Offer {
            id,
            token,
            peers,
            files,
            in_band,
        })
    }
    pub fn receive_into(
        &self,
        id: String,
        token: String,
        peer: PeerIdentity,
        files: Vec<TransferFile>,
        destination: Arc<dyn Destination>,
        resumable: bool,
    ) -> Result<()> {
        wire::validate_claim(&id, &token)?;
        wire::validate_manifest(&files.iter().map(TransferFile::manifest).collect::<Vec<_>>())?;
        self.send(Command::Receive {
            id,
            token,
            peer,
            files,
            destination,
            resumable,
        })
    }
    pub fn receive(
        &self,
        id: String,
        token: String,
        peer: PeerIdentity,
        files: Vec<TransferFile>,
        directory: PathBuf,
        resumable: bool,
    ) -> Result<()> {
        self.receive_into(
            id,
            token,
            peer,
            files,
            directory_destination(directory),
            resumable,
        )
    }
    pub fn accept(&self, id: &str, destination: Arc<dyn Destination>) -> Result<()> {
        self.resume(id, destination)
    }
    pub fn resume(&self, id: &str, destination: Arc<dyn Destination>) -> Result<()> {
        self.send(Command::Resume {
            id: id.into(),
            destination,
        })
    }
    pub fn resume_offer(&self, id: &str) -> Result<()> {
        self.send(Command::ResumeOffer(id.into()))
    }
    pub fn decline(&self, id: String, token: String, peer: PeerIdentity) -> Result<()> {
        wire::validate_claim(&id, &token)?;
        self.send(Command::Decline { id, token, peer })
    }
    pub fn decline_incoming(&self, id: &str) -> Result<()> {
        self.send(Command::DeclineIncoming(id.into()))
    }
    pub fn cancel(&self, id: &str) -> Result<()> {
        self.send(Command::Cancel(id.into()))
    }
    fn send(&self, command: Command) -> Result<()> {
        self.tx
            .try_send(command)
            .map_err(|_| "File transfer is busy. Try again.".into())
    }
}
/// Start a bounded transfer service with durable private staging. The state
/// directory and its ancestors must be protected from other users. Privileged
/// callers must install their Source policy before registering offers or enabling
/// allowed peers; restored outgoing offers are denied until current policy exists.
pub async fn start(
    endpoint: Arc<FipsEndpoint>,
    state_dir: PathBuf,
    events: flume::Sender<TransferEvent>,
) -> Result<(TransferSender, JoinHandle<()>)> {
    let tcp = fips_tcp_endpoint::FipsTcpEndpoint::bind(
        endpoint,
        PORT,
        fips_tcp::Config {
            max_connections: MAX_TRANSFERS,
            max_connections_per_peer: 4,
            send_buffer: 64 * 1024,
            receive_buffer: u16::MAX as usize,
            ..Default::default()
        },
        rand::random(),
    )
    .await
    .map_err(|e| e.to_string())?;
    let store = disk::Store::open(state_dir)?;
    let (tx, rx) = flume::bounded(32);
    let policy = Arc::new(RwLock::new(Policy::default()));
    let snapshots = Arc::new(RwLock::new(store.snapshots()));
    let source = store.source.clone();
    let sender = TransferSender {
        tx,
        policy: policy.clone(),
        snapshots: snapshots.clone(),
        source,
    };
    Ok((
        sender,
        tokio::spawn(runtime::run(tcp, store, rx, events, policy, snapshots)),
    ))
}
pub fn peer_hex(peer: PeerIdentity) -> String {
    let hex = peer.pubkey().to_string();
    if hex.len() == 66 {
        hex[2..].to_owned()
    } else {
        hex
    }
}
pub fn directory_destination(directory: PathBuf) -> Arc<dyn Destination> {
    Arc::new(disk::DirectoryDestination::new(directory))
}

#[cfg(test)]
mod legacy_tests;
#[cfg(test)]
mod tests;
