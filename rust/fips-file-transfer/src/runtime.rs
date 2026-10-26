use crate::{
    disk::{self, Record, Store},
    wire::{self, Control, Reader, Rejection},
    *,
};
mod protocol;
mod work;

use fips_tcp::{ConnectionId, MarkerStatus, SendMarker, State};
use fips_tcp_endpoint::FipsTcpEndpoint;
use sha2::{Digest, Sha256};
use std::{
    cell::RefCell,
    collections::{HashSet, VecDeque},
    time::{Duration, Instant},
};

enum Role {
    Await,
    Sending,
    Receiving { ready: bool },
    Announcing,
    Declining,
    Finish,
}
struct Pending {
    bytes: Vec<u8>,
    offset: usize,
    marker: Option<SendMarker>,
}
struct Connection {
    id: String,
    peer: PeerIdentity,
    role: Role,
    reader: Reader,
    queue: VecDeque<Vec<u8>>,
    pending: Option<Pending>,
    offset: u64,
    last_activity: Instant,
    idle_limit: Duration,
    last_progress: Instant,
    last_checkpoint: u64,
    done: bool,
    completion: Option<Vec<String>>,
    work: Option<work::Work>,
    hasher: Sha256,
}
impl Connection {
    fn new(id: String, peer: PeerIdentity, role: Role) -> Self {
        Self {
            id,
            peer,
            role,
            reader: Reader::default(),
            queue: VecDeque::new(),
            pending: None,
            offset: 0,
            last_activity: Instant::now(),
            idle_limit: Duration::from_secs(60),
            last_progress: Instant::now(),
            last_checkpoint: 0,
            done: false,
            completion: None,
            work: None,
            hasher: Sha256::new(),
        }
    }
    fn control(&mut self, c: Control) -> Result<()> {
        if self.queue.len() >= 4 {
            return Err("File control queue full".into());
        }
        self.queue.push_back(wire::control(&c)?);
        Ok(())
    }
}
struct Runtime {
    store: Store,
    connections: HashMap<ConnectionId, Connection>,
    destinations: HashMap<String, Arc<dyn Destination>>,
    local_work: HashMap<String, Connection>,
    active: HashSet<String>,
    events: flume::Sender<TransferEvent>,
    pending_events: RefCell<VecDeque<TransferEvent>>,
    policy: Arc<RwLock<Policy>>,
    snapshots: Arc<RwLock<Vec<TransferSnapshot>>>,
}
impl Runtime {
    fn emit(&self, e: TransferEvent) {
        self.pending_events.borrow_mut().push_back(e);
    }
    fn publish(&self) {
        if let Ok(mut rows) = self.snapshots.write() {
            *rows = self.store.snapshots();
        }
    }
    fn authorized(&self, r: &Record, peer: PeerIdentity) -> bool {
        let Ok(policy) = self.policy.read() else {
            return false;
        };
        let hex = peer_hex(peer);
        if policy
            .restrictions
            .get(&r.id)
            .is_some_and(|p| !p.contains(&hex))
        {
            return false;
        }
        if r.in_band && !policy.allowed.contains(&hex) {
            return false;
        }
        if r.incoming {
            r.peer.as_deref() == Some(&peer.npub())
        } else {
            (r.in_band || self.active.contains(&r.id))
                && r.peers.contains(&peer.npub())
                && r.peer.as_ref().is_none_or(|p| *p == peer.npub())
        }
    }
    fn terminal(&mut self, id: &str, status: TransferStatus, error: Option<String>) -> Result<()> {
        let Some(mut r) = self.store.records.get(id).cloned() else {
            return Ok(());
        };
        if r.status == TransferStatus::Completed {
            self.store.records.insert(id.into(), r);
            return Ok(());
        }
        r.status = status;
        r.error = error;
        self.store.put(r.clone())?;
        if r.incoming {
            self.store.remove_parts(&r)?;
        }
        self.destinations.remove(id);
        Ok(())
    }
    fn pause(&mut self, id: &str, error: String) {
        let Some(mut r) = self.store.records.remove(id) else {
            return;
        };
        if r.status.terminal() {
            if r.incoming && r.status == TransferStatus::Completed && !r.completion_confirmed {
                self.emit(TransferEvent::Paused {
                    transfer_id: id.into(),
                    peer: r.peer_hex(),
                    error,
                });
            }
            self.store.records.insert(id.into(), r);
            return;
        }
        if r.resumable {
            r.status = TransferStatus::Paused;
            r.error = Some(error.clone());
            let saved = self.store.checkpoint(&r);
            self.emit(TransferEvent::Paused {
                transfer_id: id.into(),
                peer: r.peer_hex(),
                error: if saved.is_ok() {
                    error
                } else {
                    "Could not save transfer progress".into()
                },
            });
        } else {
            r.status = TransferStatus::Failed;
            r.error = Some(error.clone());
            if r.incoming {
                let _ = self.store.remove_parts(&r);
            }
            let _ = self.store.persist(&r);
            self.emit(TransferEvent::Failed {
                transfer_id: id.into(),
                peer: r.peer_hex(),
                error,
            });
        }
        self.store.records.insert(id.into(), r);
    }
    async fn open_receive(
        &mut self,
        id: &str,
        destination: Arc<dyn Destination>,
        tcp: &mut FipsTcpEndpoint,
        time: u64,
    ) -> Result<()> {
        if self.connections.len() + self.local_work.len() >= MAX_TRANSFERS {
            return Err("Too many file transfers".into());
        }
        if self.connections.values().any(|c| c.id == id) || self.local_work.contains_key(id) {
            return Err("File transfer is already active".into());
        }
        let mut r = self
            .store
            .records
            .get(id)
            .cloned()
            .ok_or("File offer unavailable")?;
        if !r.incoming {
            return self.announce(id, tcp, time).await;
        }
        if r.status == TransferStatus::Completed && r.export_pending {
            if self.local_work.contains_key(id) {
                return Err("Files are already being saved".into());
            }
            let mut c = Connection::new(id.into(), r.peer_identity()?, Role::Finish);
            self.export_received(&mut c, r, destination, true);
            self.local_work.insert(id.into(), c);
            return Ok(());
        }
        if matches!(
            r.status,
            TransferStatus::Declined | TransferStatus::Cancelled | TransferStatus::Failed
        ) || r.expires <= disk::now()
        {
            return Err("File offer unavailable".into());
        }
        let peer = r.peer_identity()?;
        if !self.authorized(&r, peer) {
            return Err("Device authorization changed".into());
        }
        if r.status != TransferStatus::Completed {
            self.store.prepare_receive(&r)?;
        }
        let mut c = Connection::new(
            id.into(),
            peer,
            Role::Receiving {
                ready: !r.resumable,
            },
        );
        c.offset = r.offset;
        c.idle_limit = Duration::from_secs(
            (60 + r.offset.div_ceil(1024 * 1024)).min(r.expires.saturating_sub(disk::now()).max(1)),
        );
        c.last_checkpoint = r.offset;
        if r.resumable {
            self.receive_prefix(&mut c, r.clone());
        } else {
            c.control(Control::Accept {
                id: id.into(),
                token: r.token.clone(),
            })?;
        }
        if r.status != TransferStatus::Completed {
            r.status = TransferStatus::Connecting;
            r.error = None;
            self.store.put(r)?;
        }
        self.destinations.insert(id.into(), destination);
        if c.work.is_some() {
            self.local_work.insert(id.into(), c);
            return Ok(());
        }
        let connection = tcp
            .connect(peer, time)
            .await
            .map_err(|e| format!("transport: {e}"))?;
        self.connections.insert(connection, c);
        Ok(())
    }
    async fn announce(&mut self, id: &str, tcp: &mut FipsTcpEndpoint, time: u64) -> Result<()> {
        if self.connections.len() + self.local_work.len() >= MAX_TRANSFERS
            || self.connections.values().any(|c| c.id == id)
        {
            return Err("File transfer is already active".into());
        }
        let r = self.store.records.get(id).ok_or("File offer unavailable")?;
        if r.incoming
            || matches!(
                r.status,
                TransferStatus::Cancelled | TransferStatus::Declined | TransferStatus::Failed
            )
        {
            return Err("Receiving device must resume this transfer".into());
        }
        let peer = PeerIdentity::from_npub(
            r.peer
                .as_ref()
                .or_else(|| r.peers.first())
                .ok_or("No receiving device")?,
        )
        .map_err(|e| e.to_string())?;
        if !self.authorized(r, peer) {
            return Err("Device authorization changed".into());
        }
        let mut c = Connection::new(id.into(), peer, Role::Announcing);
        c.control(Control::Offer {
            id: id.into(),
            token: r.token.clone(),
            version: 2,
            files: r.manifest(),
        })?;
        let stream = tcp
            .connect(peer, time)
            .await
            .map_err(|e| format!("transport: {e}"))?;
        self.connections.insert(stream, c);
        Ok(())
    }
    async fn command(
        &mut self,
        command: Command,
        tcp: &mut FipsTcpEndpoint,
        time: u64,
    ) -> Result<()> {
        match command {
            Command::Offer {
                id,
                token,
                peers,
                files,
                in_band,
            } => {
                let names = peers.iter().map(PeerIdentity::npub).collect::<Vec<_>>();
                if let Some(old) = self.store.records.get(&id) {
                    if old.incoming
                        || old.token != token
                        || wire::manifest_hash(&old.manifest())?
                            != wire::manifest_hash(
                                &files.iter().map(TransferFile::manifest).collect::<Vec<_>>(),
                            )?
                    {
                        return Err("File transfer identity changed".into());
                    }
                    if old.status.terminal() && old.status != TransferStatus::Completed {
                        return Err("File offer has ended".into());
                    }
                    let mut restored = old.clone();
                    restored.peers = names.clone();
                    self.store.put(restored)?;
                } else {
                    if self.store.records.len() >= 256
                        || self
                            .store
                            .records
                            .values()
                            .filter(|r| !r.status.terminal())
                            .count()
                            >= MAX_TRANSFERS
                    {
                        return Err("Too many file transfers".into());
                    }
                    self.store.put(Record {
                        id: id.clone(),
                        token,
                        files,
                        peers: names,
                        peer: None,
                        incoming: false,
                        in_band,
                        resumable: true,
                        status: TransferStatus::Offered,
                        offset: 0,
                        local_paths: vec![],
                        expires: disk::now() + 24 * 60 * 60,
                        error: None,
                        completion_confirmed: false,
                        complete_prefix: None,
                        export_pending: false,
                    })?;
                }
                self.active.insert(id.clone());
                if in_band {
                    self.announce(&id, tcp, time).await?;
                }
            }
            Command::Receive {
                id,
                token,
                peer,
                files,
                destination,
                resumable,
            } => {
                if let Some(old) = self.store.records.get(&id) {
                    if !old.incoming
                        || old.peer.as_deref() != Some(&peer.npub())
                        || old.token != token
                        || wire::manifest_hash(&old.manifest())?
                            != wire::manifest_hash(
                                &files.iter().map(TransferFile::manifest).collect::<Vec<_>>(),
                            )?
                    {
                        return Err("File transfer identity changed".into());
                    }
                } else {
                    if self.store.records.len() >= 256
                        || self
                            .store
                            .records
                            .values()
                            .filter(|r| !r.status.terminal())
                            .count()
                            >= MAX_TRANSFERS
                    {
                        return Err("Too many file transfers".into());
                    }
                    self.store.put(Record {
                        id: id.clone(),
                        token,
                        files,
                        peers: vec![],
                        peer: Some(peer.npub()),
                        incoming: true,
                        in_band: false,
                        resumable,
                        status: TransferStatus::Offered,
                        offset: 0,
                        local_paths: vec![],
                        expires: disk::now() + 24 * 60 * 60,
                        error: None,
                        completion_confirmed: false,
                        complete_prefix: None,
                        export_pending: false,
                    })?;
                }
                self.open_receive(&id, destination, tcp, time).await?;
            }
            Command::Resume { id, destination } => {
                self.open_receive(&id, destination, tcp, time).await?
            }
            Command::ResumeOffer(id) => {
                let r = self
                    .store
                    .records
                    .get(&id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                if !r.incoming && !r.in_band && r.peer.is_none() && !r.status.terminal() {
                    let mut record = r;
                    record.status = TransferStatus::Offered;
                    record.error = None;
                    self.store.put(record.clone())?;
                    self.emit(TransferEvent::Offered {
                        transfer_id: id,
                        peer: String::new(),
                        files: record.manifest(),
                    });
                } else {
                    self.announce(&id, tcp, time).await?;
                }
            }
            Command::Decline { id, token, peer } => {
                self.terminal(&id, TransferStatus::Declined, None)?;
                let mut c = Connection::new(id.clone(), peer, Role::Declining);
                c.control(Control::Decline { id, token })?;
                let stream = tcp
                    .connect(peer, time)
                    .await
                    .map_err(|e| format!("transport: {e}"))?;
                self.connections.insert(stream, c);
            }
            Command::DeclineIncoming(id) => {
                let r = self
                    .store
                    .records
                    .get(&id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                if !r.incoming || r.status.terminal() {
                    return Err("File offer unavailable".into());
                }
                let peer = r.peer_identity()?;
                self.terminal(&id, TransferStatus::Declined, None)?;
                let mut c = Connection::new(id.clone(), peer, Role::Declining);
                c.control(Control::Decline { id, token: r.token })?;
                let stream = tcp
                    .connect(peer, time)
                    .await
                    .map_err(|e| format!("transport: {e}"))?;
                self.connections.insert(stream, c);
            }
            Command::Cancel(id) => {
                let peer = self
                    .store
                    .records
                    .get(&id)
                    .map(Record::peer_hex)
                    .unwrap_or_default();
                self.local_work.remove(&id);
                self.terminal(&id, TransferStatus::Cancelled, None)?;
                for c in self.connections.values_mut().filter(|c| c.id == id) {
                    c.work = None;
                    c.role = Role::Finish;
                    c.queue.clear();
                    c.control(Control::Cancel)?;
                }
                self.emit(TransferEvent::Cancelled {
                    transfer_id: id,
                    peer,
                });
            }
        }
        Ok(())
    }
    async fn progress(
        &mut self,
        id: ConnectionId,
        c: &mut Connection,
        tcp: &mut FipsTcpEndpoint,
        time: u64,
    ) -> Result<bool> {
        if c.work.is_some() {
            self.poll_work(c).await?;
            if c.work.is_some() {
                return Ok(true);
            }
        }
        if c.last_activity.elapsed() > c.idle_limit {
            return Err("transport: File transfer timed out".into());
        }
        let state = tcp.state(id).ok_or("transport: File connection closed")?;
        if matches!(state, State::SynSent | State::SynReceived) {
            return Ok(true);
        }
        if !matches!(state, State::Established | State::CloseWait) {
            return Err("transport: File connection closed".into());
        }
        let bytes = tcp
            .read(id, CHUNK + 1024, time)
            .await
            .map_err(|e| format!("transport: {e}"))?;
        if !bytes.is_empty() {
            c.last_activity = Instant::now();
            for packet in c.reader.push(&bytes)? {
                self.packet(packet, c)?;
                if matches!(c.role, Role::Finish) || c.work.is_some() {
                    break;
                }
            }
        }
        if state == State::CloseWait && !matches!(c.role, Role::Finish) {
            return Err("transport: File connection closed before completion".into());
        }
        if c.pending.as_ref().is_some_and(|p| {
            p.offset == p.bytes.len()
                && p.marker
                    .is_some_and(|m| tcp.marker_status(&m) == MarkerStatus::Acked)
        }) {
            c.pending = None;
            c.last_activity = Instant::now();
        }
        if c.pending.is_none() {
            if c.queue.is_empty() && matches!(c.role, Role::Sending) {
                let r = self
                    .store
                    .records
                    .get(&c.id)
                    .ok_or("File offer unavailable")?;
                if let Some(bytes) = self.store.read_chunk(r, c.offset)? {
                    c.offset += bytes.len() as u64;
                    c.hasher.update(&bytes);
                    c.queue.push_back(wire::data(&bytes));
                } else if !c.done {
                    c.idle_limit = Duration::from_secs(
                        (60 + 2 * r.total().div_ceil(1024 * 1024))
                            .min(r.expires.saturating_sub(disk::now()).max(1)),
                    );
                    let mut completed = r.clone();
                    if completed.complete_prefix.is_none() {
                        completed.complete_prefix =
                            Some(format!("{:x}", c.hasher.clone().finalize()));
                        self.store.put(completed)?;
                    }
                    c.control(Control::Done)?;
                    c.done = true;
                }
            }
            if let Some(bytes) = c.queue.pop_front() {
                c.pending = Some(Pending {
                    bytes,
                    offset: 0,
                    marker: None,
                });
            } else if matches!(c.role, Role::Finish | Role::Declining) {
                if let Some(paths) = c.completion.take() {
                    if let Some(mut record) = self.store.records.get(&c.id).cloned() {
                        record.completion_confirmed = true;
                        self.store.put(record)?;
                    }
                    self.completed_event(&c.id, c.peer, paths);
                }
                if matches!(c.role, Role::Declining) {
                    self.emit(TransferEvent::Declined {
                        transfer_id: c.id.clone(),
                        peer: peer_hex(c.peer),
                    });
                }
                return Ok(false);
            }
        }
        if let Some(p) = c.pending.as_mut() {
            if p.offset < p.bytes.len() {
                let (n, marker) = tcp
                    .write_with_marker(id, &p.bytes[p.offset..], time)
                    .await
                    .map_err(|e| format!("transport: {e}"))?;
                if n > 0 {
                    p.offset += n;
                    p.marker = Some(marker);
                    c.last_activity = Instant::now();
                }
            }
        }
        if c.last_progress.elapsed() >= Duration::from_millis(250)
            && matches!(c.role, Role::Sending | Role::Receiving { .. })
        {
            if let Some(r) = self.store.records.get_mut(&c.id) {
                if !r.incoming {
                    r.offset = c.offset;
                }
            }
            if let Some(r) = self.store.records.get(&c.id) {
                self.emit(TransferEvent::Progress {
                    transfer_id: c.id.clone(),
                    peer: peer_hex(c.peer),
                    transferred_bytes: c.offset,
                    total_bytes: r.total(),
                });
            }
            c.last_progress = Instant::now();
        }
        Ok(true)
    }
}
pub(crate) async fn run(
    mut tcp: FipsTcpEndpoint,
    store: Store,
    commands: flume::Receiver<Command>,
    events: flume::Sender<TransferEvent>,
    policy: Arc<RwLock<Policy>>,
    snapshots: Arc<RwLock<Vec<TransferSnapshot>>>,
) {
    let mut rt = Runtime {
        store,
        connections: HashMap::new(),
        destinations: HashMap::new(),
        local_work: HashMap::new(),
        active: HashSet::new(),
        events,
        pending_events: RefCell::new(VecDeque::new()),
        policy,
        snapshots,
    };
    let start = Instant::now();
    let mut last_expiry = Instant::now();
    loop {
        let time = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let interval = Duration::from_millis(if rt.connections.is_empty() { 200 } else { 10 });
        tokio::select! {
            command=commands.recv_async()=>match command{
                Ok(command)=>{let id=match &command{Command::Offer{id,..}|Command::Receive{id,..}|Command::Resume{id,..}|Command::Decline{id,..}|Command::ResumeOffer(id)|Command::DeclineIncoming(id)|Command::Cancel(id)=>id.clone()};if let Err(error)=rt.command(command,&mut tcp,time).await{if error.starts_with("transport:"){rt.pause(&id,error);}else{rt.emit(TransferEvent::Failed{transfer_id:id,peer:String::new(),error});}}},
                Err(_)=>break,
            },
            received=tokio::time::timeout(interval,tcp.receive(time))=>if matches!(received,Ok(Err(fips_tcp_endpoint::AdapterError::Closed))){break;}
        }
        let time = start.elapsed().as_millis().min(u64::MAX as u128) as u64;
        let _ = tcp.poll(time).await;
        while let Some(id) = tcp.accept() {
            if let Some(peer) = tcp.peer(id) {
                if rt.connections.len() + rt.local_work.len() < MAX_TRANSFERS {
                    rt.connections
                        .insert(id, Connection::new(String::new(), peer, Role::Await));
                } else {
                    let _ = tcp.abort(id).await;
                }
            }
        }
        for id in rt.connections.keys().copied().collect::<Vec<_>>() {
            let Some(mut c) = rt.connections.remove(&id) else {
                continue;
            };
            if !c.id.is_empty()
                && rt
                    .store
                    .records
                    .get(&c.id)
                    .is_some_and(|r| !rt.authorized(r, c.peer))
                && !matches!(c.role, Role::Finish | Role::Declining)
            {
                let error = "Device authorization changed".to_string();
                let _ = rt.terminal(&c.id, TransferStatus::Failed, Some(error.clone()));
                rt.emit(TransferEvent::Failed {
                    transfer_id: c.id.clone(),
                    peer: peer_hex(c.peer),
                    error,
                });
                c.work = None;
                c.queue.clear();
                let _ = c.control(Control::Rejected {
                    id: c.id.clone(),
                    reason: Rejection::Unavailable,
                });
                c.role = Role::Finish;
            }
            match rt.progress(id, &mut c, &mut tcp, time).await {
                Ok(true) => {
                    rt.connections.insert(id, c);
                }
                Ok(false) => {
                    let _ = tcp.close(id, time).await;
                }
                Err(error) => {
                    // Stream loss can resume; malformed controls and integrity errors
                    // terminate the transfer, retaining only its replay-protection record.
                    if error.starts_with("transport:") {
                        rt.pause(&c.id, error);
                    } else {
                        let _ = rt.terminal(&c.id, TransferStatus::Failed, Some(error.clone()));
                        if !c.id.is_empty() {
                            rt.emit(TransferEvent::Failed {
                                transfer_id: c.id.clone(),
                                peer: peer_hex(c.peer),
                                error,
                            });
                            c.work = None;
                            c.queue.clear();
                            c.reader = Reader::default();
                            let _ = c.control(Control::Cancel);
                            c.role = Role::Finish;
                            rt.connections.insert(id, c);
                            continue;
                        }
                    }
                    let _ = tcp.abort(id).await;
                }
            }
        }
        for id in rt.local_work.keys().cloned().collect::<Vec<_>>() {
            if let Some(mut c) = rt.local_work.remove(&id) {
                let receives = matches!(c.role, Role::Receiving { .. });
                if receives
                    && rt
                        .store
                        .records
                        .get(&id)
                        .is_some_and(|r| r.expires <= disk::now() || !rt.authorized(r, c.peer))
                {
                    c.work = None;
                    let error = "File offer is no longer available".to_string();
                    let _ = rt.terminal(&id, TransferStatus::Failed, Some(error.clone()));
                    rt.emit(TransferEvent::Failed {
                        transfer_id: id,
                        peer: peer_hex(c.peer),
                        error,
                    });
                    continue;
                }
                if let Err(error) = rt.poll_work(&mut c).await {
                    if receives {
                        let _ = rt.terminal(&id, TransferStatus::Failed, Some(error.clone()));
                        rt.emit(TransferEvent::Failed {
                            transfer_id: id.clone(),
                            peer: peer_hex(c.peer),
                            error,
                        });
                    } else {
                        rt.emit(TransferEvent::Paused {
                            transfer_id: id.clone(),
                            peer: peer_hex(c.peer),
                            error,
                        });
                    }
                    continue;
                }
                if c.work.is_some() {
                    rt.local_work.insert(id, c);
                } else if receives {
                    match tcp.connect(c.peer, time).await {
                        Ok(connection) => {
                            c.last_activity = Instant::now();
                            rt.connections.insert(connection, c);
                        }
                        Err(error) => rt.pause(&id, format!("transport: {error}")),
                    }
                }
            }
        }
        if last_expiry.elapsed() >= Duration::from_secs(60) {
            let now = disk::now();
            for connection in rt.connections.values_mut() {
                if rt
                    .store
                    .records
                    .get(&connection.id)
                    .is_some_and(|r| r.expires <= now)
                {
                    connection.work = None;
                    connection.queue.clear();
                    let _ = connection.control(Control::Rejected {
                        id: connection.id.clone(),
                        reason: Rejection::Expired,
                    });
                    connection.role = Role::Finish;
                }
            }
            let _ = rt.store.expire(now);
            rt.active.retain(|id| rt.store.records.contains_key(id));
            rt.destinations
                .retain(|id, _| rt.store.records.contains_key(id));
            if let Ok(mut policy) = rt.policy.write() {
                policy
                    .restrictions
                    .retain(|id, _| rt.store.records.contains_key(id));
            }
            last_expiry = Instant::now();
        }
        rt.publish();
        // Apply bounded backpressure instead of losing completion events when a
        // caller supplies a bounded channel. Each pass handles at most 16 streams.
        let events = rt.pending_events.borrow_mut().drain(..).collect::<Vec<_>>();
        for event in events {
            if rt.events.send_async(event).await.is_err() {
                return;
            }
        }
    }
    for id in rt
        .connections
        .values()
        .map(|c| c.id.clone())
        .collect::<Vec<_>>()
    {
        rt.pause(&id, "File connection stopped".into());
    }
    rt.publish();
    let events = rt.pending_events.borrow_mut().drain(..).collect::<Vec<_>>();
    for event in events {
        let _ = rt.events.send_async(event).await;
    }
}
