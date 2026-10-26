use crate::{
    CHUNK, Destination, ManifestFile, Result, TransferFile, TransferSnapshot, TransferStatus, wire,
};
use fips_core::PeerIdentity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

pub(crate) fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Record {
    pub id: String,
    pub token: String,
    pub files: Vec<TransferFile>,
    pub peers: Vec<String>,
    pub peer: Option<String>,
    pub incoming: bool,
    pub in_band: bool,
    pub resumable: bool,
    pub status: TransferStatus,
    pub offset: u64,
    pub local_paths: Vec<String>,
    pub expires: u64,
    pub error: Option<String>,
    #[serde(default)]
    pub completion_confirmed: bool,
    #[serde(default)]
    pub complete_prefix: Option<String>,
    #[serde(default)]
    pub export_pending: bool,
}
impl Record {
    pub fn manifest(&self) -> Vec<ManifestFile> {
        self.files.iter().map(TransferFile::manifest).collect()
    }
    pub fn total(&self) -> u64 {
        self.files.iter().map(|f| f.size_bytes).sum()
    }
    pub fn peer_identity(&self) -> Result<PeerIdentity> {
        PeerIdentity::from_npub(
            self.peer
                .as_deref()
                .ok_or("Receiving device is not selected")?,
        )
        .map_err(|e| e.to_string())
    }
    pub fn peer_hex(&self) -> String {
        self.peer
            .as_ref()
            .or_else(|| {
                if self.in_band {
                    self.peers.first()
                } else {
                    None
                }
            })
            .and_then(|peer| PeerIdentity::from_npub(peer).ok())
            .map(crate::peer_hex)
            .unwrap_or_default()
    }
    pub fn snapshot(&self) -> TransferSnapshot {
        TransferSnapshot {
            id: self.id.clone(),
            peer: self.peer_hex(),
            files: self.manifest(),
            status: if self.incoming
                && self.status == TransferStatus::Completed
                && (!self.completion_confirmed || self.export_pending)
            {
                TransferStatus::Paused
            } else {
                self.status
            },
            is_sender: !self.incoming,
            transferred_bytes: self.offset,
            total_bytes: self.total(),
            local_paths: self.local_paths.clone(),
            error: self.error.clone(),
        }
    }
}
pub(crate) struct Store {
    cancelled: Option<Arc<AtomicBool>>,
    pub root: PathBuf,
    pub records: HashMap<String, Record>,
    pub source: Arc<RwLock<Arc<dyn crate::Source>>>,
}
impl Store {
    pub fn open(root: PathBuf) -> Result<Self> {
        private_dir(&root)?;
        let mut store = Self {
            root,
            cancelled: None,
            records: HashMap::new(),
            source: Arc::new(RwLock::new(Arc::new(RegularSource))),
        };
        for entry in fs::read_dir(&store.root).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            if store.records.len() >= 256 {
                return Err("Too many saved file transfers".into());
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if !wire::valid_transfer_id(&name)
                || !entry.file_type().map_err(|e| e.to_string())?.is_dir()
            {
                continue;
            }
            let path = entry.path().join("record.json");
            if !path.exists() {
                continue;
            }
            let mut input = readonly_file(&path)?;
            if input.metadata().map_err(|e| e.to_string())?.len() > 256 * 1024 {
                return Err("Saved file transfer is too large".into());
            }
            let mut bytes = Vec::new();
            Read::by_ref(&mut input)
                .take(256 * 1024 + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| e.to_string())?;
            if bytes.len() > 256 * 1024 {
                return Err("Saved file transfer is too large".into());
            }
            let mut record: Record =
                serde_json::from_slice(&bytes).map_err(|_| "Saved file transfer is invalid")?;
            wire::validate_claim(&record.id, &record.token)?;
            wire::validate_manifest(&record.manifest())?;
            if record.id != name || record.offset > record.total() {
                return Err("Saved file transfer is invalid".into());
            }
            if !record.status.terminal() && record.status != TransferStatus::Offered {
                record.status = TransferStatus::Paused;
                record.error = Some("Transfer interrupted. Resume when ready.".into());
            }
            if record.incoming && !record.status.terminal() {
                store.truncate_parts(&record)?;
            }
            store.records.insert(record.id.clone(), record);
        }
        store.expire(now())?;
        Ok(store)
    }
    pub fn worker(&self, cancelled: Arc<AtomicBool>) -> Self {
        Self {
            root: self.root.clone(),
            records: HashMap::new(),
            source: self.source.clone(),
            cancelled: Some(cancelled),
        }
    }
    fn check_cancelled(&self) -> Result<()> {
        if self
            .cancelled
            .as_ref()
            .is_some_and(|v| v.load(Ordering::Relaxed))
        {
            Err("File transfer stopped".into())
        } else {
            Ok(())
        }
    }
    /// Expiration removes only this engine's known journal and staging names.
    /// A completed private result without an exported copy remains user data.
    pub fn expire(&mut self, current_time: u64) -> Result<()> {
        let expired = self
            .records
            .values()
            .filter(|r| {
                r.expires <= current_time
                    && !(r.incoming
                        && r.status == TransferStatus::Completed
                        && r.local_paths
                            .iter()
                            .any(|path| Path::new(path).starts_with(self.dir(&r.id))))
            })
            .cloned()
            .collect::<Vec<_>>();
        for record in expired {
            self.remove_parts(&record)?;
            for name in ["record.next", "record.json"] {
                match fs::remove_file(self.dir(&record.id).join(name)) {
                    Ok(()) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(e.to_string()),
                }
            }
            // Other contents are never recursively removed.
            let _ = fs::remove_dir(self.dir(&record.id));
            self.records.remove(&record.id);
        }
        Ok(())
    }
    pub fn snapshots(&self) -> Vec<TransferSnapshot> {
        let mut rows = self
            .records
            .values()
            .map(Record::snapshot)
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| a.id.cmp(&b.id));
        rows
    }
    pub fn dir(&self, id: &str) -> PathBuf {
        self.root.join(id)
    }
    pub fn part(&self, id: &str, index: usize) -> PathBuf {
        self.dir(id).join(format!("{index}.part"))
    }
    pub fn put(&mut self, record: Record) -> Result<()> {
        self.persist(&record)?;
        self.records.insert(record.id.clone(), record);
        Ok(())
    }
    pub fn persist(&self, r: &Record) -> Result<()> {
        wire::validate_claim(&r.id, &r.token)?;
        let dir = self.dir(&r.id);
        private_dir(&dir)?;
        let bytes = serde_json::to_vec(r).map_err(|e| e.to_string())?;
        let temp = dir.join("record.next");
        let mut file = private_file(&temp, false)?;
        file.set_len(0).map_err(|e| e.to_string())?;
        file.write_all(&bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        fs::rename(temp, dir.join("record.json")).map_err(|e| e.to_string())?;
        sync_dir(&dir)
    }
    pub fn prepare_receive(&self, r: &Record) -> Result<()> {
        private_dir(&self.dir(&r.id))?;
        for i in 0..r.files.len() {
            let _ = private_file(&self.part(&r.id, i), false)?;
        }
        self.truncate_parts(r)
    }
    fn truncate_parts(&self, r: &Record) -> Result<()> {
        let mut remaining = r.offset;
        for (i, file) in r.files.iter().enumerate() {
            let length = remaining.min(file.size_bytes);
            remaining -= length;
            let path = self.part(&r.id, i);
            if !path.exists() {
                if length > 0 {
                    return Err("Saved partial file is missing".into());
                }
                continue;
            }
            let handle = private_file(&path, false)?;
            if handle.metadata().map_err(|e| e.to_string())?.len() < length {
                return Err("Saved partial file is incomplete".into());
            }
            handle.set_len(length).map_err(|e| e.to_string())?;
        }
        Ok(())
    }
    pub fn write(&self, r: &mut Record, mut bytes: &[u8]) -> Result<()> {
        if bytes.is_empty() || bytes.len() > CHUNK || r.offset + bytes.len() as u64 > r.total() {
            return Err("Invalid file data length".into());
        }
        while !bytes.is_empty() {
            let (index, offset) = locate(&r.files, r.offset).ok_or("Unexpected file data")?;
            let length = (r.files[index].size_bytes - offset).min(bytes.len() as u64) as usize;
            let mut file = private_file(&self.part(&r.id, index), false)?;
            file.seek(SeekFrom::Start(offset))
                .and_then(|_| file.write_all(&bytes[..length]))
                .map_err(|e| e.to_string())?;
            r.offset += length as u64;
            bytes = &bytes[length..];
        }
        Ok(())
    }
    /// Persist only offsets whose preceding bytes have reached stable storage.
    pub fn checkpoint(&self, r: &Record) -> Result<()> {
        if r.incoming && r.status != TransferStatus::Completed {
            for i in 0..r.files.len() {
                readonly_file(&self.part(&r.id, i))?
                    .sync_all()
                    .map_err(|e| e.to_string())?;
            }
        }
        self.persist(r)
    }
    pub fn prefix_hash(&self, r: &Record, offset: u64) -> Result<String> {
        self.prefix_hasher(r, offset)
            .map(|hash| format!("{:x}", hash.finalize()))
    }
    pub fn prefix_hasher(&self, r: &Record, offset: u64) -> Result<Sha256> {
        let paths = r
            .files
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if r.incoming {
                    self.part(&r.id, i)
                } else {
                    f.path.clone()
                }
            })
            .collect::<Vec<_>>();
        hash_prefix(
            &r.files,
            &paths,
            offset,
            &self.cancelled,
            if r.incoming {
                None
            } else {
                Some(
                    self.source
                        .read()
                        .map_err(|_| "File source unavailable")?
                        .clone(),
                )
            },
        )
    }
    pub fn read_chunk(&self, r: &Record, offset: u64) -> Result<Option<Vec<u8>>> {
        let Some((index, within)) = locate(&r.files, offset) else {
            return Ok(None);
        };
        let input = &r.files[index];
        let mut file = self
            .source
            .read()
            .map_err(|_| "File source unavailable")?
            .open(&input.path)?;
        let meta = file.metadata().map_err(|e| e.to_string())?;
        if !meta.is_file() || meta.len() != input.size_bytes {
            return Err("Selected file changed".into());
        }
        file.seek(SeekFrom::Start(within))
            .map_err(|e| e.to_string())?;
        let mut bytes = vec![0; (input.size_bytes - within).min(CHUNK as u64) as usize];
        file.read_exact(&mut bytes).map_err(|e| e.to_string())?;
        Ok(Some(bytes))
    }
    pub fn verify(&self, r: &Record) -> Result<()> {
        if r.offset != r.total() {
            return Err("File transfer ended early".into());
        }
        for (i, expected) in r.files.iter().enumerate() {
            self.check_cancelled()?;
            let mut input = readonly_file(&self.part(&r.id, i))?;
            input.sync_all().map_err(|e| e.to_string())?;
            let mut hash = Sha256::new();
            let mut buf = [0; CHUNK];
            loop {
                self.check_cancelled()?;
                let n = input.read(&mut buf).map_err(|e| e.to_string())?;
                if n == 0 {
                    break;
                }
                hash.update(&buf[..n]);
            }
            if format!("{:x}", hash.finalize()) != expected.sha256.to_ascii_lowercase() {
                return Err("Received file did not match the offer".into());
            }
        }
        Ok(())
    }
    pub fn commit_private(&self, r: &mut Record) -> Result<()> {
        // Persist durable ownership before exporting; retries replay this receipt.
        r.local_paths = (0..r.files.len())
            .map(|i| self.part(&r.id, i).to_string_lossy().into_owned())
            .collect();
        r.status = TransferStatus::Completed;
        r.export_pending = true;
        r.error = None;
        self.persist(r)
    }
    pub fn export(&self, r: &Record, dest: &dyn Destination) -> Result<Vec<String>> {
        let result = (|| {
            self.check_cancelled()?;
            dest.prepare(r.id.clone(), r.manifest())?;
            for i in 0..r.files.len() {
                let mut input = readonly_file(&self.part(&r.id, i))?;
                loop {
                    self.check_cancelled()?;
                    let mut bytes = vec![0; CHUNK];
                    let n = input.read(&mut bytes).map_err(|e| e.to_string())?;
                    if n == 0 {
                        break;
                    }
                    bytes.truncate(n);
                    dest.write(i as u32, bytes)?;
                }
                dest.finish_file(i as u32)?;
            }
            self.check_cancelled()?;
            dest.commit()
        })();
        if result.is_err() {
            dest.abort();
        }
        result
    }
    pub fn remove_parts(&self, r: &Record) -> Result<()> {
        for i in 0..r.files.len() {
            match fs::remove_file(self.part(&r.id, i)) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.to_string()),
            }
        }
        Ok(())
    }
}
fn locate(files: &[TransferFile], mut offset: u64) -> Option<(usize, u64)> {
    for (i, f) in files.iter().enumerate() {
        if offset < f.size_bytes {
            return Some((i, offset));
        }
        offset -= f.size_bytes;
    }
    None
}
fn hash_prefix(
    files: &[TransferFile],
    paths: &[PathBuf],
    mut length: u64,
    cancelled: &Option<Arc<AtomicBool>>,
    source: Option<Arc<dyn crate::Source>>,
) -> Result<Sha256> {
    if length > files.iter().map(|f| f.size_bytes).sum::<u64>() {
        return Err("Invalid resume offset".into());
    }
    let mut hash = Sha256::new();
    let mut buf = [0u8; CHUNK];
    for (f, path) in files.iter().zip(paths) {
        let mut remaining = length.min(f.size_bytes);
        length -= remaining;
        if remaining == 0 {
            continue;
        }
        let mut input = if let Some(source) = &source {
            source.open(path)?
        } else {
            readonly_file(path)?
        };
        while remaining > 0 {
            if cancelled
                .as_ref()
                .is_some_and(|v| v.load(Ordering::Relaxed))
            {
                return Err("File transfer stopped".into());
            }
            let n = remaining.min(CHUNK as u64) as usize;
            input.read_exact(&mut buf[..n]).map_err(|e| e.to_string())?;
            hash.update(&buf[..n]);
            remaining -= n as u64;
        }
    }
    Ok(hash)
}
fn private_dir(path: &Path) -> Result<()> {
    if !path.try_exists().map_err(|e| e.to_string())? {
        fs::create_dir_all(path).map_err(|e| e.to_string())?;
    }
    if fs::symlink_metadata(path)
        .map_err(|e| e.to_string())?
        .file_type()
        .is_symlink()
    {
        return Err("File transfer directory must not be a symbolic link".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
    }
    Ok(())
}
fn private_file(path: &Path, exclusive: bool) -> Result<File> {
    let mut o = OpenOptions::new();
    o.read(true).write(true);
    if exclusive {
        o.create_new(true);
    } else {
        o.create(true);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        o.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    o.open(path).map_err(|e| e.to_string())
}
fn sync_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

pub(crate) struct DirectoryDestination {
    root: PathBuf,
    state: Mutex<Option<Output>>,
}
struct Output {
    directory: PathBuf,
    files: Vec<PathBuf>,
    names: Vec<PathBuf>,
    committed: bool,
}
impl DirectoryDestination {
    pub fn new(root: PathBuf) -> Self {
        Self {
            root,
            state: Mutex::new(None),
        }
    }
}
impl Destination for DirectoryDestination {
    fn prepare(&self, id: String, files: Vec<ManifestFile>) -> Result<()> {
        if !wire::valid_transfer_id(&id) {
            return Err("Invalid transfer".into());
        }
        wire::validate_manifest(&files)?;
        let mut slot = self
            .state
            .lock()
            .map_err(|_| "File destination unavailable")?;
        if slot.is_some() {
            return Err("File destination already in use".into());
        }
        if !self.root.is_dir() {
            return Err("Choose a destination folder".into());
        }
        let directory = self.root.join(format!("Received files {id}"));
        fs::create_dir(&directory).map_err(|e| e.to_string())?;
        let mut out = Output {
            directory,
            files: vec![],
            names: vec![],
            committed: false,
        };
        for (i, f) in files.iter().enumerate() {
            let path = out.directory.join(format!("{}-{}", i + 1, f.filename));
            let _ = private_file(&path, true)?;
            out.files.push(path.clone());
            out.names.push(path);
        }
        *slot = Some(out);
        Ok(())
    }
    fn write(&self, index: u32, bytes: Vec<u8>) -> Result<()> {
        if bytes.len() > CHUNK {
            return Err("File chunk too large".into());
        }
        let state = self
            .state
            .lock()
            .map_err(|_| "File destination unavailable")?;
        let path = state
            .as_ref()
            .and_then(|s| s.files.get(index as usize))
            .ok_or("File destination closed")?;
        OpenOptions::new()
            .append(true)
            .open(path)
            .and_then(|mut f| f.write_all(&bytes))
            .map_err(|e| e.to_string())
    }
    fn finish_file(&self, index: u32) -> Result<()> {
        let state = self
            .state
            .lock()
            .map_err(|_| "File destination unavailable")?;
        let path = state
            .as_ref()
            .and_then(|s| s.files.get(index as usize))
            .ok_or("File destination closed")?;
        File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }
    fn commit(&self) -> Result<Vec<String>> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| "File destination unavailable")?;
        let s = state.as_mut().ok_or("File destination closed")?;
        if !s.committed {
            sync_dir(&s.directory)?;
            s.committed = true;
        }
        Ok(s.names
            .iter()
            .map(|p| p.to_string_lossy().into_owned())
            .collect())
    }
    fn abort(&self) {
        if let Ok(mut s) = self.state.lock() {
            s.take();
        }
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        if !self.committed {
            for p in self.files.iter() {
                let _ = fs::remove_file(p);
            }
            let _ = fs::remove_dir(&self.directory);
        }
    }
}

struct RegularSource;
impl crate::Source for RegularSource {
    fn open(&self, path: &Path) -> Result<File> {
        readonly_file(path)
    }
}
fn readonly_file(path: &Path) -> Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|e| e.to_string())?;
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("File source is not a regular file".into());
    }
    Ok(file)
}
