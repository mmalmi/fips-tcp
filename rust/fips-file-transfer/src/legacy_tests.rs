use crate::wire::Reader;
use crate::*;
use sha2::{Digest, Sha256};
use std::ops::Deref;
use std::{
    fs,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4, UdpSocket},
    time::Duration,
};

const TOKEN: &str = "0102030405060708010203040506070801020304050607080102030405060708";

struct LegacySender(TransferSender);
impl Deref for LegacySender {
    type Target = TransferSender;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl LegacySender {
    fn receive(
        &self,
        id: String,
        token: String,
        peer: PeerIdentity,
        files: Vec<TransferFile>,
        directory: PathBuf,
    ) -> Result<()> {
        self.0.receive(id, token, peer, files, directory, false)
    }
}

struct Peer {
    endpoint: Arc<FipsEndpoint>,
    identity: PeerIdentity,
    sender: LegacySender,
    events: flume::Receiver<TransferEvent>,
    task: JoinHandle<()>,
    state: tempfile::TempDir,
}
impl Peer {
    async fn new(address: SocketAddrV4) -> Self {
        let mut config = fips_core::Config::new();
        config.node.control.enabled = false;
        config.node.discovery.local.rendezvous_addr = address;
        config.node.discovery.local.retry_interval_ms = 20;
        config.node.discovery.lan.enabled = false;
        config.node.discovery.nostr.enabled = false;
        config.node.routing.mode = fips_core::config::RoutingMode::ReplyLearned;
        let endpoint = Arc::new(
            FipsEndpoint::builder()
                .config(config)
                .local_rendezvous()
                .without_system_tun()
                .bind()
                .await
                .unwrap(),
        );
        let identity = PeerIdentity::from_npub(endpoint.npub()).unwrap();
        let (tx, events) = flume::unbounded();
        let state = tempfile::tempdir().unwrap();
        let (sender, task) = start(endpoint.clone(), state.path().into(), tx)
            .await
            .unwrap();
        let sender = LegacySender(sender);
        Self {
            endpoint,
            identity,
            sender,
            events,
            task,
            state,
        }
    }
    async fn wait_for(&self, other: &Peer) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if self
                    .endpoint
                    .peers()
                    .await
                    .unwrap()
                    .iter()
                    .any(|p| p.connected && p.npub == other.endpoint.npub())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("FIPS authentication timed out");
    }
    async fn event(&self, predicate: impl Fn(&TransferEvent) -> bool) -> TransferEvent {
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                let event = self.events.recv_async().await.unwrap();
                if predicate(&event) {
                    return event;
                }
            }
        })
        .await
        .expect("file transfer event timed out")
    }
    async fn stop(self) {
        self.task.abort();
        self.endpoint.shutdown().await.unwrap();
    }
}
fn address() -> SocketAddrV4 {
    let socket = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    match socket.local_addr().unwrap() {
        SocketAddr::V4(a) => a,
        _ => unreachable!(),
    }
}
fn source(directory: &std::path::Path, name: &str, bytes: &[u8]) -> TransferFile {
    let path = directory.join(name);
    fs::write(&path, bytes).unwrap();
    TransferFile {
        filename: name.into(),
        size_bytes: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        path,
    }
}
async fn pair() -> (Peer, Peer) {
    let address = address();
    let a = Peer::new(address).await;
    let b = Peer::new(address).await;
    a.wait_for(&b).await;
    b.wait_for(&a).await;
    (a, b)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fips_multiple_files_are_sent_only_after_recipient_accepts() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let content = vec![0xab; 180_123];
    let files = vec![
        source(source_dir.path(), "empty.txt", b""),
        source(source_dir.path(), "photo.bin", &content),
        source(source_dir.path(), "notes.txt", b"hello"),
    ];
    a.sender
        .register_offer(
            "multi".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(a.events.is_empty());
    assert!(b.events.is_empty());
    assert_eq!(
        fs::read_dir(destination.path()).unwrap().count(),
        0,
        "no bytes are received before acceptance"
    );
    b.sender
        .receive(
            "multi".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    let accepted = a
        .event(|e| matches!(e, TransferEvent::Accepted { .. }))
        .await;
    assert!(
        matches!(accepted, TransferEvent::Accepted { peer, .. } if peer == peer_hex(b.identity))
    );
    let completed = b
        .event(|e| {
            matches!(
                e,
                TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
            )
        })
        .await;
    let TransferEvent::Completed { local_paths, .. } = completed else {
        panic!("{completed:?}")
    };
    assert_eq!(local_paths.len(), 3);
    assert_eq!(fs::read(&local_paths[0]).unwrap(), b"");
    assert_eq!(fs::read(&local_paths[1]).unwrap(), content);
    assert_eq!(fs::read(&local_paths[2]).unwrap(), b"hello");
    assert!(matches!(
        a.event(|e| matches!(
            e,
            TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
        ))
        .await,
        TransferEvent::Completed { .. }
    ));
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_unauthorized_device_and_bad_token_cannot_claim_offer() {
    let rendezvous = address();
    let a = Peer::new(rendezvous).await;
    let b = Peer::new(rendezvous).await;
    let outsider = Peer::new(rendezvous).await;
    a.wait_for(&b).await;
    b.wait_for(&a).await;
    a.wait_for(&outsider).await;
    outsider.wait_for(&a).await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "secret.txt", b"recipient only")];
    a.sender
        .register_offer(
            "private".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    outsider
        .sender
        .receive(
            "private".into(),
            TOKEN.into(),
            a.identity,
            files.clone(),
            destination.path().into(),
        )
        .unwrap();
    outsider
        .event(|e| matches!(e, TransferEvent::Failed { .. }))
        .await;
    assert!(
        a.events.is_empty(),
        "unauthorized request did not claim offer"
    );
    assert!(!destination.path().join("Received files private").exists());
    b.sender
        .receive(
            "private".into(),
            "ff".repeat(32),
            a.identity,
            files.clone(),
            destination.path().into(),
        )
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert!(a.events.is_empty(), "bad token did not claim offer");
    b.task.abort();
    let _ = b.task.await;
    let retry_state = tempfile::tempdir().unwrap();
    let (tx, events) = flume::unbounded();
    let (sender, task) = start(b.endpoint.clone(), retry_state.path().into(), tx)
        .await
        .unwrap();
    let b = Peer {
        sender: LegacySender(sender),
        events,
        task,
        state: retry_state,
        ..b
    };
    b.sender
        .receive(
            "private".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    assert!(matches!(
        b.event(|e| matches!(
            e,
            TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
        ))
        .await,
        TransferEvent::Completed { .. }
    ));
    a.stop().await;
    b.stop().await;
    outsider.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_offer_cannot_be_replayed_by_another_authorized_device() {
    let rendezvous = address();
    let a = Peer::new(rendezvous).await;
    let b = Peer::new(rendezvous).await;
    let c = Peer::new(rendezvous).await;
    a.wait_for(&b).await;
    b.wait_for(&a).await;
    a.wait_for(&c).await;
    c.wait_for(&a).await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let other_destination = tempfile::tempdir().unwrap();
    let files = vec![source(
        source_dir.path(),
        "note.txt",
        b"one accepting device",
    )];
    a.sender
        .register_offer(
            "once".into(),
            TOKEN.into(),
            vec![a.identity, b.identity, c.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .receive(
            "once".into(),
            TOKEN.into(),
            a.identity,
            files.clone(),
            destination.path().into(),
        )
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    c.sender
        .receive(
            "once".into(),
            TOKEN.into(),
            a.identity,
            files,
            other_destination.path().into(),
        )
        .unwrap();
    c.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert_eq!(fs::read_dir(other_destination.path()).unwrap().count(), 0);
    a.stop().await;
    b.stop().await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_removes_partial_files_and_notifies_both_devices() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let files = vec![source(
        source_dir.path(),
        "large.bin",
        &vec![7; 16 * 1024 * 1024],
    )];
    a.sender
        .register_offer(
            "cancel".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .receive(
            "cancel".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    a.event(|e| matches!(e, TransferEvent::Accepted { .. }))
        .await;
    b.sender.cancel("cancel").unwrap();
    b.event(|e| matches!(e, TransferEvent::Cancelled { .. }))
        .await;
    a.event(|e| matches!(e, TransferEvent::Cancelled { .. }))
        .await;
    assert!(!destination.path().join("Received files cancel").exists());
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_accepts_deliver_to_exactly_one_authorized_device() {
    let rendezvous = address();
    let a = Peer::new(rendezvous).await;
    let b = Peer::new(rendezvous).await;
    let c = Peer::new(rendezvous).await;
    a.wait_for(&b).await;
    b.wait_for(&a).await;
    a.wait_for(&c).await;
    c.wait_for(&a).await;
    let source_dir = tempfile::tempdir().unwrap();
    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "one.bin", &vec![9; 120_000])];
    a.sender
        .register_offer(
            "race".into(),
            TOKEN.into(),
            vec![b.identity, c.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .receive(
            "race".into(),
            TOKEN.into(),
            a.identity,
            files.clone(),
            first.path().into(),
        )
        .unwrap();
    c.sender
        .receive(
            "race".into(),
            TOKEN.into(),
            a.identity,
            files,
            second.path().into(),
        )
        .unwrap();
    let terminal = |e: &TransferEvent| {
        matches!(
            e,
            TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
        )
    };
    let (b_result, c_result) = tokio::join!(b.event(terminal), c.event(terminal));
    let b_won = matches!(b_result, TransferEvent::Completed { .. });
    let c_won = matches!(c_result, TransferEvent::Completed { .. });
    assert_ne!(b_won, c_won, "exactly one accepting device receives bytes");
    assert_eq!(first.path().join("Received files race").exists(), b_won);
    assert_eq!(second.path().join("Received files race").exists(), c_won);
    a.stop().await;
    b.stop().await;
    c.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn roster_revocation_aborts_active_transfer_and_removes_partial_files() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let files = vec![source(
        source_dir.path(),
        "private.bin",
        &vec![3; 2 * 1024 * 1024],
    )];
    a.sender
        .register_offer(
            "revoke".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .receive(
            "revoke".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    a.event(|e| matches!(e, TransferEvent::Accepted { .. }))
        .await;
    a.sender.restrict_offer("revoke", Vec::new()).unwrap();
    a.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    b.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert!(!destination.path().join("Received files revoke").exists());
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hash_mismatch_fails_and_removes_received_bytes() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let mut files = vec![source(source_dir.path(), "broken.txt", b"actual file")];
    files[0].sha256 = "00".repeat(32);
    a.sender
        .register_offer("hash".into(), TOKEN.into(), vec![b.identity], files.clone())
        .unwrap();
    b.sender
        .receive(
            "hash".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    let failed = b.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert!(
        matches!(failed, TransferEvent::Failed { error, .. } if error.contains("did not match"))
    );
    assert!(!destination.path().join("Received files hash").exists());
    a.stop().await;
    b.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn decline_never_reads_source_files_and_revokes_the_offer() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let destination = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "declined.txt", b"not sent")];
    a.sender
        .register_offer(
            "decline".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    fs::remove_file(&files[0].path).unwrap();
    b.sender
        .decline("decline".into(), TOKEN.into(), a.identity)
        .unwrap();
    a.event(|e| matches!(e, TransferEvent::Declined { .. }))
        .await;
    b.event(|e| matches!(e, TransferEvent::Declined { .. }))
        .await;
    b.sender
        .receive(
            "decline".into(),
            TOKEN.into(),
            a.identity,
            files,
            destination.path().into(),
        )
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert_eq!(fs::read_dir(destination.path()).unwrap().count(), 0);
    a.stop().await;
    b.stop().await;
}

#[test]
fn bounded_frames_and_safe_filenames() {
    let mut reader = Reader::default();
    let framed = wire::data(&[42; 19]);
    assert!(reader.push(&framed[..2]).unwrap().is_empty());
    assert_eq!(
        reader.push(&framed[2..]).unwrap()[0],
        [vec![1], vec![42; 19]].concat()
    );
    assert!(reader.push(&u32::MAX.to_be_bytes()).is_err());
    let mut file = TransferFile {
        filename: "../escape".into(),
        size_bytes: 0,
        sha256: "00".repeat(32),
        path: PathBuf::new(),
    };
    assert!(wire::validate_manifest(&[file.manifest()]).is_err());
    file.filename = "ok.txt".into();
    assert!(wire::validate_manifest(&[file.manifest()]).is_ok());
    assert!(wire::validate_claim("../escape", TOKEN).is_err());
}

impl Peer {
    async fn restart_transport(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
        let (tx, events) = flume::unbounded();
        let (sender, task) = start(self.endpoint.clone(), self.state.path().into(), tx)
            .await
            .unwrap();
        self.sender = LegacySender(sender);
        self.events = events;
        self.task = task;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn v2_restart_both_engines_resumes_only_durable_bytes() {
    let (mut a, mut b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let bytes = vec![0x79; 8 * 1024 * 1024];
    let files = vec![source(source_dir.path(), "large.bin", &bytes)];
    a.sender
        .register_offer(
            "resume".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .0
        .receive(
            "resume".into(),
            TOKEN.into(),
            a.identity,
            files.clone(),
            output.path().into(),
            true,
        )
        .unwrap();
    b.event(|e|matches!(e,TransferEvent::Progress{transferred_bytes,..} if *transferred_bytes>=1024*1024)).await;
    b.restart_transport().await;
    a.restart_transport().await;
    let snapshot = b
        .sender
        .snapshots()
        .into_iter()
        .find(|r| r.id == "resume")
        .unwrap();
    assert_eq!(snapshot.status, TransferStatus::Paused);
    assert!(snapshot.transferred_bytes >= 1024 * 1024);
    a.sender
        .register_offer("resume".into(), TOKEN.into(), vec![b.identity], files)
        .unwrap();
    b.sender
        .resume("resume", directory_destination(output.path().into()))
        .unwrap();
    let event = b
        .event(|e| {
            matches!(
                e,
                TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
            )
        })
        .await;
    let TransferEvent::Completed { local_paths, .. } = event else {
        panic!("{event:?}")
    };
    assert_eq!(fs::read(&local_paths[0]).unwrap(), bytes);
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
    a.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    a.stop().await;
    b.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completed_receipt_replays_without_source_or_duplicate_output() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "hello.txt", b"hello")];
    a.sender
        .register_offer(
            "receipt".into(),
            TOKEN.into(),
            vec![b.identity],
            files.clone(),
        )
        .unwrap();
    b.sender
        .0
        .receive(
            "receipt".into(),
            TOKEN.into(),
            a.identity,
            files.clone(),
            output.path().into(),
            true,
        )
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    a.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    fs::remove_file(&files[0].path).unwrap();
    // Receiver-driven final confirmation uses the durable aggregate digest.
    b.sender
        .resume("receipt", directory_destination(output.path().into()))
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    a.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    // Sender-driven confirmation probes return the same durable receipt.
    a.sender.resume_offer("receipt").unwrap();
    a.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
    a.stop().await;
    b.stop().await;
}
struct FailedDestination;
impl Destination for FailedDestination {
    fn prepare(&self, _: String, _: Vec<ManifestFile>) -> Result<()> {
        Err("Destination full".into())
    }
    fn write(&self, _: u32, _: Vec<u8>) -> Result<()> {
        unreachable!()
    }
    fn finish_file(&self, _: u32) -> Result<()> {
        unreachable!()
    }
    fn commit(&self) -> Result<Vec<String>> {
        unreachable!()
    }
    fn abort(&self) {}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_export_can_be_saved_after_sender_goes_offline() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "hello.txt", b"hello")];
    a.sender
        .register_offer("save".into(), TOKEN.into(), vec![b.identity], files.clone())
        .unwrap();
    b.sender
        .receive_into(
            "save".into(),
            TOKEN.into(),
            a.identity,
            files,
            Arc::new(FailedDestination),
            true,
        )
        .unwrap();
    b.event(|e| matches!(e,TransferEvent::Paused{error,..} if error.contains("export")))
        .await;
    a.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    a.stop().await;
    b.sender
        .resume("save", directory_destination(output.path().into()))
        .unwrap();
    let event = b
        .event(|e| {
            matches!(
                e,
                TransferEvent::Completed { .. } | TransferEvent::Failed { .. }
            )
        })
        .await;
    let TransferEvent::Completed { local_paths, .. } = event else {
        panic!("{event:?}")
    };
    assert_eq!(fs::read(&local_paths[0]).unwrap(), b"hello");
    b.stop().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_band_offers_default_deny_then_require_consent() {
    let (a, b) = pair().await;
    let source_dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let files = vec![source(source_dir.path(), "hello.txt", b"hello")];
    a.sender.set_allowed_peers(vec![b.identity]).unwrap();
    a.sender
        .send_offer("denied".into(), TOKEN.into(), b.identity, files.clone())
        .unwrap();
    a.event(|e| matches!(e, TransferEvent::Failed { .. })).await;
    assert!(b.sender.snapshots().is_empty());
    b.sender.set_allowed_peers(vec![a.identity]).unwrap();
    a.sender
        .send_offer("allowed".into(), TOKEN.into(), b.identity, files)
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Offered { .. }))
        .await;
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 0);
    assert_eq!(
        a.sender
            .snapshots()
            .iter()
            .find(|r| r.id == "allowed")
            .unwrap()
            .peer,
        peer_hex(b.identity)
    );
    b.sender
        .accept("allowed", directory_destination(output.path().into()))
        .unwrap();
    b.event(|e| matches!(e, TransferEvent::Completed { .. }))
        .await;
    a.stop().await;
    b.stop().await;
}
