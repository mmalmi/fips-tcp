use super::*;
use std::fs;

fn record(id: &str) -> Record {
    let peer = PeerIdentity::from_pubkey(
        fips_core::Identity::from_secret_bytes(&[1; 32])
            .unwrap()
            .pubkey(),
    );
    Record {
        id: id.into(),
        token: "22".repeat(32),
        files: vec![TransferFile {
            filename: "hello.txt".into(),
            size_bytes: 2,
            sha256: format!("{:x}", Sha256::digest(b"ab")),
            path: PathBuf::new(),
        }],
        peers: vec![peer.npub()],
        peer: Some(peer.npub()),
        incoming: false,
        in_band: false,
        resumable: true,
        status: TransferStatus::Completed,
        offset: 2,
        local_paths: vec![],
        expires: u64::MAX,
        error: None,
        completion_confirmed: true,
        complete_prefix: None,
        export_pending: false,
    }
}

fn runtime(store: Store) -> Runtime {
    let (events, _) = flume::unbounded();
    Runtime {
        store,
        connections: HashMap::new(),
        destinations: HashMap::new(),
        local_work: HashMap::new(),
        active: HashSet::new(),
        events,
        pending_events: RefCell::new(VecDeque::new()),
        policy: Arc::new(RwLock::new(Policy::default())),
        snapshots: Arc::new(RwLock::new(vec![])),
    }
}

#[test]
fn restored_completed_history_is_not_rebuilt_while_idle() {
    let state = tempfile::tempdir().unwrap();
    let mut store = Store::open(state.path().into()).unwrap();
    for i in 0..32 {
        store.put(record(&format!("completed-{i:02}"))).unwrap();
    }
    drop(store);
    let mut rt = runtime(Store::open(state.path().into()).unwrap());
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap().len(), 32);
    let mut previous = rt.snapshots.read().unwrap().as_ptr();
    let mut replacements = 0;
    let started = Instant::now();
    for _ in 0..1000 {
        rt.publish();
        let current = rt.snapshots.read().unwrap().as_ptr();
        // A replacement is allocated while the previous nonempty Vec is live.
        replacements += usize::from(current != previous);
        previous = current;
    }
    eprintln!(
        "idle snapshots: 32 records, 1000 turns, {replacements} replacements, {:?}",
        started.elapsed()
    );
    assert_eq!(replacements, 0, "unchanged history must remain cached");

    rt.terminal("completed-00", TransferStatus::Cancelled, None)
        .unwrap();
    rt.pause("completed-00", "connection closed".into());
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap().as_ptr(), previous);
}

#[test]
fn silent_record_and_pause_changes_are_published() {
    let state = tempfile::tempdir().unwrap();
    let mut rt = runtime(Store::open(state.path().into()).unwrap());
    rt.publish();
    let mut r = record("connecting");
    r.status = TransferStatus::Connecting;
    r.offset = 1;
    rt.store.put(r).unwrap();
    assert!(rt.pending_events.borrow().is_empty());
    rt.publish();
    let rows = rt.snapshots.read().unwrap();
    assert_eq!(rows[0].status, TransferStatus::Connecting);
    assert_eq!(rows[0].transferred_bytes, 1);
    drop(rows);
    rt.pause("connecting", "connection interrupted".into());
    rt.publish();
    let rows = rt.snapshots.read().unwrap();
    assert_eq!(rows[0].status, TransferStatus::Paused);
    assert_eq!(rows[0].error.as_deref(), Some("connection interrupted"));
    assert_eq!(rows[0].transferred_bytes, 1);
}

#[test]
fn data_progress_is_published_even_when_a_later_file_write_fails() {
    let state = tempfile::tempdir().unwrap();
    let mut rt = runtime(Store::open(state.path().into()).unwrap());
    let mut r = record("receiving");
    r.incoming = true;
    r.status = TransferStatus::Connecting;
    r.offset = 0;
    r.files.push(TransferFile {
        filename: "second.txt".into(),
        size_bytes: 2,
        sha256: format!("{:x}", Sha256::digest(b"cd")),
        path: PathBuf::new(),
    });
    let mut connection = Connection::new(
        r.id.clone(),
        r.peer_identity().unwrap(),
        Role::Receiving { ready: true },
    );
    rt.store.prepare_receive(&r).unwrap();
    rt.store.put(r).unwrap();
    rt.publish();
    rt.packet(wire::data(b"a")[4..].into(), &mut connection)
        .unwrap();
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap()[0].transferred_bytes, 1);
    assert_eq!(
        rt.snapshots.read().unwrap()[0].status,
        TransferStatus::Transferring
    );

    let second = rt.store.part("receiving", 1);
    fs::remove_file(&second).unwrap();
    fs::create_dir(&second).unwrap();
    assert!(
        rt.packet(wire::data(b"bcd")[4..].into(), &mut connection)
            .is_err()
    );
    assert!(rt.pending_events.borrow().is_empty());
    rt.publish();
    assert_eq!(fs::read(rt.store.part("receiving", 0)).unwrap(), b"ab");
    assert_eq!(rt.snapshots.read().unwrap()[0].transferred_bytes, 2);
}

#[test]
fn expiry_publishes_removals_before_a_later_cleanup_error() {
    let state = tempfile::tempdir().unwrap();
    let mut rt = runtime(Store::open(state.path().into()).unwrap());
    for id in ["expired-a", "expired-b"] {
        let mut r = record(id);
        r.expires = 1;
        rt.store.put(r).unwrap();
    }
    rt.publish();
    let blocked = rt.store.records.values().nth(1).unwrap().id.clone();
    fs::create_dir(rt.store.dir(&blocked).join("record.next")).unwrap();
    assert!(rt.store.expire(2).is_err());
    assert!(rt.pending_events.borrow().is_empty());
    rt.publish();
    let rows = rt.snapshots.read().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, blocked);
    let previous = rows.as_ptr();
    drop(rows);
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap().as_ptr(), previous);
}

#[tokio::test]
async fn sender_progress_refreshes_only_when_the_offset_changes() {
    let endpoint = Arc::new(
        FipsEndpoint::builder()
            .without_system_tun()
            .bind()
            .await
            .unwrap(),
    );
    let peer = PeerIdentity::from_npub(endpoint.npub()).unwrap();
    let mut tcp = FipsTcpEndpoint::bind(
        endpoint.clone(),
        PORT,
        fips_tcp::Config::default(),
        0x1234_5678,
    )
    .await
    .unwrap();
    let stream = tcp.connect(peer, 0).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut accepted = None;
        loop {
            accepted = accepted.or_else(|| tcp.accept());
            if tcp.state(stream) == Some(State::Established)
                && accepted.is_some_and(|id| tcp.state(id) == Some(State::Established))
            {
                break;
            }
            tcp.receive(0).await.unwrap();
        }
    })
    .await
    .expect("TCP/FIPS loopback handshake timed out");

    let state = tempfile::tempdir().unwrap();
    let mut rt = runtime(Store::open(state.path().into()).unwrap());
    let mut r = record("sender-progress");
    r.peer = Some(peer.npub());
    r.peers = vec![peer.npub()];
    r.status = TransferStatus::Transferring;
    r.offset = 0;
    rt.store.put(r).unwrap();
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap()[0].transferred_bytes, 0);
    let mut connection = Connection::new("sender-progress".into(), peer, Role::Sending);
    connection.offset = 1;
    // Retain a pending frame without reading a source or changing the journal.
    connection.pending = Some(Pending {
        bytes: vec![],
        offset: 0,
        marker: None,
    });
    connection.last_progress = Instant::now() - Duration::from_millis(250);
    assert!(
        rt.progress(stream, &mut connection, &mut tcp, 1)
            .await
            .unwrap()
    );
    rt.publish();
    let rows = rt.snapshots.read().unwrap();
    assert_eq!(rows[0].transferred_bytes, 1);
    assert_eq!(rows[0].status, TransferStatus::Transferring);
    let previous = rows.as_ptr();
    drop(rows);

    connection.last_progress = Instant::now() - Duration::from_millis(250);
    assert!(
        rt.progress(stream, &mut connection, &mut tcp, 2)
            .await
            .unwrap()
    );
    rt.publish();
    assert_eq!(rt.snapshots.read().unwrap().as_ptr(), previous);
    let events = rt.pending_events.borrow();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| matches!(
        event,
        TransferEvent::Progress {
            transferred_bytes: 1,
            total_bytes: 2,
            ..
        }
    )));
    drop(events);
    endpoint.shutdown().await.unwrap();
}
