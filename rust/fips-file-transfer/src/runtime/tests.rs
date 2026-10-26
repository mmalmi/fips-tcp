use super::*;

#[tokio::test]
async fn duplicate_resume_keeps_the_claim_and_retries_the_shared_prefix_vector() {
    let vectors: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../protocol/file-transfer-v2-vectors.json"
    ))
    .unwrap();
    let control: Control = serde_json::from_value(
        vectors["controls"]
            .as_array()
            .unwrap()
            .iter()
            .find(|v| v["control"]["type"] == "resume")
            .unwrap()["control"]
            .clone(),
    )
    .unwrap();
    let Control::Resume {
        id, token, offset, ..
    } = &control
    else {
        unreachable!()
    };
    let peer = PeerIdentity::from_pubkey(
        fips_core::Identity::from_secret_bytes(&[1; 32])
            .unwrap()
            .pubkey(),
    );
    let state = tempfile::tempdir().unwrap();
    let source = tempfile::tempdir().unwrap();
    let manifests: Vec<ManifestFile> =
        serde_json::from_value(vectors["manifest"]["files"].clone()).unwrap();
    let data: [&[u8]; 3] = [b"hello", b"", &[0, 1, 255]];
    let files = manifests
        .into_iter()
        .zip(data)
        .enumerate()
        .map(|(i, (f, bytes))| {
            let path = source.path().join(i.to_string());
            std::fs::write(&path, bytes).unwrap();
            TransferFile {
                filename: f.filename,
                size_bytes: f.size_bytes,
                sha256: f.sha256,
                path,
            }
        })
        .collect();
    let mut store = Store::open(state.path().into()).unwrap();
    store
        .put(Record {
            id: id.clone(),
            token: token.clone(),
            files,
            peers: vec![peer.npub()],
            peer: Some(peer.npub()),
            incoming: false,
            in_band: false,
            resumable: true,
            status: TransferStatus::Transferring,
            offset: *offset,
            local_paths: vec![],
            expires: u64::MAX,
            error: None,
            completion_confirmed: false,
            complete_prefix: None,
            export_pending: false,
        })
        .unwrap();
    let (events, _receiver) = flume::unbounded();
    let mut runtime = Runtime {
        store,
        connections: HashMap::new(),
        destinations: HashMap::new(),
        local_work: HashMap::new(),
        active: HashSet::from([id.clone()]),
        events,
        pending_events: RefCell::new(VecDeque::new()),
        policy: Arc::new(RwLock::new(Policy::default())),
        snapshots: Arc::new(RwLock::new(vec![])),
    };
    let old = ConnectionId::from_raw(1);
    runtime
        .connections
        .insert(old, Connection::new(id.clone(), peer, Role::Sending));
    let packet = wire::control(&control).unwrap()[4..].to_vec();
    let mut duplicate = Connection::new(String::new(), peer, Role::Await);
    runtime.packet(packet.clone(), &mut duplicate).unwrap();
    assert!(matches!(duplicate.role, Role::Finish));
    assert!(duplicate.id.is_empty());
    assert!(
        duplicate.queue.is_empty(),
        "a busy stream must not invalidate the durable offer"
    );
    assert_eq!(
        runtime.store.records[id].status,
        TransferStatus::Transferring
    );
    assert_eq!(runtime.store.records[id].offset, *offset);
    assert!(runtime.pending_events.borrow().is_empty());
    assert!(runtime.connections.contains_key(&old));

    runtime.connections.remove(&old);
    let mut retried = Connection::new(String::new(), peer, Role::Await);
    runtime.packet(packet, &mut retried).unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while retried.work.is_some() {
            runtime.poll_work(&mut retried).await.unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(retried.role, Role::Sending));
    assert_eq!(retried.offset, *offset);
    let ready = retried.queue.pop_front().unwrap();
    assert_eq!(
        wire::parse_control(&ready[4..]).unwrap(),
        Control::Ready {
            id: id.clone(),
            offset: *offset
        }
    );

    // A late or repeated Cancel cannot contradict a durable completed receipt.
    let mut record = runtime.store.records[id].clone();
    record.status = TransferStatus::Completed;
    runtime.store.put(record).unwrap();
    runtime.pending_events.borrow_mut().clear();
    runtime.receipt(&mut retried).unwrap();
    let cancel = wire::control(&Control::Cancel).unwrap()[4..].to_vec();
    runtime.packet(cancel.clone(), &mut retried).unwrap();
    runtime.packet(cancel, &mut retried).unwrap();
    assert_eq!(runtime.store.records[id].status, TransferStatus::Completed);
    assert!(runtime.pending_events.borrow().is_empty());
    let receipt = retried.queue.pop_front().unwrap();
    assert_eq!(
        wire::parse_control(&receipt[4..]).unwrap(),
        Control::Received
    );
}
