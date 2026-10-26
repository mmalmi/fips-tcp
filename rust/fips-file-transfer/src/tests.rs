use crate::{
    disk::{Record, Store},
    *,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    fs,
    sync::atomic::{AtomicUsize, Ordering},
};

fn record(id: &str, bytes: &[u8]) -> Record {
    Record {
        id: id.into(),
        token: "22".repeat(32),
        files: vec![TransferFile {
            filename: "hello.txt".into(),
            size_bytes: bytes.len() as u64,
            sha256: format!("{:x}", Sha256::digest(bytes)),
            path: PathBuf::new(),
        }],
        peers: vec![],
        peer: None,
        incoming: true,
        in_band: false,
        resumable: true,
        status: TransferStatus::Transferring,
        offset: 0,
        local_paths: vec![],
        expires: u64::MAX,
        error: None,
        completion_confirmed: false,
        complete_prefix: None,
        export_pending: false,
    }
}
#[test]
fn shared_rust_typescript_frames_and_manifest_are_identical() {
    let values: Value = serde_json::from_str(include_str!(
        "../../../protocol/file-transfer-v2-vectors.json"
    ))
    .unwrap();
    let files: Vec<ManifestFile> =
        serde_json::from_value(values["manifest"]["files"].clone()).unwrap();
    assert_eq!(
        wire::manifest_hash(&files).unwrap(),
        values["manifest"]["sha256"].as_str().unwrap()
    );
    for value in values["controls"].as_array().unwrap() {
        let control: wire::Control = serde_json::from_value(value["control"].clone()).unwrap();
        let frame = wire::control(&control).unwrap();
        assert_eq!(
            frame.iter().map(|b| format!("{b:02x}")).collect::<String>(),
            value["frame_hex"].as_str().unwrap()
        );
        let mut reader = wire::Reader::default();
        let packets = reader.push(&frame).unwrap();
        assert_eq!(wire::parse_control(&packets[0]).unwrap(), control);
    }
}
#[test]
fn restart_truncates_uncheckpointed_bytes_and_keeps_durable_prefix() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().into()).unwrap();
    let mut r = record("restart", b"abcdefgh");
    store.prepare_receive(&r).unwrap();
    store.write(&mut r, b"abcd").unwrap();
    store.checkpoint(&r).unwrap();
    store.write(&mut r, b"ef").unwrap();
    store.records.insert(r.id.clone(), r);
    drop(store);
    let store = Store::open(dir.path().into()).unwrap();
    let r = &store.records["restart"];
    assert_eq!(r.offset, 4);
    assert_eq!(r.status, TransferStatus::Paused);
    assert_eq!(fs::read(store.part("restart", 0)).unwrap(), b"abcd");
    assert_eq!(
        store.prefix_hash(r, 4).unwrap(),
        format!("{:x}", Sha256::digest(b"abcd"))
    );
}
#[test]
fn cancelled_checkpoint_does_not_reopen_removed_partials() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().into()).unwrap();
    let mut r = record("cancelled", b"abcdefgh");
    store.prepare_receive(&r).unwrap();
    store.write(&mut r, b"abcd").unwrap();
    r.status = TransferStatus::Cancelled;
    store.put(r.clone()).unwrap();
    store.remove_parts(&r).unwrap();
    drop(store);
    let store = Store::open(dir.path().into()).unwrap();
    assert_eq!(store.records["cancelled"].status, TransferStatus::Cancelled);
    assert!(!store.part("cancelled", 0).exists());
}
#[test]
fn completed_journal_survives_restart_without_exporting_twice() {
    let dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().into()).unwrap();
    let mut r = record("complete", b"hello");
    store.prepare_receive(&r).unwrap();
    store.write(&mut r, b"hello").unwrap();
    store.verify(&r).unwrap();
    store.commit_private(&mut r).unwrap();
    let dest = directory_destination(output.path().into());
    r.local_paths = store.export(&r, dest.as_ref()).unwrap();
    r.export_pending = false;
    store.put(r).unwrap();
    drop(store);
    let store = Store::open(dir.path().into()).unwrap();
    let r = &store.records["complete"];
    assert_eq!(r.status, TransferStatus::Completed);
    assert_eq!(r.snapshot().status, TransferStatus::Paused);
    assert_eq!(fs::read(&r.local_paths[0]).unwrap(), b"hello");
    assert_eq!(fs::read_dir(output.path()).unwrap().count(), 1);
}
#[test]
fn corrupt_received_bytes_never_commit() {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(dir.path().into()).unwrap();
    let mut r = record("corrupt", b"hello");
    store.prepare_receive(&r).unwrap();
    store.write(&mut r, b"hallo").unwrap();
    assert!(store.verify(&r).is_err());
    assert_ne!(r.status, TransferStatus::Completed);
}
struct DeniedSource(AtomicUsize);
impl Source for DeniedSource {
    fn open(&self, _: &Path) -> Result<fs::File> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err("Source denied".into())
    }
}
#[test]
fn restored_source_paths_always_use_caller_policy() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().into()).unwrap();
    let mut r = record("sender", b"secret");
    r.incoming = false;
    r.files[0].path = dir.path().join("private");
    fs::write(&r.files[0].path, b"secret").unwrap();
    store.put(r).unwrap();
    drop(store);
    let store = Store::open(dir.path().into()).unwrap();
    let source = Arc::new(DeniedSource(AtomicUsize::new(0)));
    *store.source.write().unwrap() = source.clone();
    let r = &store.records["sender"];
    assert!(store.prefix_hash(r, 6).is_err());
    assert!(store.read_chunk(r, 0).is_err());
    assert_eq!(source.0.load(Ordering::Relaxed), 2);
}
#[test]
fn expiry_preserves_exported_files_and_unexported_private_results() {
    let dir = tempfile::tempdir().unwrap();
    let output = tempfile::tempdir().unwrap();
    let mut store = Store::open(dir.path().into()).unwrap();
    for id in ["exported", "private"] {
        let mut r = record(id, b"hello");
        store.prepare_receive(&r).unwrap();
        store.write(&mut r, b"hello").unwrap();
        store.verify(&r).unwrap();
        store.commit_private(&mut r).unwrap();
        if id == "exported" {
            r.local_paths = store
                .export(&r, directory_destination(output.path().into()).as_ref())
                .unwrap();
        }
        r.expires = 1;
        store.put(r).unwrap();
    }
    store.expire(2).unwrap();
    assert!(!store.records.contains_key("exported"));
    assert!(store.records.contains_key("private"));
    assert_eq!(
        fs::read(output.path().join("Received files exported/1-hello.txt")).unwrap(),
        b"hello"
    );
    assert_eq!(fs::read(store.part("private", 0)).unwrap(), b"hello");
}
