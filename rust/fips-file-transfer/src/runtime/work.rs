use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

enum Output {
    Prefix(Sha256),
    Verified,
    Exported(Vec<String>),
}
enum Purpose {
    ReceivePrefix,
    SendPrefix {
        record: Record,
        offset: u64,
        expected: String,
    },
    Verify,
    Export {
        local: bool,
    },
}
pub(super) struct Work {
    task: JoinHandle<Result<Output>>,
    cancelled: Arc<AtomicBool>,
    purpose: Purpose,
}
impl Drop for Work {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }
}
impl Runtime {
    fn work_store(&self) -> (Store, Arc<AtomicBool>) {
        let cancel = Arc::new(AtomicBool::new(false));
        (self.store.worker(cancel.clone()), cancel)
    }
    pub(super) fn receive_prefix(&self, c: &mut Connection, r: Record) {
        let (store, cancelled) = self.work_store();
        c.work = Some(Work {
            cancelled,
            purpose: Purpose::ReceivePrefix,
            task: tokio::task::spawn_blocking(move || {
                store.prefix_hasher(&r, r.offset).map(Output::Prefix)
            }),
        });
    }
    pub(super) fn send_prefix(
        &self,
        c: &mut Connection,
        record: Record,
        offset: u64,
        expected: String,
    ) {
        let (store, cancelled) = self.work_store();
        let r = record.clone();
        c.work = Some(Work {
            cancelled,
            purpose: Purpose::SendPrefix {
                record,
                offset,
                expected,
            },
            task: tokio::task::spawn_blocking(move || {
                store.prefix_hasher(&r, offset).map(Output::Prefix)
            }),
        });
    }
    pub(super) fn verify_received(&self, c: &mut Connection, record: Record) {
        let (store, cancelled) = self.work_store();
        c.work = Some(Work {
            cancelled,
            purpose: Purpose::Verify,
            task: tokio::task::spawn_blocking(move || {
                store.verify(&record).map(|_| Output::Verified)
            }),
        });
    }
    pub(super) async fn poll_work(&mut self, c: &mut Connection) -> Result<bool> {
        if !c.work.as_ref().is_some_and(|w| w.task.is_finished()) {
            return Ok(false);
        }
        let mut work = c.work.take().ok_or("File work unavailable")?;
        let result = (&mut work.task).await.map_err(|_| "File work stopped")?;
        c.last_activity = Instant::now();
        match (&work.purpose, result) {
            (Purpose::ReceivePrefix, Ok(Output::Prefix(prefix_sha256))) => {
                let r = self
                    .store
                    .records
                    .get(&c.id)
                    .ok_or("File offer unavailable")?;
                c.control(Control::Resume {
                    id: c.id.clone(),
                    token: r.token.clone(),
                    manifest: wire::manifest_hash(&r.manifest())?,
                    offset: r.offset,
                    prefix_sha256: format!("{:x}", prefix_sha256.finalize()),
                })?;
            }
            (
                Purpose::SendPrefix {
                    record,
                    offset,
                    expected,
                },
                Ok(Output::Prefix(hash)),
            ) if &format!("{:x}", hash.clone().finalize()) == expected => {
                c.hasher = hash;
                let mut r = record.clone();
                if !self.authorized(&r, c.peer) {
                    return Err("Device authorization changed".into());
                }
                r.peer = Some(c.peer.npub());
                r.resumable = true;
                r.offset = *offset;
                if r.status != TransferStatus::Completed {
                    r.status = TransferStatus::Transferring;
                }
                self.store.put(r)?;
                c.offset = *offset;
                c.role = Role::Sending;
                c.control(Control::Ready {
                    id: c.id.clone(),
                    offset: *offset,
                })?;
                self.emit(TransferEvent::Accepted {
                    transfer_id: c.id.clone(),
                    peer: peer_hex(c.peer),
                });
            }
            (Purpose::SendPrefix { .. }, _) => {
                c.control(Control::Rejected {
                    id: c.id.clone(),
                    reason: Rejection::Mismatch,
                })?;
                c.role = Role::Finish;
            }
            (Purpose::Verify, Ok(Output::Verified)) => {
                let mut r = self
                    .store
                    .records
                    .get(&c.id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                self.store.commit_private(&mut r)?;
                self.store.records.insert(r.id.clone(), r.clone());
                if let Some(destination) = self.destinations.remove(&c.id) {
                    self.export_received(c, r, destination, false);
                } else {
                    self.receipt(c)?;
                }
            }
            (Purpose::Export { local }, outcome) => {
                let mut r = self
                    .store
                    .records
                    .get(&c.id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                match outcome {
                    Ok(Output::Exported(paths)) => {
                        r.local_paths = paths;
                        r.export_pending = false;
                        r.error = None;
                    }
                    Err(error) => {
                        r.error = Some(format!("Received; could not export files: {error}"))
                    }
                    _ => return Err("Unexpected file work result".into()),
                }
                if *local && !r.export_pending {
                    r.completion_confirmed = true;
                }
                self.store.put(r.clone())?;
                if *local {
                    self.completed_event(&c.id, c.peer, r.local_paths);
                } else {
                    self.receipt(c)?;
                }
            }
            (_, Err(error)) => return Err(error),
            _ => return Err("Unexpected file work result".into()),
        }
        Ok(true)
    }
    pub(super) fn export_received(
        &self,
        c: &mut Connection,
        r: Record,
        destination: Arc<dyn Destination>,
        local: bool,
    ) {
        let (store, cancelled) = self.work_store();
        c.work = Some(Work {
            cancelled,
            purpose: Purpose::Export { local },
            task: tokio::task::spawn_blocking(move || {
                store.export(&r, destination.as_ref()).map(Output::Exported)
            }),
        });
    }
    pub(super) fn completed_event(&self, id: &str, peer: PeerIdentity, paths: Vec<String>) {
        if let Some(r) = self.store.records.get(id) {
            if r.export_pending {
                self.emit(TransferEvent::Paused {
                    transfer_id: id.into(),
                    peer: peer_hex(peer),
                    error: r
                        .error
                        .clone()
                        .unwrap_or_else(|| "Files received. Choose a folder to save them.".into()),
                });
                return;
            }
        }
        self.emit(TransferEvent::Completed {
            transfer_id: id.into(),
            peer: peer_hex(peer),
            local_paths: paths,
        });
    }
    pub(super) fn receipt(&self, c: &mut Connection) -> Result<()> {
        let r = self
            .store
            .records
            .get(&c.id)
            .ok_or("File offer unavailable")?;
        c.completion = Some(r.local_paths.clone());
        c.control(Control::Received)?;
        c.role = Role::Finish;
        Ok(())
    }
}
