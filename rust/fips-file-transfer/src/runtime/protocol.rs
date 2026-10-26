use super::*;

impl Runtime {
    fn reject(&self, c: &mut Connection, id: String, reason: Rejection) -> Result<()> {
        // Rejections never bind this connection to someone else's transfer.
        c.control(Control::Rejected { id, reason })?;
        c.role = Role::Finish;
        Ok(())
    }
    pub(super) fn packet(&mut self, packet: Vec<u8>, c: &mut Connection) -> Result<()> {
        if packet.first() == Some(&1) {
            if !matches!(c.role, Role::Receiving { ready: true }) {
                return Err("Unexpected file data".into());
            }
            let mut r = self
                .store
                .records
                .remove(&c.id)
                .ok_or("File offer unavailable")?;
            let mut result = self.store.write(&mut r, &packet[1..]);
            if result.is_ok() {
                r.status = TransferStatus::Transferring;
                c.offset = r.offset;
                if r.offset - c.last_checkpoint >= 1024 * 1024 {
                    result = self.store.checkpoint(&r);
                    if result.is_ok() {
                        c.last_checkpoint = r.offset;
                    }
                }
            }
            self.store.records.insert(c.id.clone(), r);
            return result;
        }
        let control = wire::parse_control(&packet)?;
        match control {
            Control::Offer {
                id,
                token,
                version,
                files,
            } if matches!(c.role, Role::Await) => {
                wire::validate_claim(&id, &token)?;
                wire::validate_manifest(&files)?;
                if version != 2 {
                    return self.reject(c, id, Rejection::Unavailable);
                }
                if let Some(r) = self.store.records.get(&id) {
                    if !r.incoming
                        || r.peer.as_deref() != Some(&c.peer.npub())
                        || r.token != token
                        || r.manifest() != files
                        || r.expires <= disk::now()
                        || matches!(
                            r.status,
                            TransferStatus::Cancelled
                                | TransferStatus::Declined
                                | TransferStatus::Failed
                        )
                        || !self.authorized(r, c.peer)
                    {
                        return self.reject(c, id, Rejection::Unavailable);
                    }
                    // An exact authenticated probe replays the durable receipt without
                    // touching outputs, including external signed Iris offers.
                    if r.status == TransferStatus::Completed {
                        c.id = id;
                        c.completion = Some(r.local_paths.clone());
                        c.control(Control::Received)?;
                        c.role = Role::Finish;
                        return Ok(());
                    }
                } else {
                    if !self
                        .policy
                        .read()
                        .map_err(|_| "Authorization unavailable")?
                        .allowed
                        .contains(&peer_hex(c.peer))
                    {
                        return self.reject(c, id, Rejection::Unavailable);
                    }
                    if self.store.records.len() >= 256
                        || self
                            .store
                            .records
                            .values()
                            .filter(|r| !r.status.terminal())
                            .count()
                            >= MAX_TRANSFERS
                    {
                        return self.reject(c, id, Rejection::Unavailable);
                    }
                    let transfer_files = files
                        .iter()
                        .map(|f| TransferFile {
                            filename: f.filename.clone(),
                            size_bytes: f.size_bytes,
                            sha256: f.sha256.clone(),
                            path: PathBuf::new(),
                        })
                        .collect();
                    self.store.put(Record {
                        id: id.clone(),
                        token,
                        files: transfer_files,
                        peers: vec![],
                        peer: Some(c.peer.npub()),
                        incoming: true,
                        in_band: true,
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
                    self.emit(TransferEvent::Offered {
                        transfer_id: id.clone(),
                        peer: peer_hex(c.peer),
                        files,
                    });
                }
                c.id = id.clone();
                c.control(Control::Offered { id })?;
                c.role = Role::Finish;
            }
            Control::Offered { id } if matches!(c.role, Role::Announcing) && id == c.id => {
                if let Some(r) = self.store.records.get(&id).cloned() {
                    r.status = TransferStatus::Offered;
                    r.error = None;
                    self.store.put(r.clone())?;
                    self.emit(TransferEvent::Offered {
                        transfer_id: id,
                        peer: peer_hex(c.peer),
                        files: r.manifest(),
                    });
                }
                c.role = Role::Finish;
            }
            Control::Accept { id, token } | Control::Decline { id, token }
                if matches!(c.role, Role::Await) =>
            {
                let decline = matches!(wire::parse_control(&packet)?, Control::Decline { .. });
                wire::validate_claim(&id, &token)?;
                let mut r = self
                    .store
                    .records
                    .get(&id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                if r.token != token
                    || !self.authorized(&r, c.peer)
                    || r.status.terminal()
                    || r.expires <= disk::now()
                {
                    return Err("File offer unavailable".into());
                }
                if self.connections.values().any(|other| {
                    other.id == id && (matches!(other.role, Role::Sending) || other.work.is_some())
                }) {
                    return Err("File offer unavailable".into());
                }
                c.id = id.clone();
                r.peer = Some(c.peer.npub());
                r.resumable = false;
                if decline {
                    r.status = TransferStatus::Declined;
                    self.store.put(r)?;
                    self.emit(TransferEvent::Declined {
                        transfer_id: id,
                        peer: peer_hex(c.peer),
                    });
                    c.role = Role::Finish;
                } else {
                    r.offset = 0;
                    r.status = TransferStatus::Transferring;
                    self.store.put(r)?;
                    c.role = Role::Sending;
                    self.emit(TransferEvent::Accepted {
                        transfer_id: id,
                        peer: peer_hex(c.peer),
                    });
                }
            }
            Control::Resume {
                id,
                token,
                manifest,
                offset,
                prefix_sha256,
            } if matches!(c.role, Role::Await) => {
                wire::validate_claim(&id, &token)?;
                let Some(r) = self.store.records.get(&id).cloned() else {
                    return self.reject(c, id, Rejection::Unavailable);
                };
                if r.token != token
                    || r.incoming
                    || !self.authorized(&r, c.peer)
                    || matches!(
                        r.status,
                        TransferStatus::Cancelled
                            | TransferStatus::Declined
                            | TransferStatus::Failed
                    )
                {
                    return self.reject(c, id, Rejection::Unavailable);
                }
                if r.expires <= disk::now() {
                    return self.reject(c, id, Rejection::Expired);
                }
                if manifest != wire::manifest_hash(&r.manifest())?
                    || offset > r.total()
                    || (r.status == TransferStatus::Completed && offset != r.total())
                {
                    return self.reject(c, id, Rejection::Mismatch);
                }
                if self.connections.values().any(|other| {
                    other.id == id && (matches!(other.role, Role::Sending) || other.work.is_some())
                }) {
                    return self.reject(c, id, Rejection::Unavailable);
                }
                c.id = id;
                if offset == r.total() && r.complete_prefix.is_some() {
                    if r.complete_prefix.as_deref() != Some(prefix_sha256.as_str()) {
                        return self.reject(c, c.id.clone(), Rejection::Mismatch);
                    }
                    c.offset = offset;
                    c.role = Role::Sending;
                    c.control(Control::Ready {
                        id: c.id.clone(),
                        offset,
                    })?;
                } else {
                    self.send_prefix(c, r, offset, prefix_sha256);
                }
            }
            Control::Ready { id, offset }
                if matches!(c.role, Role::Receiving { ready: false })
                    && id == c.id
                    && offset == c.offset =>
            {
                c.idle_limit = Duration::from_secs(60);
                c.role = Role::Receiving { ready: true };
                self.emit(TransferEvent::Accepted {
                    transfer_id: id,
                    peer: peer_hex(c.peer),
                });
            }
            Control::Done if matches!(c.role, Role::Receiving { ready: true }) => {
                let r = self
                    .store
                    .records
                    .get(&c.id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                if r.status == TransferStatus::Completed {
                    self.receipt(c)?;
                } else {
                    self.verify_received(c, r);
                }
            }
            Control::Received
                if (matches!(c.role, Role::Sending) && c.done)
                    || matches!(c.role, Role::Announcing) =>
            {
                let mut r = self
                    .store
                    .records
                    .get(&c.id)
                    .cloned()
                    .ok_or("File offer unavailable")?;
                r.status = TransferStatus::Completed;
                r.offset = r.total();
                r.error = None;
                self.store.put(r)?;
                self.emit(TransferEvent::Completed {
                    transfer_id: c.id.clone(),
                    peer: peer_hex(c.peer),
                    local_paths: vec![],
                });
                c.role = Role::Finish;
            }
            Control::Rejected { id, reason } if id == c.id => {
                let error = format!("File transfer rejected: {reason:?}");
                self.terminal(&id, TransferStatus::Failed, Some(error.clone()))?;
                self.emit(TransferEvent::Failed {
                    transfer_id: id,
                    peer: peer_hex(c.peer),
                    error,
                });
                c.role = Role::Finish;
            }
            Control::Cancel if !c.id.is_empty() => {
                self.terminal(&c.id, TransferStatus::Cancelled, None)?;
                self.emit(TransferEvent::Cancelled {
                    transfer_id: c.id.clone(),
                    peer: peer_hex(c.peer),
                });
                c.role = Role::Finish;
            }
            _ => return Err("Unexpected file control".into()),
        }
        Ok(())
    }
}
