impl<P> Stack<P>
where
    P: Clone + Eq + Hash,
{
    /// Abort every retained tuple for one peer, including unaccepted SYN states.
    /// Uses the same reset and immediate cleanup as aborting each stream by ID.
    pub fn abort_peer(&mut self, peer: &P) -> Result<usize, StackError> {
        let ids = self
            .connections
            .iter()
            .filter_map(|(id, connection)| (&connection.peer == peer).then_some(*id))
            .collect::<Vec<_>>();
        for id in &ids {
            self.abort(*id)?;
        }
        Ok(ids.len())
    }

    /// Abort one retained tuple, emit one active reset, and release it immediately.
    pub fn abort(&mut self, id: ConnectionId) -> Result<(), StackError> {
        let (peer, local_port, remote_port, reset) = {
            let connection = self
                .connections
                .get(&id)
                .ok_or(StackError::UnknownConnection)?;
            (
                connection.peer.clone(),
                connection.local_port,
                connection.remote_port,
                connection.reset_segment().encode()?,
            )
        };
        self.outbound.retain(|outbound| {
            if outbound.peer != peer {
                return true;
            }
            match Segment::decode(&outbound.bytes) {
                Ok(segment) => {
                    segment.src_port != local_port || segment.dst_port != remote_port
                }
                Err(_) => true,
            }
        });
        self.outbound.push(Outbound { peer, bytes: reset });
        self.remove_connection(id);
        Ok(())
    }
}
