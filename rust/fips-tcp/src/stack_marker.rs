impl<P> Stack<P>
where
    P: Clone + Eq + Hash,
{
    /// Accept payload into the local send buffer and return its ACK boundary.
    /// An empty payload marks the boundary of all bytes accepted previously.
    pub fn write_with_marker(
        &mut self,
        id: ConnectionId,
        bytes: &[u8],
        now_ms: u64,
    ) -> Result<(usize, SendMarker), StackError> {
        let connection = self
            .connections
            .get_mut(&id)
            .ok_or(StackError::UnknownConnection)?;
        let (accepted, segments) = connection.write(bytes, now_ms, &self.config)?;
        let marker = connection.send_progress.marker(id);
        self.emit(id, segments)?;
        Ok((accepted, marker))
    }

    /// Report whether the marker's payload boundary was cumulatively ACKed.
    pub fn marker_status(&self, marker: &SendMarker) -> MarkerStatus {
        self.connections
            .get(&marker.connection_id())
            .map_or(MarkerStatus::ConnectionGone, |connection| {
                connection.send_progress.status(marker)
            })
    }
}
