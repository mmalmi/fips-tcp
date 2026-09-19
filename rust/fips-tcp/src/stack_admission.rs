use std::sync::Arc;

struct ConnectionReservation<P> {
    slots: usize,
    eligible: Arc<dyn Fn(&P) -> bool + Send + Sync>,
}

impl<P: Clone + Eq + Hash> Stack<P> {
    /// Reserve part of the existing connection limit for locally selected peers.
    /// Ordinary new tuples require `retained < max_connections - slots`;
    /// eligible peers may use the full limit. The per-peer limit always applies.
    ///
    /// The classifier must be a cheap synchronous local decision. It is never
    /// supplied by TCP headers and is consulted only for new allocation. Zero
    /// disables the reservation. Installing or changing it does not evict or
    /// reclassify existing tuples, so install it before accepting traffic when
    /// capacity must be available immediately.
    pub fn set_connection_reservation(
        &mut self,
        slots: usize,
        eligible: Arc<dyn Fn(&P) -> bool + Send + Sync>,
    ) -> Result<(), StackError> {
        if slots >= self.config.max_connections {
            return Err(StackError::InvalidConfig(
                "reserved connections must leave ordinary capacity",
            ));
        }
        self.reservation = (slots != 0).then_some(ConnectionReservation { slots, eligible });
        Ok(())
    }

    fn ensure_connection_capacity(&self, peer: &P) -> Result<(), StackError> {
        let peer_connections = self
            .connections
            .values()
            .filter(|connection| &connection.peer == peer)
            .count();
        if self.connections.len() >= self.config.max_connections
            || peer_connections >= self.config.max_connections_per_peer
        {
            return Err(StackError::ConnectionLimit);
        }
        if let Some(reservation) = &self.reservation
            && self.connections.len() >= self.config.max_connections - reservation.slots
            && !(reservation.eligible)(peer)
        {
            return Err(StackError::ConnectionLimit);
        }
        Ok(())
    }
}
