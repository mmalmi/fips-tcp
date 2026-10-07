use std::hash::{Hash, Hasher};

use fips_core::PeerIdentity;

/// Carry the authenticated identity through TCP instead of encoding and parsing
/// an npub for each segment. Matching remains the same x-only public key.
#[derive(Clone, Copy, Debug)]
pub(super) struct PeerKey(pub(super) PeerIdentity);

impl From<PeerIdentity> for PeerKey {
    fn from(peer: PeerIdentity) -> Self {
        // The former npub round trip selected even parity. Preserve that
        // behavior for callers with a full odd-parity key, without rebuilding
        // the curve point for identities that are already canonical.
        let peer = if peer.pubkey_full().serialize()[0] == 3 {
            PeerIdentity::from_pubkey(peer.pubkey())
        } else {
            peer
        };
        Self(peer)
    }
}

impl PartialEq for PeerKey {
    fn eq(&self, other: &Self) -> bool {
        self.0.pubkey() == other.0.pubkey()
    }
}

impl Eq for PeerKey {}

impl Hash for PeerKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.pubkey().hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fips_tcp::{Config, Stack, StackError};
    use std::collections::HashSet;

    fn identity(prefix: &str) -> PeerIdentity {
        PeerIdentity::from_pubkey_full(
            format!("{prefix}79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798")
                .parse()
                .unwrap(),
        )
    }

    #[test]
    fn typed_peer_preserves_canonical_npub_identity_and_matching() {
        let even = identity("02");
        let odd = identity("03");
        assert_ne!(even, odd);
        let mut keys = HashSet::new();
        for peer in [even, odd] {
            let key = PeerKey::from(peer);
            assert_eq!(key.0, PeerIdentity::from_npub(&peer.npub()).unwrap());
            keys.insert(key);
        }
        assert_eq!(keys.len(), 1);
    }

    #[test]
    fn typed_peer_preserves_tcp_bytes_and_per_peer_admission() {
        let config = Config {
            max_connections: 2,
            max_connections_per_peer: 1,
            ..Config::default()
        };
        let mut legacy = Stack::new(config.clone(), 123);
        let mut typed = Stack::new(config, 123);
        let even = identity("02");
        let odd = identity("03");
        let old_id = legacy.connect(even.npub(), 39017, 0).unwrap();
        let new_id = typed.connect(PeerKey::from(even), 39017, 0).unwrap();
        assert_eq!(old_id, new_id);
        assert_eq!(legacy.ports(old_id), typed.ports(new_id));
        assert!(matches!(
            legacy.connect(odd.npub(), 39017, 1),
            Err(StackError::ConnectionLimit)
        ));
        assert!(matches!(
            typed.connect(PeerKey::from(odd), 39017, 1),
            Err(StackError::ConnectionLimit)
        ));
        let old_packets = legacy.drain_outbound();
        let new_packets = typed.drain_outbound();
        assert_eq!(old_packets.len(), new_packets.len());
        for (old, new) in old_packets.into_iter().zip(new_packets) {
            assert_eq!(old.peer, new.peer.0.npub());
            assert_eq!(old.bytes, new.bytes);
        }
        assert_eq!(legacy.abort_peer(&odd.npub()).unwrap(), 1);
        assert_eq!(typed.abort_peer(&PeerKey::from(odd)).unwrap(), 1);
        assert_eq!(legacy.state(old_id), typed.state(new_id));
    }
}
