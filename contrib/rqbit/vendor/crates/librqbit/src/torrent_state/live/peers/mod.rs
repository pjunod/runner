use std::{collections::HashSet, net::SocketAddr, sync::Arc};

use anyhow::Context;
use dashmap::DashMap;
use librqbit_core::lengths::ValidPieceIndex;
use parking_lot::RwLock;
use peer_binary_protocol::{Message, Request};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::{
    peer_connection::WriterRequest,
    session_stats::atomic::AtomicSessionStats,
    torrent_state::utils::{atomic_inc, TimedExistence},
    type_aliases::{PeerHandle, BF},
};

use self::stats::{atomic::AggregatePeerStatsAtomic, snapshot::AggregatePeerStats};

use super::peer::{LivePeerState, Peer, PeerRx, PeerState, PeerTx};

pub mod stats;

#[derive(Clone, Default)]
pub(crate) struct KnownPeerSemaphores {
    pub per_torrent: Option<Arc<Semaphore>>,
    pub total: Option<Arc<Semaphore>>,
}

#[derive(Debug)]
pub(crate) struct KnownPeerPermits {
    _per_torrent: Option<OwnedSemaphorePermit>,
    _total: Option<OwnedSemaphorePermit>,
}

impl KnownPeerSemaphores {
    pub fn try_acquire(&self) -> Option<KnownPeerPermits> {
        let per_torrent = match &self.per_torrent {
            Some(semaphore) => Some(semaphore.clone().try_acquire_owned().ok()?),
            None => None,
        };
        let total = match &self.total {
            Some(semaphore) => Some(semaphore.clone().try_acquire_owned().ok()?),
            None => None,
        };
        Some(KnownPeerPermits {
            _per_torrent: per_torrent,
            _total: total,
        })
    }
}

pub(crate) struct PeerStates {
    pub session_stats: Arc<AtomicSessionStats>,

    // This keeps track of live addresses we connected to, for PEX.
    pub live_outgoing_peers: RwLock<HashSet<PeerHandle>>,
    pub stats: AggregatePeerStatsAtomic,
    pub states: DashMap<PeerHandle, Peer>,
    pub known_peer_semaphores: KnownPeerSemaphores,
}

impl Drop for PeerStates {
    fn drop(&mut self) {
        for (_, p) in std::mem::take(&mut self.states).into_iter() {
            p.destroy(self);
        }
    }
}

impl PeerStates {
    pub fn stats(&self) -> AggregatePeerStats {
        AggregatePeerStats::from(&self.stats)
    }

    pub fn add_if_not_seen(&self, addr: SocketAddr) -> Option<PeerHandle> {
        use dashmap::mapref::entry::Entry;
        match self.states.entry(addr) {
            Entry::Occupied(_) => None,
            Entry::Vacant(vac) => {
                let permits = self.known_peer_semaphores.try_acquire()?;
                vac.insert(Peer::new_with_outgoing_address(addr, permits));
                atomic_inc(&self.stats.queued);
                atomic_inc(&self.session_stats.peers.queued);

                atomic_inc(&self.stats.seen);
                atomic_inc(&self.session_stats.peers.seen);
                Some(addr)
            }
        }
    }
    pub fn with_peer<R>(&self, addr: PeerHandle, f: impl FnOnce(&Peer) -> R) -> Option<R> {
        self.states.get(&addr).map(|e| f(e.value()))
    }

    pub fn with_peer_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut Peer) -> R,
    ) -> Option<R> {
        use crate::torrent_state::utils::timeit;
        timeit(reason, || self.states.get_mut(&addr))
            .map(|e| f(TimedExistence::new(e, reason).value_mut()))
    }

    pub fn with_live<R>(&self, addr: PeerHandle, f: impl FnOnce(&LivePeerState) -> R) -> Option<R> {
        self.with_peer(addr, |peer| peer.get_live().map(f))
            .flatten()
    }

    pub fn with_live_mut<R>(
        &self,
        addr: PeerHandle,
        reason: &'static str,
        f: impl FnOnce(&mut LivePeerState) -> R,
    ) -> Option<R> {
        self.with_peer_mut(addr, reason, |peer| peer.get_live_mut().map(f))
            .flatten()
    }

    pub fn drop_peer(&self, handle: PeerHandle) -> Option<Peer> {
        let p = self.states.remove(&handle).map(|r| r.1)?;
        let s = p.get_state();
        self.stats.dec(s);
        self.session_stats.peers.dec(s);

        Some(p)
    }

    pub fn is_peer_interested(&self, handle: PeerHandle) -> bool {
        self.with_live(handle, |live| live.peer_interested)
            .unwrap_or(false)
    }

    pub fn mark_peer_interested(&self, handle: PeerHandle, is_interested: bool) -> Option<bool> {
        self.with_live_mut(handle, "mark_peer_interested", |live| {
            let prev = live.peer_interested;
            live.peer_interested = is_interested;
            prev
        })
    }

    pub fn update_bitfield(&self, handle: PeerHandle, bitfield: BF) -> Option<()> {
        self.with_live_mut(handle, "update_bitfield", |live| {
            live.bitfield = bitfield;
        })
    }

    pub fn mark_peer_connecting(&self, h: PeerHandle) -> anyhow::Result<(PeerRx, PeerTx)> {
        let rx = self
            .with_peer_mut(h, "mark_peer_connecting", |peer| {
                peer.idle_to_connecting(self).context("invalid peer state")
            })
            .context("peer not found in states")??;
        Ok(rx)
    }

    pub fn reset_peer_backoff(&self, handle: PeerHandle) {
        self.with_peer_mut(handle, "reset_peer_backoff", |p| {
            p.stats.backoff.reset();
        });
    }

    pub fn mark_peer_not_needed(&self, handle: PeerHandle) -> Option<PeerState> {
        let prev = self.with_peer_mut(handle, "mark_peer_not_needed", |peer| {
            peer.set_not_needed(self)
        })?;
        Some(prev)
    }

    pub(crate) fn on_steal(
        &self,
        from_peer: SocketAddr,
        to_peer: SocketAddr,
        stolen_idx: ValidPieceIndex,
    ) {
        self.with_peer(to_peer, |p| {
            atomic_inc(&p.stats.counters.times_i_stole);
        });
        self.with_peer(from_peer, |p| {
            atomic_inc(&p.stats.counters.times_stolen_from_me);
        });
        self.stats.inc_steals();
        self.session_stats.peers.inc_steals();

        self.with_live_mut(from_peer, "send_cancellations", |live| {
            let to_remove = live
                .inflight_requests
                .iter()
                .filter(|r| r.piece_index == stolen_idx)
                .copied()
                .collect::<Vec<_>>();
            for req in to_remove {
                let _ = live
                    .tx
                    .send(WriterRequest::Message(Message::Cancel(Request {
                        index: stolen_idx.get(),
                        begin: req.offset,
                        length: req.size,
                    })));
            }
        });
    }
}

#[cfg(test)]
mod known_peer_budget_tests {
    use super::{KnownPeerSemaphores, PeerStates};
    use crate::session_stats::atomic::AtomicSessionStats;
    use crate::torrent_state::live::peer::PeerState;
    use std::{net::SocketAddr, sync::Arc};
    use tokio::sync::Semaphore;

    fn peer_states(limit: usize) -> PeerStates {
        PeerStates {
            session_stats: Arc::new(AtomicSessionStats::default()),
            live_outgoing_peers: Default::default(),
            stats: Default::default(),
            states: Default::default(),
            known_peer_semaphores: KnownPeerSemaphores {
                per_torrent: Some(Arc::new(Semaphore::new(limit))),
                total: None,
            },
        }
    }

    #[test]
    fn per_torrent_limit_is_exact_and_released() {
        let semaphores = KnownPeerSemaphores {
            per_torrent: Some(Arc::new(Semaphore::new(2))),
            total: None,
        };
        let first = semaphores.try_acquire().unwrap();
        let second = semaphores.try_acquire().unwrap();
        assert!(semaphores.try_acquire().is_none());
        drop(first);
        assert!(semaphores.try_acquire().is_some());
        drop(second);
    }

    #[test]
    fn session_limit_is_shared_between_torrents() {
        let total = Arc::new(Semaphore::new(3));
        let torrent_a = KnownPeerSemaphores {
            per_torrent: Some(Arc::new(Semaphore::new(3))),
            total: Some(total.clone()),
        };
        let torrent_b = KnownPeerSemaphores {
            per_torrent: Some(Arc::new(Semaphore::new(3))),
            total: Some(total),
        };
        let first = torrent_a.try_acquire().unwrap();
        let second = torrent_a.try_acquire().unwrap();
        let third = torrent_b.try_acquire().unwrap();
        assert!(torrent_b.try_acquire().is_none());
        drop(second);
        assert!(torrent_b.try_acquire().is_some());
        drop((first, third));
    }

    #[test]
    fn peer_records_hold_slots_until_removed() {
        let peers = peer_states(2);
        let first = SocketAddr::from(([127, 0, 0, 1], 1));
        let second = SocketAddr::from(([127, 0, 0, 1], 2));
        let third = SocketAddr::from(([127, 0, 0, 1], 3));
        assert_eq!(peers.add_if_not_seen(first), Some(first));
        assert_eq!(peers.add_if_not_seen(second), Some(second));
        assert_eq!(peers.add_if_not_seen(third), None);
        drop(peers.drop_peer(first));
        assert_eq!(peers.add_if_not_seen(third), Some(third));
    }

    #[test]
    fn failed_shared_acquisition_releases_the_local_slot() {
        let per_torrent = Arc::new(Semaphore::new(1));
        let semaphores = KnownPeerSemaphores {
            per_torrent: Some(per_torrent.clone()),
            total: Some(Arc::new(Semaphore::new(0))),
        };
        assert!(semaphores.try_acquire().is_none());
        assert_eq!(per_torrent.available_permits(), 1);
    }

    #[test]
    fn alternate_outgoing_address_is_queued_once_with_record_handle() {
        let peers = peer_states(1);
        let handle = SocketAddr::from(([127, 0, 0, 1], 1));
        let outgoing = SocketAddr::from(([127, 0, 0, 1], 2));
        assert_eq!(peers.add_if_not_seen(handle), Some(handle));

        let mut peer = peers.states.get_mut(&handle).unwrap();
        peer.outgoing_address = Some(outgoing);
        peer.set_state(PeerState::NotNeeded, &peers);
        assert_eq!(
            peer.reconnect_not_needed_peer(&peers),
            Some((handle, outgoing))
        );
        assert_eq!(peer.reconnect_not_needed_peer(&peers), None);
    }
}
