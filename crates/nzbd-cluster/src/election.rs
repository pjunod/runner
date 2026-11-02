//! Leader election over the shared volume (CLUSTERING.md §4).
//!
//! One lease file, renewed by the leader every `lease_interval`. Staleness
//! is judged by **observed non-progression on the local monotonic clock**
//! — wall-clock skew between nodes is irrelevant. Takeover is
//! write–wait–verify with a priority-staggered candidacy; brief dual-claim
//! windows during a race are converged in one round and made harmless by
//! epoch fencing on every state write.

use crate::control::{ControlStore, LeaseClaim, LeaseToken};
use crate::layout::SharedLayout;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LeaderRecord {
    pub epoch: u64,
    pub node: String,
    pub api_url: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Default)]
pub struct LeaderView {
    pub record: Option<LeaderRecord>,
    pub is_me: bool,
}

impl LeaderView {
    pub fn leader_url(&self) -> Option<&str> {
        self.record.as_ref().map(|r| r.api_url.as_str())
    }
    pub fn epoch(&self) -> u64 {
        self.record.as_ref().map(|r| r.epoch).unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct ElectionCfg {
    pub cluster_id: String,
    pub node: String,
    pub api_url: String,
    pub eligible: bool,
    pub priority: u32,
    pub lease_interval: Duration,
    pub takeover_after: Duration,
}

pub fn spawn_election(
    layout: SharedLayout,
    cfg: ElectionCfg,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) -> watch::Receiver<LeaderView> {
    let (tx, rx) = watch::channel(LeaderView::default());
    tracker.spawn(election_task(layout, cfg, tx, cancel));
    rx
}

/// Start election with a replicated lease as the authority boundary. The
/// shared `leader.json` is retained only as a discovery projection for
/// worker-only nodes; winning or rewriting it grants no authority.
pub fn spawn_replicated_election(
    layout: SharedLayout,
    cfg: ElectionCfg,
    control: ControlStore,
    owner_incarnation: String,
    cancel: CancellationToken,
    tracker: &TaskTracker,
) -> watch::Receiver<LeaderView> {
    let (tx, rx) = watch::channel(LeaderView::default());
    tracker.spawn(replicated_election_task(
        layout,
        cfg,
        control,
        owner_incarnation,
        tx,
        cancel,
    ));
    rx
}

async fn replicated_election_task(
    layout: SharedLayout,
    cfg: ElectionCfg,
    control: ControlStore,
    owner_incarnation: String,
    tx: watch::Sender<LeaderView>,
    cancel: CancellationToken,
) {
    let resource = "cluster/leader";
    let mut token: Option<LeaseToken> = None;
    let mut seq = 0u64;
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if let Some(current) = token.take() {
            match control.renew(&current, cfg.takeover_after).await {
                Ok(Some(renewed)) => {
                    seq = seq.saturating_add(1);
                    let record = LeaderRecord {
                        epoch: renewed.fence,
                        node: cfg.node.clone(),
                        api_url: cfg.api_url.clone(),
                        seq,
                    };
                    if let Err(error) = layout.write_json(&layout.leader_file(), &record) {
                        tracing::warn!(%error, "leader discovery projection write failed");
                    }
                    publish(&tx, Some(record), true);
                    token = Some(renewed);
                }
                Ok(None) => {
                    tracing::warn!("replicated leader lease was rejected; demoting");
                    publish(&tx, projected_leader(&layout, None), false);
                }
                Err(error) => {
                    tracing::warn!(%error, "replicated leader renewal failed; demoting");
                    publish(&tx, projected_leader(&layout, None), false);
                }
            }
        } else if cfg.eligible {
            // Bias simultaneous replicated claims by configured priority,
            // but keep the whole stagger below one election interval. A
            // multi-interval delay can outlive a worker's conservative lease
            // deadline and turn a live-leader handoff into needless reclaim.
            let interval_ms = cfg.lease_interval.as_millis().max(1) as u64;
            let step_ms = (interval_ms / 17).max(1);
            let priority_ms = step_ms * u64::from(cfg.priority.min(16));
            let jitter_ms = hash_jitter(&cfg.node) % step_ms;
            let stagger = Duration::from_millis(priority_ms + jitter_ms);
            sleep_or_cancel(&cancel, stagger).await;
            if cancel.is_cancelled() {
                break;
            }
            match control
                .acquire(
                    resource,
                    &cfg.cluster_id,
                    1,
                    "cluster-leader",
                    &cfg.node,
                    &owner_incarnation,
                    "leader",
                    &serde_json::json!({"lifecycleProtocol":crate::proto::LIFECYCLE_PROTOCOL})
                        .to_string(),
                    1,
                    cfg.takeover_after,
                )
                .await
            {
                Ok(LeaseClaim::Acquired(acquired)) => {
                    seq = 1;
                    let record = LeaderRecord {
                        epoch: acquired.fence,
                        node: cfg.node.clone(),
                        api_url: cfg.api_url.clone(),
                        seq,
                    };
                    if let Err(error) = layout.write_json(&layout.leader_file(), &record) {
                        tracing::warn!(%error, "leader discovery projection write failed");
                    }
                    tracing::info!(epoch = acquired.fence, "took replicated office");
                    publish(&tx, Some(record), true);
                    token = Some(acquired);
                }
                Ok(LeaseClaim::Held { fence, .. }) => {
                    publish(&tx, projected_leader(&layout, Some(fence)), false);
                }
                Err(error) => {
                    tracing::warn!(%error, "replicated leader claim failed");
                    publish(&tx, projected_leader(&layout, None), false);
                }
            }
        }
        sleep_or_cancel(&cancel, cfg.lease_interval).await;
    }
    if let Some(token) = token {
        let _ = control.release(&token).await;
    }
}

fn projected_leader(layout: &SharedLayout, fence: Option<u64>) -> Option<LeaderRecord> {
    let record = SharedLayout::read_json::<LeaderRecord>(&layout.leader_file())?;
    if fence.is_none_or(|expected| expected == record.epoch) {
        Some(record)
    } else {
        None
    }
}

async fn election_task(
    layout: SharedLayout,
    cfg: ElectionCfg,
    tx: watch::Sender<LeaderView>,
    cancel: CancellationToken,
) {
    let path = layout.leader_file();
    let mut im_leader = false;
    let mut my_epoch = 0u64;
    let mut last_seen: Option<(u64, u64)> = None; // (epoch, seq)
    let mut last_change = Instant::now();

    loop {
        if cancel.is_cancelled() {
            break;
        }
        let rec: Option<LeaderRecord> = SharedLayout::read_json(&path);

        if im_leader {
            match &rec {
                Some(r) if r.node == cfg.node && r.epoch == my_epoch => {
                    // Renew.
                    let renewed = LeaderRecord {
                        epoch: my_epoch,
                        node: cfg.node.clone(),
                        api_url: cfg.api_url.clone(),
                        seq: r.seq + 1,
                    };
                    if let Err(e) = layout.write_json(&path, &renewed) {
                        tracing::warn!(error = %e, "lease renewal failed (shared volume?)");
                        // Can't renew ⇒ can't guarantee leadership. Demote;
                        // fencing protects state either way.
                        im_leader = false;
                        publish(&tx, rec.clone(), false);
                    } else {
                        publish(&tx, Some(renewed), true);
                    }
                }
                Some(r) if r.epoch >= my_epoch && r.node != cfg.node => {
                    tracing::warn!(new_leader = %r.node, epoch = r.epoch, "deposed");
                    im_leader = false;
                    last_seen = Some((r.epoch, r.seq));
                    last_change = Instant::now();
                    publish(&tx, rec.clone(), false);
                }
                _ => {
                    // Missing / older-epoch file: reassert.
                    let renewed = LeaderRecord {
                        epoch: my_epoch,
                        node: cfg.node.clone(),
                        api_url: cfg.api_url.clone(),
                        seq: 1,
                    };
                    if layout.write_json(&path, &renewed).is_ok() {
                        publish(&tx, Some(renewed), true);
                    } else {
                        im_leader = false;
                        publish(&tx, None, false);
                    }
                }
            }
            sleep_or_cancel(&cancel, cfg.lease_interval).await;
            continue;
        }

        // Follower: observe progression.
        let stale = match &rec {
            Some(r) => {
                if last_seen != Some((r.epoch, r.seq)) {
                    last_seen = Some((r.epoch, r.seq));
                    last_change = Instant::now();
                    publish(&tx, rec.clone(), false);
                }
                last_change.elapsed() >= cfg.takeover_after
            }
            None => true, // no leader ever, or the file was lost
        };

        if stale && cfg.eligible {
            // Priority-staggered candidacy: each priority step waits out a
            // full write–wait–verify window of every better-priority node,
            // so priority reliably wins when candidates race from the same
            // observation. Jitter (< one step) splits equal priorities.
            // Keep OBSERVING while staggering — a proxying node must learn
            // about a freshly-elected leader immediately, and a
            // better-priority winner aborts our candidacy.
            let step = cfg.lease_interval * 3;
            let jitter_ms = hash_jitter(&cfg.node) % cfg.lease_interval.as_millis().max(1) as u64;
            let stagger = step * cfg.priority.min(16) + Duration::from_millis(jitter_ms);
            let deadline = Instant::now() + stagger;
            let mut aborted = false;
            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() || cancel.is_cancelled() {
                    break;
                }
                sleep_or_cancel(&cancel, remaining.min(cfg.lease_interval / 2)).await;
                let fresh: Option<LeaderRecord> = SharedLayout::read_json(&path);
                let changed = match (&fresh, last_seen) {
                    (Some(r), Some(seen)) => (r.epoch, r.seq) != seen,
                    (Some(_), None) => true,
                    (None, _) => false,
                };
                if changed {
                    if let Some(r) = fresh {
                        last_seen = Some((r.epoch, r.seq));
                        last_change = Instant::now();
                        publish(&tx, Some(r), false);
                    }
                    aborted = true;
                    break;
                }
            }
            if cancel.is_cancelled() {
                break;
            }
            if aborted {
                continue; // someone took over during the stagger
            }

            // Write–wait–verify.
            let current: Option<LeaderRecord> = SharedLayout::read_json(&path);
            let new_epoch = current.map(|r| r.epoch).unwrap_or(0) + 1;
            let claim = LeaderRecord {
                epoch: new_epoch,
                node: cfg.node.clone(),
                api_url: cfg.api_url.clone(),
                seq: 1,
            };
            if let Err(e) = layout.write_json(&path, &claim) {
                tracing::warn!(error = %e, "candidacy write failed");
                sleep_or_cancel(&cancel, cfg.lease_interval).await;
                continue;
            }
            sleep_or_cancel(&cancel, cfg.lease_interval * 2).await;
            if cancel.is_cancelled() {
                break;
            }
            match SharedLayout::read_json::<LeaderRecord>(&path) {
                Some(r) if r.node == cfg.node && r.epoch == new_epoch => {
                    tracing::info!(epoch = new_epoch, "took office");
                    im_leader = true;
                    my_epoch = new_epoch;
                    publish(&tx, Some(r), true);
                }
                other => {
                    tracing::debug!(?other, "lost the takeover race");
                    if let Some(r) = other {
                        last_seen = Some((r.epoch, r.seq));
                        last_change = Instant::now();
                        publish(&tx, Some(r), false);
                    }
                }
            }
            continue;
        }

        sleep_or_cancel(&cancel, cfg.lease_interval / 2).await;
    }
}

fn publish(tx: &watch::Sender<LeaderView>, record: Option<LeaderRecord>, is_me: bool) {
    tx.send_if_modified(|v| {
        let next = LeaderView { record, is_me };
        let changed = v.record != next.record || v.is_me != next.is_me;
        *v = next;
        changed
    });
}

async fn sleep_or_cancel(cancel: &CancellationToken, d: Duration) {
    tokio::select! {
        _ = cancel.cancelled() => {}
        _ = tokio::time::sleep(d) => {}
    }
}

fn hash_jitter(node: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    node.hash(&mut h);
    h.finish()
}

/// The engine's snapshot-commit fencing guard (CLUSTERING.md §6.4): the
/// leader re-verifies the lease file names it, at its current epoch,
/// immediately before every snapshot rename.
pub fn persist_guard(
    layout: SharedLayout,
    view: watch::Receiver<LeaderView>,
    node: String,
) -> std::sync::Arc<dyn Fn() -> bool + Send + Sync> {
    std::sync::Arc::new(move || {
        let v = view.borrow().clone();
        if !v.is_me {
            return false;
        }
        let my_epoch = v.epoch();
        match SharedLayout::read_json::<LeaderRecord>(&layout.leader_file()) {
            Some(r) => r.node == node && r.epoch == my_epoch,
            None => false,
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(node: &str, priority: u32) -> ElectionCfg {
        ElectionCfg {
            cluster_id: "test".into(),
            node: node.into(),
            api_url: format!("http://{node}.test"),
            eligible: true,
            priority,
            lease_interval: Duration::from_millis(100),
            takeover_after: Duration::from_millis(400),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn exactly_one_leader_and_failover() {
        let tmp = tempfile::tempdir().unwrap();
        let tracker = TaskTracker::new();

        let mk = |name: &str, prio: u32, cancel: &CancellationToken| {
            spawn_election(
                SharedLayout::new(tmp.path(), name).unwrap(),
                cfg(name, prio),
                cancel.clone(),
                &tracker,
            )
        };
        let c_a = CancellationToken::new();
        let c_b = CancellationToken::new();
        let c_c = CancellationToken::new();
        let a = mk("a", 0, &c_a);
        let b = mk("b", 1, &c_b);
        let c = mk("c", 2, &c_c);

        // Exactly one leader emerges and every node agrees on it (the
        // single-leader invariant; priority biases but a cold-start race
        // may crown any candidate).
        let views = [&a, &b, &c];
        let deadline = Instant::now() + Duration::from_secs(10);
        let first_leader = loop {
            assert!(Instant::now() < deadline, "no leader elected");
            let leaders = views.iter().filter(|rx| rx.borrow().is_me).count();
            let named: Vec<_> = views
                .iter()
                .filter_map(|rx| rx.borrow().record.as_ref().map(|r| r.node.clone()))
                .collect();
            if leaders == 1 && named.len() == 3 && named.windows(2).all(|w| w[0] == w[1]) {
                break named[0].clone();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };
        let epoch1 = a.borrow().epoch();
        assert!(epoch1 >= 1);

        // Kill the leader: a survivor takes over with a higher epoch;
        // exactly one leader again and the survivors agree.
        match first_leader.as_str() {
            "a" => c_a.cancel(),
            "b" => c_b.cancel(),
            _ => c_c.cancel(),
        }
        let survivors: Vec<_> = views
            .iter()
            .zip(["a", "b", "c"])
            .filter(|(_, n)| *n != first_leader)
            .map(|(rx, n)| ((*rx).clone(), n))
            .collect();
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            assert!(Instant::now() < deadline, "no failover");
            let leaders = survivors.iter().filter(|(rx, _)| rx.borrow().is_me).count();
            let named: Vec<_> = survivors
                .iter()
                .filter_map(|(rx, _)| rx.borrow().record.as_ref().map(|r| r.node.clone()))
                .collect();
            if leaders == 1
                && named.len() == survivors.len()
                && named.windows(2).all(|w| w[0] == w[1])
                && named[0] != first_leader
            {
                let new_epoch = survivors[0].0.borrow().epoch();
                assert!(new_epoch > epoch1, "takeover bumps the epoch");
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }

        c_a.cancel();
        c_b.cancel();
        c_c.cancel();
        tracker.close();
        tracker.wait().await;
    }

    #[tokio::test]
    async fn persist_guard_fences_stale_epochs() {
        let tmp = tempfile::tempdir().unwrap();
        let layout = SharedLayout::new(tmp.path(), "g").unwrap();
        let rec = LeaderRecord {
            epoch: 5,
            node: "me".into(),
            api_url: "http://me".into(),
            seq: 1,
        };
        layout.write_json(&layout.leader_file(), &rec).unwrap();

        let (tx, rx) = watch::channel(LeaderView {
            record: Some(rec.clone()),
            is_me: true,
        });
        let guard = persist_guard(layout.clone(), rx, "me".into());
        assert!(guard());

        // A successor bumps the epoch on disk: the old guard must reject.
        layout
            .write_json(
                &layout.leader_file(),
                &LeaderRecord {
                    epoch: 6,
                    node: "other".into(),
                    api_url: "http://other".into(),
                    seq: 1,
                },
            )
            .unwrap();
        assert!(!guard());
        drop(tx);
    }
}
