//! Seq-stamped engine events with a replay ring (`docs/archive/INTEGRATION_PLAN.md` N2; the wire contract is `docs/INTEGRATION.md` §2).
//!
//! The engine's broadcast channel is lossy by design: a slow subscriber is
//! dropped rather than allowed to stall the queue owner. That is the right
//! trade for a UI, and the wrong one for a consumer that imports things —
//! a monarr that blinks at the wrong moment used to have no way to know it
//! had missed a completion, let alone which one.
//!
//! This module fixes that without changing the engine's channel. One pump
//! task subscribes to the engine, stamps every event with a monotone `seq`
//! and keeps the last [`RING`] of them. SSE connections read from the ring
//! (for `Last-Event-ID` replay) and then follow the live fan-out. Because
//! exactly one task does the stamping, every consumer sees the same
//! numbers in the same order — which is the entire point of the id.
//!
//! Two things this deliberately does NOT do:
//!
//! * It does not persist. `seq` is process-lifetime and restarts at 1.
//!   The wire id is therefore `<boot>-<seq>`, where `boot` is unique to
//!   this process: a bare seq cannot tell "you missed events 51-60" from
//!   "we restarted and these are a *different* events 51-60", and the
//!   second one served as a clean resume is silent corruption of a
//!   consumer's view. `Last-Event-ID` is opaque to SSE, so carrying the
//!   epoch in it costs nothing; the plain `seq` still rides in the body
//!   for consumers that want an ordering.
//! * It does not make SSE reliable enough to depend on. The ring is a
//!   convenience over the real recovery path (`?since_seq=` on history).
//!   A client that ignores `reset` and trusts the stream is still wrong.

use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

/// Events kept for replay. At the observed event rate (a handful per job,
/// plus one per finished file) this covers a multi-hour disconnect on a
/// busy queue; the memory cost is a few hundred KB of already-serialized
/// JSON. Bigger would be a log, and a log is what history is for.
pub const RING: usize = 1024;

/// One stamped event, already rendered to its wire form.
#[derive(Clone, Debug)]
pub struct SeqFrame {
    pub seq: u64,
    /// SSE `event:` name — the snake_case variant name.
    pub name: &'static str,
    /// SSE `data:` payload: the event JSON with `"seq"` embedded, so a
    /// consumer that reads only the body still has the cursor.
    pub data: Arc<str>,
}

/// A `Last-Event-ID` value: `<boot>-<seq>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventId {
    pub boot: u64,
    pub seq: u64,
}

impl EventId {
    pub fn parse(raw: &str) -> Option<EventId> {
        let (boot, seq) = raw.trim().split_once('-')?;
        Some(EventId {
            boot: boot.parse().ok()?,
            seq: seq.parse().ok()?,
        })
    }
}

impl std::fmt::Display for EventId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.boot, self.seq)
    }
}

pub struct EventHub {
    /// Identifies this process's numbering. Any value that does not
    /// repeat across a restart works; the boot instant in nanoseconds is
    /// the cheapest one that never collides in practice.
    boot: u64,
    next: AtomicU64,
    ring: Mutex<VecDeque<SeqFrame>>,
    tx: broadcast::Sender<SeqFrame>,
    /// `nzbd_events_emitted_total{event=…}` — counted here rather than at
    /// each emit site, because here is the one place every event passes.
    counts: Mutex<BTreeMap<&'static str, u64>>,
    /// Currently-open SSE streams (`nzbd_sse_clients`).
    sse_clients: AtomicI64,
}

/// What a reconnecting client gets.
pub enum Replay {
    /// Nothing to replay: a fresh stream, or the client is already current.
    Live,
    /// These frames were missed, oldest first; then follow live.
    Frames(Vec<SeqFrame>),
    /// The gap cannot be covered — the id fell out of the ring, or the
    /// daemon restarted and seq numbering began again. The client must
    /// poll-reconcile (`GET /api/v1/jobs` + `?since_seq=`) before trusting
    /// the stream. Saying so explicitly is the whole difference between a
    /// consumer that knows it is behind and one that silently is.
    Reset,
}

impl EventHub {
    /// Start the pump. Must be called from within a Tokio runtime; every
    /// caller builds the router inside one.
    pub fn spawn(engine: &nzbd_engine::EngineHandle) -> Arc<EventHub> {
        let boot = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(1);
        let hub = Arc::new(EventHub::new(boot));
        let mut rx = engine.subscribe();
        let pump = hub.clone();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(ev) => pump.publish(&ev),
                    // The pump itself fell behind the engine: events were
                    // dropped before they were ever numbered, so no seq
                    // gap exists to betray them and neither the ring nor a
                    // reconnecting client would ever know. Publish the
                    // loss AS an event — it takes a seq, lands in the
                    // ring, and reaches live and replaying clients alike,
                    // so the one loss mode this design otherwise cannot
                    // signal becomes the one it signals loudest.
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        tracing::warn!(skipped = n, "event hub lagged behind the engine broadcast");
                        pump.publish_raw("lagged", serde_json::json!({ "skipped": n }));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
        hub
    }

    fn new(boot: u64) -> EventHub {
        EventHub {
            boot,
            next: AtomicU64::new(1),
            ring: Mutex::new(VecDeque::with_capacity(RING)),
            tx: broadcast::channel(RING).0,
            counts: Mutex::new(BTreeMap::new()),
            sse_clients: AtomicI64::new(0),
        }
    }

    /// This process's event-id epoch.
    pub fn boot(&self) -> u64 {
        self.boot
    }

    fn publish(&self, ev: &nzbd_engine::Event) {
        let (name, body) = crate::event_json(ev);
        self.publish_raw(name, body);
    }

    fn publish_raw(&self, name: &'static str, mut body: Value) {
        let seq = self.next.fetch_add(1, Ordering::Relaxed);
        if let Some(o) = body.as_object_mut() {
            o.insert("seq".into(), Value::from(seq));
        }
        let frame = SeqFrame {
            seq,
            name,
            data: Arc::from(body.to_string()),
        };
        {
            let mut ring = self.ring.lock().unwrap();
            if ring.len() == RING {
                ring.pop_front();
            }
            ring.push_back(frame.clone());
        }
        *self.counts.lock().unwrap().entry(name).or_insert(0) += 1;
        let _ = self.tx.send(frame);
    }

    /// Subscribe to live frames. Callers subscribe **before** asking for
    /// [`EventHub::replay`], so an event published between the two calls is
    /// duplicated (harmless, and the caller drops it by seq) rather than
    /// lost (not harmless at all).
    pub fn subscribe(&self) -> broadcast::Receiver<SeqFrame> {
        self.tx.subscribe()
    }

    /// Resolve a `Last-Event-ID` into what the client should receive.
    pub fn replay(&self, last_event_id: Option<EventId>) -> Replay {
        let Some(id) = last_event_id else {
            return Replay::Live;
        };
        if id.boot != self.boot {
            // A different process numbered that id. Its seq 50 and ours
            // are unrelated events; serving ours as the continuation of
            // theirs would look perfectly contiguous and be nonsense.
            return Replay::Reset;
        }
        let last = id.seq;
        let highest = self.next.load(Ordering::Relaxed).saturating_sub(1);
        if last > highest {
            return Replay::Reset; // an id we never issued
        }
        let ring = self.ring.lock().unwrap();
        let oldest = ring.front().map(|f| f.seq);
        match oldest {
            // Ring empty: nothing has been emitted since boot. The client
            // can only be current (last == highest == 0) or quoting a
            // vanished numbering.
            None if last == highest => Replay::Live,
            None => Replay::Reset,
            // We hold `oldest..=highest`. Covering the client requires
            // `oldest <= last + 1`; anything older fell out of the ring.
            Some(oldest) if oldest > last + 1 => Replay::Reset,
            Some(_) => {
                let frames: Vec<SeqFrame> = ring.iter().filter(|f| f.seq > last).cloned().collect();
                if frames.is_empty() {
                    Replay::Live
                } else {
                    Replay::Frames(frames)
                }
            }
        }
    }

    /// `(event name, count)` for `/metrics`.
    pub fn counts(&self) -> Vec<(&'static str, u64)> {
        self.counts
            .lock()
            .unwrap()
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect()
    }

    pub fn sse_clients(&self) -> i64 {
        self.sse_clients.load(Ordering::Relaxed)
    }

    /// RAII counter for one open SSE stream. A guard rather than a pair of
    /// calls because the stream can end at any await point (client hangs
    /// up, shutdown, engine gone) and a gauge that only decrements on the
    /// tidy path drifts upward forever.
    pub fn stream_guard(self: &Arc<Self>) -> StreamGuard {
        self.sse_clients.fetch_add(1, Ordering::Relaxed);
        StreamGuard { hub: self.clone() }
    }
}

pub struct StreamGuard {
    hub: Arc<EventHub>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.hub.sse_clients.fetch_sub(1, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nzbd_engine::Event;
    use nzbd_types::JobId;

    const BOOT: u64 = 7;

    fn hub() -> Arc<EventHub> {
        Arc::new(EventHub::new(BOOT))
    }

    fn id(seq: u64) -> Option<EventId> {
        Some(EventId { boot: BOOT, seq })
    }

    fn added(hub: &EventHub, n: u32) {
        hub.publish(&Event::JobAdded {
            job: JobId(n),
            name: format!("job-{n}"),
        });
    }

    #[test]
    fn frames_are_numbered_from_one_and_carry_seq_in_the_body() {
        let h = hub();
        added(&h, 1);
        added(&h, 2);
        let ring = h.ring.lock().unwrap();
        assert_eq!(ring[0].seq, 1);
        assert_eq!(ring[1].seq, 2);
        assert!(
            ring[1].data.contains("\"seq\":2"),
            "the cursor must be readable from the body alone: {}",
            ring[1].data
        );
    }

    #[test]
    fn reconnect_replays_exactly_what_was_missed() {
        let h = hub();
        for n in 1..=5 {
            added(&h, n);
        }
        match h.replay(id(2)) {
            Replay::Frames(f) => {
                assert_eq!(
                    f.iter().map(|f| f.seq).collect::<Vec<_>>(),
                    vec![3, 4, 5],
                    "only the missed tail, in order"
                );
            }
            _ => panic!("expected a replay"),
        }
        assert!(
            matches!(h.replay(id(5)), Replay::Live),
            "a caught-up client replays nothing"
        );
        assert!(matches!(h.replay(None), Replay::Live));
    }

    #[test]
    fn a_gap_the_ring_cannot_cover_is_a_reset_not_a_silent_hole() {
        let h = hub();
        for n in 0..(RING as u32 + 10) {
            added(&h, n);
        }
        // seq 1 is long gone; handing this client the ring's contents
        // would look contiguous and be missing 10 events.
        assert!(matches!(h.replay(id(1)), Replay::Reset));
        // The oldest still-held event is replayable from one before it.
        let oldest = h.ring.lock().unwrap().front().unwrap().seq;
        assert!(matches!(h.replay(id(oldest - 1)), Replay::Frames(_)));
    }

    #[test]
    fn an_id_from_a_previous_process_is_a_reset() {
        let h = hub();
        added(&h, 1);
        assert!(
            matches!(h.replay(id(9_999)), Replay::Reset),
            "a seq we never issued"
        );
        // And — the case a bare seq cannot catch — an id from an EARLIER
        // process whose numbering this one has already caught up past.
        // Those are different events wearing the same numbers.
        for n in 2..=20 {
            added(&h, n);
        }
        assert!(
            matches!(
                h.replay(Some(EventId {
                    boot: BOOT - 1,
                    seq: 5
                })),
                Replay::Reset
            ),
            "a restart must not be served as a clean resume"
        );
    }

    #[test]
    fn a_pump_lag_is_published_rather_than_swallowed() {
        let h = hub();
        added(&h, 1);
        // Events dropped before they were numbered leave no seq gap, so
        // the loss has to become an event of its own or nothing downstream
        // can ever learn about it.
        h.publish_raw("lagged", serde_json::json!({ "skipped": 12 }));
        match h.replay(id(1)) {
            Replay::Frames(f) => {
                assert_eq!(f.len(), 1);
                assert_eq!(f[0].name, "lagged");
                assert!(f[0].data.contains("\"skipped\":12"));
            }
            _ => panic!("the lag must be replayable like any other frame"),
        }
    }

    #[test]
    fn event_ids_round_trip_through_the_wire_form() {
        let id = EventId { boot: 42, seq: 7 };
        assert_eq!(id.to_string(), "42-7");
        assert_eq!(EventId::parse("42-7"), Some(id));
        assert_eq!(EventId::parse(" 42-7 "), Some(id));
        // Anything else is not an id we issued; the caller treats a failed
        // parse as "no Last-Event-ID", which is a fresh stream, not a
        // silently-accepted resume.
        assert_eq!(EventId::parse("7"), None);
        assert_eq!(EventId::parse("beef-7"), None);
    }

    #[test]
    fn a_fresh_daemon_serves_a_zero_cursor_as_live() {
        let h = hub();
        assert!(matches!(h.replay(id(0)), Replay::Live));
        assert!(matches!(h.replay(id(3)), Replay::Reset));
    }

    #[test]
    fn stream_guard_returns_the_gauge_to_zero() {
        let h = hub();
        {
            let _a = h.stream_guard();
            let _b = h.stream_guard();
            assert_eq!(h.sse_clients(), 2);
        }
        assert_eq!(h.sse_clients(), 0);
    }
}
