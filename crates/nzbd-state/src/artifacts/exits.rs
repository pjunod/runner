//! No-wedge task 2/3: every state a row can sit in has a registered exit.
//!
//! Owner's rule (Paul, 2026-11-11): nothing in Runner may sit in a state that
//! needs a person. Every state ends by success, by a bounded retry that ends
//! in a typed failure following `failure_action`, or by a TTL that disposes
//! of it. Runner's own job directory is a closed scope. The one exception is
//! content Runner did not create (unowned / adopted folders): it is never
//! deleted automatically and stays an operator-visible state that blocks
//! nothing else.
//!
//! [`STATE_EXITS`] and [`HOLD_EXITS`] are the registry. A test in this module
//! scans the crate sources for every state and hold string the code writes and
//! fails if one is not registered, so a new wedge state cannot be added
//! silently. [`Inventory::exit_wedged_states`] carries out the exits for
//! `review`/`retained`/held rows: at open (upgrade and restart conversion,
//! database-only actions, no grace) and on every maintenance tick (all
//! actions, after [`HOLD_EXIT_SECONDS`]).
use super::relocation::Relocation;
use super::*;

/// How long a review/retained hold may stand before its automatic exit runs
/// on the maintenance tick. Long enough for the flow that set it to finish
/// its own follow-up (a retry, a reconcile); short enough that nothing waits
/// on a person.
pub const HOLD_EXIT_SECONDS: i64 = 3600;

/// How a state ends.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exit {
    /// Final; nothing follows.
    Terminal,
    /// Ends when the named event happens (the owning work completes).
    Event(&'static str),
    /// Ends on a durable timer (retry ladder, TTL, hold deadline, or
    /// [`HOLD_EXIT_SECONDS`]).
    Timer(&'static str),
    /// Ends only by a person. Allowed only for content Runner did not create.
    Operator(&'static str),
    /// Retained by an explicit person-set policy (Keep). Not a wedge: the
    /// person's own setting is the exit.
    Policy(&'static str),
}

impl Exit {
    pub fn describe(self) -> String {
        match self {
            Exit::Terminal => "terminal".into(),
            Exit::Event(what) => format!("event: {what}"),
            Exit::Timer(what) => format!("timer: {what}"),
            Exit::Operator(what) => format!("operator (unowned only): {what}"),
            Exit::Policy(what) => format!("policy: {what}"),
        }
    }
}

/// One registered state string. `scope` names the table or enum.
#[derive(Clone, Copy, Debug)]
pub struct StateExit {
    pub scope: &'static str,
    pub state: &'static str,
    pub exit: Exit,
}

const fn s(scope: &'static str, state: &'static str, exit: Exit) -> StateExit {
    StateExit { scope, state, exit }
}

/// Every state string the inventory, recoveries, relocation intents and
/// delivery cleanup write, with its exit. Job statuses and hold causes are
/// enums in `nzbd-types`; the test checks them separately.
pub const STATE_EXITS: &[StateExit] = &[
    // artifacts.state
    s("artifact", "allocating", Exit::Event("allocation commit; startup classifies an interrupted one (missing → source_gone, present → unowned adoption)")),
    s("artifact", "active", Exit::Event("the job finishes: delivery, failure policy, or retirement")),
    s("artifact", "active_unverified", Exit::Event("queue admission verifies it (→ active) or it stays an unowned adoption candidate")),
    s("artifact", "transitioning", Exit::Event("relocation/intent finishes or unwinds (intent reconciliation, then the review-op exit)")),
    s("artifact", "retiring", Exit::Event("writer stop; startup reclassifies an interrupted one")),
    s("artifact", "completed", Exit::Event("delivery cleanup removes the closed-scope source")),
    s("artifact", "retained", Exit::Timer("its hold's exit (HOLD_EXITS); an unheld retained owned orphan is parked with the failed TTL")),
    s("artifact", "unknown", Exit::Operator("discovered content Runner did not create; never deleted automatically")),
    s("artifact", "parked_failed", Exit::Timer("failed-retention TTL → retention deletion (Keep / keep_on_failure: policy)")),
    s("artifact", "recovery_staged", Exit::Event("consumer import receipt; a cancelled/failed recovery's copy is disposed")),
    s("artifact", "recovery_imported", Exit::Timer("retention TTL → deletion")),
    s("artifact", "deleting", Exit::Event("the delete operation finishes or retries")),
    s("artifact", "delete_failed", Exit::Timer("delete retry ladder; a conflict re-snapshots the closed scope and retries")),
    s("artifact", "deleted", Exit::Terminal),
    s("artifact", "source_gone", Exit::Terminal),
    // operations.state
    s("operation", "queued", Exit::Timer("not_before / next_retry, then runs")),
    s("operation", "running", Exit::Event("the worker finishes; startup resumes an interrupted one")),
    s("operation", "retry", Exit::Timer("next_retry ladder")),
    s("operation", "review", Exit::Timer("exit_wedged_states: delete → retry or cancel; relocate → abandon (unwind) or close; transforms abandoned (W9); restored-backup → cancel")),
    s("operation", "retained", Exit::Event("startup-orphan receipt row; closes when the orphan is reclaimed or disposed")),
    s("operation", "preview", Exit::Event("retention preview: applied or superseded by the next preview")),
    s("operation", "succeeded", Exit::Terminal),
    s("operation", "cancelled", Exit::Terminal),
    s("operation", "failed", Exit::Terminal),
    s("operation", "pending", Exit::Event("its worker claims it")),
    s("operation", "stopping", Exit::Event("worker stop acknowledgement")),
    s("operation", "transferred", Exit::Terminal),
    // recoveries.state
    s("recovery", "staging", Exit::Event("publication finishes or fails; reconcile_recoveries resumes it")),
    s("recovery", "publishing", Exit::Event("publication finishes or fails; reconcile_recoveries resumes it")),
    s("recovery", "published", Exit::Timer("consumer claim/import receipt or cancellation; cancelled after failed_retention_days without a claim")),
    s("recovery", "claimed", Exit::Timer("consumer import receipt or cancellation; cancelled after failed_retention_days without a claim heartbeat")),
    s("recovery", "cancel_pending", Exit::Event("worker quiescence acknowledgement (taken as given after failed_retention_days of silence); unclaimed ones cancel on reconcile")),
    s("recovery", "imported", Exit::Timer("receipt-scoped source prune (automatic after HOLD_EXIT_SECONDS), then the source's TTL")),
    s("recovery", "partial", Exit::Timer("receipt-scoped source prune (automatic after HOLD_EXIT_SECONDS), then the source's TTL")),
    s("recovery", "review", Exit::Timer("restored backup: cancelled by exit_wedged_states; its staged copy is disposed")),
    s("recovery", "pruned", Exit::Terminal),
    // relocation_intents.state
    s("intent", "open", Exit::Event("restart reconciliation decides finish or unwind")),
    s("intent", "finishing", Exit::Event("finish completes; a refusal goes to the review-op exit")),
    s("intent", "unwinding", Exit::Event("unwind completes; a refusal goes to the review-op exit (close, never delete)")),
    s("intent", "committed", Exit::Terminal),
    s("intent", "unwound", Exit::Terminal),
    // deliveries.cleanup_status
    s("delivery cleanup", "pending", Exit::Event("the delivery's source cleanup runs")),
    s("delivery cleanup", "running", Exit::Event("cleanup worker; startup turns an interrupted one into retry")),
    s("delivery cleanup", "retry", Exit::Timer("cleanup_next_retry_at")),
    s("delivery cleanup", "blocked", Exit::Timer("policy change wakes it; otherwise retried after HOLD_EXIT_SECONDS")),
    s("delivery cleanup", "succeeded", Exit::Terminal),
    s("delivery cleanup", "done", Exit::Terminal),
    s("delivery cleanup", "removed", Exit::Terminal),
    // URL admission receipt custody
    s("url admission", "runner_owned", Exit::Event("the admitted job's own lifecycle")),
    // daemon control status (in memory)
    s("daemon", "starting", Exit::Event("startup completes or the process exits")),
];

/// What [`Inventory::exit_wedged_states`] does with a hold.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HoldAction {
    /// Custody of in-flight work; its own event releases it.
    InFlight,
    /// A lock on an otherwise healthy row: remove the hold; the row returns
    /// to its own lifecycle (and its blocked cleanup is woken).
    Release,
    /// The row may hold payload bytes: park it as a failed source with the
    /// failed-retention TTL (or now, for a copy whose original is retained).
    Park { immediate: bool, clear_keep: bool },
    /// A disposal refused on a manifest conflict: re-snapshot the closed
    /// scope and retry the delete.
    RetryDelete,
    /// The directory a writer retirement expected is gone or replaced.
    Reclassify,
    /// An interrupted move: abandon it (unwind, or close without deleting).
    AbandonMove,
    /// Bound to a recovery; follows the recovery's state.
    Recovery,
    /// Content Runner did not create. Operator-visible, blocks nothing.
    Adoption,
}

#[derive(Clone, Copy, Debug)]
pub struct HoldExit {
    pub hold: &'static str,
    /// Matches any hold starting with `hold`.
    pub prefix: bool,
    pub action: HoldAction,
    pub exit: Exit,
}

const fn h(hold: &'static str, action: HoldAction, exit: Exit) -> HoldExit {
    HoldExit {
        hold,
        prefix: false,
        action,
        exit,
    }
}

const PARK_TTL: HoldAction = HoldAction::Park {
    immediate: false,
    clear_keep: false,
};
const PARK_PIN: HoldAction = HoldAction::Park {
    immediate: false,
    clear_keep: true,
};
const DISPOSE_COPY: HoldAction = HoldAction::Park {
    immediate: true,
    clear_keep: true,
};

/// Every hold string the code writes. `review` on an unowned row is the
/// adoption flow; on an owned row it is a refused disposal or a released
/// lock (see [`Inventory::exit_wedged_states`]).
pub const HOLD_EXITS: &[HoldExit] = &[
    h("waiting for writer stop", HoldAction::InFlight, Exit::Event("writer stop; startup reclassifies an interrupted retirement")),
    h("transform workspace", HoldAction::InFlight, Exit::Event("transform finishes; workspace retirement disposes the scratch")),
    HoldExit { hold: "recovery:", prefix: true, action: HoldAction::Recovery, exit: Exit::Event("recovery finishes; a failed/cancelled one releases, an imported one is receipt-pruned after HOLD_EXIT_SECONDS") },
    h("awaiting complete import receipt", HoldAction::Recovery, Exit::Event("consumer import receipt; a cancelled/failed recovery's copy is disposed")),
    h("review", HoldAction::RetryDelete, Exit::Timer("unowned: adoption (operator); owned: refused disposal re-snapshotted and retried, otherwise released")),
    h("review: files changed; adoption required", HoldAction::Adoption, Exit::Operator("adopted content changed; never deleted automatically")),
    h("review: preexisting accepted output", HoldAction::Adoption, Exit::Operator("output Runner did not create")),
    h("review: cancelled recovery", HoldAction::Release, Exit::Timer("released; the source keeps its bytes and lifecycle")),
    h("review: cancelled recovery staging", DISPOSE_COPY, Exit::Timer("copy disposed; the recovery source holds the original")),
    h("review: unselected source files remain", HoldAction::Release, Exit::Timer("released; the source follows its TTL")),
    h("review: unselected source files remain after receipt cleanup", HoldAction::Release, Exit::Timer("released; the source follows its TTL")),
    h("review: relocation staging", PARK_TTL, Exit::Event("its relocate operation ends; left over after that, parked with the failed TTL")),
    h("review: abandoned relocation staging", DISPOSE_COPY, Exit::Timer("copy disposed; abandon verified the original source")),
    h("review: interrupted move", HoldAction::AbandonMove, Exit::Timer("move abandoned (unwind; else Runner-placed duplicates removed and an only-copy residue parked with the failed TTL); the job fails io and parks the source")),
    h("review: workspace cleanup", HoldAction::RetryDelete, Exit::Timer("scratch re-snapshotted (closed scope) and its retirement retried")),
    h("review: subprocess quiescence unconfirmed", HoldAction::Release, Exit::Timer("released at the next open (no subprocess outlives the process) or after HOLD_EXIT_SECONDS")),
    h("review: interrupted transform", HoldAction::Release, Exit::Timer("released; post-processing re-runs the transform (W9)")),
    h("review: interrupted writer retirement", HoldAction::Reclassify, Exit::Timer("gone → source_gone; replaced → unowned adoption; present → released")),
    h("review: allocation has no recovered job", PARK_PIN, Exit::Timer("owned: parked with the failed TTL (W9); unowned: adoption")),
    h("review: superseded by accepted generation", PARK_PIN, Exit::Timer("materialization predecessor parked with the failed TTL")),
    h("review: superseded private processing source", PARK_PIN, Exit::Timer("parked with the failed TTL")),
    h("review: previous external job generation", PARK_PIN, Exit::Timer("owned: parked with the failed TTL; unowned: adoption")),
    h("review: managed seed metainfo mismatch", PARK_TTL, Exit::Timer("seed directory (closed scope) parked with the failed TTL")),
    h("review retained source", PARK_TTL, Exit::Timer("parked with the failed TTL")),
    h("restored backup: review required", HoldAction::Release, Exit::Timer("released; the row gets back the Keep it had before the restore")),
];

/// The registered exit for a hold string.
pub fn hold_exit(hold: &str) -> Option<&'static HoldExit> {
    HOLD_EXITS
        .iter()
        .find(|rule| !rule.prefix && rule.hold == hold)
        .or_else(|| {
            HOLD_EXITS
                .iter()
                .find(|rule| rule.prefix && hold.starts_with(rule.hold))
        })
}

/// What one sweep did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ExitReport {
    pub released: usize,
    pub parked: usize,
    pub retried: usize,
    pub reclassified: usize,
    pub abandoned: usize,
    pub cancelled_operations: usize,
    pub recoveries: usize,
    pub deliveries: usize,
}

impl ExitReport {
    pub fn total(&self) -> usize {
        self.released
            + self.parked
            + self.retried
            + self.reclassified
            + self.abandoned
            + self.cancelled_operations
            + self.recoveries
            + self.deliveries
    }
}

fn cancel(op: &mut Operation, why: &str) {
    op.state = "cancelled".into();
    op.error = Some(format!(
        "automatic exit: {why} (was: {})",
        op.error.as_deref().unwrap_or("review")
    ));
}

impl Inventory {
    /// Carry out the registered exit of every `review`/`retained`/held row
    /// older than `grace` seconds. `touch_files` false runs only the
    /// database-only exits (at open: no worker is running yet and storage
    /// may still be coming up); the tick passes true. Idempotent: an exited
    /// row no longer matches. Per-row failures are logged and retried on
    /// the next sweep; the sweep itself never fails open.
    pub fn exit_wedged_states(&self, grace: i64, touch_files: bool) -> Result<ExitReport> {
        let cutoff = now().saturating_sub(grace.max(0));
        let mut report = ExitReport::default();
        self.exit_review_operations(cutoff, touch_files, &mut report)?;
        self.exit_review_recoveries(&mut report)?;
        if touch_files {
            self.exit_stale_handoffs(&mut report)?;
        }
        self.exit_held_artifacts(cutoff, touch_files, &mut report)?;
        if touch_files {
            self.exit_blocked_deliveries(cutoff, &mut report)?;
        }
        if report.total() > 0 {
            tracing::info!(
                ?report,
                touch_files,
                "automatic exits for review/retained states"
            );
        }
        Ok(report)
    }

    fn exit_review_operations(
        &self,
        cutoff: i64,
        touch_files: bool,
        report: &mut ExitReport,
    ) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT data FROM operations WHERE state='review' AND json_extract(data,'$.kind') NOT IN ('par_restore','par_repair','extract') ORDER BY id LIMIT 500",
            )?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in raws {
            let op: Operation = serde_json::from_str(&raw)?;
            let result = match op.kind.as_str() {
                "delete" | "prune" => self.exit_review_delete(op, touch_files, report),
                "relocate" if touch_files => self.exit_review_relocation(op, cutoff, report),
                "retire_workspace" if touch_files => self.exit_review_workspace(op, report),
                "relocate" | "retire_workspace" => Ok(()),
                _ => {
                    let _guard = self.mutation_guard()?;
                    let mut op = self.operation(&op.id)?;
                    if op.state == "review" {
                        cancel(
                            &mut op,
                            "no reviewed continuation exists for this operation",
                        );
                        save_operation(&self.db.lock().unwrap(), &op)?;
                        report.cancelled_operations += 1;
                    }
                    Ok(())
                }
            };
            if let Err(error) = result {
                tracing::warn!(%error, "automatic exit of a review operation deferred");
            }
        }
        Ok(())
    }

    fn exit_review_delete(
        &self,
        op: Operation,
        touch_files: bool,
        report: &mut ExitReport,
    ) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut op = self.operation(&op.id)?;
        if op.state != "review" {
            return Ok(());
        }
        let error = op.error.clone().unwrap_or_default();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let artifact = match read::<Artifact>(&tx, "artifacts", &op.artifact) {
            Ok(artifact) => Some(artifact),
            Err(Error::NotFound) => None,
            Err(error) => return Err(error),
        };
        match artifact {
            None => cancel(&mut op, "the artifact is gone"),
            Some(a) if a.terminal() => cancel(&mut op, "the payload is already gone"),
            Some(a) if !a.owned => cancel(
                &mut op,
                "unowned content is never deleted automatically; it stays visible for adoption",
            ),
            Some(_) if op.kind == "prune" => cancel(&mut op, "a prune is re-requested by its policy"),
            Some(mut a)
                if error.starts_with("pre-existing artifact is exempt")
                    || error.starts_with("pre-upgrade destructive work") =>
            {
                // Closed scope: an owned payload is Runner's own download,
                // whenever it was allocated.
                // Not a policy edit: the revision the delete authorization
                // is bound to stays, so an explicit delete still runs.
                a.automatic_cleanup_eligible = true;
                a.updated_at = now();
                save_artifact(&tx, &a)?;
                op.state = "retry".into();
                op.next_retry = 0;
                op.error = Some(format!("automatic exit: closed scope; retrying ({error})"));
                report.retried += 1;
                save_operation(&tx, &op)?;
                tx.commit()?;
                return Ok(());
            }
            Some(_)
                if error.starts_with("deletion generation changed")
                    || error.starts_with("explicit cancellation receipt changed")
                    || error.starts_with("restored backup") =>
            {
                cancel(
                    &mut op,
                    "the authorization names another generation; the current one follows its own lifecycle",
                )
            }
            // A conflict during deletion: the artifact's hold (`review`,
            // RetryDelete) re-snapshots the closed scope and retries this
            // operation on the tick.
            Some(_) => {
                drop(tx);
                let _ = touch_files;
                return Ok(());
            }
        }
        save_operation(&tx, &op)?;
        event(
            &tx,
            &op.artifact,
            "automatic_exit",
            op.error.as_deref().unwrap_or(""),
        )?;
        tx.commit()?;
        report.cancelled_operations += 1;
        Ok(())
    }

    /// A relocation stuck in review is abandoned: the original is retained
    /// (unwound by identity), the target is closed without deleting
    /// anything. When abandon refuses, the move is cancelled and both sides
    /// are parked with the failed TTL, so neither copy is lost and neither
    /// waits on a person.
    fn exit_review_relocation(
        &self,
        op: Operation,
        cutoff: i64,
        report: &mut ExitReport,
    ) -> Result<()> {
        let source = self.get(&op.artifact)?;
        if source.updated_at > cutoff || source.terminal() {
            return Ok(());
        }
        if self.reconciliation_in_use(&source) {
            return Ok(());
        }
        match self.abandon_relocation(&op.id, source.revision, &source.generation) {
            Ok(_) => {
                report.abandoned += 1;
                Ok(())
            }
            Err(error) => {
                let movement: Relocation = serde_json::from_str(&op.request)?;
                let open_intent = movement
                    .intent
                    .as_deref()
                    .map(|generation| self.relocation_intent(generation))
                    .transpose()?
                    .flatten()
                    .is_some_and(|intent| intent.state.is_open());
                if open_intent {
                    // An open per-file move is only ever finished or
                    // unwound by reconciliation; the next sweep retries.
                    return Err(error);
                }
                let ttl = i64::from(self.settings()?.failed_retention_days) * 86400;
                let _guard = self.mutation_guard()?;
                let mut op = self.operation(&op.id)?;
                if op.state != "review" {
                    return Ok(());
                }
                cancel(
                    &mut op,
                    &format!("abandon refused ({error}); both sides parked"),
                );
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                let mut ids = vec![op.artifact.clone()];
                if let Some(scratch) = &movement.scratch {
                    ids.push(scratch.id.clone());
                }
                for id in ids {
                    if let Ok(mut a) = read::<Artifact>(&tx, "artifacts", &id) {
                        if a.owned && !a.terminal() {
                            park(&mut a, ttl, false);
                            save_artifact(&tx, &a)?;
                            report.parked += 1;
                        }
                    }
                }
                save_operation(&tx, &op)?;
                event(
                    &tx,
                    &op.artifact,
                    "automatic_exit",
                    op.error.as_deref().unwrap_or(""),
                )?;
                tx.commit()?;
                report.cancelled_operations += 1;
                Ok(())
            }
        }
    }

    fn exit_review_workspace(&self, op: Operation, report: &mut ExitReport) -> Result<()> {
        let scratch = match self.get(&op.artifact) {
            Ok(scratch) => scratch,
            Err(Error::NotFound) => return Ok(()),
            Err(error) => return Err(error),
        };
        if scratch.terminal() || self.reconciliation_in_use(&scratch) {
            return Ok(());
        }
        self.resnapshot_closed_scope(scratch, report)
    }

    /// Re-read the manifest of an owned row's own directory (closed scope:
    /// everything in it came from the download) and retry its disposal.
    /// A directory that is gone is `source_gone`; one replaced by another
    /// object is no longer this row (the new folder is discovered as
    /// unowned content).
    fn resnapshot_closed_scope(&self, a: Artifact, report: &mut ExitReport) -> Result<()> {
        let observed = match self.verify(&a) {
            Ok(dir) => match fs::manifest(&dir, 100_000) {
                Ok(files) => Some(files),
                // Names a manifest refuses (symlink, special file, non-UTF-8,
                // depth > 64, > 100k entries): closed scope, so the tree is
                // removed by raw name and the disposal retries on the empty
                // directory instead of failing on the same names forever.
                Err(Error::Conflict(reason)) => {
                    let removal = fs::remove_tree_contents(&dir)?;
                    tracing::info!(artifact = %a.id, %reason, removed = removal.removed,
                        "closed scope emptied by raw name; its disposal retries");
                    Some(fs::manifest(&dir, 100_000)?)
                }
                Err(error) => return Err(error),
            },
            Err(Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(Error::Conflict(_)) => None,
            Err(error) => return Err(error),
        };
        let _guard = self.mutation_guard()?;
        let mut current = self.get(&a.id)?;
        if current.generation != a.generation || current.revision != a.revision {
            return Ok(());
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let ops = {
            let mut statement = tx.prepare("SELECT data FROM operations WHERE artifact=?1 AND state IN ('review','retry') AND json_extract(data,'$.kind') IN ('delete','retire_workspace')")?;
            let rows = statement.query_map([&a.id], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        current.hold = None;
        current.revision += 1;
        current.updated_at = now();
        match observed {
            Some(files) => {
                current.files = files;
                for raw in ops {
                    let mut op: Operation = serde_json::from_str(&raw)?;
                    op.state = "retry".into();
                    op.next_retry = 0;
                    op.error = Some(format!(
                        "automatic exit: closed scope re-snapshotted; retrying ({})",
                        op.error.as_deref().unwrap_or("conflict")
                    ));
                    save_operation(&tx, &op)?;
                }
                report.retried += 1;
            }
            None => {
                current.state = "source_gone".into();
                for raw in ops {
                    let mut op: Operation = serde_json::from_str(&raw)?;
                    cancel(
                        &mut op,
                        "the directory is gone or is no longer this payload",
                    );
                    save_operation(&tx, &op)?;
                }
                report.reclassified += 1;
            }
        }
        save_artifact(&tx, &current)?;
        event(
            &tx,
            &current.id,
            "automatic_exit",
            "closed scope re-snapshotted",
        )?;
        tx.commit()?;
        Ok(())
    }

    /// Finding 12: a recovery handoff waiting on its consumer (`published`,
    /// `claimed`, or a claimed `cancel_pending`) with no claim heartbeat for
    /// `failed_retention_days` is cancelled through the existing path; the
    /// silent consumer's acknowledgement is taken as given (a late claim or
    /// receipt from it is refused). Its holds then take their own exits.
    fn exit_stale_handoffs(&self, report: &mut ExitReport) -> Result<()> {
        let ttl = i64::from(self.settings()?.failed_retention_days) * 86400;
        let cutoff = now().saturating_sub(ttl.max(0));
        let stale = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare(
                "SELECT id FROM recoveries WHERE (state IN ('published','claimed') OR (state='cancel_pending' AND json_extract(data,'$.consumer') IS NOT NULL)) AND COALESCE(json_extract(data,'$.updated_at'),0)<=?1 LIMIT 100",
            )?;
            let rows = statement.query_map([cutoff], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for id in stale {
            let recovery = self.recovery(&id)?;
            let consumer = recovery.consumer.clone();
            let result = (|| {
                if consumer.is_some() && recovery.state != "cancel_pending" {
                    self.cancel_recovery(&id, consumer.as_deref(), false)?;
                }
                self.cancel_recovery(&id, consumer.as_deref(), true)
            })();
            match result {
                Ok(_) => report.recoveries += 1,
                Err(Error::Conflict(why)) => {
                    tracing::info!(recovery = %id, %why, "stale recovery handoff not cancelled yet");
                }
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }

    fn exit_review_recoveries(&self, report: &mut ExitReport) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        let raws = {
            let mut statement = tx.prepare("SELECT data FROM recoveries WHERE state='review'")?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        for raw in &raws {
            let mut recovery: super::recovery::Recovery = serde_json::from_str(raw)?;
            recovery.state = "cancelled".into();
            recovery.error = Some(format!(
                "automatic exit: cancelled ({})",
                recovery.error.as_deref().unwrap_or("review")
            ));
            recovery.updated_at = now();
            super::recovery::save(&tx, &recovery)?;
            // The staged copy; the recovery source still holds the original.
            if let Ok(mut staged) =
                read::<Artifact>(&tx, "artifacts", &format!("recovery-{}", recovery.id))
            {
                if !staged.terminal() {
                    staged.hold = Some("review: cancelled recovery staging".into());
                    staged.revision += 1;
                    save_artifact(&tx, &staged)?;
                }
            }
            report.recoveries += 1;
        }
        tx.commit()?;
        Ok(())
    }

    fn exit_blocked_deliveries(&self, cutoff: i64, report: &mut ExitReport) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let changed = self.db.lock().unwrap().execute(
            "UPDATE deliveries SET cleanup_status='retry',cleanup_next_retry_at=0 WHERE cleanup_status='blocked' AND COALESCE(cleanup_last_attempt_at,cleanup_blocked_since,0)<=?1",
            [cutoff],
        )?;
        report.deliveries += changed;
        Ok(())
    }

    fn exit_held_artifacts(
        &self,
        cutoff: i64,
        touch_files: bool,
        report: &mut ExitReport,
    ) -> Result<()> {
        let raws = {
            let db = self.db.lock().unwrap();
            let mut statement = db.prepare("SELECT data FROM artifacts WHERE state NOT IN ('deleted','source_gone') AND json_extract(data,'$.hold') IS NOT NULL AND updated_at<=?1 ORDER BY id LIMIT 500")?;
            let rows = statement.query_map([cutoff], |row| row.get::<_, String>(0))?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        let ttl = i64::from(self.settings()?.failed_retention_days) * 86400;
        for raw in raws {
            let a: Artifact = serde_json::from_str(&raw)?;
            if let Err(error) = self.exit_held_artifact(a, ttl, touch_files, report) {
                tracing::warn!(%error, "automatic exit of a held artifact deferred");
            }
        }
        Ok(())
    }

    fn exit_held_artifact(
        &self,
        a: Artifact,
        ttl: i64,
        touch_files: bool,
        report: &mut ExitReport,
    ) -> Result<()> {
        let Some(hold) = a.hold.clone() else {
            return Ok(());
        };
        let Some(rule) = hold_exit(&hold) else {
            return Ok(());
        };
        if self.reconciliation_in_use(&a) || self.artifact_in_use(&a)? {
            return Ok(());
        }
        let live_source =
            a.job.is_some() && matches!(a.state.as_str(), "active" | "transitioning" | "retiring");
        let action = match rule.action {
            HoldAction::InFlight | HoldAction::Adoption => return Ok(()),
            _ if !a.owned => return Ok(()), // unowned: adoption, never disposed
            HoldAction::RetryDelete if hold == "review" && a.state != "delete_failed" => {
                HoldAction::Release
            }
            HoldAction::Park { .. } if live_source => HoldAction::Release,
            HoldAction::Park { .. } if hold == "review: relocation staging" => {
                if self.relocation_owns_scratch(&a.id)? {
                    return Ok(());
                }
                rule.action
            }
            HoldAction::Park {
                immediate: true, ..
            } if hold == "review: abandoned relocation staging"
                && !self.relocation_original_retained(&a.id)? =>
            {
                PARK_TTL
            }
            HoldAction::Recovery => match self.recovery_hold_exit(&a, &hold)? {
                RecoveryExit::Pending => return Ok(()),
                RecoveryExit::Action(action) => action,
                RecoveryExit::Prune(recovery) => {
                    if touch_files {
                        self.prune_receipted_source(&recovery)?;
                        report.retried += 1;
                    }
                    return Ok(());
                }
            },
            action => action,
        };
        match action {
            HoldAction::Release => self.release_hold(&a, &hold, report),
            HoldAction::Park {
                immediate,
                clear_keep,
            } => {
                let _guard = self.mutation_guard()?;
                let mut current = self.get(&a.id)?;
                if current.revision != a.revision || current.hold.as_deref() != Some(&hold) {
                    return Ok(());
                }
                let mut db = self.db.lock().unwrap();
                let tx = db.transaction()?;
                // Finding 10: clear only the Keep the pin set; an operator's
                // explicit Keep (or the Keep a row had before) stays.
                let keep = if clear_keep {
                    keep_after_forced(&tx, &current.id)?
                } else {
                    current.keep
                };
                park(&mut current, if immediate { 0 } else { ttl }, false);
                current.keep = keep;
                save_artifact(&tx, &current)?;
                event(
                    &tx,
                    &current.id,
                    "automatic_exit",
                    &format!("parked ({hold})"),
                )?;
                tx.commit()?;
                report.parked += 1;
                Ok(())
            }
            HoldAction::RetryDelete if touch_files => self.resnapshot_closed_scope(a, report),
            HoldAction::Reclassify if touch_files => self.reclassify_retirement(a, &hold, report),
            HoldAction::AbandonMove if touch_files => {
                let op = {
                    let db = self.db.lock().unwrap();
                    db.query_row(
                        "SELECT data FROM operations WHERE artifact=?1 AND state='review' AND json_extract(data,'$.kind')='relocate' LIMIT 1",
                        [&a.id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                };
                match op {
                    // The review-operation exit abandons it.
                    Some(_) => Ok(()),
                    None if self.open_intent_for_artifact(&a.id)? => Ok(()),
                    None => self.release_hold(&a, &hold, report),
                }
            }
            _ => Ok(()),
        }
    }

    fn release_hold(&self, a: &Artifact, hold: &str, report: &mut ExitReport) -> Result<()> {
        let _guard = self.mutation_guard()?;
        let mut current = self.get(&a.id)?;
        if current.revision != a.revision || current.hold.as_deref() != Some(hold) {
            return Ok(());
        }
        current.hold = None;
        current.revision += 1;
        current.updated_at = now();
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        if hold == RESTORE_HOLD {
            // Finding 8: the restore forced Keep on every row; releasing its
            // hold gives back the Keep the row had (or an operator gave it).
            current.keep = keep_after_forced(&tx, &current.id)?;
        }
        save_artifact(&tx, &current)?;
        event(
            &tx,
            &current.id,
            "automatic_exit",
            &format!("released ({hold})"),
        )?;
        Self::wake_relocation_cleanup(&tx, &current.id, &current.generation)?;
        tx.commit()?;
        report.released += 1;
        Ok(())
    }

    fn reclassify_retirement(
        &self,
        a: Artifact,
        hold: &str,
        report: &mut ExitReport,
    ) -> Result<()> {
        let present = a.path.symlink_metadata().is_ok();
        let ours = present && self.verify(&a).is_ok();
        if ours && !matches!(a.state.as_str(), "retiring" | "retained") {
            return self.release_hold(&a, hold, report);
        }
        let _guard = self.mutation_guard()?;
        let mut current = self.get(&a.id)?;
        if current.revision != a.revision || current.hold.as_deref() != Some(hold) {
            return Ok(());
        }
        if ours {
            // The writer's directory is intact but its retirement never
            // finished: Runner's own payload with no writer, a failed source.
            let ttl = i64::from(self.settings()?.failed_retention_days) * 86400;
            park(&mut current, ttl, false);
        } else {
            if present {
                // Another object sits at the path: it is not this payload.
                current.owned = false;
                current.keep = true;
                current.hold = Some("review".into());
                current.identity = fs::open_dir(&current.path)
                    .and_then(|dir| fs::identity_for_file(&dir))
                    .ok();
            } else {
                current.state = "source_gone".into();
                current.hold = None;
            }
            current.revision += 1;
            current.updated_at = now();
        }
        let mut db = self.db.lock().unwrap();
        let tx = db.transaction()?;
        save_artifact(&tx, &current)?;
        event(
            &tx,
            &current.id,
            "automatic_exit",
            &format!("reclassified ({hold})"),
        )?;
        tx.commit()?;
        report.reclassified += 1;
        Ok(())
    }

    /// Where the recovery a hold is bound to stands.
    fn recovery_hold_exit(&self, a: &Artifact, hold: &str) -> Result<RecoveryExit> {
        let id = if let Some(id) = hold.strip_prefix("recovery:") {
            id.to_string()
        } else if let Some(id) = a.id.strip_prefix("recovery-") {
            id.to_string()
        } else {
            return Ok(RecoveryExit::Pending);
        };
        let state = match self.recovery(&id) {
            Ok(recovery) => recovery.state,
            Err(Error::NotFound) => "pruned".into(),
            Err(error) => return Err(error),
        };
        let staged = !hold.starts_with("recovery:");
        Ok(match state.as_str() {
            "failed" | "cancelled" | "pruned" if staged => RecoveryExit::Action(DISPOSE_COPY),
            "failed" | "cancelled" | "pruned" => RecoveryExit::Action(HoldAction::Release),
            "imported" | "partial" if !staged => RecoveryExit::Prune(id),
            _ => RecoveryExit::Pending,
        })
    }

    fn relocation_owns_scratch(&self, scratch: &str) -> Result<bool> {
        Ok(self.db.lock().unwrap().query_row(
            "SELECT EXISTS(SELECT 1 FROM operations WHERE json_extract(data,'$.kind')='relocate' AND state IN ('queued','running','retry','review') AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?1)",
            [scratch],
            |row| row.get(0),
        )?)
    }

    /// The abandoned relocation whose scratch this is verified its original
    /// and the original is still a live row: the scratch is a redundant copy.
    fn relocation_original_retained(&self, scratch: &str) -> Result<bool> {
        let raw: Option<String> = self.db.lock().unwrap().query_row(
            "SELECT data FROM operations WHERE json_extract(data,'$.kind')='relocate' AND state='cancelled' AND json_extract(json_extract(data,'$.request'),'$.scratch.id')=?1 LIMIT 1",
            [scratch],
            |row| row.get(0),
        ).optional()?;
        let Some(raw) = raw else {
            return Ok(false);
        };
        let op: Operation = serde_json::from_str(&raw)?;
        Ok(self
            .get(&op.artifact)
            .is_ok_and(|source| !source.terminal()))
    }
}

enum RecoveryExit {
    /// The recovery is still in flight; its own events end the hold.
    Pending,
    Action(HoldAction),
    /// Imported: the receipt-scoped source prune runs automatically.
    Prune(String),
}

/// Park an owned row as a failed source: due after `ttl` seconds (now for a
/// redundant copy). Keep / keep_on_failure still retain it (policy).
/// The hold a restored backup puts on every live row.
pub(crate) const RESTORE_HOLD: &str = "restored backup: review required";

/// The Keep a row has once a hold that forced Keep on it is lifted: what
/// the row had when a restore quarantined it, or the operator's latest
/// explicit retention choice (Keep, adoption) — otherwise no Keep. A row
/// quarantined before this was recorded (upgrade) keeps Keep only when an
/// explicit operator Keep event exists.
pub(crate) fn keep_after_forced(db: &rusqlite::Connection, artifact: &str) -> Result<bool> {
    use rusqlite::OptionalExtension;
    let latest: Option<(String, String)> = db
        .query_row(
            "SELECT kind,detail FROM events WHERE artifact=?1 AND kind IN ('restore_quarantine','retention','adopted') ORDER BY at DESC, rowid DESC LIMIT 1",
            [artifact],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()?;
    Ok(match latest {
        Some((kind, detail)) => match kind.as_str() {
            "restore_quarantine" => detail == "keep=true",
            "retention" => detail == "Keep",
            _ => true, // adopted: kept indefinitely
        },
        None => false,
    })
}

fn park(a: &mut Artifact, ttl: i64, clear_keep: bool) {
    a.hold = None;
    if clear_keep {
        a.keep = false;
    }
    a.state = "parked_failed".into();
    a.automatic_cleanup_eligible = true;
    a.retention_seconds = ttl.max(1) as u64;
    a.deadline = Some(now().saturating_add(ttl.max(0)));
    a.revision += 1;
    a.updated_at = now();
}

/// The exit of every job status (`nzbd_types::JobStatus`). Exhaustive: a new
/// status does not compile until it says how it ends.
pub fn job_status_exit(status: &nzbd_types::JobStatus) -> Exit {
    use nzbd_types::JobStatus;
    match status {
        JobStatus::Queued => Exit::Event("a download slot leases it"),
        JobStatus::Downloading => {
            Exit::Event("all articles done or failed → post-processing or failed/health")
        }
        JobStatus::Paused => {
            Exit::Policy("stopped by a person (or seed policy); the person resumes it")
        }
        JobStatus::Held => {
            Exit::Timer("hold cause retry policy; the hold deadline ends it failed/hold_expired")
        }
        JobStatus::Fetching => Exit::Event("URL fetch succeeds or fails failed/fetch"),
        JobStatus::PostQueued => Exit::Event("a post-processing slot admits it"),
        JobStatus::Post { .. } => {
            Exit::Event("the stage finishes; the job ends completed or failed/<reason>")
        }
        JobStatus::Completed => Exit::Terminal,
        JobStatus::Failed => Exit::Terminal,
        JobStatus::Deleted => Exit::Terminal,
    }
}

/// Every job status, for the registry test and the STATUS table.
pub fn job_statuses() -> Vec<nzbd_types::JobStatus> {
    use nzbd_types::JobStatus;
    vec![
        JobStatus::Queued,
        JobStatus::Downloading,
        JobStatus::Paused,
        JobStatus::Held,
        JobStatus::Fetching,
        JobStatus::PostQueued,
        JobStatus::Post {
            stage: nzbd_types::PostStage::ParVerify,
        },
        JobStatus::Completed,
        JobStatus::Failed,
        JobStatus::Deleted,
    ]
}

/// The exit of a job hold cause: every cause has a retry policy and a
/// deadline after which the job ends `failed/hold_expired`.
pub fn hold_cause_exit(cause: &nzbd_types::HoldCause) -> Exit {
    match cause.policy() {
        nzbd_types::HoldPolicy::ScheduledRetry => {
            Exit::Timer("retry ladder; deadline → failed/hold_expired")
        }
        nzbd_types::HoldPolicy::ResumeSameJob => {
            Exit::Timer("resumes when the resource returns; deadline → failed/hold_expired")
        }
        nzbd_types::HoldPolicy::ReobserveMount => {
            Exit::Timer("mount re-checked every pass; deadline → failed/hold_expired")
        }
    }
}

/// The exit of a terminal row's salvage state (`job_terminals.salvage`).
pub fn salvage_exit(state: super::SalvageState) -> Exit {
    use super::SalvageState;
    match state {
        SalvageState::None | SalvageState::SettledNone => {
            Exit::Event("disposal follows failure_action (keep_on_failure: policy)")
        }
        SalvageState::Pending => {
            Exit::Event("assessment (advisory, W8): settles published or settled_none")
        }
        SalvageState::Published => Exit::Event("salvage delivery commits; then failure_action"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;
    use std::path::Path;

    /// The first string literal after `marker` in `line`.
    fn literal_after<'a>(line: &'a str, marker: &str) -> Vec<&'a str> {
        let mut found = Vec::new();
        let mut rest = line;
        while let Some(at) = rest.find(marker) {
            rest = &rest[at + marker.len()..];
            if let Some(start) = rest.find('"') {
                let prefix = &rest[..start];
                // Only `if … { "x" } else { "y" }` or a direct literal.
                if prefix.trim().is_empty() || prefix.contains('{') {
                    if let Some(len) = rest[start + 1..].find('"') {
                        found.push(&rest[start + 1..start + 1 + len]);
                    }
                }
            }
        }
        found
    }

    fn sql_after<'a>(line: &'a str, marker: &str) -> Vec<&'a str> {
        let mut found = Vec::new();
        let mut rest = line;
        while let Some(at) = rest.find(marker) {
            rest = &rest[at + marker.len()..];
            if let Some(len) = rest.find('\'') {
                found.push(&rest[..len]);
            }
        }
        found
    }

    fn production_sources(dir: &Path, out: &mut Vec<(String, String)>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                production_sources(&path, out);
                continue;
            }
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if !name.ends_with(".rs") || name.contains("tests") {
                continue;
            }
            let mut text = std::fs::read_to_string(&path).unwrap();
            // Drop an inline `#[cfg(test)] mod x {` and everything after it.
            if let Some(at) = text.find("#[cfg(test)]\nmod ").filter(|&at| {
                text[at..]
                    .lines()
                    .nth(1)
                    .is_some_and(|line| line.trim_end().ends_with('{'))
            }) {
                text.truncate(at);
            }
            out.push((path.display().to_string(), text));
        }
    }

    /// Every state and hold string the production code writes, across the
    /// workspace crates.
    fn written_strings() -> (BTreeSet<String>, BTreeSet<String>) {
        let crates = Path::new(env!("CARGO_MANIFEST_DIR")).parent().unwrap();
        let mut sources = Vec::new();
        for krate in std::fs::read_dir(crates).unwrap() {
            let src = krate.unwrap().path().join("src");
            if src.is_dir() {
                production_sources(&src, &mut sources);
            }
        }
        let mut states = BTreeSet::new();
        let mut holds = BTreeSet::new();
        for (_, text) in &sources {
            let lines: Vec<&str> = text.lines().collect();
            for (index, line) in lines.iter().enumerate() {
                for marker in [".state = ", "state: ", ".state = if "] {
                    for value in literal_after(line, marker) {
                        if value.chars().all(|c| c.is_ascii_lowercase() || c == '_')
                            && !value.is_empty()
                        {
                            states.insert(value.to_string());
                        }
                    }
                }
                for marker in ["SET state='", "cleanup_status='", ",state='"] {
                    for value in sql_after(line, marker) {
                        states.insert(value.to_string());
                    }
                }
                for marker in ["hold = Some(", "hold: Some("] {
                    for value in literal_after(line, marker) {
                        holds.insert(value.to_string());
                    }
                }
                if line.contains("get_or_insert_with(|| \"")
                    && lines[index.saturating_sub(2)..=index]
                        .iter()
                        .any(|line| line.contains("hold"))
                {
                    for value in literal_after(line, "get_or_insert_with(|| ") {
                        holds.insert(value.to_string());
                    }
                }
            }
        }
        (states, holds)
    }

    #[test]
    fn every_written_state_and_hold_has_a_registered_exit() {
        let (states, holds) = written_strings();
        assert!(
            states.contains("review") && holds.contains("review: interrupted move"),
            "the scan must see the inventory's own writes: {states:?} {holds:?}"
        );
        let registered: BTreeSet<&str> = STATE_EXITS.iter().map(|entry| entry.state).collect();
        // Job/torrent/runtime enums that also use `.state = "..."` but are
        // not persisted lifecycle states of this registry. Each must still be
        // registered somewhere below, so a new string cannot slip through.
        let unregistered: Vec<_> = states
            .iter()
            .filter(|state| {
                !registered.contains(state.as_str())
                    && !OTHER_STATE_STRINGS.contains(&state.as_str())
            })
            .collect();
        assert!(
            unregistered.is_empty(),
            "state strings written with no registered exit (add them to STATE_EXITS): {unregistered:?}"
        );
        let missing: Vec<_> = holds
            .iter()
            .filter(|hold| hold_exit(hold).is_none())
            .collect();
        assert!(
            missing.is_empty(),
            "hold strings written with no registered exit (add them to HOLD_EXITS): {missing:?}"
        );
    }

    /// State strings the scan finds that belong to in-memory or wire state
    /// machines outside the persisted inventory (torrent phases, cluster
    /// worker phases). Listed so the scan stays closed.
    const OTHER_STATE_STRINGS: &[&str] = &[];

    #[test]
    fn only_unowned_content_waits_for_a_person() {
        for entry in STATE_EXITS {
            if let Exit::Operator(_) = entry.exit {
                assert_eq!(
                    (entry.scope, entry.state),
                    ("artifact", "unknown"),
                    "only discovered (unowned) content may need an operator"
                );
            }
        }
        for rule in HOLD_EXITS {
            match rule.exit {
                Exit::Operator(_) => assert_eq!(
                    rule.action,
                    HoldAction::Adoption,
                    "{}: an operator exit is only for unowned/adopted content",
                    rule.hold
                ),
                _ => assert_ne!(rule.action, HoldAction::Adoption, "{}", rule.hold),
            }
        }
        for status in job_statuses() {
            assert!(
                !matches!(job_status_exit(&status), Exit::Operator(_)),
                "{status:?}"
            );
        }
        for cause in nzbd_types::HoldCause::ALL {
            assert!(
                matches!(hold_cause_exit(&cause), Exit::Timer(_)),
                "{cause:?}"
            );
        }
        for state in [
            super::super::SalvageState::None,
            super::super::SalvageState::Pending,
            super::super::SalvageState::Published,
            super::super::SalvageState::SettledNone,
        ] {
            assert!(!matches!(salvage_exit(state), Exit::Operator(_)));
        }
        for state in [
            super::super::intents::IntentState::Open,
            super::super::intents::IntentState::Finishing,
            super::super::intents::IntentState::Unwinding,
            super::super::intents::IntentState::Committed,
            super::super::intents::IntentState::Unwound,
        ] {
            assert!(
                STATE_EXITS
                    .iter()
                    .any(|entry| entry.scope == "intent" && entry.state == state.as_str()),
                "{state:?}"
            );
        }
    }

    /// Prints the STATUS.md table (run with `--nocapture`).
    #[test]
    fn exit_table() {
        let mut table = String::from("| Scope | State / hold | Exit |\n|---|---|---|\n");
        for status in job_statuses() {
            table += &format!(
                "| job | `{}` | {} |\n",
                status.as_str(),
                job_status_exit(&status).describe()
            );
        }
        for cause in nzbd_types::HoldCause::ALL {
            table += &format!(
                "| job hold cause | `{}` | {} |\n",
                cause.as_str(),
                hold_cause_exit(&cause).describe()
            );
        }
        for entry in STATE_EXITS {
            table += &format!(
                "| {} | `{}` | {} |\n",
                entry.scope,
                entry.state,
                entry.exit.describe()
            );
        }
        for rule in HOLD_EXITS {
            let name = if rule.prefix {
                format!("{}<id>", rule.hold)
            } else {
                rule.hold.to_string()
            };
            table += &format!("| artifact hold | `{name}` | {} |\n", rule.exit.describe());
        }
        println!("{table}");
        assert!(table.lines().count() > STATE_EXITS.len() + HOLD_EXITS.len());
    }
}
