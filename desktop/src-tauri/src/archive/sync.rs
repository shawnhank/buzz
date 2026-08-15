//! Rust archive sync task — the backend replacement for the renderer's
//! `archiveSyncManager`.
//!
//! Opens one live relay subscription per saved archive config and forwards
//! matched events to the existing archive pipeline in debounced batches. The
//! renderer no longer sees archive traffic at all: previously every matched
//! event crossed the IPC boundary twice (relay -> renderer, renderer ->
//! `archive_events`) purely to be written to a SQLite file the backend owns.
//!
//! # Start gate
//!
//! The task is NOT self-starting. Kind 24200 is relay-*ephemeral*: frames that
//! arrive before the listener opens are permanently lost, so the renderer must
//! finish observer reconciliation (which seeds kind 24200 into the owner_p
//! subscription) before any listener opens. That ordering is the whole reason
//! `useArchiveSync` gated on `observerReconciled`, and it survives the move as
//! an explicit `start_archive_sync` command issued after the same gate.

use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use nostr::JsonUtil;
use serde_json::json;
use tauri::{AppHandle, Emitter, Manager, State};
use tokio::{
    sync::{mpsc, Mutex, Notify},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    store::SaveSubscription, ArchiveBatchResult, ArchiveCandidate, MatchedScope, ScopeType,
};
use crate::app_state::AppState;
use crate::native_relay_client::{self, MatchedEvent, RelaySession, Subscription};

/// Flush once this many events are buffered. Parity with the renderer manager.
const FLUSH_BATCH_SIZE: usize = 25;
/// Maximum time an event waits in the buffer before being flushed.
///
/// This is a deadline measured from the FIRST buffered event, not an idle
/// timer that each arrival extends. The renderer constant was named
/// `FLUSH_IDLE_MS`, but its `scheduleFlush` returned early when a timer was
/// already pending, so a steady trickle still flushed every 2s rather than
/// never. The behavior is preserved; the name is corrected.
const FLUSH_DEADLINE: Duration = Duration::from_millis(2_000);

/// Emitted after a batch persists new agent-metric rows, so the renderer can
/// invalidate its usage queries. Replaces the in-process `notifyAgentMetrics
/// Changed()` call the manager made on the JS side of that same batch.
const AGENT_METRICS_CHANGED_EVENT: &str = "archive-agent-metrics-changed";

type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Everything the sync loop needs from the outside world.
///
/// Injected rather than reached for so the loop's batching, demultiplexing,
/// and reload behavior are testable without a relay, a database, or a Tauri
/// app handle.
pub(crate) trait ArchiveSyncIo: Send + Sync + 'static {
    fn list_subscriptions(&self) -> BoxFuture<'_, Result<Vec<SaveSubscription>, String>>;
    fn set_subscriptions(&self, subscriptions: Vec<Subscription>) -> BoxFuture<'_, ()>;
    fn archive(
        &self,
        candidates: Vec<ArchiveCandidate>,
    ) -> BoxFuture<'_, Result<ArchiveBatchResult, String>>;
    fn notify_agent_metrics_changed(&self);
}

// ── Subscription planning ────────────────────────────────────────────────────

/// The relay subscription set for `subscriptions`, plus the scope each
/// subscription id maps back to when its events arrive.
///
/// The id encodes scope AND kinds, so a kinds change produces a different id:
/// the session then closes the old subscription and opens the new one instead
/// of leaving a stale filter live. Same reason the renderer keyed on both.
fn plan_subscriptions(
    subscriptions: &[SaveSubscription],
) -> (Vec<Subscription>, HashMap<String, MatchedScope>) {
    let mut planned = Vec::new();
    let mut scopes = HashMap::new();

    for sub in subscriptions {
        let Some(scope_type) = parse_scope_type(&sub.scope_type) else {
            eprintln!(
                "buzz-desktop: archive sync: unknown scope_type {:?}, skipping",
                sub.scope_type
            );
            continue;
        };
        // A malformed `kinds` column decodes as empty, matching the renderer
        // decoder. The resulting filter matches nothing, which is the correct
        // failure for a row we cannot interpret: archive nothing, drop nothing.
        let kinds: Vec<u64> = serde_json::from_str(&sub.kinds).unwrap_or_default();
        let id = subscription_id(&scope_type, &sub.scope_value, &kinds);
        if scopes.contains_key(&id) {
            continue;
        }
        planned.push(Subscription {
            id: id.clone(),
            filter: build_filter(&scope_type, &sub.scope_value, &kinds),
        });
        scopes.insert(
            id,
            MatchedScope {
                scope_type,
                scope_value: sub.scope_value.clone(),
            },
        );
    }

    (planned, scopes)
}

fn parse_scope_type(raw: &str) -> Option<ScopeType> {
    match raw {
        "channel_h" => Some(ScopeType::ChannelH),
        "owner_p" => Some(ScopeType::OwnerP),
        "referenced_e" => Some(ScopeType::ReferencedE),
        _ => None,
    }
}

/// `limit: 0` — live tail only. Stored events are archived by the explicit
/// backfill paths, so a non-zero limit would re-deliver history on every
/// reconnect.
fn build_filter(scope_type: &ScopeType, scope_value: &str, kinds: &[u64]) -> serde_json::Value {
    let tag = match scope_type {
        ScopeType::ChannelH => "#h",
        ScopeType::OwnerP => "#p",
        ScopeType::ReferencedE => "#e",
    };
    json!({ "kinds": kinds, "limit": 0, tag: [scope_value] })
}

fn subscription_id(scope_type: &ScopeType, scope_value: &str, kinds: &[u64]) -> String {
    let mut sorted = kinds.to_vec();
    sorted.sort_unstable();
    let kinds = sorted
        .iter()
        .map(|k| k.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("archive:{}:{scope_value}:{kinds}", scope_type.as_str())
}

// ── Batching ─────────────────────────────────────────────────────────────────

/// Buffered candidates plus the deadline of the oldest one.
#[derive(Default)]
struct PendingBatch {
    candidates: Vec<ArchiveCandidate>,
    /// Set when the buffer goes from empty to non-empty, cleared on take. The
    /// deadline belongs to the oldest buffered event, so a steady trickle of
    /// arrivals cannot postpone its flush indefinitely.
    deadline: Option<Instant>,
}

impl PendingBatch {
    fn push(&mut self, candidate: ArchiveCandidate) {
        if self.candidates.is_empty() {
            self.deadline = Some(Instant::now() + FLUSH_DEADLINE);
        }
        self.candidates.push(candidate);
    }

    fn is_full(&self) -> bool {
        self.candidates.len() >= FLUSH_BATCH_SIZE
    }

    fn take(&mut self) -> Vec<ArchiveCandidate> {
        self.deadline = None;
        std::mem::take(&mut self.candidates)
    }
}

// ── Sync loop ────────────────────────────────────────────────────────────────

/// Drives one archive sync session until `cancel` fires.
///
/// Reload requests coalesce: `Notify::notify_one` stores at most one permit, so
/// any number of subscription changes arriving during a reload produce exactly
/// one follow-up pass — the same guarantee the renderer's single-flight
/// `reloadPending` loop provided, without the bookkeeping.
async fn run_sync<I: ArchiveSyncIo + ?Sized>(
    io: &I,
    reload: Arc<Notify>,
    mut events: mpsc::Receiver<MatchedEvent>,
    cancel: CancellationToken,
) {
    let mut scopes: HashMap<String, MatchedScope> = HashMap::new();
    let mut pending = PendingBatch::default();

    reconcile(io, &mut scopes).await;

    loop {
        // `Instant::far_future()` is not public; a long sleep stands in for
        // "no deadline" so the select arm can be unconditional.
        let deadline = pending
            .deadline
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = reload.notified() => {
                reconcile(io, &mut scopes).await;
            }
            _ = tokio::time::sleep_until(deadline), if pending.deadline.is_some() => {
                flush(io, pending.take()).await;
            }
            received = events.recv() => {
                let Some(event) = received else { break };
                // A subscription we already closed can still have events in
                // flight; without its scope we cannot assert a match, and the
                // backend re-verifies scope claims anyway, so drop it.
                let Some(scope) = scopes.get(&event.subscription_id) else { continue };
                pending.push(ArchiveCandidate {
                    raw_event_json: event.event.as_json(),
                    matched_scope: MatchedScope {
                        scope_type: scope.scope_type.clone(),
                        scope_value: scope.scope_value.clone(),
                    },
                });
                if pending.is_full() {
                    flush(io, pending.take()).await;
                }
            }
        }
    }

    // Buffered events are already off the relay; dropping them on shutdown
    // would lose them permanently for the ephemeral scope.
    flush(io, pending.take()).await;
}

/// Reloads the saved subscriptions and applies them to the session.
///
/// A failed load leaves the previous set live rather than tearing everything
/// down: a transient SQLite error must not silently stop archiving.
async fn reconcile<I: ArchiveSyncIo + ?Sized>(io: &I, scopes: &mut HashMap<String, MatchedScope>) {
    let subscriptions = match io.list_subscriptions().await {
        Ok(subscriptions) => subscriptions,
        Err(error) => {
            eprintln!("buzz-desktop: archive sync: list_save_subscriptions failed: {error}");
            return;
        }
    };
    let (planned, next_scopes) = plan_subscriptions(&subscriptions);
    io.set_subscriptions(planned).await;
    *scopes = next_scopes;
}

/// Awaited rather than spawned: back-pressure through the session's bounded
/// event channel is what keeps a catch-up storm from queueing unbounded
/// archive work. The renderer's fire-and-forget was a property of living in
/// an event loop it could not block, not a behavior worth porting.
async fn flush<I: ArchiveSyncIo + ?Sized>(io: &I, candidates: Vec<ArchiveCandidate>) {
    if candidates.is_empty() {
        return;
    }
    match io.archive(candidates).await {
        // The backend is authoritative: a duplicate-only batch or one with no
        // kind-44200 events must not invalidate usage queries.
        Ok(result) if result.persisted_agent_metrics > 0 => io.notify_agent_metrics_changed(),
        Ok(_) => {}
        Err(error) => eprintln!("buzz-desktop: archive sync: archive_events failed: {error}"),
    }
}

// ── Production wiring ────────────────────────────────────────────────────────

struct AppIo {
    app: AppHandle,
    session: Arc<RelaySession>,
}

impl ArchiveSyncIo for AppIo {
    fn list_subscriptions(&self) -> BoxFuture<'_, Result<Vec<SaveSubscription>, String>> {
        Box::pin(async move {
            let state: State<'_, AppState> = self.app.state();
            let identity_pk = super::identity_pubkey(&state)?;
            let relay_url = crate::relay::relay_ws_url_with_override(&state);
            super::run_archive_db_task(move |conn| {
                super::store::list_save_subscriptions(conn, &identity_pk, &relay_url)
            })
            .await
        })
    }

    fn set_subscriptions(&self, subscriptions: Vec<Subscription>) -> BoxFuture<'_, ()> {
        Box::pin(async move { self.session.set_subscriptions(subscriptions).await })
    }

    fn archive(
        &self,
        candidates: Vec<ArchiveCandidate>,
    ) -> BoxFuture<'_, Result<ArchiveBatchResult, String>> {
        Box::pin(async move {
            let state: State<'_, AppState> = self.app.state();
            super::archive_candidates(&state, candidates).await
        })
    }

    fn notify_agent_metrics_changed(&self) {
        let _ = self.app.emit(AGENT_METRICS_CHANGED_EVENT, ());
    }
}

/// Managed handle for the running sync task.
#[derive(Default)]
pub struct ArchiveSyncState {
    running: Mutex<Option<RunningSync>>,
}

struct RunningSync {
    /// Identity + relay this task is bound to. A start request for the same
    /// scope is a no-op, so a renderer remount does not churn the socket.
    scope: (String, String),
    cancel: CancellationToken,
    reload: Arc<Notify>,
}

impl ArchiveSyncState {
    /// Wakes the sync task so it reloads saved subscriptions.
    ///
    /// Called by the archive commands that mutate `save_subscriptions`. This
    /// replaces the renderer's `onSubscriptionChange` notifier: the mutations
    /// were already backend commands, so routing the signal through JS only
    /// created a window where a write landed but nothing resubscribed.
    pub(super) async fn notify_subscriptions_changed(&self) {
        if let Some(running) = self.running.lock().await.as_ref() {
            running.reload.notify_one();
        }
    }

    async fn stop(&self) {
        if let Some(running) = self.running.lock().await.take() {
            running.cancel.cancel();
        }
    }
}

/// Start archive sync for the current identity.
///
/// Idempotent for the same identity + relay. Issued by the renderer only after
/// observer reconciliation completes — see the module docs for why that gate
/// cannot be moved into the backend.
#[tauri::command]
pub async fn start_archive_sync(
    app: AppHandle,
    state: State<'_, AppState>,
    sync_state: State<'_, ArchiveSyncState>,
) -> Result<(), String> {
    let keys = state.signing_keys()?;
    let relay_url = crate::relay::relay_ws_url_with_override(&state);
    let scope = (keys.public_key().to_hex(), relay_url.clone());

    let mut running = sync_state.running.lock().await;
    if running
        .as_ref()
        .is_some_and(|current| current.scope == scope)
    {
        return Ok(());
    }
    if let Some(previous) = running.take() {
        previous.cancel.cancel();
    }

    // No NIP-OA auth tag: this is the owner's own session, authenticated as
    // the identity itself, exactly like the renderer's relay client.
    let (session, events) = native_relay_client::start(relay_url, keys, None);
    let cancel = CancellationToken::new();
    let reload = Arc::new(Notify::new());
    *running = Some(RunningSync {
        scope,
        cancel: cancel.clone(),
        reload: Arc::clone(&reload),
    });
    drop(running);

    let io = AppIo {
        app: app.clone(),
        session: Arc::clone(&session),
    };
    tauri::async_runtime::spawn(async move {
        run_sync(&io, reload, events, cancel).await;
        session.shutdown();
    });
    Ok(())
}

/// Stop archive sync. Mirrors the renderer teardown that ran when the gate
/// closed (identity change, community switch, unmount).
#[tauri::command]
pub async fn stop_archive_sync(sync_state: State<'_, ArchiveSyncState>) -> Result<(), String> {
    sync_state.stop().await;
    Ok(())
}

#[cfg(test)]
#[path = "sync_tests.rs"]
mod sync_tests;
