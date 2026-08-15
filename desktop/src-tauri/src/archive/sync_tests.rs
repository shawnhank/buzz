//! Tests for the native archive sync loop.
//!
//! The loop is driven through the real `run_sync` body with a fake
//! [`ArchiveSyncIo`] and a real event channel, so batching, demultiplexing,
//! reload coalescing, and shutdown flush are exercised as the production task
//! runs them — not as a struct poked directly.

use super::*;
use nostr::{EventBuilder, Keys, Kind, Tag};
use std::sync::Mutex as StdMutex;

// ── Test doubles ─────────────────────────────────────────────────────────────

#[derive(Default)]
struct FakeIo {
    /// Successive results for `list_subscriptions`; the last one repeats so a
    /// reload that outruns the script does not panic.
    listings: StdMutex<Vec<Vec<SaveSubscription>>>,
    applied: StdMutex<Vec<Vec<Subscription>>>,
    batches: StdMutex<Vec<Vec<ArchiveCandidate>>>,
    /// What `archive` returns; drives the notify-on-metrics assertion.
    persisted_agent_metrics: StdMutex<u32>,
    archive_fails: StdMutex<bool>,
    metrics_notifications: StdMutex<u32>,
}

impl FakeIo {
    fn with_listings(listings: Vec<Vec<SaveSubscription>>) -> Self {
        Self {
            listings: StdMutex::new(listings),
            ..Default::default()
        }
    }

    fn applied(&self) -> Vec<Vec<Subscription>> {
        self.applied.lock().unwrap().clone()
    }

    /// Flattened candidates in delivery order, as `(scope_value, event_id)`.
    fn archived(&self) -> Vec<Vec<String>> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|batch| {
                batch
                    .iter()
                    .map(|c| c.matched_scope.scope_value.clone())
                    .collect()
            })
            .collect()
    }
}

impl ArchiveSyncIo for FakeIo {
    fn list_subscriptions(&self) -> BoxFuture<'_, Result<Vec<SaveSubscription>, String>> {
        Box::pin(async move {
            let mut listings = self.listings.lock().unwrap();
            if listings.is_empty() {
                return Ok(Vec::new());
            }
            if listings.len() == 1 {
                return Ok(listings[0].clone());
            }
            Ok(listings.remove(0))
        })
    }

    fn set_subscriptions(&self, subscriptions: Vec<Subscription>) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.applied.lock().unwrap().push(subscriptions);
        })
    }

    fn archive(
        &self,
        candidates: Vec<ArchiveCandidate>,
    ) -> BoxFuture<'_, Result<ArchiveBatchResult, String>> {
        Box::pin(async move {
            self.batches.lock().unwrap().push(candidates);
            if *self.archive_fails.lock().unwrap() {
                return Err("archive failed".to_string());
            }
            Ok(ArchiveBatchResult {
                persisted: 0,
                persisted_agent_metrics: *self.persisted_agent_metrics.lock().unwrap(),
                dropped: 0,
            })
        })
    }

    fn notify_agent_metrics_changed(&self) {
        *self.metrics_notifications.lock().unwrap() += 1;
    }
}

/// Yields until `condition` holds, then returns; fails the test if it never
/// does. An unbounded spin turns a broken flush into a HUNG test instead of a
/// failing one — and under a paused clock it also starves tokio's auto-advance,
/// so the deadline that would have masked the bug never even fires.
async fn wait_for(label: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..10_000 {
        if condition() {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("timed out waiting for {label}");
}

fn saved(scope_type: &str, scope_value: &str, kinds: &str) -> SaveSubscription {
    SaveSubscription {
        identity_pubkey: "owner".into(),
        relay_url: "wss://relay.test".into(),
        scope_type: scope_type.into(),
        scope_value: scope_value.into(),
        kinds: kinds.into(),
        created_at: 0,
    }
}

fn matched(subscription_id: &str) -> MatchedEvent {
    let event = EventBuilder::new(Kind::Custom(9), "hello")
        .tags([Tag::parse(vec!["h", "channel-a"]).unwrap()])
        .sign_with_keys(&Keys::generate())
        .unwrap();
    MatchedEvent {
        subscription_id: subscription_id.to_string(),
        event: Box::new(event),
    }
}

/// Runs `run_sync` on a task, handing back the controls the tests drive it
/// with. Every test cancels and joins, so a loop that fails to observe
/// cancellation hangs the test rather than passing silently.
fn spawn_sync(
    io: Arc<FakeIo>,
) -> (
    mpsc::Sender<MatchedEvent>,
    Arc<Notify>,
    CancellationToken,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = mpsc::channel(64);
    let reload = Arc::new(Notify::new());
    let cancel = CancellationToken::new();
    let handle = {
        let io = Arc::clone(&io);
        let reload = Arc::clone(&reload);
        let cancel = cancel.clone();
        tokio::spawn(async move { run_sync(io.as_ref(), reload, rx, cancel).await })
    };
    (tx, reload, cancel, handle)
}

async fn stop(cancel: CancellationToken, handle: tokio::task::JoinHandle<()>) {
    cancel.cancel();
    handle.await.expect("sync task panicked");
}

// ── Filter construction ──────────────────────────────────────────────────────

#[test]
fn filters_match_the_renderer_shape_for_every_scope() {
    // Verbatim parity with `buildFilter` in archiveSyncManager.ts: the tag key
    // per scope and `limit: 0` are the contract with the relay, and a wrong
    // tag key silently archives nothing.
    let (planned, scopes) = plan_subscriptions(&[
        saved("channel_h", "channel-a", "[9,40002]"),
        saved("owner_p", "owner-pk", "[24200]"),
        saved("referenced_e", "event-id", "[1]"),
    ]);

    let filters: Vec<_> = planned.iter().map(|s| s.filter.clone()).collect();
    assert_eq!(
        filters,
        vec![
            json!({ "kinds": [9, 40002], "limit": 0, "#h": ["channel-a"] }),
            json!({ "kinds": [24200], "limit": 0, "#p": ["owner-pk"] }),
            json!({ "kinds": [1], "limit": 0, "#e": ["event-id"] }),
        ]
    );
    assert_eq!(scopes.len(), 3);
    let scope = &scopes[&planned[0].id];
    assert_eq!(scope.scope_type, ScopeType::ChannelH);
    assert_eq!(scope.scope_value, "channel-a");
}

#[test]
fn subscription_id_changes_when_kinds_change() {
    // The id doubles as the relay subscription id, so a kinds change MUST
    // produce a different one — otherwise the session sees the same id with a
    // new filter and the old filter can stay live.
    let (before, _) = plan_subscriptions(&[saved("channel_h", "channel-a", "[9]")]);
    let (after, _) = plan_subscriptions(&[saved("channel_h", "channel-a", "[9,40002]")]);
    assert_ne!(before[0].id, after[0].id);
}

#[test]
fn subscription_id_is_stable_across_kind_ordering() {
    // Same set written in a different order is the same subscription; without
    // the sort it would churn the socket on every reload.
    let (a, _) = plan_subscriptions(&[saved("channel_h", "channel-a", "[40002,9]")]);
    let (b, _) = plan_subscriptions(&[saved("channel_h", "channel-a", "[9,40002]")]);
    assert_eq!(a[0].id, b[0].id);
}

#[test]
fn unknown_scope_type_is_skipped_not_guessed() {
    let (planned, scopes) = plan_subscriptions(&[
        saved("wat", "x", "[9]"),
        saved("channel_h", "channel-a", "[9]"),
    ]);
    assert_eq!(planned.len(), 1);
    assert_eq!(scopes.len(), 1);
    assert_eq!(scopes[&planned[0].id].scope_value, "channel-a");
}

#[test]
fn malformed_kinds_column_yields_a_matchless_filter() {
    // Mirrors the renderer decoder: a row we cannot interpret archives
    // nothing rather than subscribing to everything.
    let (planned, _) = plan_subscriptions(&[saved("channel_h", "channel-a", "not json")]);
    assert_eq!(
        planned[0].filter,
        json!({ "kinds": [], "limit": 0, "#h": ["channel-a"] })
    );
}

#[test]
fn duplicate_rows_produce_one_subscription() {
    let (planned, _) = plan_subscriptions(&[
        saved("channel_h", "channel-a", "[9]"),
        saved("channel_h", "channel-a", "[9]"),
    ]);
    assert_eq!(planned.len(), 1);
}

// ── Loop behavior ────────────────────────────────────────────────────────────

#[tokio::test]
async fn subscribes_to_saved_configs_on_start() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    let (_tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));

    // The first reconcile races the cancel; wait for it to land.
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    assert_eq!(io.applied()[0].len(), 1);
    stop(cancel, handle).await;
}

#[tokio::test]
async fn reload_signal_resubscribes_with_the_new_set() {
    let io = Arc::new(FakeIo::with_listings(vec![
        vec![saved("channel_h", "channel-a", "[9]")],
        vec![
            saved("channel_h", "channel-a", "[9]"),
            saved("owner_p", "owner-pk", "[24200]"),
        ],
    ]));
    let (_tx, reload, cancel, handle) = spawn_sync(Arc::clone(&io));

    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    reload.notify_one();
    wait_for("resubscribe", || io.applied().len() >= 2).await;

    assert_eq!(io.applied()[1].len(), 2);
    stop(cancel, handle).await;
}

#[tokio::test(start_paused = true)]
async fn flushes_when_the_batch_size_is_reached() {
    // Paused clock: the deadline can never fire, so a flush here is the size
    // bound and nothing else. Without this the test passes on an off-by-one
    // `is_full` — the deadline flushes the same 25 events 2s later and the
    // assertion cannot tell the two apart.
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    for _ in 0..(FLUSH_BATCH_SIZE - 1) {
        tx.send(matched(&id)).await.unwrap();
    }
    // One short of the bound: nothing may flush.
    wait_for("loop to drain the channel", || {
        tx.capacity() == tx.max_capacity()
    })
    .await;
    assert!(
        io.archived().is_empty(),
        "flushed before reaching the batch size"
    );

    tx.send(matched(&id)).await.unwrap();
    wait_for("flush", || !io.archived().is_empty()).await;

    // Exactly one batch of exactly FLUSH_BATCH_SIZE — a flush at the wrong
    // boundary shows up here as a split or an oversized batch.
    let archived = io.archived();
    assert_eq!(archived.len(), 1);
    assert_eq!(archived[0].len(), FLUSH_BATCH_SIZE);
    assert!(archived[0].iter().all(|scope| scope == "channel-a"));
    stop(cancel, handle).await;
}

#[tokio::test(start_paused = true)]
async fn flushes_a_partial_batch_after_the_deadline() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    tx.send(matched(&id)).await.unwrap();
    // Just under the deadline: still buffered.
    tokio::time::sleep(FLUSH_DEADLINE - Duration::from_millis(1)).await;
    assert!(io.archived().is_empty(), "flushed before the deadline");

    tokio::time::sleep(Duration::from_millis(2)).await;
    wait_for("flush", || !io.archived().is_empty()).await;
    assert_eq!(io.archived()[0].len(), 1);
    stop(cancel, handle).await;
}

#[tokio::test(start_paused = true)]
async fn a_trickle_cannot_postpone_the_deadline_indefinitely() {
    // The deadline belongs to the OLDEST buffered event. An idle timer reset
    // on each arrival would leave a steady trickle unflushed forever.
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    for _ in 0..4 {
        tx.send(matched(&id)).await.unwrap();
        tokio::time::sleep(FLUSH_DEADLINE / 2).await;
    }
    wait_for("flush", || !io.archived().is_empty()).await;
    assert!(!io.archived().is_empty());
    stop(cancel, handle).await;
}

#[tokio::test]
async fn events_for_an_unknown_subscription_are_dropped() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;

    // An event from a subscription we already closed: no scope, no archive.
    tx.send(matched("archive:channel_h:gone:[9]"))
        .await
        .unwrap();
    cancel.cancel();
    handle.await.unwrap();

    assert!(
        io.archived().is_empty(),
        "archived an event with no known scope"
    );
}

#[tokio::test]
async fn buffered_events_flush_on_shutdown() {
    // Ephemeral kind 24200 cannot be re-fetched, so a buffered event dropped
    // at teardown is lost permanently.
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "owner_p", "owner-pk", "[24200]",
    )]]));
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    tx.send(matched(&id)).await.unwrap();
    // Wait until the loop has actually taken the event off the channel;
    // cancelling first would test a race, not the shutdown flush.
    wait_for("loop to drain the channel", || {
        tx.capacity() == tx.max_capacity()
    })
    .await;
    cancel.cancel();
    handle.await.unwrap();

    let archived = io.archived();
    assert_eq!(archived.len(), 1, "shutdown did not flush the buffer");
    assert_eq!(archived[0], vec!["owner-pk".to_string()]);
}

#[tokio::test]
async fn notifies_agent_metrics_only_when_the_backend_persisted_some() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "owner_p", "owner-pk", "[44200]",
    )]]));
    *io.persisted_agent_metrics.lock().unwrap() = 2;
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    for _ in 0..FLUSH_BATCH_SIZE {
        tx.send(matched(&id)).await.unwrap();
    }
    wait_for("flush", || !io.archived().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(*io.metrics_notifications.lock().unwrap(), 1);
    stop(cancel, handle).await;
}

#[tokio::test]
async fn does_not_notify_when_nothing_was_persisted() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "owner_p", "owner-pk", "[44200]",
    )]]));
    // persisted_agent_metrics stays 0: a duplicate-only batch must not
    // invalidate the renderer's usage queries.
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    for _ in 0..FLUSH_BATCH_SIZE {
        tx.send(matched(&id)).await.unwrap();
    }
    wait_for("flush", || !io.archived().is_empty()).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(*io.metrics_notifications.lock().unwrap(), 0);
    stop(cancel, handle).await;
}

#[tokio::test]
async fn a_failed_archive_call_does_not_notify_or_stop_the_loop() {
    let io = Arc::new(FakeIo::with_listings(vec![vec![saved(
        "channel_h",
        "channel-a",
        "[9]",
    )]]));
    *io.archive_fails.lock().unwrap() = true;
    let (tx, _reload, cancel, handle) = spawn_sync(Arc::clone(&io));
    wait_for("initial subscribe", || !io.applied().is_empty()).await;
    let id = io.applied()[0][0].id.clone();

    for _ in 0..(FLUSH_BATCH_SIZE * 2) {
        tx.send(matched(&id)).await.unwrap();
    }
    wait_for("second flush", || io.archived().len() >= 2).await;
    assert_eq!(*io.metrics_notifications.lock().unwrap(), 0);
    stop(cancel, handle).await;
}

#[tokio::test]
async fn a_failed_listing_leaves_the_previous_subscriptions_live() {
    // A transient SQLite error must not silently stop archiving.
    struct FailingList(Arc<FakeIo>, StdMutex<bool>);
    impl ArchiveSyncIo for FailingList {
        fn list_subscriptions(&self) -> BoxFuture<'_, Result<Vec<SaveSubscription>, String>> {
            Box::pin(async move {
                let mut failed = self.1.lock().unwrap();
                if *failed {
                    return Err("db is busy".into());
                }
                *failed = true;
                Ok(vec![saved("channel_h", "channel-a", "[9]")])
            })
        }
        fn set_subscriptions(&self, subscriptions: Vec<Subscription>) -> BoxFuture<'_, ()> {
            self.0.set_subscriptions(subscriptions)
        }
        fn archive(
            &self,
            candidates: Vec<ArchiveCandidate>,
        ) -> BoxFuture<'_, Result<ArchiveBatchResult, String>> {
            self.0.archive(candidates)
        }
        fn notify_agent_metrics_changed(&self) {
            self.0.notify_agent_metrics_changed();
        }
    }

    let inner = Arc::new(FakeIo::default());
    let io = Arc::new(FailingList(Arc::clone(&inner), StdMutex::new(false)));
    let (tx, rx) = mpsc::channel(8);
    let reload = Arc::new(Notify::new());
    let cancel = CancellationToken::new();
    let handle = {
        let io = Arc::clone(&io);
        let reload = Arc::clone(&reload);
        let cancel = cancel.clone();
        tokio::spawn(async move { run_sync(io.as_ref(), reload, rx, cancel).await })
    };

    wait_for("initial subscribe", || !inner.applied().is_empty()).await;
    let id = inner.applied()[0][0].id.clone();
    reload.notify_one();
    tokio::time::sleep(Duration::from_millis(10)).await;

    // The failed reload applied nothing, and the original scope still
    // demultiplexes — so events keep being archived.
    assert_eq!(inner.applied().len(), 1);
    for _ in 0..FLUSH_BATCH_SIZE {
        tx.send(matched(&id)).await.unwrap();
    }
    wait_for("flush", || !inner.archived().is_empty()).await;
    stop(cancel, handle).await;
}
