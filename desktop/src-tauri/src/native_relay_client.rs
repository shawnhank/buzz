//! Shared native relay session.
//!
//! Owns the authenticated relay socket for backend features that need live
//! subscriptions (archive sync today; persona catalog and catch-up next). One
//! session per (relay, pubkey) scope, multiplexing every subscription over a
//! single socket — a second socket per feature would multiply relay connection
//! slots and duplicate the NIP-42 handshake for no benefit.
//!
//! Built on `buzz-ws-client`, which owns the wire format and the NIP-42
//! handshake. That crate is request/response shaped (one caller, `next_event`
//! off a buffer); the session lifecycle lives here instead of being pushed down
//! into it, because `buzz-cli` and `buzz-test-client` consume that crate and do
//! not want subscription bookkeeping.

use std::{collections::HashMap, sync::Arc, time::Duration};

use buzz_ws_client_pkg::{NostrWsConnection, RelayMessage};
use nostr::{Event, Keys};
use tokio::sync::{mpsc, Mutex};
use tokio_util::sync::CancellationToken;

/// Backoff floor for reconnect attempts.
const RECONNECT_BASE_DELAY: Duration = Duration::from_millis(500);
/// Backoff ceiling. Matches the renderer session's ceiling so a relay outage
/// produces one retry cadence across the app rather than two competing ones.
const RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
/// How long a read may block before the loop re-checks cancellation. Not a
/// connection timeout: an idle relay is normal, so a lapsed read just loops.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// A live subscription request: a filter plus where its events go.
#[derive(Clone)]
pub(crate) struct Subscription {
    /// Caller-stable key. Reused verbatim as the relay subscription id so a
    /// resubscribe after reconnect replaces rather than duplicates.
    pub(crate) id: String,
    pub(crate) filter: serde_json::Value,
}

/// An event delivered to the session owner, tagged with the subscription that
/// matched it. Callers demultiplex on `subscription_id`.
pub(crate) struct MatchedEvent {
    pub(crate) subscription_id: String,
    pub(crate) event: Box<Event>,
}

/// Handle to a running session. Dropping it does not stop the session; call
/// [`RelaySession::shutdown`] so the socket closes deterministically.
pub(crate) struct RelaySession {
    desired: Arc<Mutex<Vec<Subscription>>>,
    wake: mpsc::Sender<()>,
    cancel: CancellationToken,
}

impl RelaySession {
    /// Replaces the desired subscription set and wakes the loop to reconcile.
    ///
    /// Reconciliation is declarative rather than incremental: callers state
    /// what they want and the loop diffs. An incremental add/remove API would
    /// have to be replayed in order across a reconnect, which is exactly the
    /// bug class this avoids.
    ///
    /// It is also why no revision/generation guard is needed. Every reconcile
    /// re-reads the current desired set, so a change that lands mid-pass is
    /// picked up by the wake it queued rather than having to invalidate work
    /// already in flight.
    pub(crate) async fn set_subscriptions(&self, subscriptions: Vec<Subscription>) {
        *self.desired.lock().await = subscriptions;
        // A full channel already means "reconcile pending", so a failed send
        // is success: the loop has not yet consumed the previous wake.
        let _ = self.wake.try_send(());
    }

    pub(crate) fn shutdown(&self) {
        self.cancel.cancel();
    }
}

/// Starts a session against `relay_url` authenticated as `keys`.
///
/// Returns the handle plus the receiver for matched events. The session
/// reconnects on drop with exponential backoff and resubscribes the current
/// desired set — never a snapshot captured at connect time, so a subscription
/// change during an outage is honored by the reconnect that follows.
pub(crate) fn start(
    relay_url: String,
    keys: Keys,
    auth_tag: Option<nostr::Tag>,
) -> (Arc<RelaySession>, mpsc::Receiver<MatchedEvent>) {
    let (event_tx, event_rx) = mpsc::channel(256);
    let (wake, wake_rx) = mpsc::channel(1);
    let session = Arc::new(RelaySession {
        desired: Arc::new(Mutex::new(Vec::new())),
        wake,
        cancel: CancellationToken::new(),
    });

    tauri::async_runtime::spawn(run_session(
        relay_url,
        keys,
        auth_tag,
        Arc::clone(&session),
        wake_rx,
        event_tx,
    ));

    (session, event_rx)
}

async fn run_session(
    relay_url: String,
    keys: Keys,
    auth_tag: Option<nostr::Tag>,
    session: Arc<RelaySession>,
    mut wake_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<MatchedEvent>,
) {
    let mut delay = RECONNECT_BASE_DELAY;
    loop {
        if session.cancel.is_cancelled() {
            return;
        }

        match NostrWsConnection::connect_authenticated(&relay_url, &keys, auth_tag.as_ref()).await {
            Ok(conn) => {
                // A connection that authenticated is healthy regardless of how
                // long it then lived, so backoff resets here rather than on
                // clean exit — a socket that drops after one event must not
                // inherit the previous failure's delay.
                delay = RECONNECT_BASE_DELAY;
                run_connection(conn, &session, &mut wake_rx, &event_tx).await;
            }
            Err(error) => {
                eprintln!("buzz-desktop: native_relay_client: connect failed: {error}");
            }
        }

        if session.cancel.is_cancelled() {
            return;
        }
        tokio::select! {
            _ = session.cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }
        delay = (delay * 2).min(RECONNECT_MAX_DELAY);
    }
}

/// Drives one connected socket until it drops or the session is cancelled.
async fn run_connection(
    mut conn: NostrWsConnection,
    session: &RelaySession,
    wake_rx: &mut mpsc::Receiver<()>,
    event_tx: &mpsc::Sender<MatchedEvent>,
) {
    // Subscription ids currently open ON THIS SOCKET. Deliberately local: a new
    // socket has none, so reconnect resubscribes the full desired set without
    // any explicit "resubscribe" path that could drift from the normal one.
    let mut open: HashMap<String, serde_json::Value> = HashMap::new();

    if !reconcile(&mut conn, session, &mut open).await {
        return;
    }

    loop {
        tokio::select! {
            _ = session.cancel.cancelled() => {
                let _ = conn.disconnect().await;
                return;
            }
            Some(()) = wake_rx.recv() => {
                if !reconcile(&mut conn, session, &mut open).await {
                    return;
                }
            }
            message = conn.next_event(READ_TIMEOUT) => {
                match message {
                    Ok(RelayMessage::Event { subscription_id, event }) => {
                        // Only forward events for a subscription we still want.
                        // A CLOSE races in flight with events already queued at
                        // the relay, so this is the last line of defense
                        // against delivering out-of-scope events after a change.
                        if !open.contains_key(&subscription_id) {
                            continue;
                        }
                        if event_tx
                            .send(MatchedEvent { subscription_id, event })
                            .await
                            .is_err()
                        {
                            // Receiver gone: nobody is consuming this session.
                            let _ = conn.disconnect().await;
                            return;
                        }
                    }
                    Ok(RelayMessage::Closed { subscription_id, message }) => {
                        // The relay dropped it; forget it so the next reconcile
                        // reopens rather than assuming it is still live.
                        open.remove(&subscription_id);
                        eprintln!(
                            "buzz-desktop: native_relay_client: relay closed {subscription_id}: {message}"
                        );
                    }
                    Ok(_) => {}
                    Err(error) => {
                        if !is_read_timeout(&error) {
                            eprintln!("buzz-desktop: native_relay_client: read failed: {error}");
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Brings the socket's open subscriptions in line with the desired set.
///
/// Returns false when the socket failed and the caller should reconnect.
async fn reconcile(
    conn: &mut NostrWsConnection,
    session: &RelaySession,
    open: &mut HashMap<String, serde_json::Value>,
) -> bool {
    let desired = session.desired.lock().await.clone();

    for id in open.keys().cloned().collect::<Vec<_>>() {
        if desired.iter().any(|s| s.id == id) {
            continue;
        }
        if conn
            .send_raw(&serde_json::json!(["CLOSE", id]))
            .await
            .is_err()
        {
            return false;
        }
        open.remove(&id);
    }

    for sub in desired {
        // A filter change under the same id must reopen, not be skipped: the
        // relay replaces a subscription by id, so re-sending REQ is the update.
        if open.get(&sub.id) == Some(&sub.filter) {
            continue;
        }
        if conn
            .send_raw(&serde_json::json!(["REQ", sub.id, sub.filter]))
            .await
            .is_err()
        {
            return false;
        }
        open.insert(sub.id, sub.filter);
    }

    true
}

/// A lapsed read is an idle relay, not a failure. Distinguished by variant
/// rather than by message text so a reworded error cannot turn every idle
/// period into a reconnect storm.
fn is_read_timeout(error: &buzz_ws_client_pkg::WsClientError) -> bool {
    matches!(error, buzz_ws_client_pkg::WsClientError::Timeout)
}

#[cfg(test)]
mod relay_backed_tests {
    use super::*;
    use nostr::{EventBuilder, Tag};

    /// Relay-backed proof that the session's wire shape is one a real relay
    /// accepts and answers.
    ///
    /// Every other test in this commit drives `run_sync` through a fake
    /// [`crate::archive::sync::ArchiveSyncIo`], which is the right default:
    /// batching and demultiplexing are the logic worth pinning, and they must
    /// not need a socket. But a fake cannot fail the one way this layer
    /// actually can — by sending a REQ the relay rejects, or by filtering on a
    /// tag key that matches nothing. The JS manager's filters were validated by
    /// years of production traffic; this port's have been validated by my
    /// reading of that code, which is exactly the claim a real relay can check
    /// and I cannot.
    ///
    /// `#[ignore]`d because it needs a relay on `BUZZ_TEST_RELAY_URL`. Run:
    ///
    /// ```text
    /// ./scripts/start-isolated-test-relay.sh          # ws://localhost:3030
    /// BUZZ_TEST_RELAY_URL=ws://localhost:3030 \
    ///   cargo test -p buzz-desktop -- --ignored archive_sync_session
    /// ```
    #[tokio::test]
    #[ignore = "requires a local relay (set BUZZ_TEST_RELAY_URL)"]
    async fn archive_sync_session_receives_live_events_from_a_real_relay() {
        let Ok(relay_url) = std::env::var("BUZZ_TEST_RELAY_URL") else {
            panic!("set BUZZ_TEST_RELAY_URL to a running relay");
        };

        let owner = Keys::generate();
        let author = Keys::generate();
        let owner_pk = owner.public_key();

        // Kind 1 rather than the archive's own kind 24200. Publishing a real
        // observer frame requires a registered agent-owner binding in the
        // relay's database — a relay ACL concern that says nothing about this
        // layer. What this test can prove, and what no fake can, is the wire
        // shape: that the `#p` tag key and the `limit: 0` live tail produce a
        // REQ a real relay accepts and answers. Scope demultiplexing on the
        // archive side is covered in `archive/sync_tests.rs`.
        let (session, mut events) = start(relay_url.clone(), owner.clone(), None);
        session
            .set_subscriptions(vec![Subscription {
                id: "archive:owner_p:test".to_string(),
                filter: serde_json::json!({
                    "kinds": [1],
                    "limit": 0,
                    "#p": [owner_pk.to_hex()],
                }),
            }])
            .await;

        // The subscription must be live at the relay before the event is
        // published. A `limit: 0` filter is a live tail: it replays nothing,
        // so anything published into a not-yet-open subscription is missed.
        // That is the same ordering hazard the renderer start gate exists to
        // prevent for the ephemeral archive kind.
        tokio::time::sleep(Duration::from_secs(1)).await;

        let mut publisher = NostrWsConnection::connect_authenticated(&relay_url, &author, None)
            .await
            .expect("publisher connect");
        let frame = EventBuilder::text_note("archive-sync-probe")
            .tag(Tag::public_key(owner_pk))
            .sign_with_keys(&author)
            .expect("sign event");
        let frame_id = frame.id.to_hex();
        let ok = publisher.send_event(frame).await.expect("publish frame");
        assert!(
            ok.accepted,
            "relay rejected the observer frame, so a delivery timeout below would \
             blame the subscription for a publish failure: {}",
            ok.message
        );

        let received = tokio::time::timeout(Duration::from_secs(10), events.recv())
            .await
            .expect("timed out waiting for the relay to deliver the frame")
            .expect("session channel closed");

        assert_eq!(
            received.subscription_id, "archive:owner_p:test",
            "delivered event must carry the subscription id the loop demultiplexes on"
        );
        assert_eq!(
            received.event.id.to_hex(),
            frame_id,
            "must deliver the published frame"
        );

        session.shutdown();
    }
}
