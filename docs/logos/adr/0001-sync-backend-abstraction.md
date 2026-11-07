# ADR 0001: A sync-backend abstraction in reflection-node

Status: accepted (2026-11-07)

## Context

Reflection has three crates: `reflection-app` (GTK), `reflection-doc`
(GObject document layer over a Loro CRDT) and `reflection-node` (networking
and persistence on p2panda).

`reflection-doc` only uses a narrow surface of `reflection-node`:

- out: `TopicStream::publish_delta`, `publish_snapshot`, `publish_ephemeral`,
  `unsubscribe`, `set_name`;
- in: the `TopicSubscription` trait (`bytes_received`,
  `ephemeral_bytes_received`, `author_joined`, `author_left`, `error`);
- plus `Node::new`, `Node::stream`, `Node::topics`, `Node::delete_topic`,
  `set_connection_mode`, `shutdown`.

That surface is already transport-neutral. But inside `reflection-node`,
`NodeInner` holds a concrete `p2panda::Node`. `topic_stream.rs` and
`author_tracker.rs` call `p2panda::Node::stream_from` and `ephemeral_stream`
directly and match on p2panda's `StreamEvent`.

p2panda itself has no public transport seam. `p2panda::Node` always runs
iroh, and its stream pipeline (`processed_stream`) is `pub(crate)` and takes a
concrete iroh `SyncHandle`. So another transport cannot be added at the
p2panda layer without a much larger upstream change to p2panda.

## Decision

Add a `SyncBackend` trait inside `reflection-node`. It cuts at exactly the two
p2panda calls Reflection makes today:

```rust
pub trait SyncBackend: Send + Sync + 'static {
    /// Eventually consistent, persisted stream of operations on a topic.
    /// Replays from the start (as Reflection does today), then goes live.
    async fn stream(&self, topic: Topic) -> Result<(Box<dyn OpPublisher>, OpEvents), CreateStreamError>;

    /// Unpersisted, signed, best-effort stream to peers online now.
    async fn ephemeral_stream(&self, topic: Topic) -> Result<(Box<dyn EphemeralPublisher>, EphemeralEvents), CreateStreamError>;

    async fn set_connection_mode(&self, mode: ConnectionMode) -> Result<(), NodeError>;
    async fn shutdown(&self);
}
```

- `OpPublisher` has `publish(bytes)` and `prune(bytes)`, which map to the
  p2panda `StreamPublisher::publish` and `prune`.
- `OpEvents` is a stream of backend-neutral events: `Operation { author,
  bytes }`, `SyncStarted`/`SyncEnded` (for logging), and `Error(..)`.
- `EphemeralPublisher` has `publish(bytes)`. `EphemeralEvents` yields
  `(author, timestamp, bytes)`.

The author tracker (online/offline presence), the topic store (topics and
authors in SQLite) and the `TopicStream` wrapper stay *above* the trait and
are shared by every backend. The p2panda backend is a thin wrapper that keeps
today's behaviour.

The public API of `reflection-node` does not change. `Node::new(signing_key,
network_id, db_location)` keeps its signature. A new `Node::with_backend`
lets an embedder pass a backend.

## Consequences

- An upstream PR for this alone is a pure refactor: same behaviour, same
  tests, no new dependencies.
- New backends (Logos, and later Bluetooth, which `ConnectionMode::Bluetooth`
  already hints at) only implement four methods.
- `TopicSubscriptionError` currently wraps p2panda error types. It gains a
  `Backend(String)` variant for errors from other backends. The existing
  variants stay as they are.
- Rejected alternative: abstract at `reflection-doc` level (the
  `TopicStream`/`TopicSubscription` pair). It would force every backend to
  reimplement presence and the topic store.
- Rejected alternative: add Logos inside p2panda-net. That is the most
  general place but needs p2panda's sync to work over broadcast, which it does
  not (log sync is two-party sessions over QUIC). Worth revisiting with the
  p2panda team once this works.
