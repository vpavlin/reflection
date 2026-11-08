//! Sync backends.
//!
//! A backend moves Reflection's data between peers. It provides two kinds of streams per topic:
//!
//! - an eventually consistent stream of operations, persisted and replayed from the start when
//!   subscribing, and
//! - an ephemeral stream of signed messages delivered to peers who are online right now.
//!
//! Everything above this trait (author presence, the topic store, the `TopicStream` handle given
//! to the application) is shared by all backends.
//!
//! The default backend is p2panda. Other backends are compiled in with cargo features.
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use p2panda::node::CreateStreamError;
use p2panda::{Hash, Topic, VerifyingKey};
use serde::Serialize;
use tokio_stream::Stream;

use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;
use crate::traits::TopicSubscriptionError;

#[cfg(feature = "logos")]
pub(crate) mod bridge;
#[cfg(feature = "logos")]
pub(crate) mod logos;
pub(crate) mod p2panda_backend;

/// A boxed, sendable future, used by the backend traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub(crate) type BoxStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

/// Events from the eventually consistent operation stream of a topic.
#[derive(Debug)]
pub(crate) enum OpEvent {
    /// An operation was processed. `body` is the application payload.
    Operation {
        id: Hash,
        author: VerifyingKey,
        body: Vec<u8>,
    },

    /// A sync session with a remote started (informational).
    SyncStarted {
        remote: VerifyingKey,
        session_id: u64,
        incoming_operations: u32,
        outgoing_operations: u32,
        incoming_bytes: u32,
        outgoing_bytes: u32,
    },

    /// A sync session with a remote ended (informational).
    SyncEnded {
        remote: VerifyingKey,
        session_id: u64,
        error: Option<String>,
    },

    /// Something went wrong while receiving or processing operations.
    Error(TopicSubscriptionError),
}

/// A message from the ephemeral stream of a topic.
#[derive(Debug)]
pub(crate) struct EphemeralEvent {
    pub author: VerifyingKey,
    pub timestamp: u64,
    pub message: EphemeralMessage,
}

/// Publishes operations into the eventually consistent stream of a topic.
pub(crate) trait OpPublisher: Send + Sync {
    /// Append an operation with this payload to our log.
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>>;

    /// Append an operation with this payload and the prune flag. Earlier operations of our log
    /// can be removed by every peer.
    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>>;
}

/// Publishes messages into the ephemeral stream of a topic.
pub(crate) trait EphemeralPublisher: Send + Sync {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>>;
}

pub(crate) type OpStream = (Box<dyn OpPublisher>, BoxStream<OpEvent>);
pub(crate) type EphemeralStream = (Box<dyn EphemeralPublisher>, BoxStream<EphemeralEvent>);

/// A way of syncing topics with other peers.
pub(crate) trait SyncBackend: Send + Sync + std::fmt::Debug + 'static {
    /// Subscribe to the eventually consistent stream of operations on a topic, replaying all
    /// operations we know about first.
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>>;

    /// Subscribe to the ephemeral stream of a topic.
    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>>;

    fn set_connection_mode(
        &self,
        connection_mode: ConnectionMode,
    ) -> BoxFuture<'_, Result<(), NodeError>>;

    /// Whether the user has this topic open. A backend that pays per message sent (Logos
    /// Delivery under RLN) publishes nothing of its own for an inactive topic: it still
    /// receives, and still answers peers who ask for operations it has. Topics start inactive
    /// in the backend; `TopicStream` activates them.
    fn set_active(&self, _topic: Topic, _active: bool) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    /// How often presence pings are sent on an active topic. An author is shown offline after
    /// two intervals without one.
    fn presence_interval(&self) -> Duration {
        Duration::from_secs(30)
    }

    /// Message counters, for backends that pay per message (see `TransportStats`).
    fn stats(&self) -> Option<TransportStats> {
        None
    }

    /// Pretend the network allows at most this many messages per epoch (RLN), for trying out
    /// rate tiers: once they are used up, further messages are not sent but counted as
    /// `blocked`, as RLN would drop them. `None`: no limit. Backends that don't pay per message
    /// ignore it.
    fn set_message_budget(&self, _budget: Option<u64>) {}
}

/// Messages a node sent or received, by kind.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MessageCounts {
    /// Our own operations (Loro deltas).
    pub ops: u64,
    /// Our own operations with the prune flag (Loro snapshots).
    pub snapshots: u64,
    /// Catch-up: the heads of our logs.
    pub heads: u64,
    /// Operations re-sent to a peer that lacked them.
    pub repairs: u64,
    /// Operations from the other network (bridge).
    pub relayed: u64,
    /// Presence: hello, ping, bye.
    pub presence: u64,
    /// Other ephemeral messages (cursors).
    pub cursors: u64,
    /// Bytes on the wire.
    pub bytes: u64,
    /// Messages not sent because the (pretend) budget of the epoch was used up. Not part of
    /// `messages()`.
    pub blocked: u64,
}

impl MessageCounts {
    pub fn messages(&self) -> u64 {
        self.ops
            + self.snapshots
            + self.heads
            + self.repairs
            + self.relayed
            + self.presence
            + self.cursors
    }
}

/// A peer seen on a topic.
#[derive(Clone, Debug, Serialize)]
pub struct PeerSeen {
    pub key: String,
    pub seconds_ago: u64,
}

/// Counters of one topic.
#[derive(Clone, Debug, Serialize)]
pub struct TopicStats {
    /// The topic id, hex.
    pub topic: String,
    pub active: bool,
    pub sent: MessageCounts,
    pub sent_this_epoch: MessageCounts,
    pub received: MessageCounts,
    /// Other devices heard from on this topic, most recent first.
    pub peers: Vec<PeerSeen>,
}

/// What a node sent and received, counted the way RLN meters it: messages per epoch of
/// `epoch_seconds`, epochs aligned to Unix time.
#[derive(Clone, Debug, Serialize)]
pub struct TransportStats {
    pub transport: String,
    pub epoch_seconds: u64,
    /// Seconds into the current epoch.
    pub epoch_elapsed: u64,
    /// Seconds since the counters started.
    pub uptime: u64,
    pub sent: MessageCounts,
    pub sent_this_epoch: MessageCounts,
    pub sent_last_epoch: MessageCounts,
    pub received: MessageCounts,
    pub topics: Vec<TopicStats>,
    /// The pretend budget per epoch, if any (`SyncBackend::set_message_budget`).
    pub budget: Option<u64>,
}
