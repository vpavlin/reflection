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

use p2panda::node::CreateStreamError;
use p2panda::{Hash, Topic, VerifyingKey};
use tokio_stream::Stream;

use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;
use crate::traits::TopicSubscriptionError;

#[cfg(feature = "logos")]
pub(crate) mod logos;
pub(crate) mod p2panda_backend;

pub(crate) type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
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
}
