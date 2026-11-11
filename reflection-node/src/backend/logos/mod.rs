//! Sync backend over Logos Delivery (see docs/logos).
//!
//! The protocol lives in the `logos-p2panda` crate (MIT OR Apache-2.0), so that other p2panda
//! applications can use it too. This is Reflection's adapter: message bodies are encoded the way
//! p2panda's streams encode a `Vec<u8>` message (CBOR), so operations are the same as on the
//! p2panda backend and can be bridged, and ephemeral bodies are Reflection's `EphemeralMessage`.
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "logos-ffi")]
pub use logos_p2panda::LibLogosDeliveryBus;
#[cfg(feature = "logos-ffi")]
pub(crate) use logos_p2panda::ffi_bus;
pub use logos_p2panda::{BusError, BusMessage, DeliveryBus, MemoryBus};
pub(crate) use logos_p2panda::{LogosSync, Op};

use logos_p2panda::{Config, EphemeralKind};
use p2panda::node::CreateStreamError;
use p2panda::{NetworkId, Topic};
use p2panda_core::SigningKey;
use sqlx::SqlitePool;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use super::{
    BoxFuture, EphemeralEvent, EphemeralPublisher, EphemeralStream, MessageCounts, OpEvent,
    OpPublisher, OpStream, PeerSeen, SyncBackend, TopicStats, TransportStats,
};
use crate::author_tracker::AuthorTrackerMessage;
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

/// Presence pings on an active topic (the p2panda backend pings every 30 s).
const PRESENCE_INTERVAL: Duration = Duration::from_secs(120);

/// The Logos Delivery backend.
#[derive(Clone, Debug)]
pub(crate) struct LogosBackend {
    sync: LogosSync,
}

fn classify(body: &[u8]) -> EphemeralKind {
    match p2panda_core::cbor::decode_cbor::<EphemeralMessage, _>(body) {
        Ok(EphemeralMessage::AuthorTracker(_)) => EphemeralKind::Presence,
        _ => EphemeralKind::Other,
    }
}

impl LogosBackend {
    pub(crate) async fn new(
        bus: Arc<dyn DeliveryBus>,
        signing_key: SigningKey,
        network_id: NetworkId,
        pool: SqlitePool,
    ) -> Result<Self, NodeError> {
        #[allow(unused_mut)]
        let mut config = Config {
            classify_ephemeral: Arc::new(classify),
            ..Config::default()
        };
        // Tests can't wait minutes for a catch-up retry.
        #[cfg(test)]
        {
            config.catch_up_retry = Some(Duration::from_secs(1));
            config.catch_up_retry_max = Duration::from_secs(2);
            config.repair_answer_window = Duration::from_secs(2);
        }
        let sync = LogosSync::new(bus, signing_key, network_id, pool, config)
            .await
            .map_err(|error| NodeError::Backend(error.to_string()))?;
        Ok(Self { sync })
    }

    /// The protocol endpoint, for the bridge.
    pub(crate) fn sync(&self) -> &LogosSync {
        &self.sync
    }
}

fn stream_error(error: logos_p2panda::Error) -> CreateStreamError {
    CreateStreamError(error.to_string())
}

pub(crate) fn op_event(op: &Op) -> Option<OpEvent> {
    let body = match op.body.as_deref() {
        None => Vec::new(),
        Some(bytes) => match p2panda_core::cbor::decode_cbor::<Vec<u8>, _>(bytes) {
            Ok(body) => body,
            Err(error) => {
                warn!(hash = %op.hash, "dropping operation with undecodable body: {error}");
                return None;
            }
        },
    };
    Some(OpEvent::Operation {
        id: op.hash,
        author: op.author(),
        body,
    })
}

impl SyncBackend for LogosBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            // Replays everything stored, then goes live. Catch-up starts when the topic is
            // activated (`set_active`).
            let rx = self.sync.subscribe(topic).await.map_err(stream_error)?;
            let events = UnboundedReceiverStream::new(rx).filter_map(|op| op_event(&op));
            let publisher: Box<dyn OpPublisher> = Box::new(LogosOpPublisher {
                sync: self.sync.clone(),
                topic,
            });
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>> {
        Box::pin(async move {
            let rx = self
                .sync
                .subscribe_ephemeral(topic)
                .await
                .map_err(stream_error)?;
            let events = UnboundedReceiverStream::new(rx).filter_map(|message| {
                match p2panda_core::cbor::decode_cbor::<EphemeralMessage, _>(&message.body[..]) {
                    Ok(body) => Some(EphemeralEvent {
                        author: message.author,
                        timestamp: message.timestamp,
                        message: body,
                    }),
                    Err(_) => {
                        warn!("dropping undecodable ephemeral message");
                        None
                    }
                }
            });
            let publisher: Box<dyn EphemeralPublisher> = Box::new(LogosEphemeralPublisher {
                sync: self.sync.clone(),
                topic,
            });
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn set_connection_mode(
        &self,
        _connection_mode: ConnectionMode,
    ) -> BoxFuture<'_, Result<(), NodeError>> {
        // The Delivery node is always on while the backend exists. Going offline would mean
        // unsubscribing; not needed yet.
        Box::pin(async { Ok(()) })
    }

    fn set_active(&self, topic: Topic, active: bool) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            if let Err(error) = self.sync.set_active(topic, active).await {
                warn!(%topic, "can't change activity: {error}");
            }
        })
    }

    fn presence_interval(&self) -> Duration {
        PRESENCE_INTERVAL
    }

    fn stats(&self) -> Option<TransportStats> {
        Some(transport_stats(self.sync.stats()))
    }

    fn set_message_budget(&self, budget: Option<u64>) {
        self.sync.set_message_budget(budget);
    }
}

fn counts(c: logos_p2panda::MessageCounts) -> MessageCounts {
    MessageCounts {
        ops: c.ops,
        snapshots: c.snapshots,
        heads: c.heads,
        repairs: c.repairs,
        relayed: c.relayed,
        presence: c.presence,
        cursors: c.cursors,
        bytes: c.bytes,
        blocked: c.blocked,
        repairs_capped: c.repairs_capped,
    }
}

pub(crate) fn transport_stats(s: logos_p2panda::Stats) -> TransportStats {
    TransportStats {
        transport: "logos-delivery".into(),
        epoch_seconds: s.epoch_seconds,
        epoch_elapsed: s.epoch_elapsed,
        uptime: s.uptime,
        sent: counts(s.sent),
        sent_this_epoch: counts(s.sent_this_epoch),
        sent_last_epoch: counts(s.sent_last_epoch),
        received: counts(s.received),
        topics: s
            .topics
            .into_iter()
            .map(|t| TopicStats {
                topic: t.topic,
                active: t.active,
                sent: counts(t.sent),
                sent_this_epoch: counts(t.sent_this_epoch),
                received: counts(t.received),
                peers: t
                    .peers
                    .into_iter()
                    .map(|p| PeerSeen {
                        key: p.key,
                        seconds_ago: p.seconds_ago,
                    })
                    .collect(),
                waiting: t.waiting,
                behind: t.behind,
                peers_missing_ours: t.peers_missing_ours,
            })
            .collect(),
        budget: s.budget,
    }
}

struct LogosOpPublisher {
    sync: LogosSync,
    topic: Topic,
}

impl LogosOpPublisher {
    async fn write(&self, body: Vec<u8>, prune: bool) -> Result<(), PublishError> {
        // Bodies are encoded the way p2panda's streams encode their messages, so operations are
        // the same on both networks and can be bridged.
        let body = p2panda_core::cbor::encode_cbor(&body)
            .map_err(|error| PublishError::Backend(error.to_string()))?;
        self.sync
            .publish(self.topic, body, prune)
            .await
            .map(|_| ())
            .map_err(|error| PublishError::Backend(error.to_string()))
    }
}

impl OpPublisher for LogosOpPublisher {
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, false))
    }

    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, true))
    }
}

struct LogosEphemeralPublisher {
    sync: LogosSync,
    topic: Topic,
}

impl EphemeralPublisher for LogosEphemeralPublisher {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let kind = match message {
                EphemeralMessage::AuthorTracker(_) => EphemeralKind::Presence,
                EphemeralMessage::Application(_) => EphemeralKind::Other,
            };
            let body = p2panda_core::cbor::encode_cbor(&message)
                .map_err(|error| PublishError::Backend(error.to_string()))?;
            self.sync
                .publish_ephemeral(self.topic, body, kind)
                .await
                .map_err(|error| PublishError::Backend(error.to_string()))
        })
    }
}

/// Presence pings of the bridge that go to Logos: every n-th of those sent to p2panda.
pub(crate) fn is_presence_ping(message: &EphemeralMessage) -> bool {
    matches!(
        message,
        EphemeralMessage::AuthorTracker(AuthorTrackerMessage::Ping)
    )
}
