//! A node on both networks: p2panda (iroh) and Logos Delivery, relaying operations between them.
//!
//! Operations are the same on both sides (p2panda operations, with bodies encoded the way
//! p2panda's streams encode messages), so bridging is moving whole operations:
//!
//! - This node writes through p2panda, so every operation is created once.
//! - Everything p2panda processes (our own operations and those from iroh peers) is relayed into
//!   the Logos store and sent to Logos peers.
//! - Operations from Logos peers are imported into p2panda (`StreamPublisher::import`). Its
//!   pipeline checks and stores them, iroh peers sync them from there, and the application
//!   receives them through the same p2panda stream as everything else.
//!
//! An operation the Logos store already has is not relayed again, so nothing loops. Ephemeral
//! messages (presence, cursors) are signed per network and are not bridged: this node publishes
//! its own on both and listens on both.
//!
//! Logos Delivery charges per message sent (RLN), p2panda does not. A p2panda author (the GTK
//! app) writes one operation per keystroke, so the bridge does not relay them one by one: it
//! holds an author's new operations for `RELAY_WINDOW` and then relays only the newest snapshot
//! (an operation with the prune flag, which replaces everything before it), or, if there is no
//! snapshot among them, all of them in order. Its own presence goes to Logos at the Logos rate.
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use p2panda::node::CreateStreamError;
use p2panda::streams::{StreamEvent, StreamFrom};
use p2panda::{Topic, VerifyingKey};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error};

use super::logos::{LogosBackend, Op};
use super::p2panda_backend::{P2pandaBackend, P2pandaOpPublisher, to_op_event};
use super::{
    BoxFuture, EphemeralPublisher, EphemeralStream, OpPublisher, OpStream, SyncBackend,
    TransportStats,
};
use crate::author_tracker::AuthorTrackerMessage;
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

/// How long the bridge collects an author's operations before relaying them to Logos.
const RELAY_WINDOW: Duration = Duration::from_secs(15);

#[derive(Debug)]
pub(crate) struct BridgeBackend {
    p2panda: P2pandaBackend,
    logos: LogosBackend,
}

impl BridgeBackend {
    pub(crate) fn new(p2panda: P2pandaBackend, logos: LogosBackend) -> Self {
        Self { p2panda, logos }
    }
}

impl SyncBackend for BridgeBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            let (publisher, events) = self
                .p2panda
                .node
                .stream_from::<Vec<u8>>(topic, StreamFrom::Start)
                .await?;

            // Logos -> p2panda.
            let from_logos = self.logos.subscribe_raw(topic).await?;
            let imports = UnboundedReceiverStream::new(from_logos).map(|op| op.to_operation());
            publisher
                .import(imports)
                .await
                .map_err(|error| CreateStreamError(error.to_string()))?;

            // p2panda -> Logos, collected per author (see the module docs).
            let (to_logos, relay_rx) = mpsc::unbounded_channel::<Op>();
            tokio::spawn(relay_loop(self.logos.clone(), topic, relay_rx));

            let events = events.filter_map(move |event| {
                if let StreamEvent::Processed { operation, .. } = &event {
                    let processed = operation.processed();
                    let op = Op::from_parts(
                        processed.header().clone(),
                        processed.body().map(|body| body.to_bytes()),
                    );
                    if to_logos.send(op).is_err() {
                        error!(%topic, "bridge relay to Logos stopped");
                    }
                }
                to_op_event(event)
            });

            let publisher: Box<dyn OpPublisher> = Box::new(P2pandaOpPublisher(publisher));
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>> {
        Box::pin(async move {
            let (p2panda_publisher, p2panda_events) = self.p2panda.ephemeral_stream(topic).await?;
            let (logos_publisher, logos_events) = self.logos.ephemeral(topic).await?;
            let publisher: Box<dyn EphemeralPublisher> = Box::new(BothEphemeral {
                p2panda: p2panda_publisher,
                logos: logos_publisher,
                pings: AtomicU64::new(0),
                logos_ping_every: (self.logos.presence_interval().as_secs()
                    / self.p2panda.presence_interval().as_secs().max(1))
                .max(1),
            });
            Ok((publisher, Box::pin(p2panda_events.merge(logos_events)) as _))
        })
    }

    fn set_connection_mode(
        &self,
        connection_mode: ConnectionMode,
    ) -> BoxFuture<'_, Result<(), NodeError>> {
        Box::pin(async move {
            self.p2panda.set_connection_mode(connection_mode).await?;
            self.logos.set_connection_mode(connection_mode).await
        })
    }

    fn stats(&self) -> Option<TransportStats> {
        self.logos.stats()
    }
}

/// Relay p2panda operations to Logos, collecting each author's for `RELAY_WINDOW`.
async fn relay_loop(logos: LogosBackend, topic: Topic, mut rx: mpsc::UnboundedReceiver<Op>) {
    let mut held: HashMap<VerifyingKey, Vec<Op>> = HashMap::new();
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        let flush = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            op = rx.recv() => {
                let Some(op) = op else { break };
                held.entry(op.author()).or_default().push(op);
                deadline.get_or_insert_with(|| tokio::time::Instant::now() + RELAY_WINDOW);
            }
            _ = flush => {
                deadline = None;
                for (_, ops) in held.drain() {
                    for op in to_relay(ops) {
                        logos.relay(topic, op).await;
                    }
                }
            }
        }
    }
    for (_, ops) in held.drain() {
        for op in to_relay(ops) {
            logos.relay(topic, op).await;
        }
    }
    debug!(%topic, "bridge relay to Logos ended");
}

/// Of one author's operations (in log order): the newest snapshot and what follows it, or all
/// of them if there is no snapshot.
fn to_relay(mut ops: Vec<Op>) -> Vec<Op> {
    ops.sort_by_key(|op| op.seq());
    match ops.iter().rposition(|op| op.prunes()) {
        Some(last_snapshot) => ops.split_off(last_snapshot),
        None => ops,
    }
}

struct BothEphemeral {
    p2panda: Box<dyn EphemeralPublisher>,
    logos: Box<dyn EphemeralPublisher>,
    pings: AtomicU64,
    /// Presence is paced for p2panda; only every n-th ping goes to Logos.
    logos_ping_every: u64,
}

impl EphemeralPublisher for BothEphemeral {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let to_logos = match &message {
                EphemeralMessage::AuthorTracker(AuthorTrackerMessage::Ping) => self
                    .pings
                    .fetch_add(1, Ordering::Relaxed)
                    .is_multiple_of(self.logos_ping_every),
                _ => true,
            };
            let logos = if to_logos {
                self.logos.publish(message.clone()).await
            } else {
                Ok(())
            };
            self.p2panda.publish(message).await?;
            logos
        })
    }
}
