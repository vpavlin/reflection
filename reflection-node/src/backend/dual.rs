//! A node on both networks, p2panda and Logos Delivery, where the p2panda node is run by the
//! host (`P2pandaBus`, e.g. Basecamp's `p2panda_core`) and Logos Delivery is a `DeliveryBus`
//! (e.g. Basecamp's `loam_core`). Like the bridge (bridge.rs), it moves whole signed operations
//! between the networks, so they are the same operation, with the same hash and signature, on
//! both, and a peer on only one network still meets a peer on only the other through this node.
//!
//! - This node writes through Logos (in-process, so a write never waits for the host): the
//!   operation is stored and sent to Logos peers, then imported into the host's p2panda node,
//!   which syncs it to p2panda peers.
//! - Operations from Logos peers (and those stored from earlier runs) are imported into the
//!   p2panda node too.
//! - Operations from p2panda peers are relayed to Logos, collected per author for a window
//!   (`logos_p2panda::bridge::to_logos`), as the bridge does, because Logos charges per message.
//! - The application sees every operation once, whichever network brought it first.
//!
//! Nothing loops: the Logos side ignores operations it already has, p2panda ignores operations
//! it already has, and this node drops an operation it has already handed on.
//!
//! Ephemeral messages (presence, cursors) are signed per network: this node sends its own on
//! both and listens on both.
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use p2panda::Topic;
use p2panda::node::CreateStreamError;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use super::bridge::BothEphemeral;
use super::external::{ExternalBackend, P2pandaBus, P2pandaItem, item_event};
use super::logos::{LogosBackend, LogosSync, Op};
use super::{
    BoxFuture, EphemeralPublisher, EphemeralStream, OpPublisher, OpStream, SyncBackend,
    TransportStats,
};
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

/// How long operations of one p2panda author are collected before they are relayed to Logos.
const RELAY_WINDOW: std::time::Duration = logos_p2panda::bridge::RELAY_WINDOW;

#[derive(Debug)]
pub(crate) struct DualBackend {
    p2panda: ExternalBackend,
    logos: LogosBackend,
}

impl DualBackend {
    pub(crate) fn new(p2panda: ExternalBackend, logos: LogosBackend) -> Self {
        Self { p2panda, logos }
    }
}

/// Imports into the host's p2panda node, one at a time and in order, without making the caller
/// wait for the host.
fn importer(bus: Arc<dyn P2pandaBus>, topic: Topic) -> mpsc::UnboundedSender<Op> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    tokio::spawn(async move {
        while let Some(op) = rx.recv().await {
            if let Err(error) = bus
                .import(topic, op.header_bytes.clone(), op.body.clone())
                .await
            {
                warn!(%topic, hash = %op.hash, "not imported into p2panda: {error}");
            }
        }
    });
    tx
}

/// Where an operation came from.
enum Arrival {
    Logos(Op),
    P2panda(P2pandaItem),
}

impl SyncBackend for DualBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            let from_p2panda = self.p2panda.raw_ops(topic).await?;
            let from_logos = self
                .logos
                .sync()
                .subscribe(topic)
                .await
                .map_err(|error| CreateStreamError(error.to_string()))?;

            let to_p2panda = importer(self.p2panda.bus().clone(), topic);
            let to_logos =
                logos_p2panda::bridge::to_logos(self.logos.sync().clone(), topic, RELAY_WINDOW);
            // Operations already handed on (and to the application), by hash.
            let seen: Arc<Mutex<HashSet<p2panda::Hash>>> = Arc::default();

            let arrivals = UnboundedReceiverStream::new(from_logos)
                .map(Arrival::Logos)
                .merge(UnboundedReceiverStream::new(from_p2panda).map(Arrival::P2panda));
            let events_seen = seen.clone();
            let events_to_p2panda = to_p2panda.clone();
            let events = arrivals.filter_map(move |arrival| match arrival {
                Arrival::Logos(op) => {
                    if !events_seen.lock().unwrap().insert(op.hash) {
                        return None;
                    }
                    let event = super::logos::op_event(&op);
                    let _ = events_to_p2panda.send(op);
                    event
                }
                Arrival::P2panda(item) => {
                    let P2pandaItem::Operation { hash, signed, .. } = &item else {
                        return None;
                    };
                    if !events_seen.lock().unwrap().insert(*hash) {
                        return None;
                    }
                    match signed
                        .clone()
                        .and_then(|(header, body)| Op::decode(header, body))
                    {
                        Some(op) => {
                            if to_logos.send(op).is_err() {
                                warn!(%topic, "relay to Logos stopped");
                            }
                        }
                        None => warn!(
                            %topic, %hash,
                            "the p2panda node gave no signed operation: not relayed to Logos"
                        ),
                    }
                    item_event(item)
                }
            });

            let publisher: Box<dyn OpPublisher> = Box::new(DualOpPublisher {
                sync: self.logos.sync().clone(),
                topic,
                to_p2panda,
                seen,
            });
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>> {
        Box::pin(async move {
            let (p2panda_publisher, p2panda_events) = self.p2panda.ephemeral_stream(topic).await?;
            let (logos_publisher, logos_events) = self.logos.ephemeral_stream(topic).await?;
            let publisher: Box<dyn EphemeralPublisher> = Box::new(BothEphemeral::new(
                p2panda_publisher,
                logos_publisher,
                self.p2panda.presence_interval(),
                self.logos.presence_interval(),
            ));
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

    fn set_active(&self, topic: Topic, active: bool) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            self.p2panda.set_active(topic, active).await;
            self.logos.set_active(topic, active).await;
        })
    }

    fn presence_interval(&self) -> std::time::Duration {
        self.p2panda.presence_interval()
    }

    fn stats(&self) -> Option<TransportStats> {
        self.logos.stats()
    }

    fn set_message_budget(&self, budget: Option<u64>) {
        self.logos.set_message_budget(budget);
    }
}

struct DualOpPublisher {
    sync: LogosSync,
    topic: Topic,
    to_p2panda: mpsc::UnboundedSender<Op>,
    seen: Arc<Mutex<HashSet<p2panda::Hash>>>,
}

impl DualOpPublisher {
    async fn write(&self, body: Vec<u8>, prune: bool) -> Result<(), PublishError> {
        // Encoded the way p2panda's streams encode a `Vec<u8>` message: the same operation on
        // both networks.
        let body = p2panda_core::cbor::encode_cbor(&body)
            .map_err(|error| PublishError::Backend(error.to_string()))?;
        let op = self
            .sync
            .publish(self.topic, body, prune)
            .await
            .map_err(|error| PublishError::Backend(error.to_string()))?;
        // p2panda delivers it back to us once imported: don't hand that on again.
        self.seen.lock().unwrap().insert(op.hash);
        let _ = self.to_p2panda.send(op);
        Ok(())
    }
}

impl OpPublisher for DualOpPublisher {
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, false))
    }

    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, true))
    }
}
