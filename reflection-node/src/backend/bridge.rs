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
use p2panda::Topic;
use p2panda::node::CreateStreamError;
use p2panda::streams::{StreamEvent, StreamFrom};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{debug, error};

use super::logos::{LogosBackend, Op};
use super::p2panda_backend::{P2pandaBackend, P2pandaOpPublisher, to_op_event};
use super::{BoxFuture, EphemeralPublisher, EphemeralStream, OpPublisher, OpStream, SyncBackend};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

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

            // p2panda -> Logos, in order, one operation at a time.
            let (to_logos, mut relay_rx) = mpsc::unbounded_channel::<Op>();
            let logos = self.logos.clone();
            tokio::spawn(async move {
                while let Some(op) = relay_rx.recv().await {
                    logos.relay(topic, op).await;
                }
                debug!(%topic, "bridge relay to Logos ended");
            });

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
}

struct BothEphemeral {
    p2panda: Box<dyn EphemeralPublisher>,
    logos: Box<dyn EphemeralPublisher>,
}

impl EphemeralPublisher for BothEphemeral {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let logos = self.logos.publish(message.clone()).await;
            self.p2panda.publish(message).await?;
            logos
        })
    }
}
