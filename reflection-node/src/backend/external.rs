//! A backend on a p2panda node that runs elsewhere: in another process, such as the
//! `p2panda_core` module of Logos Basecamp, which shares one p2panda node per network between
//! every app on the device (reflection-basecamp ADR 0001).
//!
//! The host implements [`P2pandaBus`]. Messages cross it as their CBOR encoding, exactly as
//! p2panda puts them on the wire: an operation's message is the CBOR byte string of Reflection's
//! payload (p2panda streams of `Vec<u8>`), an ephemeral message is the CBOR of
//! `EphemeralMessage`. Operations and ephemeral messages are signed by that node's key, which is
//! this node's key: the host opens the network with it.
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use p2panda::node::CreateStreamError;
use p2panda::{Hash, Topic, VerifyingKey};
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::warn;

use super::{
    BoxFuture, EphemeralEvent, EphemeralPublisher, EphemeralStream, OpEvent, OpPublisher, OpStream,
    SyncBackend,
};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

/// Something a p2panda node delivers for a topic.
#[derive(Clone, Debug)]
pub enum P2pandaItem {
    /// An operation (also our own ones, as p2panda's streams deliver them).
    Operation {
        hash: Hash,
        author: VerifyingKey,
        /// The message, CBOR-encoded.
        message: Vec<u8>,
        /// The whole signed operation, as encoded on the wire: its header and its body. Needed
        /// to pass the operation on to another network unchanged (`DualBackend`); a host that
        /// doesn't provide it can't do that.
        signed: Option<(Vec<u8>, Option<Vec<u8>>)>,
    },
    /// An ephemeral message from another node.
    Ephemeral {
        author: VerifyingKey,
        timestamp: u64,
        /// The message, CBOR-encoded.
        message: Vec<u8>,
    },
}

/// A p2panda node run by the host, already started on this node's network with this node's key.
pub trait P2pandaBus: Send + Sync + 'static {
    /// Subscribe to a topic: every stored operation first, then live ones and ephemeral
    /// messages.
    fn open(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<P2pandaItem>, String>>;

    /// Append a message (CBOR) to our log on the topic; `prune`: a snapshot.
    fn publish(
        &self,
        topic: Topic,
        message: Vec<u8>,
        prune: bool,
    ) -> BoxFuture<'_, Result<(), String>>;

    /// Send an ephemeral message (CBOR).
    fn ephemeral(&self, topic: Topic, message: Vec<u8>) -> BoxFuture<'_, Result<(), String>>;

    /// Whether the user has the topic open.
    fn set_active(&self, _topic: Topic, _active: bool) -> BoxFuture<'_, ()> {
        Box::pin(async {})
    }

    /// Hand the node a signed operation from elsewhere (another network), by its encoded header
    /// and body. The node checks and stores it, syncs it to its peers and delivers it on the
    /// topic like any other. The topic must be open.
    fn import(
        &self,
        _topic: Topic,
        _header: Vec<u8>,
        _body: Option<Vec<u8>>,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async { Err("this p2panda node can't import operations".to_owned()) })
    }
}

/// The backend: Reflection's streams on a [`P2pandaBus`].
pub(crate) struct ExternalBackend {
    bus: Arc<dyn P2pandaBus>,
    /// Per topic, what `open` delivered, split for the two streams Reflection asks for.
    topics: Mutex<HashMap<Topic, Split>>,
}

impl std::fmt::Debug for ExternalBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExternalBackend").finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Split {
    ops: Option<mpsc::UnboundedReceiver<P2pandaItem>>,
    ephemeral: Option<mpsc::UnboundedReceiver<P2pandaItem>>,
}

impl ExternalBackend {
    pub(crate) fn new(bus: Arc<dyn P2pandaBus>) -> Self {
        Self {
            bus,
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// Open the topic once and split what arrives into operations and ephemeral messages.
    async fn split(&self, topic: Topic) -> Result<(), CreateStreamError> {
        if self.topics.lock().unwrap().contains_key(&topic) {
            return Ok(());
        }
        let mut rx = self.bus.open(topic).await.map_err(CreateStreamError)?;
        let (ops_tx, ops_rx) = mpsc::unbounded_channel();
        let (eph_tx, eph_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            while let Some(item) = rx.recv().await {
                let _ = match item {
                    P2pandaItem::Operation { .. } => ops_tx.send(item),
                    P2pandaItem::Ephemeral { .. } => eph_tx.send(item),
                };
            }
        });
        self.topics.lock().unwrap().insert(
            topic,
            Split {
                ops: Some(ops_rx),
                ephemeral: Some(eph_rx),
            },
        );
        Ok(())
    }
}

impl ExternalBackend {
    /// What the host delivers for a topic's operations, undecoded (for `DualBackend`).
    pub(crate) async fn raw_ops(
        &self,
        topic: Topic,
    ) -> Result<mpsc::UnboundedReceiver<P2pandaItem>, CreateStreamError> {
        self.split(topic).await?;
        self.topics
            .lock()
            .unwrap()
            .get_mut(&topic)
            .and_then(|split| split.ops.take())
            .ok_or_else(|| CreateStreamError("topic already streamed".into()))
    }

    pub(crate) fn bus(&self) -> &Arc<dyn P2pandaBus> {
        &self.bus
    }
}

/// An operation item as Reflection's stream event (its message is a CBOR byte string).
pub(crate) fn item_event(item: P2pandaItem) -> Option<OpEvent> {
    match item {
        P2pandaItem::Operation {
            hash,
            author,
            message,
            ..
        } => match p2panda_core::cbor::decode_cbor::<Vec<u8>, _>(&message[..]) {
            Ok(body) => Some(OpEvent::Operation {
                id: hash,
                author,
                body,
            }),
            Err(error) => {
                warn!(%hash, "dropping operation with undecodable message: {error}");
                None
            }
        },
        P2pandaItem::Ephemeral { .. } => None,
    }
}

impl SyncBackend for ExternalBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            let rx = self.raw_ops(topic).await?;
            let events = UnboundedReceiverStream::new(rx).filter_map(item_event);
            let publisher: Box<dyn OpPublisher> = Box::new(ExternalOpPublisher {
                bus: self.bus.clone(),
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
            self.split(topic).await?;
            let rx = self
                .topics
                .lock()
                .unwrap()
                .get_mut(&topic)
                .and_then(|split| split.ephemeral.take())
                .ok_or_else(|| CreateStreamError("topic already streamed".into()))?;
            let events = UnboundedReceiverStream::new(rx).filter_map(|item| match item {
                P2pandaItem::Ephemeral {
                    author,
                    timestamp,
                    message,
                } => match p2panda_core::cbor::decode_cbor::<EphemeralMessage, _>(&message[..]) {
                    Ok(message) => Some(EphemeralEvent {
                        author,
                        timestamp,
                        message,
                    }),
                    Err(_) => {
                        warn!("dropping undecodable ephemeral message");
                        None
                    }
                },
                P2pandaItem::Operation { .. } => None,
            });
            let publisher: Box<dyn EphemeralPublisher> = Box::new(ExternalEphemeralPublisher {
                bus: self.bus.clone(),
                topic,
            });
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn set_connection_mode(
        &self,
        _connection_mode: ConnectionMode,
    ) -> BoxFuture<'_, Result<(), NodeError>> {
        Box::pin(async { Ok(()) })
    }

    fn set_active(&self, topic: Topic, active: bool) -> BoxFuture<'_, ()> {
        self.bus.set_active(topic, active)
    }

    fn presence_interval(&self) -> Duration {
        Duration::from_secs(30)
    }
}

struct ExternalOpPublisher {
    bus: Arc<dyn P2pandaBus>,
    topic: Topic,
}

impl ExternalOpPublisher {
    async fn write(&self, body: Vec<u8>, prune: bool) -> Result<(), PublishError> {
        let message = p2panda_core::cbor::encode_cbor(&body)
            .map_err(|error| PublishError::Backend(error.to_string()))?;
        self.bus
            .publish(self.topic, message, prune)
            .await
            .map_err(PublishError::Backend)
    }
}

impl OpPublisher for ExternalOpPublisher {
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, false))
    }

    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.write(body, true))
    }
}

struct ExternalEphemeralPublisher {
    bus: Arc<dyn P2pandaBus>,
    topic: Topic,
}

impl EphemeralPublisher for ExternalEphemeralPublisher {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let message = p2panda_core::cbor::encode_cbor(&message)
                .map_err(|error| PublishError::Backend(error.to_string()))?;
            self.bus
                .ephemeral(self.topic, message)
                .await
                .map_err(PublishError::Backend)
        })
    }
}

/// A [`P2pandaBus`] over a p2panda node in this process: the external backend without a host,
/// for `REFLECTION_BACKEND=p2panda-external` (upstream's tests through this backend).
#[cfg(any(test, feature = "test_utils"))]
pub struct NodeBus {
    node: p2panda::Node,
    publishers: tokio::sync::Mutex<HashMap<Topic, NodeTopic>>,
}

#[cfg(any(test, feature = "test_utils"))]
struct NodeTopic {
    ops: p2panda::streams::StreamPublisher<ciborium::Value>,
    ephemeral: p2panda::streams::EphemeralStreamPublisher<ciborium::Value>,
    imports: mpsc::UnboundedSender<p2panda::operation::Operation>,
}

#[cfg(any(test, feature = "test_utils"))]
impl NodeBus {
    pub fn new(node: p2panda::Node) -> Self {
        Self {
            node,
            publishers: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

#[cfg(any(test, feature = "test_utils"))]
fn cbor(value: &ciborium::Value) -> Vec<u8> {
    let mut bytes = Vec::new();
    let _ = ciborium::into_writer(value, &mut bytes);
    bytes
}

#[cfg(any(test, feature = "test_utils"))]
impl P2pandaBus for NodeBus {
    fn open(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<mpsc::UnboundedReceiver<P2pandaItem>, String>> {
        Box::pin(async move {
            use p2panda::streams::{StreamEvent, StreamFrom};
            let (ops, mut events) = self
                .node
                .stream_from::<ciborium::Value>(topic, StreamFrom::Start)
                .await
                .map_err(|e| e.to_string())?;
            let (ephemeral, mut ephemeral_events) = self
                .node
                .ephemeral_stream::<ciborium::Value>(topic)
                .await
                .map_err(|e| e.to_string())?;
            let (tx, rx) = mpsc::unbounded_channel();
            let ops_tx = tx.clone();
            tokio::spawn(async move {
                while let Some(event) = events.next().await {
                    if let StreamEvent::Processed { operation, .. } = event {
                        let processed = operation.processed();
                        let signed = Some((
                            processed.header().to_bytes(),
                            processed.body().map(|body| body.to_bytes()),
                        ));
                        let _ = ops_tx.send(P2pandaItem::Operation {
                            hash: operation.id(),
                            author: operation.author(),
                            message: cbor(operation.message()),
                            signed,
                        });
                        let _ = operation.ack().await;
                    }
                }
            });
            tokio::spawn(async move {
                while let Some(message) = ephemeral_events.next().await {
                    let _ = tx.send(P2pandaItem::Ephemeral {
                        author: message.author(),
                        timestamp: message.timestamp(),
                        message: cbor(message.body()),
                    });
                }
            });
            let (imports, imported) = mpsc::unbounded_channel();
            ops.import(UnboundedReceiverStream::new(imported))
                .await
                .map_err(|e| e.to_string())?;
            self.publishers.lock().await.insert(
                topic,
                NodeTopic {
                    ops,
                    ephemeral,
                    imports,
                },
            );
            Ok(rx)
        })
    }

    fn publish(
        &self,
        topic: Topic,
        message: Vec<u8>,
        prune: bool,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let value: ciborium::Value =
                ciborium::from_reader(&message[..]).map_err(|e| e.to_string())?;
            let publishers = self.publishers.lock().await;
            let topic = publishers.get(&topic).ok_or("topic not open")?;
            if prune {
                topic.ops.prune(Some(value)).await
            } else {
                topic.ops.publish(value).await
            }
            .map(|_| ())
            .map_err(|e| e.to_string())
        })
    }

    fn ephemeral(&self, topic: Topic, message: Vec<u8>) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let value: ciborium::Value =
                ciborium::from_reader(&message[..]).map_err(|e| e.to_string())?;
            let publishers = self.publishers.lock().await;
            let topic = publishers.get(&topic).ok_or("topic not open")?;
            topic
                .ephemeral
                .publish(value)
                .await
                .map_err(|e| e.to_string())
        })
    }

    fn import(
        &self,
        topic: Topic,
        header: Vec<u8>,
        body: Option<Vec<u8>>,
    ) -> BoxFuture<'_, Result<(), String>> {
        Box::pin(async move {
            let header: p2panda::operation::Header =
                p2panda_core::cbor::decode_cbor(&header[..]).map_err(|e| e.to_string())?;
            let operation = p2panda::operation::Operation {
                hash: header.hash(),
                header,
                body: body.as_deref().map(p2panda_core::Body::new),
            };
            let publishers = self.publishers.lock().await;
            let topic = publishers.get(&topic).ok_or("topic not open")?;
            topic.imports.send(operation).map_err(|e| e.to_string())
        })
    }
}
