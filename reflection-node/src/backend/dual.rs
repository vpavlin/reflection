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
//! **Logos never waits for p2panda.** Opening a topic on the host's p2panda node is a call to
//! another process that can fail or time out (a busy host opening many topics at start). The
//! topic then works on Logos at once, and the p2panda side is opened in the background, retried
//! with backoff until it succeeds; operations for p2panda are held until then, and its traffic
//! joins the topic's streams when it is up.
//!
//! Nothing loops: the Logos side ignores operations it already has, p2panda ignores operations
//! it already has, and this node drops an operation it has already handed on.
//!
//! Ephemeral messages (presence, cursors) are signed per network: this node sends its own on
//! both and listens on both.
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use p2panda::Topic;
use p2panda::node::CreateStreamError;
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tracing::{info, warn};

use super::external::{ExternalBackend, P2pandaItem, item_event};
use super::logos::{LogosBackend, LogosSync, Op, is_presence_ping};
use super::{
    BoxFuture, EphemeralEvent, EphemeralPublisher, EphemeralStream, OpPublisher, OpStream,
    SyncBackend, TransportStats,
};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

/// How long operations of one p2panda author are collected before they are relayed to Logos.
const RELAY_WINDOW: Duration = logos_p2panda::bridge::RELAY_WINDOW;
/// Retrying a p2panda open that failed: first after this long, doubling up to `OPEN_RETRY_MAX`.
const OPEN_RETRY_MIN: Duration = Duration::from_secs(5);
const OPEN_RETRY_MAX: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub(crate) struct DualBackend {
    p2panda: Arc<ExternalBackend>,
    logos: LogosBackend,
    topics: Mutex<HashMap<Topic, Arc<P2Topic>>>,
}

/// The p2panda side of one topic, opened in the background.
struct P2Topic {
    /// Operations from p2panda, for `stream` (taken once).
    ops: Mutex<Option<mpsc::UnboundedReceiver<P2pandaItem>>>,
    /// Ephemeral messages from p2panda, for `ephemeral_stream` (taken once).
    ephemeral: Mutex<Option<mpsc::UnboundedReceiver<EphemeralEvent>>>,
    /// Publishes ephemeral messages on p2panda, once it is open.
    publisher: Mutex<Option<Arc<dyn EphemeralPublisher>>>,
    /// True once the topic is open on p2panda.
    open: watch::Sender<bool>,
    /// Whether the user has the topic open, to apply once it is open on p2panda.
    active: Mutex<bool>,
}

impl std::fmt::Debug for P2Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("P2Topic")
            .field("open", &*self.open.borrow())
            .finish_non_exhaustive()
    }
}

impl DualBackend {
    pub(crate) fn new(p2panda: ExternalBackend, logos: LogosBackend) -> Self {
        Self {
            p2panda: Arc::new(p2panda),
            logos,
            topics: Mutex::new(HashMap::new()),
        }
    }

    /// The topic's p2panda side; the first call starts opening it in the background.
    fn p2topic(&self, topic: Topic) -> Arc<P2Topic> {
        let mut topics = self.topics.lock().unwrap();
        if let Some(p2) = topics.get(&topic) {
            return p2.clone();
        }
        let (ops_tx, ops_rx) = mpsc::unbounded_channel();
        let (eph_tx, eph_rx) = mpsc::unbounded_channel();
        let (open, _) = watch::channel(false);
        let p2 = Arc::new(P2Topic {
            ops: Mutex::new(Some(ops_rx)),
            ephemeral: Mutex::new(Some(eph_rx)),
            publisher: Mutex::new(None),
            open,
            active: Mutex::new(false),
        });
        topics.insert(topic, p2.clone());
        tokio::spawn(open_p2panda(
            self.p2panda.clone(),
            topic,
            p2.clone(),
            ops_tx,
            eph_tx,
        ));
        p2
    }
}

/// Open `topic` on the host's p2panda node, retrying until it works, then forward what it
/// delivers into the topic's channels.
async fn open_p2panda(
    p2panda: Arc<ExternalBackend>,
    topic: Topic,
    p2: Arc<P2Topic>,
    ops_tx: mpsc::UnboundedSender<P2pandaItem>,
    eph_tx: mpsc::UnboundedSender<EphemeralEvent>,
) {
    let mut wait = OPEN_RETRY_MIN;
    let (mut ops, (publisher, mut events)) = loop {
        let opened = async {
            let ops = p2panda.raw_ops(topic).await?;
            let ephemeral = p2panda.ephemeral_stream(topic).await?;
            Ok::<_, CreateStreamError>((ops, ephemeral))
        }
        .await;
        match opened {
            Ok(opened) => break opened,
            Err(error) => {
                // Logos keeps working meanwhile.
                warn!(%topic, "can't open the note on p2panda, retrying in {}s: {error}", wait.as_secs());
                p2panda.forget(topic);
                tokio::time::sleep(wait).await;
                wait = (wait * 2).min(OPEN_RETRY_MAX);
                if ops_tx.is_closed() && eph_tx.is_closed() {
                    return; // nobody listens any more
                }
            }
        }
    };
    *p2.publisher.lock().unwrap() = Some(Arc::from(publisher));
    let active = *p2.active.lock().unwrap();
    if active {
        p2panda.set_active(topic, true).await;
    }
    let _ = p2.open.send(true);
    info!(%topic, "note open on p2panda");
    tokio::spawn(async move {
        while let Some(event) = events.next().await {
            if eph_tx.send(event).is_err() {
                return;
            }
        }
    });
    while let Some(item) = ops.recv().await {
        if ops_tx.send(item).is_err() {
            return;
        }
    }
}

/// Imports into the host's p2panda node, one at a time and in order, without making the caller
/// wait for the host. Holds everything until the topic is open there.
fn importer(
    p2panda: Arc<ExternalBackend>,
    topic: Topic,
    p2: Arc<P2Topic>,
) -> mpsc::UnboundedSender<Op> {
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    let mut open = p2.open.subscribe();
    tokio::spawn(async move {
        if open.wait_for(|open| *open).await.is_err() {
            return;
        }
        let bus = p2panda.bus().clone();
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
            let from_logos = self
                .logos
                .sync()
                .subscribe(topic)
                .await
                .map_err(|error| CreateStreamError(error.to_string()))?;
            let p2 = self.p2topic(topic);
            let from_p2panda = p2
                .ops
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| CreateStreamError("topic already streamed".into()))?;

            let to_p2panda = importer(self.p2panda.clone(), topic, p2.clone());
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
            let (logos_publisher, logos_events) = self.logos.ephemeral_stream(topic).await?;
            let p2 = self.p2topic(topic);
            let p2panda_events = p2
                .ephemeral
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| CreateStreamError("topic already streamed".into()))?;
            let publisher: Box<dyn EphemeralPublisher> = Box::new(DualEphemeral {
                logos: logos_publisher,
                p2,
                pings: AtomicU64::new(0),
                logos_ping_every: (self.logos.presence_interval().as_secs()
                    / self.p2panda.presence_interval().as_secs().max(1))
                .max(1),
            });
            let events = UnboundedReceiverStream::new(p2panda_events).merge(logos_events);
            Ok((publisher, Box::pin(events) as _))
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
            let p2 = self.p2topic(topic);
            *p2.active.lock().unwrap() = active;
            if *p2.open.borrow() {
                self.p2panda.set_active(topic, active).await;
            }
            self.logos.set_active(topic, active).await;
        })
    }

    fn presence_interval(&self) -> Duration {
        self.p2panda.presence_interval()
    }

    fn stats(&self) -> Option<TransportStats> {
        self.logos.stats()
    }

    fn set_message_budget(&self, budget: Option<u64>) {
        self.logos.set_message_budget(budget);
    }
}

/// Ephemeral messages to both networks: always to Logos (presence thinned to its slower rate,
/// since Logos charges per message), and to p2panda once the topic is open there.
struct DualEphemeral {
    logos: Box<dyn EphemeralPublisher>,
    p2: Arc<P2Topic>,
    pings: AtomicU64,
    logos_ping_every: u64,
}

impl EphemeralPublisher for DualEphemeral {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let to_logos = !is_presence_ping(&message)
                || self
                    .pings
                    .fetch_add(1, Ordering::Relaxed)
                    .is_multiple_of(self.logos_ping_every);
            let logos = if to_logos {
                self.logos.publish(message.clone()).await
            } else {
                Ok(())
            };
            let p2panda = self.p2.publisher.lock().unwrap().clone();
            if let Some(p2panda) = p2panda {
                if let Err(error) = p2panda.publish(message).await {
                    warn!("ephemeral message not sent on p2panda: {error}");
                }
            }
            logos
        })
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
