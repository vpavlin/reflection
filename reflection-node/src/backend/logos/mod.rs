//! Sync backend over Logos Delivery (see docs/logos).
//!
//! Operations are p2panda operations (signed, per-author logs, prune flag). They travel
//! encrypted on two content topics per document, and peers that missed operations catch up by
//! broadcasting the heads of their logs (ADR 0003).
mod bus;
#[cfg(feature = "logos-ffi")]
pub(crate) mod ffi_bus;
mod store;
mod wire;

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub use bus::{BusError, BusMessage, DeliveryBus, MemoryBus};
#[cfg(feature = "logos-ffi")]
pub use ffi_bus::LibLogosDeliveryBus;

use p2panda::node::CreateStreamError;
use p2panda::operation::{Extensions, Header};
use p2panda::{Hash, NetworkId, Topic, VerifyingKey};
use p2panda_core::{Body, SigningKey};
use sqlx::SqlitePool;
use tokio::sync::{Mutex, broadcast, mpsc};
use tracing::{debug, error, info, warn};

use self::store::OpStore;
use self::wire::{Bytes, Frame, MAX_FRAME_LEN, SignedEphemeral, TopicKeys};
use super::{
    BoxFuture, EphemeralEvent, EphemeralPublisher, EphemeralStream, OpEvent, OpPublisher, OpStream,
    SyncBackend,
};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;

const CHANNEL_SIZE: usize = 1024;

/// Heads are re-sent with this backoff after subscribing, then every `HEADS_MAX_INTERVAL`.
const HEADS_FIRST_INTERVAL: Duration = Duration::from_secs(1);
const HEADS_MAX_INTERVAL: Duration = Duration::from_secs(60);

/// A repair waits a random time in this range, so that one peer answers rather than all.
const REPAIR_DELAY_MIN_MS: u64 = 50;
const REPAIR_DELAY_MAX_MS: u64 = 500;

/// Operations waiting for a missing predecessor, per author.
const MAX_PENDING_PER_AUTHOR: usize = 256;

/// A p2panda operation as stored and sent by this backend.
#[derive(Clone, Debug)]
pub(crate) struct Op {
    pub hash: Hash,
    pub header: Header,
    pub header_bytes: Vec<u8>,
    pub body: Option<Vec<u8>>,
}

impl Op {
    fn decode(header_bytes: Vec<u8>, body: Option<Vec<u8>>) -> Option<Self> {
        let header: Header = p2panda_core::cbor::decode_cbor(&header_bytes[..]).ok()?;
        let hash = header.hash();
        Some(Self {
            hash,
            header,
            header_bytes,
            body,
        })
    }

    /// Check signature, payload hash and size, like p2panda does on ingest.
    fn validate(&self) -> bool {
        let operation = p2panda::operation::Operation {
            hash: self.hash,
            header: self.header.clone(),
            body: self.body.as_deref().map(Body::new),
        };
        p2panda_core::validate_operation(&operation).is_ok()
    }

    pub fn author(&self) -> VerifyingKey {
        self.header.verifying_key
    }

    pub fn seq(&self) -> u64 {
        self.header.seq_num.into()
    }

    pub fn prunes(&self) -> bool {
        self.header.extensions.prune_flag().is_set()
    }

    fn frame(&self) -> Frame {
        Frame::Operation {
            header: Bytes(self.header_bytes.clone()),
            body: self.body.clone().map(Bytes),
        }
    }
}

/// The Logos Delivery backend.
pub(crate) struct LogosBackend {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LogosBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogosBackend").finish_non_exhaustive()
    }
}

struct Inner {
    bus: Arc<dyn DeliveryBus>,
    signing_key: SigningKey,
    network_id: NetworkId,
    store: OpStore,
    topics: Mutex<HashMap<Topic, Arc<TopicState>>>,
}

impl LogosBackend {
    pub(crate) async fn new(
        bus: Arc<dyn DeliveryBus>,
        signing_key: SigningKey,
        network_id: NetworkId,
        pool: SqlitePool,
    ) -> Result<Self, NodeError> {
        let store = OpStore::new(pool).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                bus,
                signing_key,
                network_id,
                store,
                topics: Mutex::new(HashMap::new()),
            }),
        })
    }
}

impl SyncBackend for LogosBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            let state = self.inner.topic(topic).await?;
            let (tx, rx) = mpsc::channel(CHANNEL_SIZE);

            {
                // Replay everything we have, then go live. Holding the lock means no operation
                // arrives in between; a duplicate would be harmless anyway.
                let mut senders = state.senders.lock().await;
                let ops = self
                    .inner
                    .store
                    .all(&topic)
                    .await
                    .map_err(|error| CreateStreamError(error.to_string()))?;
                for op in ops {
                    let _ = tx.send(op_event(&op)).await;
                }
                senders.ops = Some(tx);
            }

            // Tell the others what we have, so they can fill us in.
            state.restart_heads();

            let publisher: Box<dyn OpPublisher> = Box::new(LogosOpPublisher {
                inner: self.inner.clone(),
                state,
            });
            let events = tokio_stream::wrappers::ReceiverStream::new(rx);
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>> {
        Box::pin(async move {
            let state = self.inner.topic(topic).await?;
            let (tx, rx) = mpsc::channel(CHANNEL_SIZE);
            state.senders.lock().await.ephemeral = Some(tx);

            let publisher: Box<dyn EphemeralPublisher> = Box::new(LogosEphemeralPublisher {
                inner: self.inner.clone(),
                state,
            });
            let events = tokio_stream::wrappers::ReceiverStream::new(rx);
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
}

#[derive(Default)]
struct Senders {
    ops: Option<mpsc::Sender<OpEvent>>,
    ephemeral: Option<mpsc::Sender<EphemeralEvent>>,
}

/// Everything the backend keeps per document.
struct TopicState {
    topic: Topic,
    keys: TopicKeys,
    senders: Mutex<Senders>,
    /// Serializes our own writes, so sequence numbers do not race.
    write_lock: Mutex<()>,
    /// Operations that arrived before their predecessor.
    pending: Mutex<HashMap<VerifyingKey, BTreeMap<u64, Op>>>,
    /// When we last saw an operation on the wire, for repair suppression.
    seen: StdMutex<HashMap<Hash, Instant>>,
    /// Wakes the heads task to start its backoff from the beginning.
    heads_restart: tokio::sync::Notify,
    /// Debounce for heads sent because we noticed we are behind.
    last_behind_heads: StdMutex<Option<Instant>>,
}

impl TopicState {
    fn restart_heads(&self) {
        self.heads_restart.notify_one();
    }

    fn mark_seen(&self, hash: Hash) {
        let mut seen = self.seen.lock().unwrap();
        seen.insert(hash, Instant::now());
        if seen.len() > 10_000 {
            let cutoff = Instant::now() - Duration::from_secs(60);
            seen.retain(|_, at| *at > cutoff);
        }
    }

    fn seen_since(&self, hash: &Hash, since: Instant) -> bool {
        self.seen
            .lock()
            .unwrap()
            .get(hash)
            .is_some_and(|at| *at >= since)
    }
}

impl Inner {
    /// Get or set up the state of a topic: subscribe on the bus and start its tasks.
    async fn topic(self: &Arc<Self>, topic: Topic) -> Result<Arc<TopicState>, CreateStreamError> {
        let mut topics = self.topics.lock().await;
        if let Some(state) = topics.get(&topic) {
            return Ok(state.clone());
        }

        let keys = TopicKeys::new(&self.network_id, &topic);
        let state = Arc::new(TopicState {
            topic,
            keys: keys.clone(),
            senders: Mutex::new(Senders::default()),
            write_lock: Mutex::new(()),
            pending: Mutex::new(HashMap::new()),
            seen: StdMutex::new(HashMap::new()),
            heads_restart: tokio::sync::Notify::new(),
            last_behind_heads: StdMutex::new(None),
        });

        // Listen before subscribing, so nothing that arrives is lost.
        let rx = self.bus.messages();
        for content_topic in [&keys.ops_topic, &keys.ephemeral_topic] {
            self.bus
                .subscribe(content_topic.clone())
                .await
                .map_err(|error| CreateStreamError(error.to_string()))?;
        }
        info!(%topic, ops = %keys.ops_topic, "subscribed on Logos Delivery");

        tokio::spawn(receive_loop(self.clone(), state.clone(), rx));
        tokio::spawn(heads_loop(self.clone(), state.clone()));

        topics.insert(topic, state.clone());
        Ok(state)
    }

    async fn send(&self, state: &TopicState, content_topic: &str, frame: &Frame) {
        let payload = state.keys.seal(frame);
        if let Err(error) = self.bus.publish(content_topic.to_owned(), payload).await {
            warn!("failed to publish on {content_topic}: {error}");
        }
    }

    async fn send_heads(&self, state: &TopicState) {
        let heads = match self.store.heads(&state.topic).await {
            Ok(heads) => heads,
            Err(error) => {
                error!("can't read heads: {error}");
                return;
            }
        };
        let frame = Frame::Heads {
            from: self.signing_key.verifying_key(),
            heads: heads
                .iter()
                .map(|op| (op.author(), op.seq(), op.hash))
                .collect(),
        };
        self.send(state, &state.keys.ops_topic, &frame).await;
    }

    /// Send our heads soon because a peer is ahead of us, at most twice a second.
    async fn send_heads_behind(&self, state: &TopicState) {
        {
            let mut last = state.last_behind_heads.lock().unwrap();
            if last.is_some_and(|at| at.elapsed() < Duration::from_millis(500)) {
                return;
            }
            *last = Some(Instant::now());
        }
        self.send_heads(state).await;
    }

    /// Create, store and send one of our own operations.
    async fn publish(
        &self,
        state: &TopicState,
        body: Vec<u8>,
        prune: bool,
    ) -> Result<(), PublishError> {
        let _guard = state.write_lock.lock().await;
        let author = self.signing_key.verifying_key();
        let head = self
            .store
            .head(&state.topic, &author)
            .await
            .map_err(|error| PublishError::Backend(error.to_string()))?;

        let body_value = Body::new(&body);
        let mut header = Header {
            version: 1,
            verifying_key: author,
            signature: None,
            payload_size: body_value.size(),
            payload_hash: Some(body_value.hash()),
            seq_num: head.as_ref().map(|op| op.header.seq_num + 1).unwrap_or(0),
            backlink: head.as_ref().map(|op| op.hash),
            extensions: Extensions::from_topic(state.topic).set_prune_flag(prune),
        };
        header.sign(&self.signing_key);

        let header_bytes = header.to_bytes();
        if header_bytes.len() + body.len() > MAX_FRAME_LEN {
            return Err(PublishError::Backend(format!(
                "operation of {} bytes is over the {} byte limit of Logos Delivery",
                header_bytes.len() + body.len(),
                MAX_FRAME_LEN
            )));
        }

        let op = Op {
            hash: header.hash(),
            header,
            header_bytes,
            body: Some(body),
        };
        self.store
            .insert(&state.topic, &op)
            .await
            .map_err(|error| PublishError::Backend(error.to_string()))?;
        state.mark_seen(op.hash);

        self.send(state, &state.keys.ops_topic, &op.frame()).await;
        Ok(())
    }

    /// Handle an operation from the wire.
    async fn receive_op(&self, state: &TopicState, op: Op) {
        state.mark_seen(op.hash);

        match self.store.has(&op.hash).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(error) => {
                error!("can't read operation store: {error}");
                return;
            }
        }

        if !op.validate() {
            warn!(hash = %op.hash, "dropping invalid operation");
            return;
        }

        if op.header.extensions.log_id() != p2panda::operation::LogId::from_topic(state.topic) {
            warn!(hash = %op.hash, "dropping operation for another log");
            return;
        }

        let author = op.author();
        let head = match self.store.head(&state.topic, &author).await {
            Ok(head) => head,
            Err(error) => {
                error!("can't read operation store: {error}");
                return;
            }
        };

        if self.fits(&head, &op) {
            self.accept(state, op).await;
            self.drain_pending(state, author).await;
        } else if head.as_ref().is_some_and(|head| op.seq() <= head.seq()) {
            // Older than what we have: already pruned, nothing to do.
            debug!(hash = %op.hash, "ignoring old operation");
        } else {
            // A gap: keep it and ask for what is missing.
            let mut pending = state.pending.lock().await;
            let log = pending.entry(author).or_default();
            if log.len() < MAX_PENDING_PER_AUTHOR {
                log.insert(op.seq(), op);
            }
            drop(pending);
            self.send_heads_behind(state).await;
        }
    }

    /// Whether an operation directly follows the head of its log (or starts or prunes it).
    fn fits(&self, head: &Option<Op>, op: &Op) -> bool {
        if op.prunes() {
            return head.as_ref().is_none_or(|head| op.seq() > head.seq());
        }
        match head {
            None => op.seq() == 0,
            Some(head) => op.seq() == head.seq() + 1 && op.header.backlink == Some(head.hash),
        }
    }

    async fn accept(&self, state: &TopicState, op: Op) {
        if let Err(error) = self.store.insert(&state.topic, &op).await {
            error!("can't store operation: {error}");
            return;
        }
        if let Some(tx) = state.senders.lock().await.ops.as_ref() {
            let _ = tx.send(op_event(&op)).await;
        }
    }

    async fn drain_pending(&self, state: &TopicState, author: VerifyingKey) {
        loop {
            let head = match self.store.head(&state.topic, &author).await {
                Ok(head) => head,
                Err(_) => return,
            };
            let next = {
                let mut pending = state.pending.lock().await;
                let Some(log) = pending.get_mut(&author) else {
                    return;
                };
                // Drop what is now old, then take the next one if it fits.
                if let Some(head) = &head {
                    log.retain(|seq, _| *seq > head.seq());
                }
                match log.first_key_value() {
                    Some((_, op)) if self.fits(&head, op) => log.pop_first().map(|(_, op)| op),
                    _ => None,
                }
            };
            match next {
                Some(op) => self.accept(state, op).await,
                None => return,
            }
        }
    }

    /// A peer told us its heads: send what it lacks, and ask for what we lack.
    async fn receive_heads(
        self: &Arc<Self>,
        state: &Arc<TopicState>,
        from: VerifyingKey,
        heads: Vec<(VerifyingKey, u64, Hash)>,
    ) {
        if from == self.signing_key.verifying_key() {
            return;
        }

        let ours = match self.store.heads(&state.topic).await {
            Ok(ours) => ours,
            Err(error) => {
                error!("can't read heads: {error}");
                return;
            }
        };
        let theirs: HashMap<VerifyingKey, u64> = heads
            .iter()
            .map(|(author, seq, _)| (*author, *seq))
            .collect();
        let ours_by_author: HashMap<VerifyingKey, u64> =
            ours.iter().map(|op| (op.author(), op.seq())).collect();

        // What they lack.
        let mut repair: Vec<(VerifyingKey, Option<u64>)> = Vec::new();
        for op in &ours {
            match theirs.get(&op.author()) {
                Some(seq) if *seq >= op.seq() => {}
                other => repair.push((op.author(), other.copied())),
            }
        }

        // What we lack.
        let behind = heads
            .iter()
            .any(|(author, seq, _)| ours_by_author.get(author).is_none_or(|ours| ours < seq));
        if behind {
            self.send_heads_behind(state).await;
        }

        if repair.is_empty() {
            return;
        }

        let inner = self.clone();
        let state = state.clone();
        tokio::spawn(async move {
            let scheduled = Instant::now();
            tokio::time::sleep(repair_delay()).await;
            for (author, after) in repair {
                let ops = match inner.store.after(&state.topic, &author, after).await {
                    Ok(ops) => ops,
                    Err(error) => {
                        error!("can't read operations for repair: {error}");
                        continue;
                    }
                };
                for op in ops {
                    // Somebody else already sent it since we decided to: skip.
                    if state.seen_since(&op.hash, scheduled) {
                        continue;
                    }
                    state.mark_seen(op.hash);
                    inner.send(&state, &state.keys.ops_topic, &op.frame()).await;
                }
            }
        });
    }

    async fn receive_ephemeral(&self, state: &TopicState, message: SignedEphemeral) {
        if message.author == self.signing_key.verifying_key() || !message.verify(&state.topic) {
            return;
        }
        let Ok(body) = p2panda_core::cbor::decode_cbor::<EphemeralMessage, _>(&message.body.0[..])
        else {
            warn!("dropping undecodable ephemeral message");
            return;
        };
        if let Some(tx) = state.senders.lock().await.ephemeral.as_ref() {
            let _ = tx
                .send(EphemeralEvent {
                    author: message.author,
                    timestamp: message.timestamp,
                    message: body,
                })
                .await;
        }
    }
}

fn op_event(op: &Op) -> OpEvent {
    OpEvent::Operation {
        id: op.hash,
        author: op.author(),
        body: op.body.clone().unwrap_or_default(),
    }
}

fn repair_delay() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let spread = REPAIR_DELAY_MAX_MS - REPAIR_DELAY_MIN_MS;
    Duration::from_millis(REPAIR_DELAY_MIN_MS + nanos % spread)
}

async fn receive_loop(
    inner: Arc<Inner>,
    state: Arc<TopicState>,
    mut rx: broadcast::Receiver<BusMessage>,
) {
    loop {
        let message = match rx.recv().await {
            Ok(message) => message,
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                // We lost messages: catch-up will bring them back.
                warn!("receive queue overflowed, skipped {skipped} messages");
                inner.send_heads_behind(&state).await;
                continue;
            }
            Err(broadcast::error::RecvError::Closed) => return,
        };
        if message.content_topic != state.keys.ops_topic
            && message.content_topic != state.keys.ephemeral_topic
        {
            continue;
        }
        let Some(frame) = state.keys.open(&message.payload) else {
            debug!("dropping message we can't open");
            continue;
        };
        match frame {
            Frame::Operation { header, body } => {
                match Op::decode(header.0, body.map(|body| body.0)) {
                    Some(op) => inner.receive_op(&state, op).await,
                    None => warn!("dropping undecodable operation"),
                }
            }
            Frame::Heads { from, heads } => inner.receive_heads(&state, from, heads).await,
            Frame::Ephemeral(message) => inner.receive_ephemeral(&state, message).await,
        }
    }
}

async fn heads_loop(inner: Arc<Inner>, state: Arc<TopicState>) {
    let mut interval = HEADS_FIRST_INTERVAL;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {
                inner.send_heads(&state).await;
                interval = (interval * 2).min(HEADS_MAX_INTERVAL);
            }
            _ = state.heads_restart.notified() => {
                inner.send_heads(&state).await;
                interval = HEADS_FIRST_INTERVAL;
            }
        }
    }
}

struct LogosOpPublisher {
    inner: Arc<Inner>,
    state: Arc<TopicState>,
}

impl OpPublisher for LogosOpPublisher {
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.inner.publish(&self.state, body, false))
    }

    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(self.inner.publish(&self.state, body, true))
    }
}

struct LogosEphemeralPublisher {
    inner: Arc<Inner>,
    state: Arc<TopicState>,
}

impl EphemeralPublisher for LogosEphemeralPublisher {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            let body = p2panda_core::cbor::encode_cbor(&message)
                .map_err(|error| PublishError::Backend(error.to_string()))?;
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_micros() as u64)
                .unwrap_or(0);
            let signed =
                SignedEphemeral::new(&self.state.topic, &self.inner.signing_key, timestamp, body);
            self.inner
                .send(
                    &self.state,
                    &self.state.keys.ephemeral_topic,
                    &Frame::Ephemeral(signed),
                )
                .await;
            Ok(())
        })
    }
}
