//! p2panda operations over Logos Delivery.
//!
//! The protocol is in `docs/logos/spec.md` of vpavlin/reflection: signed p2panda operations
//! (per-author logs, prune flag) travel encrypted on two content topics per topic, and peers that
//! missed operations catch up by broadcasting the heads of their logs. Nothing in it depends on
//! what the operations contain, so any p2panda application can use it; Reflection is one.
//!
//! [`LogosSync`] is one endpoint on one network id: it stores operations, sends and receives
//! them through a [`DeliveryBus`], and counts what it sends the way RLN meters it. Logos Delivery
//! charges per message published, so the sender is frugal: nothing is published for a topic that
//! is not active except answers to other peers' catch-up requests, and those are capped.
//!
//! [`bridge`] has the pieces to relay a topic between a p2panda network and Logos.
pub mod bridge;
mod bus;
#[cfg(feature = "ffi")]
pub mod ffi_bus;
mod store;
mod wire;

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub use bus::{BusError, BusMessage, DeliveryBus, MemoryBus};
#[cfg(feature = "ffi")]
pub use ffi_bus::LibLogosDeliveryBus;
pub use wire::MAX_FRAME_LEN;

use p2panda::operation::{Extensions, Header};
use p2panda::{Hash, NetworkId, Topic, VerifyingKey};
use p2panda_core::{Body, SigningKey};
use serde::Serialize;
use sqlx::SqlitePool;
use tokio::sync::{Mutex, broadcast, mpsc};
use tracing::{debug, error, info, warn};

use self::store::OpStore;
use self::wire::{Bytes, Frame, SignedEphemeral, TopicKeys};

/// The two content topics (operations, ephemeral) a topic uses on Logos Delivery.
pub fn content_topics(network_id: &NetworkId, topic: &Topic) -> (String, String) {
    let keys = TopicKeys::new(network_id, topic);
    (keys.ops_topic, keys.ephemeral_topic)
}

/// A boxed, sendable future.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// When a topic becomes active (the user opens it), its heads are sent at these delays, in case
/// the first copy is lost, and then not again until the topic is next activated. Gaps found
/// while active are asked for at once (`send_heads_behind`). Nothing is sent periodically:
/// under RLN every message counts, and a topic nobody looks at needs no catch-up.
const HEADS_ON_ACTIVATE: [Duration; 3] = [
    Duration::from_millis(500),
    Duration::from_secs(5),
    Duration::from_secs(30),
];

/// RLN epoch length of Logos Delivery (`rlnEpochSizeSec`).
pub const RLN_EPOCH_SECONDS: u64 = 600;

/// A repair waits a random time in this range, so that one peer answers rather than all.
const REPAIR_DELAY_MIN_MS: u64 = 50;
const REPAIR_DELAY_MAX_MS: u64 = 500;

/// A peer's `heads` is answered at most once per this window. Anyone with the topic id can send
/// `heads` (or replay an old one), and each answer is paid from this device's RLN budget.
const REPAIR_ANSWER_WINDOW: Duration = Duration::from_secs(30);

/// Operations waiting for a missing predecessor, per author.
const MAX_PENDING_PER_AUTHOR: usize = 256;

/// The library's errors.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct Error(pub String);

impl From<sqlx::Error> for Error {
    fn from(error: sqlx::Error) -> Self {
        Self(error.to_string())
    }
}

/// A p2panda operation as stored and sent here: a header (p2panda's `Extensions`, so it carries
/// the log id of its topic and the prune flag) and an optional body, exactly as encoded.
#[derive(Clone, Debug)]
pub struct Op {
    pub hash: Hash,
    pub header: Header,
    pub header_bytes: Vec<u8>,
    pub body: Option<Vec<u8>>,
}

impl Op {
    /// An operation from its encoded header and its body.
    pub fn decode(header_bytes: Vec<u8>, body: Option<Vec<u8>>) -> Option<Self> {
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
    pub fn validate(&self) -> bool {
        p2panda_core::validate_operation(self.to_operation()).is_ok()
    }

    /// An operation created elsewhere (by p2panda, when bridging).
    pub fn from_parts(header: Header, body: Option<Vec<u8>>) -> Self {
        Self {
            hash: header.hash(),
            header_bytes: header.to_bytes(),
            header,
            body,
        }
    }

    pub fn to_operation(&self) -> p2panda::operation::Operation {
        p2panda::operation::Operation {
            hash: self.hash,
            header: self.header.clone(),
            body: self.body.as_deref().map(Body::new),
        }
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

    /// The size on the wire, header plus body.
    pub fn size(&self) -> usize {
        self.header_bytes.len() + self.body.as_ref().map_or(0, Vec::len)
    }

    fn frame(&self) -> Frame {
        Frame::Operation {
            header: Bytes(self.header_bytes.clone()),
            body: self.body.clone().map(Bytes),
        }
    }
}

/// An ephemeral message from another peer.
#[derive(Clone, Debug)]
pub struct Ephemeral {
    pub author: VerifyingKey,
    /// Microseconds since the Unix epoch, as the author signed it.
    pub timestamp: u64,
    /// The body as the author sent it (Reflection: CBOR of its `EphemeralMessage`).
    pub body: Vec<u8>,
}

/// What an ephemeral message is, for the counters.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EphemeralKind {
    /// Hello, ping, bye.
    Presence,
    /// Anything else (cursors).
    Other,
}

/// What we sent or received, for the counters.
#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Op,
    Snapshot,
    Heads,
    Repair,
    Relayed,
    Presence,
    Cursor,
}

/// Messages sent or received, by kind.
#[derive(Clone, Debug, Default, Serialize)]
pub struct MessageCounts {
    pub ops: u64,
    pub snapshots: u64,
    pub heads: u64,
    pub repairs: u64,
    pub relayed: u64,
    pub presence: u64,
    pub cursors: u64,
    pub bytes: u64,
    /// Not sent: the (pretend) budget of the epoch was used up. Not part of `messages()`.
    pub blocked: u64,
    /// Repairs not sent: the topic's repair cap of the epoch was reached, or the asking peer was
    /// answered less than `REPAIR_ANSWER_WINDOW` ago. Not part of `messages()`.
    pub repairs_capped: u64,
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

    fn add(&mut self, kind: Kind, bytes: usize) {
        match kind {
            Kind::Op => self.ops += 1,
            Kind::Snapshot => self.snapshots += 1,
            Kind::Heads => self.heads += 1,
            Kind::Repair => self.repairs += 1,
            Kind::Relayed => self.relayed += 1,
            Kind::Presence => self.presence += 1,
            Kind::Cursor => self.cursors += 1,
        }
        self.bytes += bytes as u64;
    }
}

/// A peer heard from on a topic.
#[derive(Clone, Debug, Serialize)]
pub struct PeerSeen {
    pub key: String,
    pub seconds_ago: u64,
}

/// Counters of one topic.
#[derive(Clone, Debug, Serialize)]
pub struct TopicStats {
    pub topic: String,
    pub active: bool,
    pub sent: MessageCounts,
    pub sent_this_epoch: MessageCounts,
    pub received: MessageCounts,
    pub peers: Vec<PeerSeen>,
}

/// What an endpoint sent and received, per RLN epoch (aligned to Unix time).
#[derive(Clone, Debug, Serialize)]
pub struct Stats {
    pub epoch_seconds: u64,
    pub epoch_elapsed: u64,
    pub uptime: u64,
    pub sent: MessageCounts,
    pub sent_this_epoch: MessageCounts,
    pub sent_last_epoch: MessageCounts,
    pub received: MessageCounts,
    pub topics: Vec<TopicStats>,
    pub budget: Option<u64>,
}

/// Tells presence from other ephemeral messages, for the counters.
pub type EphemeralClassifier = Arc<dyn Fn(&[u8]) -> EphemeralKind + Send + Sync>;

/// Settings of an endpoint.
#[derive(Clone)]
pub struct Config {
    /// Classifies incoming ephemeral bodies. Default: all `Other`.
    pub classify_ephemeral: EphemeralClassifier,
    /// At most this many repair messages per topic per epoch. `None`: no cap.
    pub repair_cap_per_topic: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            classify_ephemeral: Arc::new(|_| EphemeralKind::Other),
            repair_cap_per_topic: Some(50),
        }
    }
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Counters of messages sent, per RLN epoch, and received.
#[derive(Default)]
struct Counters {
    sent: MessageCounts,
    epoch: u64,
    sent_this_epoch: MessageCounts,
    sent_last_epoch: MessageCounts,
    received: MessageCounts,
}

impl Counters {
    fn roll(&mut self) {
        let epoch = unix_secs() / RLN_EPOCH_SECONDS;
        if epoch != self.epoch {
            self.sent_last_epoch = if epoch == self.epoch + 1 {
                std::mem::take(&mut self.sent_this_epoch)
            } else {
                MessageCounts::default()
            };
            self.sent_this_epoch = MessageCounts::default();
            self.epoch = epoch;
        }
    }

    fn sent(&mut self, kind: Kind, bytes: usize) {
        self.roll();
        self.sent.add(kind, bytes);
        self.sent_this_epoch.add(kind, bytes);
    }

    fn blocked(&mut self) {
        self.roll();
        self.sent.blocked += 1;
        self.sent_this_epoch.blocked += 1;
    }

    fn repair_capped(&mut self) {
        self.roll();
        self.sent.repairs_capped += 1;
        self.sent_this_epoch.repairs_capped += 1;
    }

    fn received(&mut self, kind: Kind, bytes: usize) {
        self.received.add(kind, bytes);
    }
}

/// One endpoint of the protocol on one network id.
#[derive(Clone)]
pub struct LogosSync {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for LogosSync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogosSync").finish_non_exhaustive()
    }
}

struct Inner {
    bus: Arc<dyn DeliveryBus>,
    /// This endpoint's key: it signs our own operations, heads and ephemeral messages.
    signing_key: SigningKey,
    network_id: NetworkId,
    store: OpStore,
    config: Config,
    topics: Mutex<HashMap<Topic, Arc<TopicState>>>,
    /// All topics, for `stats` (which can't wait for the async lock).
    topic_list: StdMutex<Vec<Arc<TopicState>>>,
    counters: StdMutex<Counters>,
    started: Instant,
    /// Pretend RLN budget per epoch (`set_message_budget`).
    budget: StdMutex<Option<u64>>,
}

impl LogosSync {
    /// An endpoint. Its operations are stored in `pool` (table `logos_operations`).
    pub async fn new(
        bus: Arc<dyn DeliveryBus>,
        signing_key: SigningKey,
        network_id: NetworkId,
        pool: SqlitePool,
        config: Config,
    ) -> Result<Self, Error> {
        let store = OpStore::new(pool).await?;
        Ok(Self {
            inner: Arc::new(Inner {
                bus,
                signing_key,
                network_id,
                store,
                config,
                topics: Mutex::new(HashMap::new()),
                topic_list: StdMutex::new(Vec::new()),
                counters: StdMutex::new(Counters::default()),
                started: Instant::now(),
                budget: StdMutex::new(None),
            }),
        })
    }

    /// Operations of a topic: everything stored first, in log order, then whatever is accepted
    /// from Logos peers (not what this endpoint publishes or relays itself). Several subscribers
    /// may exist; each gets everything.
    pub async fn subscribe(&self, topic: Topic) -> Result<mpsc::UnboundedReceiver<Op>, Error> {
        let state = self.inner.topic(topic).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        // Holding the lock: no operation arrives between the replay and going live.
        let mut senders = state.senders.lock().await;
        for op in self.inner.store.all(&topic).await? {
            let _ = tx.send(op);
        }
        senders.ops.push(tx);
        Ok(rx)
    }

    /// Ephemeral messages of a topic from other peers, from now on.
    pub async fn subscribe_ephemeral(
        &self,
        topic: Topic,
    ) -> Result<mpsc::UnboundedReceiver<Ephemeral>, Error> {
        let state = self.inner.topic(topic).await?;
        let (tx, rx) = mpsc::unbounded_channel();
        state.senders.lock().await.ephemeral.push(tx);
        Ok(rx)
    }

    /// Append an operation with this body (already encoded, e.g. CBOR) to this endpoint's log on
    /// the topic, store it and send it. `prune`: a snapshot, earlier operations of the log may be
    /// deleted everywhere.
    pub async fn publish(&self, topic: Topic, body: Vec<u8>, prune: bool) -> Result<Op, Error> {
        let state = self.inner.topic(topic).await?;
        self.inner.publish(&state, body, prune).await
    }

    /// Store an operation created elsewhere (signed by anyone, e.g. relayed from p2panda) and send
    /// it to Logos peers. Operations already known are ignored, which stops bridged operations
    /// from going round in circles. Returns whether it was sent.
    pub async fn relay(&self, topic: Topic, op: Op) -> Result<bool, Error> {
        let state = self.inner.topic(topic).await?;
        if self.inner.store.has(&op.hash).await? {
            return Ok(false);
        }
        if !op.validate() {
            return Err(Error(format!("not relaying invalid operation {}", op.hash)));
        }
        if op.size() > MAX_FRAME_LEN {
            return Err(Error(format!(
                "operation {} of {} bytes is over the {MAX_FRAME_LEN} byte limit of Logos Delivery",
                op.hash,
                op.size()
            )));
        }
        self.inner.store.insert(&topic, &op).await?;
        state.mark_seen(op.hash);
        self.inner
            .send(&state, &state.keys.ops_topic, &op.frame(), Kind::Relayed)
            .await;
        Ok(true)
    }

    /// Send an ephemeral message (signed by this endpoint).
    pub async fn publish_ephemeral(
        &self,
        topic: Topic,
        body: Vec<u8>,
        kind: EphemeralKind,
    ) -> Result<(), Error> {
        let state = self.inner.topic(topic).await?;
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_micros() as u64)
            .unwrap_or(0);
        let signed = SignedEphemeral::new(&topic, &self.inner.signing_key, timestamp, body);
        let kind = match kind {
            EphemeralKind::Presence => Kind::Presence,
            EphemeralKind::Other => Kind::Cursor,
        };
        self.inner
            .send(
                &state,
                &state.keys.ephemeral_topic,
                &Frame::Ephemeral(signed),
                kind,
            )
            .await;
        Ok(())
    }

    /// Whether the user has the topic open. Only active topics send heads (catch-up); becoming
    /// active sends them a few times.
    pub async fn set_active(&self, topic: Topic, active: bool) -> Result<(), Error> {
        let state = self.inner.topic(topic).await?;
        let was = state.active.swap(active, Ordering::SeqCst);
        if active && !was {
            state.restart_heads();
        }
        Ok(())
    }

    /// The latest operation of each author's log on the topic.
    pub async fn heads(&self, topic: Topic) -> Result<Vec<Op>, Error> {
        Ok(self.inner.store.heads(&topic).await?)
    }

    /// The latest operation of one author's log on the topic.
    pub async fn head(&self, topic: Topic, author: &VerifyingKey) -> Result<Option<Op>, Error> {
        Ok(self.inner.store.head(&topic, author).await?)
    }

    /// Pretend the network allows at most `budget` messages per epoch (RLN): further messages
    /// are not sent but counted as `blocked`. `None`: no limit.
    pub fn set_message_budget(&self, budget: Option<u64>) {
        *self.inner.budget.lock().unwrap() = budget;
    }

    pub fn stats(&self) -> Stats {
        self.inner.stats()
    }

    pub fn verifying_key(&self) -> VerifyingKey {
        self.inner.signing_key.verifying_key()
    }
}

#[derive(Default)]
struct Senders {
    ops: Vec<mpsc::UnboundedSender<Op>>,
    ephemeral: Vec<mpsc::UnboundedSender<Ephemeral>>,
}

/// Everything the endpoint keeps per topic.
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
    /// The user has the topic open: only then do we send heads.
    active: AtomicBool,
    counters: StdMutex<Counters>,
    /// Other devices heard from, and when.
    peers: StdMutex<HashMap<VerifyingKey, Instant>>,
    /// When each peer's `heads` was last answered with a repair.
    answered: StdMutex<HashMap<VerifyingKey, Instant>>,
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

    fn heard_from(&self, peer: VerifyingKey) {
        self.peers.lock().unwrap().insert(peer, Instant::now());
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
    async fn topic(self: &Arc<Self>, topic: Topic) -> Result<Arc<TopicState>, Error> {
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
            active: AtomicBool::new(false),
            counters: StdMutex::new(Counters::default()),
            peers: StdMutex::new(HashMap::new()),
            answered: StdMutex::new(HashMap::new()),
        });

        // Listen before subscribing, so nothing that arrives is lost.
        let rx = self.bus.messages();
        for content_topic in [&keys.ops_topic, &keys.ephemeral_topic] {
            self.bus
                .subscribe(content_topic.clone())
                .await
                .map_err(|error| Error(error.to_string()))?;
        }
        info!(%topic, ops = %keys.ops_topic, "subscribed on Logos Delivery");

        tokio::spawn(receive_loop(self.clone(), state.clone(), rx));
        tokio::spawn(heads_loop(self.clone(), state.clone()));

        topics.insert(topic, state.clone());
        self.topic_list.lock().unwrap().push(state.clone());
        Ok(state)
    }

    fn stats(&self) -> Stats {
        let mut counters = self.counters.lock().unwrap();
        counters.roll();
        let now = Instant::now();
        let mut topics: Vec<TopicStats> = self
            .topic_list
            .lock()
            .unwrap()
            .iter()
            .map(|state| {
                let mut c = state.counters.lock().unwrap();
                c.roll();
                let mut peers: Vec<PeerSeen> = state
                    .peers
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(key, at)| PeerSeen {
                        key: key.to_hex(),
                        seconds_ago: now.duration_since(*at).as_secs(),
                    })
                    .collect();
                peers.sort_by_key(|p| p.seconds_ago);
                TopicStats {
                    topic: hex::encode(state.topic.to_bytes()),
                    active: state.active.load(Ordering::SeqCst),
                    sent: c.sent.clone(),
                    sent_this_epoch: c.sent_this_epoch.clone(),
                    received: c.received.clone(),
                    peers,
                }
            })
            .collect();
        topics.sort_by(|a, b| b.active.cmp(&a.active).then(a.topic.cmp(&b.topic)));
        Stats {
            epoch_seconds: RLN_EPOCH_SECONDS,
            epoch_elapsed: unix_secs() % RLN_EPOCH_SECONDS,
            uptime: self.started.elapsed().as_secs(),
            sent: counters.sent.clone(),
            sent_this_epoch: counters.sent_this_epoch.clone(),
            sent_last_epoch: counters.sent_last_epoch.clone(),
            received: counters.received.clone(),
            topics,
            budget: *self.budget.lock().unwrap(),
        }
    }

    async fn send(&self, state: &TopicState, content_topic: &str, frame: &Frame, kind: Kind) {
        let payload = state.keys.seal(frame);
        let bytes = payload.len();
        {
            let mut counters = self.counters.lock().unwrap();
            counters.roll();
            let budget = *self.budget.lock().unwrap();
            if budget.is_some_and(|budget| counters.sent_this_epoch.messages() >= budget) {
                // What RLN would do: the epoch's messages are used up.
                counters.blocked();
                state.counters.lock().unwrap().blocked();
                debug!(?kind, "message budget used up, not sending");
                return;
            }
            counters.sent(kind, bytes);
        }
        state.counters.lock().unwrap().sent(kind, bytes);
        if let Err(error) = self.bus.publish(content_topic.to_owned(), payload).await {
            warn!("failed to publish on {content_topic}: {error}");
        }
    }

    fn count_received(&self, state: &TopicState, kind: Kind, bytes: usize) {
        self.counters.lock().unwrap().received(kind, bytes);
        state.counters.lock().unwrap().received(kind, bytes);
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
            nonce: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0),
        };
        self.send(state, &state.keys.ops_topic, &frame, Kind::Heads)
            .await;
    }

    /// Send our heads soon because a peer is ahead of us, at most twice a second. Only on an
    /// active topic: an inactive one catches up when the user opens it.
    async fn send_heads_behind(&self, state: &TopicState) {
        if !state.active.load(Ordering::SeqCst) {
            return;
        }
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
    async fn publish(&self, state: &TopicState, body: Vec<u8>, prune: bool) -> Result<Op, Error> {
        let _guard = state.write_lock.lock().await;
        let author = self.signing_key.verifying_key();
        let head = self.store.head(&state.topic, &author).await?;

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
            return Err(Error(format!(
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
        self.store.insert(&state.topic, &op).await?;
        state.mark_seen(op.hash);

        let kind = if prune { Kind::Snapshot } else { Kind::Op };
        self.send(state, &state.keys.ops_topic, &op.frame(), kind)
            .await;
        // Not delivered to our own subscribers: whoever published it already has it.
        Ok(op)
    }

    /// Hand an accepted operation to the subscribers.
    async fn deliver(&self, state: &TopicState, op: &Op) {
        let mut senders = state.senders.lock().await;
        senders.ops.retain(|tx| tx.send(op.clone()).is_ok());
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

        if fits(&head, &op) {
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

    async fn accept(&self, state: &TopicState, op: Op) {
        if let Err(error) = self.store.insert(&state.topic, &op).await {
            error!("can't store operation: {error}");
            return;
        }
        self.deliver(state, &op).await;
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
                    Some((_, op)) if fits(&head, op) => log.pop_first().map(|(_, op)| op),
                    _ => None,
                }
            };
            match next {
                Some(op) => self.accept(state, op).await,
                None => return,
            }
        }
    }

    /// May we answer this peer's `heads` with a repair now? At most once per peer per
    /// `REPAIR_ANSWER_WINDOW`, so a flood or a replayed `heads` can't make us pay again and again.
    fn may_answer(&self, state: &TopicState, from: VerifyingKey) -> bool {
        let mut answered = state.answered.lock().unwrap();
        let now = Instant::now();
        if answered
            .get(&from)
            .is_some_and(|at| now.duration_since(*at) < REPAIR_ANSWER_WINDOW)
        {
            return false;
        }
        answered.insert(from, now);
        if answered.len() > 1000 {
            answered.retain(|_, at| now.duration_since(*at) < REPAIR_ANSWER_WINDOW);
        }
        true
    }

    /// Whether the topic's repair cap for this epoch still allows one more repair message.
    fn repair_allowed(&self, state: &TopicState) -> bool {
        let Some(cap) = self.config.repair_cap_per_topic else {
            return true;
        };
        let mut counters = state.counters.lock().unwrap();
        counters.roll();
        counters.sent_this_epoch.repairs < cap
    }

    fn count_repair_capped(&self, state: &TopicState) {
        self.counters.lock().unwrap().repair_capped();
        state.counters.lock().unwrap().repair_capped();
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
        state.heard_from(from);

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
        if !self.may_answer(state, from) {
            debug!(%from, "already answered this peer's heads recently");
            self.count_repair_capped(state);
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
                    if !inner.repair_allowed(&state) {
                        inner.count_repair_capped(&state);
                        continue;
                    }
                    state.mark_seen(op.hash);
                    inner
                        .send(&state, &state.keys.ops_topic, &op.frame(), Kind::Repair)
                        .await;
                }
            }
        });
    }

    async fn receive_ephemeral(&self, state: &TopicState, message: SignedEphemeral) {
        if message.author == self.signing_key.verifying_key() || !message.verify(&state.topic) {
            return;
        }
        state.heard_from(message.author);
        let kind = match (self.config.classify_ephemeral)(&message.body.0) {
            EphemeralKind::Presence => Kind::Presence,
            EphemeralKind::Other => Kind::Cursor,
        };
        self.count_received(state, kind, message.body.0.len());
        let ephemeral = Ephemeral {
            author: message.author,
            timestamp: message.timestamp,
            body: message.body.0,
        };
        let mut senders = state.senders.lock().await;
        senders
            .ephemeral
            .retain(|tx| tx.send(ephemeral.clone()).is_ok());
    }
}

/// Whether an operation directly follows the head of its log (or starts or prunes it).
fn fits(head: &Option<Op>, op: &Op) -> bool {
    if op.prunes() {
        return head.as_ref().is_none_or(|head| op.seq() > head.seq());
    }
    match head {
        None => op.seq() == 0,
        Some(head) => op.seq() == head.seq() + 1 && op.header.backlink == Some(head.hash),
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
        let bytes = message.payload.len();
        match frame {
            Frame::Operation { header, body } => {
                match Op::decode(header.0, body.map(|body| body.0)) {
                    Some(op) => {
                        // Our own operations come back from the relay: not counted.
                        if op.author() != inner.signing_key.verifying_key() {
                            let kind = if op.prunes() {
                                Kind::Snapshot
                            } else {
                                Kind::Op
                            };
                            inner.count_received(&state, kind, bytes);
                            state.heard_from(op.author());
                        }
                        inner.receive_op(&state, op).await
                    }
                    None => warn!("dropping undecodable operation"),
                }
            }
            Frame::Heads { from, heads, .. } => {
                if from != inner.signing_key.verifying_key() {
                    inner.count_received(&state, Kind::Heads, bytes);
                }
                inner.receive_heads(&state, from, heads).await
            }
            Frame::Ephemeral(message) => inner.receive_ephemeral(&state, message).await,
        }
    }
}

/// Sends heads when the topic is activated (see `HEADS_ON_ACTIVATE`), and never otherwise.
async fn heads_loop(inner: Arc<Inner>, state: Arc<TopicState>) {
    loop {
        state.heads_restart.notified().await;
        'burst: loop {
            let mut previous = Duration::ZERO;
            for at in HEADS_ON_ACTIVATE {
                tokio::select! {
                    _ = tokio::time::sleep(at - previous) => {}
                    // Activated again: start over.
                    _ = state.heads_restart.notified() => continue 'burst,
                }
                previous = at;
                if !state.active.load(Ordering::SeqCst) {
                    break 'burst;
                }
                inner.send_heads(&state).await;
            }
            break;
        }
    }
}
