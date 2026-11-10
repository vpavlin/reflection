use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock};

use chrono::{DateTime, Utc};
use p2panda::node::SpawnError;
use p2panda::{NetworkId, RelayUrl, SigningKey, VerifyingKey};
use thiserror::Error;
use tokio::sync::Notify;
use tracing::info;

use crate::backend::SyncBackend;
use crate::backend::p2panda_backend::P2pandaBackend;
use crate::database::{database_pool, run_migrations};
pub use crate::topic_store::TrackedAuthor;
use crate::topic_store::{TopicRow, TrackedTopicStore};
use crate::topic_stream::{TopicStream, TopicStreamError, TopicStreamInner};
use crate::traits::TopicSubscription;

static DATABASE_FILE: &str = "database-v2.sqlite";

static RELAY_URL: LazyLock<RelayUrl> = LazyLock::new(|| {
    "https://euc1-1.relay.n0.iroh.link."
        .parse()
        .expect("valid relay URL")
});

static BOOTSTRAP_NODE_ID: LazyLock<VerifyingKey> = LazyLock::new(|| {
    "f992f1f5702087f89941ffb97ec3e7915996572c6814344a716c990b5537370c"
        .parse()
        .expect("valid bootstrap node id")
});

#[derive(Debug, Error)]
pub enum NodeError {
    #[error(transparent)]
    RuntimeStartup(#[from] std::io::Error),

    #[error(transparent)]
    RuntimeSpawn(#[from] tokio::task::JoinError),

    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    DatabaseMigration(#[from] sqlx::migrate::MigrateError),

    #[error(transparent)]
    NodeSpawn(#[from] SpawnError),

    #[error("{0}")]
    Backend(String),
}

/// Which sync backend `Node::new` uses, from `REFLECTION_BACKEND`:
///
/// - `p2panda` (default): p2panda over iroh;
/// - `logos-memory` / `logos`: the Logos protocol over an in-process bus / Logos Delivery;
/// - `bridge-memory` / `bridge`: both p2panda and Logos, relaying between them (bridge.rs);
/// - `dual-memory` (tests): both, with p2panda through a `P2pandaBus` (dual.rs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendChoice {
    P2panda,
    #[cfg(feature = "logos")]
    LogosMemory,
    #[cfg(feature = "logos")]
    BridgeMemory,
    #[cfg(feature = "logos-ffi")]
    Logos,
    #[cfg(feature = "logos-ffi")]
    Bridge,
    /// p2panda through the `P2pandaBus` of an in-process node (tests the external backend).
    #[cfg(any(test, feature = "test_utils"))]
    P2pandaExternal,
    /// Both networks: Logos on the in-process bus, p2panda through a `P2pandaBus` (dual.rs).
    #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
    DualMemory,
}

#[cfg(feature = "logos")]
type ExternalBus = Arc<dyn crate::logos::DeliveryBus>;

/// The transport of a node, when the caller decides instead of `REFLECTION_BACKEND`.
pub(crate) enum Transport {
    P2panda,
    #[cfg(feature = "logos")]
    Logos(ExternalBus),
    #[cfg(feature = "logos")]
    Bridge(ExternalBus),
    /// A p2panda node run by the host.
    External(Arc<dyn crate::external::P2pandaBus>),
    /// Both networks: Logos Delivery and a p2panda node run by the host.
    #[cfg(feature = "logos")]
    Dual(ExternalBus, Arc<dyn crate::external::P2pandaBus>),
    /// Both networks, for tests: the in-process Logos bus and an in-process p2panda node behind
    /// a `P2pandaBus`.
    #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
    DualMemory,
    /// As DualMemory, but the first `n` p2panda opens fail (the host timing out).
    #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
    DualMemoryFlaky(usize),
}

/// Start a Logos Delivery node through liblogosdelivery, configured from the environment:
/// `REFLECTION_LOGOS_LIB` (path to the library, required), `REFLECTION_LOGOS_CONFIG` (node
/// configuration JSON). Without a configuration, test builds peer only with other nodes of this
/// process, and other builds join the public Logos test network.
#[cfg(feature = "logos-ffi")]
async fn logos_ffi_bus() -> Result<ExternalBus, NodeError> {
    use crate::backend::logos::ffi_bus;

    let library = std::env::var("REFLECTION_LOGOS_LIB").map_err(|_| {
        NodeError::Backend("REFLECTION_BACKEND=logos needs REFLECTION_LOGOS_LIB".into())
    })?;
    let configured = std::env::var("REFLECTION_LOGOS_CONFIG").ok();
    let local = configured.is_none() && cfg!(any(test, feature = "test_utils"));
    let config = match configured {
        Some(config) => config,
        None if local => ffi_bus::local_config(),
        None => ffi_bus::default_config(),
    };

    let bus = crate::logos::LibLogosDeliveryBus::new(std::path::Path::new(&library), &config)
        .await
        .map_err(|error| NodeError::Backend(error.to_string()))?;
    if local {
        ffi_bus::register_local(&bus).await;
    }
    Ok(Arc::new(bus))
}

async fn p2panda_backend(
    signing_key: SigningKey,
    network_id: NetworkId,
    pool: sqlx::SqlitePool,
) -> Result<P2pandaBackend, NodeError> {
    Ok(P2pandaBackend::new(
        p2panda_node(signing_key, network_id, pool).await?,
    ))
}

async fn p2panda_node(
    signing_key: SigningKey,
    network_id: NetworkId,
    pool: sqlx::SqlitePool,
) -> Result<p2panda::Node, NodeError> {
    let mut builder = p2panda::Node::builder()
        .network_id(network_id)
        .signing_key(signing_key)
        .database_pool(pool);

    // Don't connect to any servers during testing.
    if cfg!(not(any(test, feature = "test_utils"))) {
        builder = builder
            .bootstrap(*BOOTSTRAP_NODE_ID, RELAY_URL.clone())
            .relay_url(RELAY_URL.clone());
    }

    Ok(builder.spawn().await?)
}

fn backend_choice() -> Result<BackendChoice, String> {
    let value = std::env::var("REFLECTION_BACKEND").unwrap_or_default();
    match value.as_str() {
        "" | "p2panda" => Ok(BackendChoice::P2panda),
        #[cfg(feature = "logos")]
        "logos-memory" => Ok(BackendChoice::LogosMemory),
        #[cfg(feature = "logos")]
        "bridge-memory" => Ok(BackendChoice::BridgeMemory),
        #[cfg(feature = "logos-ffi")]
        "logos" => Ok(BackendChoice::Logos),
        #[cfg(feature = "logos-ffi")]
        "bridge" => Ok(BackendChoice::Bridge),
        #[cfg(any(test, feature = "test_utils"))]
        "p2panda-external" => Ok(BackendChoice::P2pandaExternal),
        #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
        "dual-memory" => Ok(BackendChoice::DualMemory),
        other => Err(format!(
            "REFLECTION_BACKEND={other} is not available in this build"
        )),
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Default)]
pub enum ConnectionMode {
    #[default]
    None,
    Bluetooth,
    Network,
}

#[derive(Clone, Debug)]
pub struct TrackedTopic {
    pub topic: p2panda::Topic,
    pub name: Option<String>,
    pub last_accessed: Option<DateTime<Utc>>,
    pub authors: Vec<TrackedAuthor>,
}

#[derive(Debug)]
enum OwnedRuntimeOrHandle {
    Handle(tokio::runtime::Handle),
    OwnedRuntime(tokio::runtime::Runtime),
}

impl std::ops::Deref for OwnedRuntimeOrHandle {
    type Target = tokio::runtime::Handle;

    fn deref(&self) -> &Self::Target {
        match self {
            OwnedRuntimeOrHandle::Handle(handle) => handle,
            OwnedRuntimeOrHandle::OwnedRuntime(runtime) => runtime.handle(),
        }
    }
}

#[derive(Debug)]
pub struct Node {
    inner: Arc<NodeInner>,
    runtime: OwnedRuntimeOrHandle,
}

impl Node {
    pub async fn new(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
    ) -> Result<Self, NodeError> {
        Self::spawn(signing_key, network_id.into(), db_location, None).await
    }

    /// Create a node that syncs over Logos Delivery through the given bus, whatever
    /// `REFLECTION_BACKEND` says. Used by hosts that own the Delivery node (e.g. Logos Basecamp).
    #[cfg(feature = "logos")]
    pub async fn with_delivery_bus(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
        bus: Arc<dyn crate::logos::DeliveryBus>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::Logos(bus)),
        )
        .await
    }

    /// Create a node on both p2panda and Logos Delivery (through the given bus) that relays
    /// documents between the two networks, whatever `REFLECTION_BACKEND` says.
    #[cfg(feature = "logos")]
    pub async fn with_bridge(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
        bus: Arc<dyn crate::logos::DeliveryBus>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::Bridge(bus)),
        )
        .await
    }

    /// Create a node on both networks, whatever `REFLECTION_BACKEND` says: Logos Delivery through
    /// `delivery` and the host's p2panda node through `p2panda` (which must sign with
    /// `signing_key`). Operations are written once and passed between the networks (dual.rs).
    #[cfg(feature = "logos")]
    pub async fn with_dual(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
        delivery: Arc<dyn crate::logos::DeliveryBus>,
        p2panda: Arc<dyn crate::external::P2pandaBus>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::Dual(delivery, p2panda)),
        )
        .await
    }

    /// Create a node on both networks for tests: the in-process Logos bus, and p2panda through
    /// a `P2pandaBus` over an in-process p2panda node.
    #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
    pub async fn with_dual_memory(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::DualMemory),
        )
        .await
    }

    /// As `with_dual_memory`, but the first `fail_opens` opens on p2panda fail.
    #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
    pub async fn with_dual_memory_flaky(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        fail_opens: usize,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            None,
            Some(Transport::DualMemoryFlaky(fail_opens)),
        )
        .await
    }

    /// Create a node that syncs through a p2panda node run by the host (`P2pandaBus`), whatever
    /// `REFLECTION_BACKEND` says. The host's node must sign with `signing_key`.
    pub async fn with_p2panda_bus(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
        bus: Arc<dyn crate::external::P2pandaBus>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::External(bus)),
        )
        .await
    }

    /// Create a node on p2panda only, whatever `REFLECTION_BACKEND` says.
    #[cfg(any(test, feature = "test_utils"))]
    pub async fn with_p2panda(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_location: Option<&Path>,
    ) -> Result<Self, NodeError> {
        Self::spawn(
            signing_key,
            network_id.into(),
            db_location,
            Some(Transport::P2panda),
        )
        .await
    }

    async fn spawn(
        signing_key: SigningKey,
        network_id: NetworkId,
        db_location: Option<&Path>,
        transport: Option<Transport>,
    ) -> Result<Self, NodeError> {
        let runtime = if let Ok(handle) = tokio::runtime::Handle::try_current() {
            OwnedRuntimeOrHandle::Handle(handle)
        } else {
            OwnedRuntimeOrHandle::OwnedRuntime(
                tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?,
            )
        };

        let inner = {
            let db_file = db_location.map(|location| location.join(DATABASE_FILE));

            runtime
                .spawn(NodeInner::new(signing_key, network_id, db_file, transport))
                .await??
        };

        Ok(Self {
            inner: Arc::new(inner),
            runtime,
        })
    }

    pub async fn set_connection_mode(
        &self,
        connection_mode: ConnectionMode,
    ) -> Result<(), NodeError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.set_connection_mode(connection_mode).await })
            .await??;

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), NodeError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move {
                inner.shutdown().await;
            })
            .await?;

        Ok(())
    }

    pub async fn topics(&self) -> Result<Vec<TrackedTopic>, TopicStreamError> {
        let inner = self.inner.clone();
        let topics = self
            .runtime
            .spawn(async move { inner.topic_store.topics().await })
            .await??;

        let topics = topics
            .into_iter()
            .map(|topic| {
                let TopicRow {
                    id,
                    name,
                    last_accessed,
                    authors,
                } = topic;
                TrackedTopic {
                    topic: id,
                    name,
                    last_accessed,
                    authors,
                }
            })
            .collect();

        Ok(topics)
    }

    pub async fn stream<T>(
        &self,
        topic: impl Into<p2panda::Topic>,
        subscription: T,
    ) -> Result<TopicStream<T>, TopicStreamError>
    where
        T: TopicSubscription + 'static,
    {
        self.stream_with(topic.into(), subscription, true).await
    }

    /// Like `stream`, for a topic the user does not have open: it receives everything, but
    /// announces nothing until `TopicStream::set_active(true)`.
    pub async fn stream_in_background<T>(
        &self,
        topic: impl Into<p2panda::Topic>,
        subscription: T,
    ) -> Result<TopicStream<T>, TopicStreamError>
    where
        T: TopicSubscription + 'static,
    {
        self.stream_with(topic.into(), subscription, false).await
    }

    /// Message counters of the transport, if it pays per message (Logos Delivery).
    pub fn transport_stats(&self) -> Option<crate::TransportStats> {
        self.inner.backend.stats()
    }

    /// Pretend the transport allows at most `budget` messages per RLN epoch (Logos Delivery
    /// only): further messages are held back and counted as `blocked`. `None`: no limit.
    pub fn set_message_budget(&self, budget: Option<u64>) {
        self.inner.backend.set_message_budget(budget);
    }

    async fn stream_with<T>(
        &self,
        topic: p2panda::Topic,
        subscription: T,
        active: bool,
    ) -> Result<TopicStream<T>, TopicStreamError>
    where
        T: TopicSubscription + 'static,
    {
        let subscription = Arc::new(subscription);
        let inner = self.inner.clone();
        let inner_subscription = self
            .runtime
            .spawn(async move { inner.stream(topic, subscription, active).await })
            .await??;

        let subscription = TopicStream::new(self.runtime.clone(), inner_subscription).await;
        info!(%topic, "subscribed to topic");

        Ok(subscription)
    }

    pub async fn delete_topic(
        &self,
        topic: impl Into<p2panda::Topic>,
    ) -> Result<(), TopicStreamError> {
        let topic = topic.into();
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.delete_topic(topic).await })
            .await?
    }
}

#[derive(Debug)]
pub(crate) struct NodeInner {
    pub(crate) backend: Box<dyn SyncBackend>,
    pub(crate) shutdown_notifier: Notify,
    pub(crate) topic_store: TrackedTopicStore,
    pub(crate) verifying_key: VerifyingKey,
}

impl NodeInner {
    pub async fn new(
        signing_key: SigningKey,
        network_id: impl Into<NetworkId>,
        db_file: Option<PathBuf>,
        transport: Option<Transport>,
    ) -> Result<Self, NodeError> {
        let network_id = network_id.into();
        let verifying_key = signing_key.verifying_key();

        let pool = database_pool(db_file).await?;
        run_migrations(&pool).await?;

        let topic_store = TrackedTopicStore::from_pool(pool.clone());

        let transport = match transport {
            Some(transport) => transport,
            None => match backend_choice().map_err(NodeError::Backend)? {
                BackendChoice::P2panda => Transport::P2panda,
                #[cfg(feature = "logos")]
                BackendChoice::LogosMemory => {
                    Transport::Logos(Arc::new(crate::logos::MemoryBus::new()))
                }
                #[cfg(feature = "logos")]
                BackendChoice::BridgeMemory => {
                    Transport::Bridge(Arc::new(crate::logos::MemoryBus::new()))
                }
                #[cfg(feature = "logos-ffi")]
                BackendChoice::Logos => Transport::Logos(logos_ffi_bus().await?),
                #[cfg(feature = "logos-ffi")]
                BackendChoice::Bridge => Transport::Bridge(logos_ffi_bus().await?),
                #[cfg(any(test, feature = "test_utils"))]
                BackendChoice::P2pandaExternal => {
                    let node = p2panda_node(signing_key.clone(), network_id, pool.clone()).await?;
                    Transport::External(Arc::new(crate::backend::external::NodeBus::new(node)))
                }
                #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
                BackendChoice::DualMemory => Transport::DualMemory,
            },
        };
        #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
        let transport = match transport {
            Transport::DualMemory => {
                let node = p2panda_node(signing_key.clone(), network_id, pool.clone()).await?;
                Transport::Dual(
                    Arc::new(crate::logos::MemoryBus::new()),
                    Arc::new(crate::backend::external::NodeBus::new(node)),
                )
            }
            Transport::DualMemoryFlaky(fail) => {
                let node = p2panda_node(signing_key.clone(), network_id, pool.clone()).await?;
                Transport::Dual(
                    Arc::new(crate::logos::MemoryBus::new()),
                    Arc::new(crate::backend::external::FlakyBus {
                        inner: Arc::new(crate::backend::external::NodeBus::new(node)),
                        fail: std::sync::atomic::AtomicUsize::new(fail),
                    }),
                )
            }
            other => other,
        };

        let backend: Box<dyn SyncBackend> = match transport {
            Transport::P2panda => Box::new(p2panda_backend(signing_key, network_id, pool).await?),
            Transport::External(bus) => {
                info!("syncing through the host's p2panda node");
                Box::new(crate::backend::external::ExternalBackend::new(bus))
            }
            #[cfg(feature = "logos")]
            Transport::Logos(bus) => {
                info!("syncing over Logos Delivery");
                Box::new(
                    crate::backend::logos::LogosBackend::new(bus, signing_key, network_id, pool)
                        .await?,
                )
            }
            #[cfg(all(feature = "logos", any(test, feature = "test_utils")))]
            Transport::DualMemory | Transport::DualMemoryFlaky(_) => unreachable!("replaced above"),
            #[cfg(feature = "logos")]
            Transport::Dual(delivery, p2panda) => {
                info!("syncing over Logos Delivery and the host's p2panda node");
                let logos = crate::backend::logos::LogosBackend::new(
                    delivery,
                    signing_key,
                    network_id,
                    pool,
                )
                .await?;
                Box::new(crate::backend::dual::DualBackend::new(
                    crate::backend::external::ExternalBackend::new(p2panda),
                    logos,
                ))
            }
            #[cfg(feature = "logos")]
            Transport::Bridge(bus) => {
                info!("syncing over p2panda and Logos Delivery, bridging the two");
                let logos = crate::backend::logos::LogosBackend::new(
                    bus,
                    signing_key.clone(),
                    network_id,
                    pool.clone(),
                )
                .await?;
                let p2panda = p2panda_backend(signing_key, network_id, pool).await?;
                Box::new(crate::backend::bridge::BridgeBackend::new(p2panda, logos))
            }
        };

        Ok(Self {
            backend,
            shutdown_notifier: Notify::new(),
            topic_store,
            verifying_key,
        })
    }

    pub async fn set_connection_mode(
        &self,
        connection_mode: ConnectionMode,
    ) -> Result<(), NodeError> {
        self.backend.set_connection_mode(connection_mode).await
    }

    pub async fn shutdown(&self) {
        // Wake up all subscriptions that may still exist.
        self.shutdown_notifier.notify_waiters();
    }

    pub async fn stream<T>(
        self: Arc<Self>,
        topic: impl Into<p2panda::Topic>,
        subscribable_topic: Arc<T>,
        active: bool,
    ) -> Result<TopicStreamInner<T>, TopicStreamError>
    where
        T: TopicSubscription + 'static,
    {
        let topic = topic.into();

        self.topic_store.add_topic(&topic).await?;

        // Add ourselves as an author to the topic store.
        self.topic_store
            .add_author(&topic, &self.verifying_key)
            .await?;

        Ok(TopicStreamInner::new(
            self.clone(),
            topic,
            subscribable_topic,
            active,
        ))
    }

    pub async fn delete_topic(
        self: Arc<Self>,
        topic: impl Into<p2panda::Topic>,
    ) -> Result<(), TopicStreamError> {
        let topic = topic.into();
        self.topic_store.delete_topic(&topic).await?;
        Ok(())
    }
}
