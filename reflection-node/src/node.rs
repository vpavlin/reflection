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

/// Which sync backend `Node::new` uses: `REFLECTION_BACKEND` = `p2panda` (default),
/// `logos-memory` (Logos protocol over an in-process bus) or `logos` (Logos Delivery).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BackendChoice {
    P2panda,
    #[cfg(feature = "logos")]
    LogosMemory,
}

#[cfg(feature = "logos")]
type ExternalBus = Arc<dyn crate::logos::DeliveryBus>;
#[cfg(not(feature = "logos"))]
type ExternalBus = std::convert::Infallible;

fn backend_choice() -> Result<BackendChoice, String> {
    let value = std::env::var("REFLECTION_BACKEND").unwrap_or_default();
    match value.as_str() {
        "" | "p2panda" => Ok(BackendChoice::P2panda),
        #[cfg(feature = "logos")]
        "logos-memory" => Ok(BackendChoice::LogosMemory),
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
        Self::spawn(signing_key, network_id.into(), db_location, Some(bus)).await
    }

    async fn spawn(
        signing_key: SigningKey,
        network_id: NetworkId,
        db_location: Option<&Path>,
        bus: Option<ExternalBus>,
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
                .spawn(NodeInner::new(signing_key, network_id, db_file, bus))
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
        let topic = topic.into();
        let subscription = Arc::new(subscription);
        let inner = self.inner.clone();
        let inner_subscription = self
            .runtime
            .spawn(async move { inner.stream(topic, subscription).await })
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
        bus: Option<ExternalBus>,
    ) -> Result<Self, NodeError> {
        let network_id = network_id.into();
        let verifying_key = signing_key.verifying_key();

        let pool = database_pool(db_file).await?;
        run_migrations(&pool).await?;

        let topic_store = TrackedTopicStore::from_pool(pool.clone());

        #[cfg(feature = "logos")]
        let bus = match (bus, backend_choice().map_err(NodeError::Backend)?) {
            (Some(bus), _) => Some(bus),
            (None, BackendChoice::LogosMemory) => {
                Some(Arc::new(crate::logos::MemoryBus::new()) as ExternalBus)
            }
            (None, BackendChoice::P2panda) => None,
        };
        #[cfg(not(feature = "logos"))]
        let bus: Option<ExternalBus> = {
            backend_choice().map_err(NodeError::Backend)?;
            bus
        };

        let backend: Box<dyn SyncBackend> = match bus {
            #[cfg(feature = "logos")]
            Some(bus) => {
                info!("syncing over Logos Delivery");
                Box::new(
                    crate::backend::logos::LogosBackend::new(
                        bus,
                        signing_key,
                        network_id,
                        pool.clone(),
                    )
                    .await?,
                )
            }
            #[cfg(not(feature = "logos"))]
            Some(never) => match never {},
            None => {
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

                let node = builder.spawn().await?;
                Box::new(P2pandaBackend::new(node))
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
