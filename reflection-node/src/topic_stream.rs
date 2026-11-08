use std::mem::take;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::Utc;
use p2panda::Topic;
use p2panda::node::CreateStreamError;
use thiserror::Error;
use tokio::sync::{RwLock, oneshot};
use tokio::task::{AbortHandle, JoinError};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};

use crate::author_tracker::AuthorTracker;
use crate::backend::{EphemeralPublisher, OpEvent, OpPublisher};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::NodeInner;
use crate::traits::TopicSubscription;

#[derive(Debug, Error)]
pub enum PublishError {
    #[error(transparent)]
    Runtime(#[from] JoinError),

    #[error(transparent)]
    StreamPublish(#[from] p2panda::streams::PublishError),

    #[error(transparent)]
    EphemeralStreamPublish(#[from] p2panda::streams::EphemeralPublishError),

    #[error("streams to publish data into network are not available due to a setup error")]
    BrokenStream,

    #[error("backend failed to publish: {0}")]
    Backend(String),
}

#[derive(Debug, Error)]
pub enum TopicStreamError {
    #[error(transparent)]
    Runtime(#[from] JoinError),

    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

pub struct TopicStream<T> {
    inner: Arc<TopicStreamInner<T>>,
    runtime: tokio::runtime::Handle,
    network_monitor_task: AbortHandle,
}

impl<T> Drop for TopicStream<T> {
    fn drop(&mut self) {
        self.network_monitor_task.abort();
    }
}

impl<T> TopicStream<T>
where
    T: TopicSubscription + 'static,
{
    pub(crate) async fn new(runtime: tokio::runtime::Handle, inner: TopicStreamInner<T>) -> Self {
        let (ready_tx, ready_rx) = oneshot::channel();

        // Spawn task to establish streams to publish and subscribe to messages, the same task will
        // also await a shutdown signal to drop the streams.
        let inner = Arc::new(inner);
        let inner_clone = inner.clone();
        let network_monitor_task = runtime
            .spawn(async move {
                inner_clone.spawn_network_monitor(ready_tx).await;
            })
            .abort_handle();

        // Wait until streams with network have been established.
        let _ = ready_rx.await;

        TopicStream {
            inner,
            runtime,
            network_monitor_task,
        }
    }

    pub async fn publish_delta(&self, data: Vec<u8>) -> Result<(), PublishError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.publish_delta(data).await })
            .await?
    }

    pub async fn publish_snapshot(&self, data: Vec<u8>) -> Result<(), PublishError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.publish_snapshot(data).await })
            .await?
    }

    pub async fn publish_ephemeral(&self, data: Vec<u8>) -> Result<(), PublishError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.publish_ephemeral(data).await })
            .await?
    }

    pub async fn unsubscribe(self) -> Result<(), TopicStreamError> {
        self.network_monitor_task.abort();

        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.unsubscribe().await })
            .await??;

        info!("unsubscribed from topic {}", self.inner.topic);

        Ok(())
    }

    /// Tell the network layer whether the user has this topic open. Streams from `Node::stream`
    /// start active, those from `Node::stream_in_background` inactive. On an inactive topic we
    /// keep receiving, but announce nothing (no presence, no catch-up requests), which matters
    /// on a transport that charges per message sent (Logos Delivery under RLN).
    pub async fn set_active(&self, active: bool) {
        let inner = self.inner.clone();
        let _ = self
            .runtime
            .spawn(async move { inner.set_active(active).await })
            .await;
    }

    /// Set the name for a given topic.
    ///
    /// This information will be written to the database.
    pub async fn set_name(&self, name: Option<String>) -> Result<(), TopicStreamError> {
        let inner = self.inner.clone();
        self.runtime
            .spawn(async move { inner.set_name(name).await })
            .await?
    }
}

pub(crate) struct TopicStreamInner<T> {
    tx: RwLock<Option<Box<dyn OpPublisher>>>,
    ephemeral_tx: RwLock<Option<Arc<dyn EphemeralPublisher>>>,
    node: Arc<NodeInner>,
    topic: Topic,
    subscription: Arc<T>,
    author_tracker: Arc<AuthorTracker<T>>,
    abort_handles: RwLock<Vec<AbortHandle>>,
    active: AtomicBool,
}

impl<T> Drop for TopicStreamInner<T> {
    fn drop(&mut self) {
        for handle in self.abort_handles.get_mut() {
            handle.abort();
        }
    }
}

impl<T> TopicStreamInner<T>
where
    T: TopicSubscription + 'static,
{
    pub fn new(node: Arc<NodeInner>, topic: Topic, subscription: Arc<T>, active: bool) -> Self {
        let author_tracker = AuthorTracker::new(node.clone(), subscription.clone(), active);

        TopicStreamInner {
            tx: RwLock::new(None),
            ephemeral_tx: RwLock::new(None),
            node,
            topic,
            abort_handles: RwLock::new(Vec::new()),
            subscription,
            author_tracker,
            active: AtomicBool::new(active),
        }
    }

    pub async fn set_active(&self, active: bool) {
        self.active.store(active, Ordering::SeqCst);
        self.author_tracker.set_active(active).await;
        self.node.backend.set_active(self.topic, active).await;
    }

    pub async fn spawn_network_monitor(&self, ready_signal: oneshot::Sender<()>) {
        let result = setup_streams(
            &self.node,
            self.topic,
            &self.subscription,
            &self.author_tracker,
        )
        .await;

        match result {
            Ok((tx, ephemeral_tx, abort_handles)) => {
                *self.tx.write().await = Some(tx);
                *self.ephemeral_tx.write().await = Some(ephemeral_tx);
                *self.abort_handles.write().await = abort_handles;
                if self.active.load(Ordering::SeqCst) {
                    self.node.backend.set_active(self.topic, true).await;
                }
            }
            Err(error) => {
                self.subscription.error(error.into());
            }
        }

        // Inform caller that we're done with setting up the streams. They are ready now to be used
        // for publishing and receiving messages.
        let _ = ready_signal.send(());

        // Wait until we've received signal from node to shut down.
        let shutdown_notification = self.node.shutdown_notifier.notified();
        shutdown_notification.await;

        let _ = self.unsubscribe().await;
    }

    pub async fn unsubscribe(&self) -> Result<(), TopicStreamError> {
        let mut tx_guard = self.tx.write().await;
        let mut ephemeral_tx_guard = self.ephemeral_tx.write().await;
        let mut abort_handles_guard = self.abort_handles.write().await;

        let tx = take(tx_guard.deref_mut());
        let ephemeral_tx = take(ephemeral_tx_guard.deref_mut());
        let abort_handles = take(abort_handles_guard.deref_mut());

        self.node
            .topic_store
            .set_last_accessed_for_topic(&self.topic, Some(Utc::now()))
            .await?;

        teardown_streams(
            &self.topic,
            &self.author_tracker,
            tx,
            ephemeral_tx,
            abort_handles,
        )
        .await;

        Ok(())
    }

    pub async fn publish_delta(&self, data: Vec<u8>) -> Result<(), PublishError> {
        if let Some(tx) = self.tx.read().await.as_ref() {
            info!("delta operation sent for topic with id {}", self.topic);
            tx.publish(data).await?;
        } else {
            return Err(PublishError::BrokenStream);
        }

        Ok(())
    }

    pub async fn publish_snapshot(&self, data: Vec<u8>) -> Result<(), PublishError> {
        if let Some(tx) = self.tx.read().await.as_ref() {
            info!("snapshot saved for topic with id {}", self.topic);

            // Append an operation to our log and set the prune flag to true. This will remove
            // previous entries.
            tx.prune(data).await?;
        } else {
            return Err(PublishError::BrokenStream);
        }

        Ok(())
    }

    pub async fn publish_ephemeral(&self, data: Vec<u8>) -> Result<(), PublishError> {
        if let Some(ephemeral_tx) = self.ephemeral_tx.read().await.as_ref() {
            ephemeral_tx
                .publish(EphemeralMessage::Application(data))
                .await?;
        } else {
            return Err(PublishError::BrokenStream);
        }

        Ok(())
    }

    pub async fn set_name(&self, name: Option<String>) -> Result<(), TopicStreamError> {
        self.node
            .topic_store
            .set_name_for_topic(&self.topic, name)
            .await?;

        Ok(())
    }
}

async fn setup_streams<T>(
    node: &Arc<NodeInner>,
    id: Topic,
    subscribable_topic: &Arc<T>,
    author_tracker: &Arc<AuthorTracker<T>>,
) -> Result<
    (
        Box<dyn OpPublisher>,
        Arc<dyn EphemeralPublisher>,
        Vec<AbortHandle>,
    ),
    CreateStreamError,
>
where
    T: TopicSubscription + 'static,
{
    let mut abort_handles = Vec::with_capacity(3);

    // 1. Handle incoming operations from eventually consistent topic stream.
    // ======================================================================

    // The backend re-plays _all_ operations in the beginning, see `SyncBackend::stream`.
    let (topic_tx, mut topic_rx) = node.backend.stream(id).await?;

    let node_clone = node.clone();
    let subscribable_topic_clone = subscribable_topic.clone();
    let abort_handle = tokio::spawn(async move {
        while let Some(event) = topic_rx.next().await {
            match event {
                OpEvent::Operation { id: operation_id, author, body } => {
                    info!(
                        author = &author.to_string()[0..8],
                        "processed operation with id {}",
                        operation_id
                    );

                    // When we discover a new author we need to add them to our topic store.
                    if let Err(error) = node_clone.topic_store.add_author(&id, &author).await {
                        error!("can't store author to database: {error}");
                    }

                    // Forward the message payload up to the app layer.
                    subscribable_topic_clone.bytes_received(author, body);
                }
                OpEvent::SyncStarted {
                    remote,
                    session_id,
                    incoming_operations,
                    outgoing_operations,
                    incoming_bytes,
                    outgoing_bytes,
                } => {
                    info!(
                        %session_id,
                        remote_node_id = &remote.to_string()[0..8],
                        "sync started w. {} incoming ({} bytes) and {} outgoing operations ({} bytes)",
                        incoming_operations,
                        incoming_bytes,
                        outgoing_operations,
                        outgoing_bytes,
                    );
                }
                OpEvent::SyncEnded {
                    remote,
                    session_id,
                    error,
                } => match error {
                    Some(error) => {
                        warn!(
                            %session_id,
                            remote_node_id = &remote.to_string()[0..8],
                            "sync failed with error {error}",
                        );
                    }
                    None => {
                        info!(
                            %session_id,
                            remote_node_id = &remote.to_string()[0..8],
                            "sync ended",
                        );
                    }
                },
                OpEvent::Error(error) => {
                    error!("error occurred in operation stream: {error}");
                    subscribable_topic_clone.error(error);
                }
            }
        }
    })
    .abort_handle();

    abort_handles.push(abort_handle);

    // 2. Handle incoming messages from ephemeral topic stream.
    // ========================================================

    let (ephemeral_tx, mut ephemeral_rx) = node.backend.ephemeral_stream(id).await?;
    let ephemeral_tx: Arc<dyn EphemeralPublisher> = Arc::from(ephemeral_tx);

    author_tracker
        .set_topic_tx(Some(ephemeral_tx.clone()))
        .await;

    let author_tracker_clone = author_tracker.clone();
    let subscribable_topic_clone = subscribable_topic.clone();
    let abort_handle = tokio::spawn(async move {
        while let Some(event) = ephemeral_rx.next().await {
            match event.message {
                EphemeralMessage::Application(bytes) => {
                    subscribable_topic_clone.ephemeral_bytes_received(
                        event.author,
                        event.timestamp,
                        bytes,
                    );
                }
                EphemeralMessage::AuthorTracker(tracker) => {
                    author_tracker_clone.received(event.author, tracker).await;
                }
            }
        }
    })
    .abort_handle();

    abort_handles.push(abort_handle);

    // 3. Run task to track online status of authors.
    // ==============================================

    let author_tracker_clone = author_tracker.clone();
    let abort_handle = tokio::spawn(async move {
        author_tracker_clone.spawn().await;
    })
    .abort_handle();

    abort_handles.push(abort_handle);

    info!("network streams set up for topic {}", id);

    Ok((topic_tx, ephemeral_tx, abort_handles))
}

async fn teardown_streams<T>(
    topic: &Topic,
    author_tracker: &Arc<AuthorTracker<T>>,
    tx: Option<Box<dyn OpPublisher>>,
    ephemeral_tx: Option<Arc<dyn EphemeralPublisher>>,
    abort_handles: Vec<AbortHandle>,
) where
    T: TopicSubscription + 'static,
{
    for handle in abort_handles {
        handle.abort();
    }

    author_tracker.set_topic_tx(None).await;

    if tx.is_some() {
        info!("network streams torn down for topic {}", topic);
    }

    drop(tx);
    drop(ephemeral_tx);
}
