use std::collections::HashMap;
use std::ops::DerefMut;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use chrono::Utc;
use p2panda::VerifyingKey;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use tracing::{error, warn};

use crate::backend::EphemeralPublisher;
use crate::ephemeral_message::EphemeralMessage;
use crate::node::NodeInner;
use crate::traits::TopicSubscription;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorTrackerMessage {
    Hello,
    Ping,
    Bye,
}

impl std::fmt::Display for AuthorTrackerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AuthorTrackerMessage::Hello => write!(f, "Hello message"),
            AuthorTrackerMessage::Ping => write!(f, "Ping message"),
            AuthorTrackerMessage::Bye => write!(f, "Bye message"),
        }
    }
}

pub struct AuthorTracker<T> {
    last_ping: Mutex<HashMap<VerifyingKey, Instant>>,
    subscribable_topic: Arc<T>,
    node: Arc<NodeInner>,
    tx: RwLock<Option<Arc<dyn EphemeralPublisher>>>,
    /// Whether the user has the topic open. We only announce ourselves on active topics; we
    /// still track who else is online on all of them.
    active: AtomicBool,
    ping_interval: Duration,
}

impl<T: TopicSubscription> AuthorTracker<T> {
    pub fn new(node: Arc<NodeInner>, subscribable_topic: Arc<T>, active: bool) -> Arc<Self> {
        let ping_interval = node.backend.presence_interval();
        Arc::new(Self {
            last_ping: Mutex::new(HashMap::new()),
            subscribable_topic,
            node,
            tx: RwLock::new(None),
            active: AtomicBool::new(active),
            ping_interval,
        })
    }

    fn offline_timeout(&self) -> Duration {
        self.ping_interval * 2
    }

    /// The user opened (`true`) or closed the topic: say hello or bye.
    pub async fn set_active(&self, active: bool) {
        if self.active.load(Ordering::SeqCst) == active {
            return;
        }
        if active {
            self.active.store(true, Ordering::SeqCst);
            self.send(AuthorTrackerMessage::Hello).await;
        } else {
            self.send(AuthorTrackerMessage::Bye).await;
            self.active.store(false, Ordering::SeqCst);
        }
    }

    pub async fn set_topic_tx(&self, tx: Option<Arc<dyn EphemeralPublisher>>) {
        let mut tx_guard = self.tx.write().await;

        // Send good bye message to the network
        if let Some(tx) = tx_guard.as_ref()
            && self.active.load(Ordering::SeqCst)
        {
            send_message(tx, AuthorTrackerMessage::Bye).await;
        }

        // Set all authors that the tracker has seen to offline, authors the tracker hasn't seen
        // are already offline
        let old_authors = std::mem::take(self.last_ping.lock().await.deref_mut());
        for author in old_authors.into_keys() {
            self.subscribable_topic.author_left(author);
            self.set_last_seen(author).await;
        }

        let this_author = self.node.verifying_key;
        if tx.is_some() {
            self.subscribable_topic.author_joined(this_author);
        } else {
            self.subscribable_topic.author_left(this_author);
            self.set_last_seen(this_author).await;
        }

        *tx_guard = tx;
    }

    pub async fn received(&self, author: VerifyingKey, message: AuthorTrackerMessage) {
        match message {
            AuthorTrackerMessage::Hello => {
                self.join(author).await;
            }
            AuthorTrackerMessage::Ping => {
                self.ping(author).await;
            }
            AuthorTrackerMessage::Bye => {
                self.left(author).await;
            }
        }
    }

    async fn send(&self, message: AuthorTrackerMessage) {
        if !self.active.load(Ordering::SeqCst) {
            return;
        }
        if let Some(tx) = self.tx.read().await.as_ref() {
            send_message(tx, message).await;
        }
    }

    async fn join(&self, author: VerifyingKey) {
        self.last_ping.lock().await.insert(author, Instant::now());
        self.subscribable_topic.author_joined(author);
        self.set_last_seen(author).await;

        // Send a ping to the network to ensure that the new author knows we exist
        // Normally we send a ping every `ping_interval`
        self.send(AuthorTrackerMessage::Ping).await;
    }

    async fn ping(&self, author: VerifyingKey) {
        let old = self.last_ping.lock().await.insert(author, Instant::now());

        // If this is a new author emit author join
        if old.is_none() {
            self.subscribable_topic.author_joined(author);
        }
        self.set_last_seen(author).await;
    }

    async fn left(&self, author: VerifyingKey) {
        self.last_ping.lock().await.remove(&author);
        self.subscribable_topic.author_left(author);
        self.set_last_seen(author).await;
    }

    pub async fn spawn(&self) {
        // Send a hello to the network so other authors know we joined the topic
        self.send(AuthorTrackerMessage::Hello).await;

        let mut interval = tokio::time::interval(self.ping_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip over the first tick which completes immediately
        interval.tick().await;

        loop {
            interval.tick().await;

            // Send a ping to the network so that we won't be marked as offline
            self.send(AuthorTrackerMessage::Ping).await;
            let offline_timeout = self.offline_timeout();
            let mut expired = Vec::new();
            self.last_ping.lock().await.retain(|author, instant| {
                if instant.elapsed() > offline_timeout {
                    expired.push(*author);
                    false
                } else {
                    true
                }
            });

            for author in expired {
                self.subscribable_topic.author_left(author);
                self.set_last_seen(author).await;
            }
        }
    }

    async fn set_last_seen(&self, author: VerifyingKey) {
        if let Err(error) = self
            .node
            .topic_store
            .set_last_seen_for_author(author, Some(Utc::now()))
            .await
        {
            error!("Failed to set last seen for author {author}: {error}");
        }
    }
}

async fn send_message(tx: &Arc<dyn EphemeralPublisher>, message: AuthorTrackerMessage) {
    if let Err(err) = tx.publish(EphemeralMessage::AuthorTracker(message)).await {
        warn!("error occurred when sending ephemeral message: {err}");
    }
}
