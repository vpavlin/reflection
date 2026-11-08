//! The interface to Logos Delivery (see ADR 0004).
use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};

use thiserror::Error;
use tokio::sync::broadcast;

use crate::BoxFuture;

/// A message on a Logos Delivery content topic.
#[derive(Clone, Debug)]
pub struct BusMessage {
    pub content_topic: String,
    pub payload: Vec<u8>,
}

#[derive(Debug, Error)]
#[error("{0}")]
pub struct BusError(pub String);

/// Publish/subscribe on Logos Delivery content topics.
///
/// Implementations deliver every message on a subscribed content topic to `messages()`,
/// including the ones this node sent itself (relay echoes them).
pub trait DeliveryBus: Send + Sync + 'static {
    fn subscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>>;

    fn unsubscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>>;

    fn publish(
        &self,
        content_topic: String,
        payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), BusError>>;

    fn messages(&self) -> broadcast::Receiver<BusMessage>;
}

const BUS_CAPACITY: usize = 4096;

/// Every `MemoryBus` in this process publishes into this hub.
static HUB: LazyLock<broadcast::Sender<BusMessage>> =
    LazyLock::new(|| broadcast::channel(BUS_CAPACITY).0);

/// A process-local stand-in for Logos Delivery, used for tests and CI.
///
/// It behaves like relay: a message reaches every bus in the process that is subscribed to its
/// content topic, including the sender.
pub struct MemoryBus {
    subscriptions: Arc<Mutex<HashSet<String>>>,
    tx: broadcast::Sender<BusMessage>,
}

impl MemoryBus {
    /// Must be called within a tokio runtime.
    pub fn new() -> Self {
        let subscriptions: Arc<Mutex<HashSet<String>>> = Arc::default();
        let (tx, _) = broadcast::channel(BUS_CAPACITY);

        let mut hub_rx = HUB.subscribe();
        let subscriptions_clone = subscriptions.clone();
        let tx_clone = tx.clone();
        tokio::spawn(async move {
            loop {
                match hub_rx.recv().await {
                    Ok(message) => {
                        let subscribed = subscriptions_clone
                            .lock()
                            .unwrap()
                            .contains(&message.content_topic);
                        if subscribed {
                            // No receivers is fine: nobody is listening right now.
                            let _ = tx_clone.send(message);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => break,
                }
                // Stop when the bus itself was dropped.
                if Arc::strong_count(&subscriptions_clone) == 1 {
                    break;
                }
            }
        });

        Self { subscriptions, tx }
    }
}

impl Default for MemoryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl DeliveryBus for MemoryBus {
    fn subscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>> {
        self.subscriptions.lock().unwrap().insert(content_topic);
        Box::pin(async { Ok(()) })
    }

    fn unsubscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>> {
        self.subscriptions.lock().unwrap().remove(&content_topic);
        Box::pin(async { Ok(()) })
    }

    fn publish(
        &self,
        content_topic: String,
        payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), BusError>> {
        let _ = HUB.send(BusMessage {
            content_topic,
            payload,
        });
        Box::pin(async { Ok(()) })
    }

    fn messages(&self) -> broadcast::Receiver<BusMessage> {
        self.tx.subscribe()
    }
}
