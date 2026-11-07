//! Tests for the Logos protocol itself: catch-up, pruning, replay and isolation.
//!
//! The upstream tests (in `lib.rs` and in `reflection-doc`) already run against this backend
//! with `REFLECTION_BACKEND=logos-memory`. They only cover peers that are online at the same
//! time; these cover what the catch-up protocol (ADR 0003) adds.
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use p2panda::{Hash, SigningKey, Topic, VerifyingKey};

use crate::logos::MemoryBus;
use crate::node::Node;
use crate::traits::{TopicSubscription, TopicSubscriptionError};

#[derive(Clone, Default)]
struct Recorder {
    received: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Recorder {
    fn received(&self) -> Vec<Vec<u8>> {
        self.received.lock().unwrap().clone()
    }

    async fn wait_for(&self, count: usize) -> Vec<Vec<u8>> {
        for _ in 0..200 {
            if self.received.lock().unwrap().len() >= count {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        self.received()
    }
}

impl TopicSubscription for Recorder {
    fn bytes_received(&self, _author: VerifyingKey, data: Vec<u8>) {
        self.received.lock().unwrap().push(data);
    }
    fn ephemeral_bytes_received(&self, _author: VerifyingKey, _timestamp: u64, _data: Vec<u8>) {}
    fn author_joined(&self, _author: VerifyingKey) {}
    fn author_left(&self, _author: VerifyingKey) {}
    fn error(&self, _error: TopicSubscriptionError) {}
}

fn network() -> Hash {
    Hash::digest(b"reflection-logos-tests")
}

fn random_topic() -> Topic {
    let bytes = *Hash::digest(SigningKey::generate().verifying_key().as_bytes()).as_bytes();
    bytes.into()
}

async fn node(db: Option<&PathBuf>) -> Node {
    Node::with_delivery_bus(
        SigningKey::generate(),
        network(),
        db.map(|p| p.as_path()),
        Arc::new(MemoryBus::new()),
    )
    .await
    .unwrap()
}

#[tokio::test]
#[test_log::test]
async fn late_joiner_catches_up() {
    let topic = random_topic();

    let alice = node(None).await;
    let alice_doc = Recorder::default();
    let alice_stream = alice.stream(topic, alice_doc.clone()).await.unwrap();
    for i in 0..5u8 {
        alice_stream.publish_delta(vec![i]).await.unwrap();
    }

    // Bob was not there when Alice wrote.
    let bob = node(None).await;
    let bob_doc = Recorder::default();
    let _bob_stream = bob.stream(topic, bob_doc.clone()).await.unwrap();

    let received = bob_doc.wait_for(5).await;
    assert_eq!(received, (0..5u8).map(|i| vec![i]).collect::<Vec<_>>());

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn late_joiner_gets_snapshot_not_pruned_history() {
    let topic = random_topic();

    let alice = node(None).await;
    let alice_stream = alice.stream(topic, Recorder::default()).await.unwrap();
    alice_stream.publish_delta(b"old-1".to_vec()).await.unwrap();
    alice_stream.publish_delta(b"old-2".to_vec()).await.unwrap();
    alice_stream
        .publish_snapshot(b"snapshot".to_vec())
        .await
        .unwrap();
    alice_stream.publish_delta(b"new".to_vec()).await.unwrap();

    let bob = node(None).await;
    let bob_doc = Recorder::default();
    let _bob_stream = bob.stream(topic, bob_doc.clone()).await.unwrap();

    let received = bob_doc.wait_for(2).await;
    assert_eq!(received, vec![b"snapshot".to_vec(), b"new".to_vec()]);

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn third_peer_relays_for_an_offline_author() {
    let topic = random_topic();

    // Alice writes while Bob is there, then goes away.
    let alice = node(None).await;
    let alice_stream = alice.stream(topic, Recorder::default()).await.unwrap();
    let bob = node(None).await;
    let bob_doc = Recorder::default();
    let _bob_stream = bob.stream(topic, bob_doc.clone()).await.unwrap();

    alice_stream
        .publish_delta(b"from alice".to_vec())
        .await
        .unwrap();
    assert_eq!(bob_doc.wait_for(1).await.len(), 1);
    drop(alice_stream);
    alice.shutdown().await.unwrap();
    drop(alice);

    // Carol only ever meets Bob, and still gets Alice's change.
    let carol = node(None).await;
    let carol_doc = Recorder::default();
    let _carol_stream = carol.stream(topic, carol_doc.clone()).await.unwrap();

    assert_eq!(carol_doc.wait_for(1).await, vec![b"from alice".to_vec()]);

    bob.shutdown().await.unwrap();
    carol.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn replays_from_disk_after_restart() {
    let dir = std::env::temp_dir().join(format!(
        "reflection-logos-test-{}",
        SigningKey::generate().verifying_key().to_hex()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let topic = random_topic();

    let key = SigningKey::generate();
    {
        let alice = Node::with_delivery_bus(
            key.clone(),
            network(),
            Some(&dir),
            Arc::new(MemoryBus::new()),
        )
        .await
        .unwrap();
        let stream = alice.stream(topic, Recorder::default()).await.unwrap();
        stream.publish_delta(b"persisted".to_vec()).await.unwrap();
        stream.unsubscribe().await.unwrap();
        alice.shutdown().await.unwrap();
    }

    let alice = Node::with_delivery_bus(key, network(), Some(&dir), Arc::new(MemoryBus::new()))
        .await
        .unwrap();
    let doc = Recorder::default();
    let _stream = alice.stream(topic, doc.clone()).await.unwrap();
    assert_eq!(doc.wait_for(1).await, vec![b"persisted".to_vec()]);

    alice.shutdown().await.unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
#[test_log::test]
async fn other_documents_stay_separate() {
    let alice = node(None).await;
    let bob = node(None).await;

    let alice_stream = alice
        .stream(random_topic(), Recorder::default())
        .await
        .unwrap();
    let bob_doc = Recorder::default();
    let _bob_stream = bob.stream(random_topic(), bob_doc.clone()).await.unwrap();

    alice_stream
        .publish_delta(b"secret".to_vec())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(bob_doc.received().is_empty());

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}
