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

/// A p2panda node and a Logos node that never talk directly, joined by a bridge node.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn bridge_joins_p2panda_and_logos() {
    let topic = random_topic();

    let on_p2panda = Node::with_p2panda(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let bridge = Node::with_bridge(
        SigningKey::generate(),
        network(),
        None,
        Arc::new(MemoryBus::new()),
    )
    .await
    .unwrap();
    let on_logos = node(None).await;

    let p2panda_doc = Recorder::default();
    let logos_doc = Recorder::default();
    let bridge_doc = Recorder::default();
    let p2panda_stream = on_p2panda.stream(topic, p2panda_doc.clone()).await.unwrap();
    let _bridge_stream = bridge.stream(topic, bridge_doc.clone()).await.unwrap();
    let logos_stream = on_logos.stream(topic, logos_doc.clone()).await.unwrap();

    p2panda_stream
        .publish_delta(b"from p2panda".to_vec())
        .await
        .unwrap();
    logos_stream
        .publish_delta(b"from logos".to_vec())
        .await
        .unwrap();

    let wait = |doc: Recorder, want: &'static [u8]| async move {
        for _ in 0..1200 {
            if doc.received().iter().any(|m| m == want) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    };
    assert!(
        wait(logos_doc.clone(), b"from p2panda").await,
        "Logos side never got the p2panda edit"
    );
    assert!(
        wait(p2panda_doc.clone(), b"from logos").await,
        "p2panda side never got the Logos edit"
    );
    assert!(wait(bridge_doc.clone(), b"from logos").await);
    assert!(wait(bridge_doc.clone(), b"from p2panda").await);

    // Each side sees each operation once.
    let count = |doc: &Recorder, want: &[u8]| doc.received().iter().filter(|m| *m == want).count();
    assert_eq!(count(&logos_doc, b"from p2panda"), 1);
    assert_eq!(count(&p2panda_doc, b"from logos"), 1);

    on_p2panda.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    on_logos.shutdown().await.unwrap();
}

/// A Logos node that joins late catches up through the bridge, snapshot included.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn late_logos_joiner_catches_up_through_bridge() {
    let topic = random_topic();
    let on_p2panda = Node::with_p2panda(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let bridge = Node::with_bridge(
        SigningKey::generate(),
        network(),
        None,
        Arc::new(MemoryBus::new()),
    )
    .await
    .unwrap();
    let bridge_doc = Recorder::default();
    let p2panda_stream = on_p2panda.stream(topic, Recorder::default()).await.unwrap();
    let _bridge_stream = bridge.stream(topic, bridge_doc.clone()).await.unwrap();

    p2panda_stream.publish_delta(b"old".to_vec()).await.unwrap();
    p2panda_stream
        .publish_snapshot(b"snapshot".to_vec())
        .await
        .unwrap();
    // The bridge has it before anyone on Logos asks.
    for _ in 0..1200 {
        if bridge_doc.received().iter().any(|m| m == b"snapshot") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let late = node(None).await;
    let late_doc = Recorder::default();
    let _late_stream = late.stream(topic, late_doc.clone()).await.unwrap();
    for _ in 0..1200 {
        if late_doc.received().iter().any(|m| m == b"snapshot") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(late_doc.received(), vec![b"snapshot".to_vec()]);

    on_p2panda.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    late.shutdown().await.unwrap();
}

/// Once a p2panda node and the bridge are in a live sync session, a Logos edit reaches the
/// p2panda node right away, not at the next sync session (needs p2panda's import to push to
/// live sessions, see the [patch] in Cargo.toml).
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn bridge_pushes_logos_edits_to_live_p2panda_peers() {
    let topic = random_topic();
    let on_p2panda = Node::with_p2panda(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let bridge = Node::with_bridge(
        SigningKey::generate(),
        network(),
        None,
        Arc::new(MemoryBus::new()),
    )
    .await
    .unwrap();
    let on_logos = node(None).await;
    let p2panda_doc = Recorder::default();
    let logos_doc = Recorder::default();
    let p2panda_stream = on_p2panda.stream(topic, p2panda_doc.clone()).await.unwrap();
    let _bridge_stream = bridge.stream(topic, Recorder::default()).await.unwrap();
    let logos_stream = on_logos.stream(topic, logos_doc.clone()).await.unwrap();

    // Establish the session: the first edit crosses at whatever pace sync takes.
    p2panda_stream
        .publish_delta(b"first".to_vec())
        .await
        .unwrap();
    for _ in 0..1200 {
        if logos_doc.received().iter().any(|m| m == b"first") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(logos_doc.received().iter().any(|m| m == b"first"));
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Now a live edit from Logos.
    logos_stream.publish_delta(b"live".to_vec()).await.unwrap();
    let started = std::time::Instant::now();
    while started.elapsed() < Duration::from_secs(15) {
        if p2panda_doc.received().iter().any(|m| m == b"live") {
            on_p2panda.shutdown().await.unwrap();
            bridge.shutdown().await.unwrap();
            on_logos.shutdown().await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("the live Logos edit did not reach the p2panda node within 15 s");
}

/// Messages a node sent so far, by kind.
fn sent(node: &Node) -> crate::MessageCounts {
    node.transport_stats().unwrap().sent
}

#[tokio::test]
#[test_log::test]
async fn background_topics_publish_nothing() {
    let topic = random_topic();

    let alice = node(None).await;
    let alice_stream = alice.stream(topic, Recorder::default()).await.unwrap();

    // Bob has the note but not open: he receives, and sends nothing at all.
    let bob = node(None).await;
    let bob_doc = Recorder::default();
    let bob_stream = bob
        .stream_in_background(topic, bob_doc.clone())
        .await
        .unwrap();
    alice_stream.publish_delta(b"hello".to_vec()).await.unwrap();
    assert_eq!(bob_doc.wait_for(1).await, vec![b"hello".to_vec()]);
    tokio::time::sleep(Duration::from_secs(6)).await;
    assert_eq!(sent(&bob).messages(), 0, "{:?}", sent(&bob));

    // Opening it announces him: presence, and heads so he catches up.
    bob_stream.set_active(true).await;
    tokio::time::sleep(Duration::from_secs(1)).await;
    let bob_sent = sent(&bob);
    assert_eq!(bob_sent.presence, 1, "{bob_sent:?}");
    assert_eq!(bob_sent.heads, 1, "{bob_sent:?}");

    alice.shutdown().await.unwrap();
    bob.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn heads_are_not_sent_periodically() {
    let topic = random_topic();
    let alice = node(None).await;
    let _alice_stream = alice.stream(topic, Recorder::default()).await.unwrap();

    // 0.5 s and 5 s after opening; the last one is at 30 s, then nothing.
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(sent(&alice).heads, 2, "{:?}", sent(&alice));

    alice.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn background_peer_still_answers_catch_up() {
    let topic = random_topic();

    // Alice writes while Bob has the note in the background, then goes away.
    let alice_bus = Arc::new(MemoryBus::new());
    let alice = Node::with_delivery_bus(SigningKey::generate(), network(), None, alice_bus.clone())
        .await
        .unwrap();
    let alice_stream = alice.stream(topic, Recorder::default()).await.unwrap();
    let bob = node(None).await;
    let bob_doc = Recorder::default();
    let _bob_stream = bob
        .stream_in_background(topic, bob_doc.clone())
        .await
        .unwrap();
    alice_stream
        .publish_delta(b"from alice".to_vec())
        .await
        .unwrap();
    assert_eq!(bob_doc.wait_for(1).await.len(), 1);
    drop(alice_stream);
    alice.shutdown().await.unwrap();
    drop(alice);
    // Her backend's tasks outlive the node in this test; take her off the bus.
    let (ops_topic, ephemeral_topic) = logos_p2panda::content_topics(&network().into(), &topic);
    for content_topic in [ops_topic, ephemeral_topic] {
        crate::logos::DeliveryBus::unsubscribe(&*alice_bus, content_topic)
            .await
            .unwrap();
    }

    // Carol opens the note: Bob answers her heads, though he never announces anything himself.
    let carol = node(None).await;
    let carol_doc = Recorder::default();
    let _carol_stream = carol.stream(topic, carol_doc.clone()).await.unwrap();
    assert_eq!(carol_doc.wait_for(1).await, vec![b"from alice".to_vec()]);
    let bob_sent = sent(&bob);
    assert_eq!(bob_sent.repairs, 1, "{bob_sent:?}");
    assert_eq!(bob_sent.heads + bob_sent.presence, 0, "{bob_sent:?}");

    bob.shutdown().await.unwrap();
    carol.shutdown().await.unwrap();
}

/// The bridge does not relay a p2panda author's keystrokes one by one: it waits, then sends
/// the newest snapshot and what follows it.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn bridge_relays_snapshots_not_every_keystroke() {
    let topic = random_topic();
    let on_p2panda = Node::with_p2panda(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let bridge = Node::with_bridge(
        SigningKey::generate(),
        network(),
        None,
        Arc::new(MemoryBus::new()),
    )
    .await
    .unwrap();
    let on_logos = node(None).await;

    let logos_doc = Recorder::default();
    let p2panda_stream = on_p2panda.stream(topic, Recorder::default()).await.unwrap();
    let _bridge_stream = bridge.stream(topic, Recorder::default()).await.unwrap();
    let _logos_stream = on_logos.stream(topic, logos_doc.clone()).await.unwrap();

    for i in 0..10u8 {
        p2panda_stream.publish_delta(vec![i]).await.unwrap();
    }
    p2panda_stream
        .publish_snapshot(b"snapshot".to_vec())
        .await
        .unwrap();
    p2panda_stream
        .publish_delta(b"after".to_vec())
        .await
        .unwrap();

    for _ in 0..1200 {
        if logos_doc.received().iter().any(|m| m == b"after") {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        logos_doc.received(),
        vec![b"snapshot".to_vec(), b"after".to_vec()]
    );
    let relayed = bridge.transport_stats().unwrap().sent.relayed;
    assert_eq!(relayed, 2);

    on_p2panda.shutdown().await.unwrap();
    bridge.shutdown().await.unwrap();
    on_logos.shutdown().await.unwrap();
}

#[tokio::test]
#[test_log::test]
async fn pretend_budget_blocks_what_rln_would_drop() {
    let topic = random_topic();
    let alice = node(None).await;
    alice.set_message_budget(Some(3));
    let alice_stream = alice
        .stream_in_background(topic, Recorder::default())
        .await
        .unwrap();
    for i in 0..5u8 {
        alice_stream.publish_delta(vec![i]).await.unwrap();
    }
    let stats = alice.transport_stats().unwrap();
    assert_eq!(stats.budget, Some(3));
    assert_eq!(stats.sent_this_epoch.messages(), 3, "{stats:?}");
    assert_eq!(stats.sent_this_epoch.blocked, 2, "{stats:?}");

    // Lifting the limit lets messages through again.
    alice.set_message_budget(None);
    alice_stream.publish_delta(vec![9]).await.unwrap();
    assert_eq!(sent(&alice).ops, 4);

    alice.shutdown().await.unwrap();
}

/// A node on both networks with p2panda run by the host (`DualBackend`, as Basecamp's
/// reflection_core with networks "both"): a p2panda-only node and a Logos-only node, joined only
/// through it, see each other's edits exactly once, and its own edits reach both.
#[tokio::test(flavor = "multi_thread")]
#[test_log::test]
async fn dual_node_joins_p2panda_and_logos() {
    let topic = random_topic();

    let on_p2panda = Node::with_p2panda(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let dual = Node::with_dual_memory(SigningKey::generate(), network(), None)
        .await
        .unwrap();
    let on_logos = node(None).await;

    let p2panda_doc = Recorder::default();
    let logos_doc = Recorder::default();
    let dual_doc = Recorder::default();
    let p2panda_stream = on_p2panda.stream(topic, p2panda_doc.clone()).await.unwrap();
    let dual_stream = dual.stream(topic, dual_doc.clone()).await.unwrap();
    let logos_stream = on_logos.stream(topic, logos_doc.clone()).await.unwrap();

    p2panda_stream
        .publish_delta(b"from p2panda".to_vec())
        .await
        .unwrap();
    logos_stream
        .publish_delta(b"from logos".to_vec())
        .await
        .unwrap();
    dual_stream
        .publish_delta(b"from dual".to_vec())
        .await
        .unwrap();

    let wait = |doc: Recorder, want: &'static [u8]| async move {
        for _ in 0..1200 {
            if doc.received().iter().any(|m| m == want) {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        false
    };
    assert!(
        wait(logos_doc.clone(), b"from p2panda").await,
        "Logos side never got the p2panda edit"
    );
    assert!(
        wait(p2panda_doc.clone(), b"from logos").await,
        "p2panda side never got the Logos edit"
    );
    assert!(
        wait(logos_doc.clone(), b"from dual").await,
        "Logos never got the dual node's edit"
    );
    assert!(
        wait(p2panda_doc.clone(), b"from dual").await,
        "p2panda never got the dual node's edit"
    );
    assert!(wait(dual_doc.clone(), b"from logos").await);
    assert!(wait(dual_doc.clone(), b"from p2panda").await);

    // Settle, then: each side sees each operation once.
    tokio::time::sleep(Duration::from_secs(2)).await;
    let count = |doc: &Recorder, want: &[u8]| doc.received().iter().filter(|m| *m == want).count();
    assert_eq!(count(&logos_doc, b"from p2panda"), 1);
    assert_eq!(count(&logos_doc, b"from dual"), 1);
    assert_eq!(count(&p2panda_doc, b"from logos"), 1);
    assert_eq!(count(&p2panda_doc, b"from dual"), 1);
    assert_eq!(count(&dual_doc, b"from logos"), 1);
    assert_eq!(count(&dual_doc, b"from p2panda"), 1);

    on_p2panda.shutdown().await.unwrap();
    dual.shutdown().await.unwrap();
    on_logos.shutdown().await.unwrap();
}
