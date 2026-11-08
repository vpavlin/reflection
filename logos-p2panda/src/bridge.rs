//! Relaying a topic between a p2panda network and Logos Delivery.
//!
//! Operations are the same on both sides (signed p2panda operations), so bridging is moving whole
//! operations, and an operation already known on one side is not sent again, so nothing loops.
//!
//! - p2panda -> Logos: hand every operation p2panda processes to [`to_logos`]. Logos charges per
//!   message, and a p2panda author (the GTK Reflection) writes one operation per keystroke, so
//!   operations are collected per author for a window and only the newest snapshot (an operation
//!   with the prune flag, which replaces everything before it) and what follows it is relayed,
//!   or all of them if there is no snapshot. Logos peers accept a snapshot past a gap.
//! - Logos -> p2panda: [`from_logos`] gives the topic's operations from Logos as p2panda
//!   operations, for `StreamPublisher::import`. p2panda checks and stores them and syncs them on.
//!   (Upstream p2panda 0.7 does not push imported operations to live sync sessions; the bridge
//!   needs vpavlin/p2panda `live-push-imports`.)
use std::collections::HashMap;
use std::time::Duration;

use p2panda::{Topic, VerifyingKey};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::{Stream, StreamExt};
use tracing::{debug, warn};

use crate::{Error, LogosSync, Op};

/// How long operations of one author are collected before they are relayed to Logos.
pub const RELAY_WINDOW: Duration = Duration::from_secs(15);

/// Start relaying a topic to Logos: send every operation the p2panda side processes into the
/// returned sender (`Op::from_parts(header, body)`). Runs until the sender is dropped.
pub fn to_logos(logos: LogosSync, topic: Topic, window: Duration) -> mpsc::UnboundedSender<Op> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(relay_loop(logos, topic, rx, window));
    tx
}

/// The topic's operations from Logos (stored ones first, then live), as p2panda operations.
/// Marks the topic active on Logos, so this endpoint catches up when it starts.
pub async fn from_logos(
    logos: &LogosSync,
    topic: Topic,
) -> Result<impl Stream<Item = p2panda::operation::Operation> + Send + 'static, Error> {
    let rx = logos.subscribe(topic).await?;
    logos.set_active(topic, true).await?;
    Ok(UnboundedReceiverStream::new(rx).map(|op| op.to_operation()))
}

async fn relay_loop(
    logos: LogosSync,
    topic: Topic,
    mut rx: mpsc::UnboundedReceiver<Op>,
    window: Duration,
) {
    let mut held: HashMap<VerifyingKey, Vec<Op>> = HashMap::new();
    let mut deadline: Option<tokio::time::Instant> = None;
    loop {
        let flush = async {
            match deadline {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            op = rx.recv() => {
                let Some(op) = op else { break };
                held.entry(op.author()).or_default().push(op);
                deadline.get_or_insert_with(|| tokio::time::Instant::now() + window);
            }
            _ = flush => {
                deadline = None;
                flush_held(&logos, topic, &mut held).await;
            }
        }
    }
    flush_held(&logos, topic, &mut held).await;
    debug!(%topic, "bridge relay to Logos ended");
}

async fn flush_held(logos: &LogosSync, topic: Topic, held: &mut HashMap<VerifyingKey, Vec<Op>>) {
    for (_, ops) in held.drain() {
        for op in to_relay(ops) {
            if let Err(error) = logos.relay(topic, op).await {
                warn!(%topic, "not relayed to Logos: {error}");
            }
        }
    }
}

/// Of one author's operations (in log order): the newest snapshot and what follows it, or all
/// of them if there is no snapshot.
pub fn to_relay(mut ops: Vec<Op>) -> Vec<Op> {
    ops.sort_by_key(|op| op.seq());
    match ops.iter().rposition(|op| op.prunes()) {
        Some(last_snapshot) => ops.split_off(last_snapshot),
        None => ops,
    }
}
