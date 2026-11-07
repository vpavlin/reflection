//! The default backend: p2panda over iroh.
use p2panda::Topic;
use p2panda::node::CreateStreamError;
use p2panda::streams::{EphemeralStreamPublisher, StreamEvent, StreamFrom, StreamPublisher};
use tokio_stream::StreamExt;

use super::{
    BoxFuture, EphemeralEvent, EphemeralPublisher, EphemeralStream, OpEvent, OpPublisher, OpStream,
    SyncBackend,
};
use crate::ephemeral_message::EphemeralMessage;
use crate::node::{ConnectionMode, NodeError};
use crate::topic_stream::PublishError;
use crate::traits::TopicSubscriptionError;

#[derive(Debug)]
pub(crate) struct P2pandaBackend {
    pub(crate) node: p2panda::Node,
}

impl P2pandaBackend {
    pub(crate) fn new(node: p2panda::Node) -> Self {
        Self { node }
    }
}

impl SyncBackend for P2pandaBackend {
    fn stream(&self, topic: Topic) -> BoxFuture<'_, Result<OpStream, CreateStreamError>> {
        Box::pin(async move {
            // Always start from re-playing _all_ operations in the beginning. This is due to
            // Reflection not keeping materialised document state around and we need to repeat
            // materialising the document at the beginning (in memory).
            //
            // This cost is acceptable since we're frequently pruning the log and the number of
            // operations to process is rather small.
            let (tx, rx) = self
                .node
                .stream_from::<Vec<u8>>(topic, StreamFrom::Start)
                .await?;

            let events = rx.filter_map(to_op_event);

            let publisher: Box<dyn OpPublisher> = Box::new(P2pandaOpPublisher(tx));
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn ephemeral_stream(
        &self,
        topic: Topic,
    ) -> BoxFuture<'_, Result<EphemeralStream, CreateStreamError>> {
        Box::pin(async move {
            let (tx, rx) = self
                .node
                .ephemeral_stream::<EphemeralMessage>(topic)
                .await?;

            let events = rx.map(|message| EphemeralEvent {
                author: message.author(),
                timestamp: message.timestamp(),
                message: message.body().to_owned(),
            });

            let publisher: Box<dyn EphemeralPublisher> = Box::new(P2pandaEphemeralPublisher(tx));
            Ok((publisher, Box::pin(events) as _))
        })
    }

    fn set_connection_mode(
        &self,
        _connection_mode: ConnectionMode,
    ) -> BoxFuture<'_, Result<(), NodeError>> {
        // TODO: This is a no-op currently and requires work in `p2panda-net` upstream.
        // See related issue: https://github.com/p2panda/p2panda/issues/1093
        Box::pin(async { Ok(()) })
    }
}

/// A p2panda stream event as the backend-neutral `OpEvent`.
pub(crate) fn to_op_event(event: StreamEvent<Vec<u8>>) -> Option<OpEvent> {
    match event {
        StreamEvent::Processed { operation, .. } => Some(OpEvent::Operation {
            id: operation.id(),
            author: operation.author(),
            body: operation.message().to_owned(),
        }),
        StreamEvent::SyncStarted {
            remote_node_id,
            session_id,
            incoming_operations,
            outgoing_operations,
            incoming_bytes,
            outgoing_bytes,
            ..
        } => Some(OpEvent::SyncStarted {
            remote: remote_node_id,
            session_id,
            incoming_operations,
            outgoing_operations,
            incoming_bytes,
            outgoing_bytes,
        }),
        StreamEvent::SyncEnded {
            remote_node_id,
            session_id,
            error,
            ..
        } => Some(OpEvent::SyncEnded {
            remote: remote_node_id,
            session_id,
            error: error.map(|error| error.to_string()),
        }),
        StreamEvent::DecodeFailed { error, .. } => {
            Some(OpEvent::Error(TopicSubscriptionError::DecodeMessage(error)))
        }
        StreamEvent::ReplayFailed { error, .. } => {
            Some(OpEvent::Error(TopicSubscriptionError::ReplayStream(error)))
        }
        StreamEvent::ProcessingFailed { event, error, .. } => {
            tracing::error!(
                "error occurred while processing operation {}: {error}",
                event.header().hash()
            );
            Some(OpEvent::Error(error.into()))
        }
        StreamEvent::AckFailed { error, .. } => {
            Some(OpEvent::Error(TopicSubscriptionError::AckedMessage(error)))
        }
        _ => None,
    }
}

pub(crate) struct P2pandaOpPublisher(pub(crate) StreamPublisher<Vec<u8>>);

impl OpPublisher for P2pandaOpPublisher {
    fn publish(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            self.0.publish(body).await?;
            Ok(())
        })
    }

    fn prune(&self, body: Vec<u8>) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            self.0.prune(Some(body)).await?;
            Ok(())
        })
    }
}

struct P2pandaEphemeralPublisher(EphemeralStreamPublisher<EphemeralMessage>);

impl EphemeralPublisher for P2pandaEphemeralPublisher {
    fn publish(&self, message: EphemeralMessage) -> BoxFuture<'_, Result<(), PublishError>> {
        Box::pin(async move {
            self.0.publish(message).await?;
            Ok(())
        })
    }
}
