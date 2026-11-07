# ADR 0007: Bridging p2panda and Logos Delivery

Status: accepted (2026-11-07)

## Context

With ADR 0005 a node syncs over p2panda (iroh) or over Logos Delivery, not both. People on
upstream Reflection and people on Logos (for example in Logos Basecamp) can't edit the same
document.

Both backends use p2panda operations (ADR 0002), and both networks address a document by the
same topic. One difference remained: p2panda's streams store a message in the operation body as
CBOR (`encode_cbor(&message)`), while the Logos backend stored the raw bytes.

## Decision

1. **The Logos backend encodes bodies like p2panda** (CBOR of the `Vec<u8>` message). The same
   edit is then the same operation, with the same hash and signature, on both networks. This
   changes the Logos wire format (spec section 4.1); only test builds used the old one.
2. **`BridgeBackend`** (`REFLECTION_BACKEND=bridge` / `bridge-memory`, `Node::with_bridge`) runs
   both backends with one identity and one database:
   - it writes through p2panda only, so each operation is created once;
   - every operation p2panda processes (its own and those synced from iroh peers) is relayed
     into the Logos store and sent to Logos peers, in order, per document;
   - operations from Logos peers are imported into p2panda with `StreamPublisher::import`.
     p2panda checks and stores them, syncs them to iroh peers, and hands them to the
     application through the same stream;
   - an operation already in the Logos store is not relayed again, so nothing loops.
3. Ephemeral messages (presence, cursors) are signed per network and are not bridged. The bridge
   publishes its own on both networks and listens on both.

## Consequences

- Any node can be a bridge, for example a GTK Reflection started with `REFLECTION_BACKEND=bridge`.
  It relays only the documents it has open, because a document's id is also its Logos key.
- Logos nodes catch up through the bridge like through any Logos peer (the bridge's Logos store
  holds the history, snapshots included), and p2panda nodes catch up through p2panda sync.
- Tests: the upstream suite passes unchanged on `bridge-memory`; `bridge_joins_p2panda_and_logos`
  (a p2panda-only node and a Logos-only node, joined only by a bridge, see each other's edits
  exactly once) and `late_logos_joiner_catches_up_through_bridge`. Disabling either direction
  makes the first one fail.
- Presence across networks is not shown: someone on p2panda does not see someone on Logos as
  online, though they see each other's edits.
