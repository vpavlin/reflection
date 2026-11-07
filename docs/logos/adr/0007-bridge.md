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

## Notes from testing on the real networks (2026-11-07)

Three processes: a plain p2panda Reflection (`reflection-doc/examples/note.rs`), a bridge (the
same example with `REFLECTION_BACKEND=bridge`, liblogosdelivery v0.39 on the Logos test network)
and the Basecamp app's `reflection_core` 0.1.1 in a headless Basecamp 0.3 session on loam_core.
Two things had to change before edits crossed:

1. **Loam's payload convention.** loam_core sends the base64 *text* of a frame as the message
   payload and base64-decodes what it receives. The liblogosdelivery bus sent raw frames, so
   neither side could open the other's messages. The bus now sends base64 text and reads both
   (spec section 3). The Basecamp side needed no change.
2. **Live push of imported operations (p2panda).** `StreamPublisher::publish()` pushes new
   operations to peers in a live sync session; `import()` only stored them, so a Logos edit
   reached already-connected p2panda peers only at their next sync session, which in practice
   was "not within minutes". A one-commit patch to p2panda 0.7.0 pushes imported operations the
   same way (`vpavlin/p2panda` branch `live-push-imports`, used through `[patch.crates-io]`). It
   is a candidate for upstream. `bridge_pushes_logos_edits_to_live_p2panda_peers` fails without
   it and passes with it.

Result: a note created in Basecamp reached the p2panda node through the bridge, and live edits
crossed in about 2 s from Basecamp to p2panda and under 1 s the other way.
