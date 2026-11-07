# ADR 0002: p2panda operations as the Logos envelope and store

Status: accepted (2026-11-07)

## Context

The Logos backend has to carry Loro deltas and snapshots between peers on a
relay network. Every message needs an author identity and integrity. Peers
must be able to tell what they are missing, and history must stay bounded.

p2panda already solves exactly this at the data layer, independently of its
network:

- `p2panda_core::Operation` is a signed (ed25519) header plus body. The
  header carries the author, a per-author sequence number, a backlink to the
  previous operation, a timestamp and extensions.
- Reflection's extensions include a log id and a **prune flag**. A pruning
  operation (a snapshot) lets every peer drop that author's earlier
  operations.
- `p2panda::forge::OperationForge` and `p2panda_store::SqliteStore` are
  public. They create, sign and persist operations and logs.

Reflection identities are already p2panda `SigningKey`s.

## Decision

The Logos backend reuses the p2panda data layer unchanged:

- Local writes go through `OperationForge::create_operation` into the same
  `SqliteStore` the p2panda backend uses (same database, same schema).
- On the wire, the CBOR-encoded `Operation` (header plus body) is the payload,
  wrapped in a small versioned frame (see [spec](../spec.md)).
- On receive, the backend verifies the signature and the log position before
  storing and forwarding the body to the document layer.
- A snapshot is published as a pruning operation, exactly like p2panda's
  `StreamPublisher::prune`.

## Consequences

- Same identities and the same signatures on both backends. A document's
  history is valid on either network, so a later bridge between the two
  networks needs no re-signing.
- No new crypto and no new storage code: the Logos backend owns only the
  network side.
- The per-author `(seq, hash)` of each log head is the natural input for
  catch-up ([ADR 0003](0003-broadcast-catch-up.md)).
- p2panda's ingest pipeline (`processor`) is `pub(crate)`. We implement the
  receive checks ourselves: verify the signature, check that `seq` and the
  backlink follow the stored head, and handle prune. This is a small, well-
  defined piece. If upstream exposes the pipeline later, we switch to it.
- Coupling: we depend on p2panda's public data types staying stable. Both
  backends live in one crate and share one `Cargo.lock`, so a p2panda bump
  updates both together.
