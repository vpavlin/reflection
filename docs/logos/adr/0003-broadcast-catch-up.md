# ADR 0003: Broadcast catch-up instead of two-party log sync

Status: accepted (2026-11-07)

## Context

p2panda syncs logs in two-party sessions over QUIC: two peers connect, compare
log heights and exchange what the other is missing. Logos Delivery is
publish/subscribe relay on content topics. There is no addressed connection
to a peer.

Relay alone delivers only to peers that are subscribed when a message is
sent. A peer that joins later, or that was offline, misses everything before.
Store nodes on the Logos network keep recent messages, but not forever and
not on every network. Reflection must converge without depending on them.

## Decision

Each document topic has an **ops** content topic. Besides operations, peers
broadcast small **heads** messages on it:

- `Heads { from, heads: [(author, seq, hash)] }`: "this is the latest
  operation I have from each author".

Rules:

1. **On subscribe**, a peer sends its `Heads`, then repeats it with backoff
   (1 s, 2 s, 4 s … up to 60 s) while it keeps learning new operations.
   After that it sends once every 60 s.
2. **On receiving `Heads`**, a peer compares them with its own logs. For each
   author where it has operations the sender lacks, it schedules a
   **repair**: re-broadcast those operations, from the sender's `seq + 1`.
   If the sender is behind the last prune point, the repair starts from the
   latest pruning operation (the snapshot).
3. **Suppression**: a repair waits a random 50–500 ms before sending. If the
   same operations are seen on the wire in the meantime (another peer
   answered), the repair is cancelled. Many peers do not flood one joiner.
4. **On receiving an operation** with a gap (its `seq` is past our head + 1
   and it is not a prune), the peer keeps it in a small pending buffer and
   sends its own `Heads` at once, which triggers a repair from whoever has the
   missing operations.
5. A peer that learns of a newer head it lacks (from someone else's `Heads`)
   also sends its `Heads`. This is how a peer learns that it is behind.
6. Operations are idempotent. Duplicates (same hash) are dropped.

Store recovery: when Logos Delivery delivers messages from a store node
(`message_received` with `source = store`), they go through the same receive
path. It is a bonus, not a requirement.

## Consequences

- Convergence holds as long as, eventually, two peers holding the missing
  data are online at the same time. Any peer can answer for any author. That
  is better than p2panda today, where only direct sync partners exchange data
  (compare upstream #187).
- Traffic is bounded: heads are small, repairs start at the snapshot, and
  suppression keeps duplicates rare. Under RLN rate limits, repairs must also
  be paced (a later change; not needed while RLN is off).
- Waku messages are limited to about 150 KB. A snapshot of a large document
  can exceed that. Version 1 refuses to send an operation over 140 KB and logs
  an error. Chunking (or a Logos Storage CID for big snapshots) is a later
  ADR.
- Late joiners need someone online. That matches Reflection's own model
  ("collaborate when you meet").
