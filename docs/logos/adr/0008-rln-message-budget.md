# ADR 0008: Publish only for open documents (RLN message budget)

Status: accepted (2026-11-08)

## Context

Logos Delivery is moving to RLN: every message a node publishes needs a proof from an
on-chain membership, and a membership allows a fixed number of messages per 10-minute epoch
(100 to 600), shared by every app on that node. Receiving is free.

Version 1 of the protocol published on every subscribed document, whether anyone looked at it
or not:

| Per document, per device | Per epoch |
|---|---|
| heads: on subscribe, then after 1, 2, 4 ... s, then every 60 s | ~10 (14 in the first) |
| presence: hello, then a ping every 30 s | 20 |

The Basecamp app subscribes to every note it has, so four idle notes alone (~120 per epoch)
were over a 100-message membership. Writing was worse: the GTK app publishes one operation per
keystroke, and the bridge relayed each of them to Logos.

## Decision

A document is **active** when the user has it open, and **in the background** otherwise. A
device publishes nothing of its own for a background document.

1. `TopicStream::set_active(bool)` and `Node::stream_in_background` (`Node::stream` stays
   active, so upstream behaviour does not change). `SyncBackend::set_active` tells the backend;
   the p2panda backend ignores it.
2. **Heads** (spec 4.2) are sent only when a document becomes active: 0.5 s, 5 s and 30 s after,
   in case the first copy is lost, and never periodically. Gaps found while active are still
   asked for at once. A background document neither asks (gaps wait in the pending buffer until
   the user opens it) nor announces.
3. **Repairs are still answered** from background documents. A request comes from someone who
   opened the document; the answer is the cost of their opening it, and without it data held
   only by an idle device would never reach anyone.
4. **Presence** is sent only for active documents: hello on open, bye on close, and a ping every
   `SyncBackend::presence_interval` (Logos: 120 s, p2panda: 30 s as before). Online means "has
   this document open".
5. **The bridge** relays a p2panda author's operations after collecting them for 15 s: the
   newest snapshot and what follows it, or all of them if there is no snapshot. Logos peers
   accept a snapshot past a gap (spec 4.1). Its own presence reaches Logos at the Logos rate.
6. **Counters**: `Node::transport_stats` returns what was sent and received, by kind (ops,
   snapshots, heads, repairs, relayed, presence, cursors), per RLN epoch and per document, with
   the devices heard from. Apps show it to whoever wants to see where their budget goes.

How often an application publishes its own edits is its business: the Basecamp app merges the
edits of the open note into at most one operation every few seconds, and publishes one snapshot
after the user stops (reflection-basecamp).

## Consequences

- An idle device costs nothing, however many documents it has.
- An open, idle document costs about 3 heads on opening and 5 pings per epoch.
- A missed operation on a background document is fetched when the user opens it, not before.
  If the last edit of a burst is lost while active, the snapshot that follows repairs it.
- Presence is coarser: someone who closes the app without a bye shows online for up to 4
  minutes.
- Version 1 peers still interoperate: nothing on the wire changed, only when things are sent.
