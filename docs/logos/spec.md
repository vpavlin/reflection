# Reflection over Logos Delivery: protocol, version 1

This is the wire protocol of the Logos sync backend (`reflection-node`, feature `logos`). Any
implementation that follows it can edit the same documents, for example the GTK app and the
Logos Basecamp app.

## 1. Inputs

- **Network id** `N` (32 bytes). Reflection uses `blake3("reflection-v2")`.
- **Document id** `T` (32 bytes), the p2panda topic of the document. **It is the document's
  shared secret**: whoever knows it can read and write the document.
- Each peer has an ed25519 key pair (the p2panda `SigningKey`), the same identity as on the
  p2panda backend.

## 2. Content topics and keys

```
derive(label) = blake3(label || N || T)                         (32 bytes)

name          = hex(derive("reflection-logos-topic-v1")[0..16])
ops topic     = "/reflection/1/" + name + "-ops/cbor"
ephemeral     = "/reflection/1/" + name + "-eph/cbor"
key           = derive("reflection-logos-key-v1")
```

Neither the document id nor any content appears in clear on the relay network. Relays see two
opaque content topics per document.

## 3. Sealing

Every message on both content topics is:

```
message = 0x01 || nonce (24 bytes) || XChaCha20-Poly1305(key, nonce, plaintext, aad = 0x01)
nonce   = blake3(key || plaintext)[0..24]
```

The nonce is deterministic. The same frame always seals to the same bytes, which relay
de-duplication then drops, and no random number generator is needed. Different plaintexts get
different nonces.

`plaintext` is the CBOR encoding of a **frame** (section 4). A message that fails to open, or
whose first byte is not `0x01`, is dropped.

## 4. Frames

CBOR, serde externally-tagged enum. Byte fields are CBOR byte strings.

```
{ "op":    { "header": bytes, "body": bytes | null } }
{ "heads": { "from": verifying_key, "heads": [ [author, seq, hash], ... ], "nonce": u64 } }
{ "eph":   { "author": verifying_key, "timestamp": u64, "body": bytes, "signature": sig } }
```

### 4.1 `op`: an operation

A p2panda operation (`p2panda_core::Header<p2panda::operation::Extensions>`, CBOR) and its body.

- `header.extensions` carries the log id (derived from `T`), a timestamp and the **prune flag**.
- `header.seq_num` and `header.backlink` chain each author's operations into a log.
- `body` is the CBOR encoding of the application payload (a byte string as p2panda's streams
  encode a `Vec<u8>` message): a Loro update (a delta) or a Loro snapshot (with the prune flag
  set). Encoding it as p2panda does makes Logos and p2panda operations identical, so they can be
  bridged (ADR 0007). Until 2026-11-07 the body was the raw payload.

A receiver accepts an operation if:

1. the signature, payload hash and payload size are valid (`p2panda_core::validate_operation`),
2. the log id is the one for `T`, and
3. it fits the author's log:
   - a first operation: `seq = 0`, or
   - the next one: `seq = head.seq + 1` and `backlink = head.hash`, or
   - a prune: prune flag set and `seq > head.seq` (no backlink check). Earlier entries of that
     author's log are deleted.

An operation with `seq <= head.seq` is old and dropped. One with a gap waits in a pending buffer
(at most 256 per author) until its predecessors arrive, and the receiver sends its heads (4.2).

Operations go out on the **ops** topic. One operation is at most 140 KiB (header plus body).
Larger ones are refused by the sender in version 1.

### 4.2 `heads`: catch-up

The latest `(author, seq, hash)` of each author's log on this document, as the sender has it.
Sent on the **ops** topic:

- when subscribing, and then after 1, 2, 4, … seconds, capped at one every 60 seconds;
- at most every 500 ms when the sender notices it is behind (a gap, a newer head in someone
  else's `heads`, or a dropped receive queue).

A receiver compares with its own logs:

- **They lack something**: for each author where the receiver's head is newer (or the sender has
  nothing), it schedules a **repair**. After a delay of 50 to 500 ms it re-sends that author's
  operations after the sender's `seq`. If the receiver pruned past that point, it sends from its
  oldest operation, which is the snapshot. An operation already seen on the wire since the
  repair was scheduled is skipped, so usually one peer answers, not all.
- **We lack something**: it sends its own heads, so whoever has the data repairs.

`heads` from oneself (`from` = own key) are ignored.

`nonce` (the send time in nanoseconds) makes each heads message unique. A transport that drops
repeated payloads, as Loam does (it dedups by frame hash), would otherwise drop a re-sent heads
whose content has not changed, and with it the request for a repair.

### 4.3 `eph`: ephemeral messages

Presence (hello, ping, bye) and cursor positions. They are not stored or repaired.

```
signature = ed25519(author, cbor(["reflection-eph-v1", T, author, timestamp, body]))
```

- `timestamp` is microseconds since the Unix epoch.
- `body` is the CBOR encoding of Reflection's `EphemeralMessage`.

Sent on the **ephemeral** topic. Messages with a bad signature, or from oneself, are dropped.

## 5. Local state

The backend stores operations in a table `logos_operations(hash, topic, author, seq, header,
body)` in Reflection's SQLite database. On subscribe it replays every stored operation of the
document, each log in order, before going live. That is how a document is restored after a
restart, the same as the p2panda backend does.

## 6. What version 1 does not do

- Operations over 140 KiB (large snapshots): chunking, or a Logos Storage CID, later.
- RLN pacing: repairs are not rate limited.
- Using Logos Delivery store nodes for history. Store-recovered messages go through the same
  receive path when the bus delivers them, but the protocol does not depend on them.
- Read-only members, or removing a member: everyone with the document id is an equal writer,
  as in Reflection today.
