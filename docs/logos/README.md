# Reflection on Logos Delivery

This folder documents the work to let Reflection sync over
[Logos Delivery](https://github.com/logos-messaging/logos-delivery) (the Waku
relay network) as an alternative to the default p2panda/iroh networking. It
also covers how a Logos Basecamp app is built on top of it.

The goal is twofold:

1. **Upstreamable**: one small, behaviour-neutral change to Reflection. It
   introduces a sync-backend abstraction in `reflection-node`, with p2panda as
   the default and only required backend. A Logos backend then plugs in behind
   a cargo feature, without touching the app, the document layer or the tests.
2. **A Basecamp app**: a separate repository (`reflection-basecamp`) builds a
   Logos Basecamp module on this fork. It shares the document layer and the
   wire format, so the GTK app and Basecamp can edit the same notes over Logos.

## Ground rules

- **Upstream tests are the contract.** The 6 tests in `reflection-node` and
  `reflection-doc` must pass unchanged on every backend. They are not edited
  to make a backend pass. If a backend needs different behaviour, the backend
  changes, not the test. See [ADR 0005](adr/0005-backend-selection-and-tests.md).
- **No behaviour change by default.** Without the `logos` feature the build is
  byte-for-byte the same networking stack as upstream.
- **Upstream CI stays green.** CI runs `--all-features`, so the Logos feature
  must build and test with no native library installed. See [ADR 0004](adr/0004-delivery-bus.md).

## Running the tests on each backend

```sh
# default: p2panda
cargo test --workspace --exclude reflection --all-features

# Logos protocol over an in-process bus (also in CI)
REFLECTION_BACKEND=logos-memory cargo test --workspace --exclude reflection --all-features

# both, bridged (a p2panda network and a Logos network in one node)
REFLECTION_BACKEND=bridge-memory cargo test --workspace --exclude reflection --all-features

# real Logos Delivery nodes, peered on localhost within the test process
L=/path/to/delivery_module   # holds liblogosdelivery.so (v0.39) and librln.so
REFLECTION_BACKEND=logos REFLECTION_LOGOS_LIB=$L/liblogosdelivery.so LD_LIBRARY_PATH=$L \
  cargo test --workspace --exclude reflection --all-features
```

To run the GTK app on Logos Delivery, build it with the `logos-ffi` feature and set
`REFLECTION_BACKEND=logos` and `REFLECTION_LOGOS_LIB`. Without `REFLECTION_LOGOS_CONFIG` it
joins the public Logos test network (`{"mode": "Core", "preset": "logos.test"}`).

## Spec

- [spec.md](spec.md): the Logos wire protocol (content topics, envelopes,
  catch-up) and the backend interface.

## Decisions

| ADR | Decision |
|-----|----------|
| [0001](adr/0001-sync-backend-abstraction.md) | Add a `SyncBackend` abstraction to `reflection-node`, cut at the p2panda `Node` API Reflection already uses |
| [0002](adr/0002-p2panda-operations-as-envelope.md) | On Logos, keep p2panda operations (signed, per-author logs, prune) as the envelope and the local store |
| [0003](adr/0003-broadcast-catch-up.md) | Replace two-party log sync with a broadcast "heads" catch-up over relay |
| [0004](adr/0004-delivery-bus.md) | Talk to Logos Delivery through a tiny `DeliveryBus` trait: in-memory, liblogosdelivery (dlopen) and host adaptors |
| [0005](adr/0005-backend-selection-and-tests.md) | Pick the backend with a cargo feature and an env var; run the unchanged tests on every backend |
| [0007](adr/0007-bridge.md) | A bridge node relays documents between p2panda and Logos Delivery |
| [0006](adr/0006-basecamp-app-split.md) | The Basecamp app lives in its own repo and depends on this fork |
| [0008](adr/0008-rln-message-budget.md) | Publish only for open documents: heads on opening, presence while open, a rate-limited bridge (RLN) |

## Status

| Phase | What | State |
|-------|------|-------|
| P0 | Baseline: upstream tests on the fork (6/6) | done |
| P1 | `SyncBackend` abstraction, p2panda backend, 6/6 unchanged | done |
| P2 | Logos backend + in-memory bus, 6/6 unchanged, plus 5 catch-up tests | done |
| P3 | liblogosdelivery bus, 6/6 unchanged on real Delivery nodes (v0.39.0) | done |
| P4 | Upstream PR (abstraction first, Logos backend second) | planned |
| P5 | [`reflection-basecamp`](https://github.com/vpavlin/reflection-basecamp): Rust core module on loam_core + QML view; two headless Basecamp nodes sync on the Logos test network | done (0.1.0 on lan03) |

## Upstream context

The abstraction is in the spirit of p2panda/reflection#116 ("separate the node
backend from Reflection code"). It also gives an answer to several open issues:

- #187: changes only sync when both sides are online. Logos Delivery store
  nodes keep recent messages, and the catch-up protocol lets any peer that has
  the operations answer, not only the author.
- #240: GDPR concerns about the iroh relay and bootstrap node. The Logos
  network is an alternative with no single operator.
- #145: settings for relays and bootstrap nodes. The backend choice is one
  more such setting.
