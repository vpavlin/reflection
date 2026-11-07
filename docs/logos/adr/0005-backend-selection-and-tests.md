# ADR 0005: Backend selection, and the unchanged tests as the contract

Status: accepted (2026-11-07)

## Context

Upstream has 6 tests:

- `reflection-node`: `create_topic`, `subscribe_topic` (two nodes, a snapshot
  published by one arrives at the other).
- `reflection-doc`: `create_document`, `basic_sync`, `sync_multiple_changes`,
  `sync_longer_text` (two services in one process edit one document and
  converge).

They build nodes with `Node::new(key, network_id, None)` and
`Service::new(&key, None)`. Neither takes a backend. We want these exact tests
to prove the Logos backend correct, without editing them.

## Decision

- Cargo features on `reflection-node` (and passed through by
  `reflection-doc`):
  - `logos`: the Logos backend plus `MemoryBus`. Pure Rust.
  - `logos-ffi`: adds `LibLogosDeliveryBus` (implies `logos`).
- Runtime selection in `Node::new`, read once from the environment:
  - unset or `p2panda`: p2panda (default, unchanged);
  - `REFLECTION_BACKEND=logos-memory`: Logos backend over `MemoryBus`;
  - `REFLECTION_BACKEND=logos`: Logos backend over `LibLogosDeliveryBus`.
    Library path from `REFLECTION_LOGOS_LIB`. Node config (preset, entry
    nodes) from `REFLECTION_LOGOS_CONFIG`.
  - If a Logos value is set but the feature is not compiled in, `Node::new`
    fails with a clear error rather than silently using p2panda.
- In test builds (`cfg(test)` / `test_utils`), p2panda nodes do not dial the
  public bootstrap. Likewise, the FFI bus in tests peers in-process nodes with
  each other on localhost instead of joining the public network.
- CI: the existing jobs stay as they are (they use the default backend). One
  more job runs the same `cargo test` with `REFLECTION_BACKEND=logos-memory`.
  The FFI run needs the native library and is a manual job.

## Consequences

- Passing 6/6 on `logos-memory` proves the protocol. Passing 6/6 on `logos`
  proves the real network path. Neither needs a test change.
- An env var is a blunt tool for an app setting. For the GTK app a settings
  entry can call `Node::with_backend` later. The env var stays for tests and
  power users.
- If a test cannot pass on a backend for a fundamental reason, that is
  recorded here as an explicit exception, not hidden by editing the test.
  None known so far.
