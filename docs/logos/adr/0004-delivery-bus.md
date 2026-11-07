# ADR 0004: A DeliveryBus trait with pluggable adaptors

Status: accepted (2026-11-07)

## Context

There are three ways a Reflection node can reach Logos Delivery:

1. **Embedded library.** The standalone GTK app loads `liblogosdelivery` (the
   Nim library behind every Logos Delivery client) and runs its own node.
2. **Host-provided.** Inside Logos Basecamp, the node is owned by the host:
   a `delivery_module` or the shared `loam_core` transport. The Rust code must
   send and receive through calls the host gives it.
3. **Tests.** CI has no native library and no network. Upstream CI runs
   `--all-features`, so the Logos feature has to build and pass there.

The `liblogosdelivery` C API changes between releases. The 0.38 line uses
`logosdelivery_create_node` with one callback type. The 0.39 line (nim-ffi
0.3) uses `logosdelivery_ctx_create` and typed reply callbacks. Events moved
from one global callback to `logosdelivery_add_event_listener`. Linking the
library at build time would tie Reflection's build to one release and to a
native toolchain.

## Decision

The Logos backend talks to the network only through:

```rust
pub trait DeliveryBus: Send + Sync + 'static {
    async fn subscribe(&self, content_topic: &str) -> Result<(), BusError>;
    async fn unsubscribe(&self, content_topic: &str) -> Result<(), BusError>;
    async fn publish(&self, content_topic: &str, payload: Vec<u8>) -> Result<(), BusError>;
    /// All messages on subscribed content topics, including our own echoes.
    fn messages(&self) -> broadcast::Receiver<BusMessage>;
}
```

Adaptors:

| Adaptor | Where | Use |
|---------|-------|-----|
| `MemoryBus` | `reflection-node`, feature `logos` | Process-local hub. Tests and CI. Simulates relay: delivers only to subscribers, echoes to the sender. |
| `LibLogosDeliveryBus` | `reflection-node`, feature `logos-ffi` | Loads `liblogosdelivery` at **runtime** (`libloading`), so the build needs no native library. Targets the 0.39 C ABI. |
| Host bus | `reflection-basecamp` repo | C ABI callbacks from the Basecamp core module to `loam_core` / `delivery_module`. |

## Consequences

- `cargo test --all-features` builds everywhere. The FFI adaptor compiles
  against Rust declarations only.
- Supporting another `liblogosdelivery` release is one adaptor change, not a
  backend change.
- The in-memory bus tests the whole Logos protocol (envelopes, catch-up,
  suppression) deterministically. The FFI bus tests real Logos Delivery.
- Self-echo: relay delivers our own messages back. The bus reports them and
  the backend drops them by operation hash, the same as any duplicate.
