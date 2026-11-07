# ADR 0006: The Basecamp app is a separate repo on top of this fork

Status: accepted (2026-11-07), amended the same day (see "Amendment")

## Context

Logos Basecamp apps are a C++/Qt core module (a Qt plugin, loaded by the Logos
host) plus a QML view. Reflection is Rust: a GTK app over a GObject document
layer.

We want collaborative notes in Basecamp that interoperate with the GTK app
over Logos. We also want the Reflection fork to stay upstreamable.

## Decision

Two repositories:

1. **`vpavlin/reflection`** (this fork): only changes we would offer upstream.
   That means the backend abstraction, the Logos backend and the bus adaptors.
   No Basecamp code.
2. **`vpavlin/reflection-basecamp`**:
   - a Rust `staticlib` crate that depends on `reflection-doc` and
     `reflection-node` from the fork (git dependency, pinned);
   - a C ABI over it (open/join document, insert/delete text, read text,
     cursors, events out);
   - a host `DeliveryBus` adaptor that sends and receives through `loam_core`
     (the shared Logos transport), so the node is the host's, not a second one;
   - `reflection_core`, the Basecamp core module (C++), and `reflection_ui`,
     the QML view.

`reflection-doc` uses GObject and a glib `MainContext`. Upstream already
supports running a document on a given context
(`Service::join_document_with_main_context`). The core module runs one glib
context on a private thread and marshals calls to it, so Qt's own event loop
is never shared.

## Consequences

- The fork's diff stays small and reviewable. Basecamp churn never touches it.
- The Basecamp app links GPL-3.0 crates, so it is GPL-3.0, unlike our usual
  MIT/Apache app repos. That is fine for an app.
- Interop: same document ids, same operations and same content topics. A GTK
  Reflection on the Logos backend and the Basecamp app edit the same note.
- glib must be bundled for darwin and linux-arm64 Basecamp variants. That is
  part of the module build, not the fork.

## Amendment (2026-11-07): a Rust core module, no glib, transport via loam_core

Three findings change the "how" (the repo split stands):

1. **logos-module-builder 0.3.1+ builds core modules written in Rust** (`interface: "cdylib"`,
   `codegen.rust`). The crate is the module: the builder derives the contract from a Rust trait,
   generates typed clients for dependencies (`modules().<dep>`, sync, async and events as an
   iterator), and links the staticlib. No C++ layer and no hand-written C ABI are needed.
2. **`reflection-doc` is not needed in Basecamp.** It is a GObject layer. Its wire behaviour is
   small: one Loro text container, local updates published as deltas, a debounced Loro
   snapshot published as a prune, and remote bytes imported. Cursors are postcard-encoded Loro
   cursors. The core module re-implements exactly that on `reflection-node` and the same `loro`
   version, so no glib is linked into Basecamp (which also keeps macOS and arm64 builds simple).
   Wire compatibility with the GTK app is checked by a test that runs `reflection-doc` and the
   Basecamp document layer against each other over the Logos protocol.
3. **The host bus goes through `loam_core`, not `delivery_module`.** A Basecamp host has one
   `delivery_module`, and its node belongs to whichever module created it (for our apps that
   is `loam_core`, the shared transport). `reflection_core` therefore depends on `loam_core`:
   `join(topic)` subscribes, `sendSealed(topic, base64)` publishes, and the `received` event
   delivers. It shares the node with Scala and the other apps, and later with phones.
