# ADR 0006: The Basecamp app is a separate repo on top of this fork

Status: accepted (2026-11-07)

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
