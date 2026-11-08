//! `DeliveryBus` over `liblogosdelivery`, loaded at runtime (feature `logos-ffi`, ADR 0004).
//!
//! Targets the C ABI of logos-delivery v0.39 (nim-ffi, `abi = cbor`):
//!
//! - constructor: `int logosdelivery_create_node(req, req_len, cb, ud)`; the reply is the
//!   decimal address of the node context, as a CBOR text string;
//! - methods: `int logosdelivery_<name>(ctx, cb, ud, req, req_len)`; replies are CBOR;
//! - requests are CBOR maps keyed by the Nim parameter names;
//! - events: `logosdelivery_add_event_listener(ctx, name, cb, ud)` delivers JSON.
//!
//! Nothing is linked at build time, so the crate builds and tests without the library.
use std::collections::BTreeMap;
use std::ffi::{CString, c_char, c_int, c_void};
use std::path::Path;
use std::sync::Mutex;

use base64::Engine;
use tokio::sync::{broadcast, oneshot};
use tracing::{info, warn};

use crate::BoxFuture;
use crate::bus::{BusError, BusMessage, DeliveryBus};

const RET_OK: c_int = 0;
const RET_MISSING_CALLBACK: c_int = 2;
const RET_STALE_WARN: c_int = 3;

const BUS_CAPACITY: usize = 4096;

type FfiCallback = extern "C" fn(c_int, *const c_char, usize, *mut c_void);
type CreateFn = unsafe extern "C" fn(*const u8, usize, FfiCallback, *mut c_void) -> c_int;
type MethodFn =
    unsafe extern "C" fn(*mut c_void, FfiCallback, *mut c_void, *const u8, usize) -> c_int;
type AddListenerFn =
    unsafe extern "C" fn(*mut c_void, *const c_char, FfiCallback, *mut c_void) -> u64;

struct Api {
    path: std::path::PathBuf,
    create_node: CreateFn,
    start_node: MethodFn,
    stop_node: MethodFn,
    subscribe: MethodFn,
    unsubscribe: MethodFn,
    send: MethodFn,
    get_node_info: MethodFn,
    add_event_listener: AddListenerFn,
}

/// The library is loaded once per process and never unloaded: its node threads keep running
/// and calling back into it until the process exits, so unloading it would crash.
static API: Mutex<Option<&'static Api>> = Mutex::new(None);

impl Api {
    fn get(path: &Path) -> Result<&'static Self, BusError> {
        let mut api = API.lock().unwrap();
        if let Some(api) = *api {
            if api.path != path {
                return Err(BusError(format!(
                    "{} is already loaded, can't load {} as well",
                    api.path.display(),
                    path.display()
                )));
            }
            return Ok(api);
        }
        let loaded: &'static Api = Box::leak(Box::new(Self::load(path)?));
        *api = Some(loaded);
        Ok(loaded)
    }

    fn load(path: &Path) -> Result<Self, BusError> {
        let error = |e: libloading::Error| BusError(format!("{}: {e}", path.display()));
        // SAFETY: loading a library runs its initialisers; liblogosdelivery is a regular
        // shared library meant to be loaded this way.
        let library = unsafe { libloading::Library::new(path) }.map_err(error)?;
        // SAFETY: the symbol types match the logos-delivery v0.39 C ABI described above.
        unsafe {
            Ok(Self {
                create_node: *library.get(b"logosdelivery_create_node\0").map_err(error)?,
                start_node: *library.get(b"logosdelivery_start_node\0").map_err(error)?,
                stop_node: *library.get(b"logosdelivery_stop_node\0").map_err(error)?,
                subscribe: *library.get(b"logosdelivery_subscribe\0").map_err(error)?,
                unsubscribe: *library.get(b"logosdelivery_unsubscribe\0").map_err(error)?,
                send: *library.get(b"logosdelivery_send\0").map_err(error)?,
                get_node_info: *library
                    .get(b"logosdelivery_get_node_info\0")
                    .map_err(error)?,
                add_event_listener: *library
                    .get(b"logosdelivery_add_event_listener\0")
                    .map_err(error)?,
                path: path.to_owned(),
            })
            .inspect(|_| std::mem::forget(library))
        }
    }
}

type Reply = (c_int, Vec<u8>);

/// Completes one call: takes back the sender boxed into `user_data`.
extern "C" fn on_reply(ret: c_int, msg: *const c_char, len: usize, user_data: *mut c_void) {
    if ret == RET_STALE_WARN {
        // Progress ping, the real reply comes later.
        return;
    }
    // SAFETY: `user_data` is the `Box<oneshot::Sender<Reply>>` leaked in `call`, and the library
    // calls the reply callback exactly once with a non-stale code.
    let tx = unsafe { Box::from_raw(user_data as *mut oneshot::Sender<Reply>) };
    let bytes = if msg.is_null() || len == 0 {
        Vec::new()
    } else {
        // SAFETY: `msg` points at `len` bytes, valid for the duration of the callback.
        unsafe { std::slice::from_raw_parts(msg as *const u8, len) }.to_vec()
    };
    let _ = tx.send((ret, bytes));
}

/// Receives events; `user_data` is a leaked `broadcast::Sender<BusMessage>`.
extern "C" fn on_event(_ret: c_int, msg: *const c_char, len: usize, user_data: *mut c_void) {
    if msg.is_null() || len == 0 {
        return;
    }
    // SAFETY: as in `on_reply`.
    let bytes = unsafe { std::slice::from_raw_parts(msg as *const u8, len) };
    // SAFETY: `user_data` is the sender leaked in `LibLogosDeliveryBus::new`, never freed.
    let tx = unsafe { &*(user_data as *const broadcast::Sender<BusMessage>) };
    if let Some(message) = parse_received(bytes) {
        let _ = tx.send(message);
    }
}

/// `message_received` event → bus message.
fn parse_received(bytes: &[u8]) -> Option<BusMessage> {
    let event: serde_json::Value = serde_json::from_slice(bytes).ok()?;
    if event.get("eventType")?.as_str()? != "message_received" {
        return None;
    }
    let message = event.get("message")?;
    let content_topic = message.get("contentTopic")?.as_str()?.to_owned();
    let wire = base64::engine::general_purpose::STANDARD
        .decode(message.get("payload")?.as_str()?)
        .ok()?;
    let payload = from_wire(wire);
    Some(BusMessage {
        content_topic,
        payload,
    })
}

/// Message payloads travel as the base64 text of the frame, as Loam sends them (loam_core, the
/// shared transport of the Basecamp apps and phones, base64-decodes every payload it receives).
/// See spec section 3.
fn to_wire(frame: &[u8]) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .encode(frame)
        .into_bytes()
}

/// The frame in a received payload: the base64 text Loam sends, or raw bytes from an older peer.
fn from_wire(wire: Vec<u8>) -> Vec<u8> {
    let looks_like_text = !wire.is_empty()
        && wire
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'/' | b'=' | b'-' | b'_'));
    if looks_like_text {
        let text: String = wire
            .iter()
            .map(|&b| match b {
                b'-' => '+',
                b'_' => '/',
                b => b as char,
            })
            .filter(|c| *c != '=')
            .collect();
        if let Ok(frame) = base64::engine::general_purpose::STANDARD_NO_PAD.decode(text) {
            return frame;
        }
    }
    wire
}

fn request(fields: &[(&str, &str)]) -> Vec<u8> {
    let map: BTreeMap<&str, &str> = fields.iter().copied().collect();
    p2panda_core::cbor::encode_cbor(&map).expect("encodable request")
}

/// A Logos Delivery node run in this process through `liblogosdelivery`.
pub struct LibLogosDeliveryBus {
    api: &'static Api,
    ctx: usize,
    tx: broadcast::Sender<BusMessage>,
}

impl LibLogosDeliveryBus {
    /// Load the library, create and start a node with the given configuration JSON.
    pub async fn new(library: &Path, config_json: &str) -> Result<Self, BusError> {
        let api = Api::get(library)?;

        let (reply_tx, reply_rx) = oneshot::channel::<Reply>();
        let req = request(&[("configJson", config_json)]);
        let user_data = Box::into_raw(Box::new(reply_tx)) as *mut c_void;
        // SAFETY: `req` outlives the call (the library copies it); `user_data` is reclaimed by
        // `on_reply`.
        let ret = unsafe { (api.create_node)(req.as_ptr(), req.len(), on_reply, user_data) };
        if ret == RET_MISSING_CALLBACK {
            // SAFETY: the callback was not and will not be called.
            drop(unsafe { Box::from_raw(user_data as *mut oneshot::Sender<Reply>) });
            return Err(BusError("create_node: missing callback".into()));
        }
        let address = decode_reply("create_node", reply_rx.await)?;
        let ctx: usize = address
            .parse()
            .map_err(|_| BusError(format!("create_node returned {address:?}")))?;

        let (tx, _) = broadcast::channel(BUS_CAPACITY);
        // Leaked on purpose: the library may call the listener until the process exits.
        let event_tx = Box::into_raw(Box::new(tx.clone())) as *mut c_void;
        let name = CString::new("onMessageReceived").unwrap();
        // SAFETY: `ctx` is a live node context, `event_tx` stays valid forever.
        let id = unsafe {
            (api.add_event_listener)(ctx as *mut c_void, name.as_ptr(), on_event, event_tx)
        };
        if id == 0 {
            return Err(BusError("add_event_listener failed".into()));
        }

        let bus = Self { api, ctx, tx };
        bus.call("start_node", bus.api.start_node, &[]).await?;
        info!("Logos Delivery node started");
        Ok(bus)
    }

    /// The node's listen addresses.
    pub async fn multiaddrs(&self) -> Result<Vec<String>, BusError> {
        let addrs = self
            .call(
                "get_node_info",
                self.api.get_node_info,
                &[("nodeInfoId", "MyMultiaddresses")],
            )
            .await?;
        Ok(addrs
            .split(',')
            .map(|a| a.trim().to_owned())
            .filter(|a| !a.is_empty())
            .collect())
    }

    async fn call(
        &self,
        name: &str,
        method: MethodFn,
        fields: &[(&str, &str)],
    ) -> Result<String, BusError> {
        let (reply_tx, reply_rx) = oneshot::channel::<Reply>();
        let req = request(fields);
        let user_data = Box::into_raw(Box::new(reply_tx)) as *mut c_void;
        // SAFETY: as in `new`.
        let ret = unsafe {
            method(
                self.ctx as *mut c_void,
                on_reply,
                user_data,
                req.as_ptr(),
                req.len(),
            )
        };
        if ret == RET_MISSING_CALLBACK {
            // SAFETY: the callback was not and will not be called.
            drop(unsafe { Box::from_raw(user_data as *mut oneshot::Sender<Reply>) });
            return Err(BusError(format!("{name}: missing callback")));
        }
        decode_reply(name, reply_rx.await)
    }
}

fn decode_reply(
    name: &str,
    reply: Result<Reply, oneshot::error::RecvError>,
) -> Result<String, BusError> {
    let (ret, bytes) = reply.map_err(|_| BusError(format!("{name}: no reply")))?;
    if ret != RET_OK {
        return Err(BusError(format!(
            "{name}: {}",
            String::from_utf8_lossy(&bytes)
        )));
    }
    if bytes.is_empty() {
        return Ok(String::new());
    }
    p2panda_core::cbor::decode_cbor::<String, _>(&bytes[..])
        .map_err(|error| BusError(format!("{name}: undecodable reply: {error}")))
}

impl Drop for LibLogosDeliveryBus {
    fn drop(&mut self) {
        // Stop the node; the reply is not awaited. Its listener and context stay allocated:
        // the library has no way to tell us when the last callback has run.
        let (reply_tx, _reply_rx) = oneshot::channel::<Reply>();
        let req = request(&[]);
        let user_data = Box::into_raw(Box::new(reply_tx)) as *mut c_void;
        // SAFETY: as in `call`.
        let ret = unsafe {
            (self.api.stop_node)(
                self.ctx as *mut c_void,
                on_reply,
                user_data,
                req.as_ptr(),
                req.len(),
            )
        };
        if ret == RET_MISSING_CALLBACK {
            // SAFETY: the callback was not and will not be called.
            drop(unsafe { Box::from_raw(user_data as *mut oneshot::Sender<Reply>) });
        }
    }
}

impl DeliveryBus for LibLogosDeliveryBus {
    fn subscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            self.call(
                "subscribe",
                self.api.subscribe,
                &[("contentTopicStr", &content_topic)],
            )
            .await
            .map(|_| ())
        })
    }

    fn unsubscribe(&self, content_topic: String) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            self.call(
                "unsubscribe",
                self.api.unsubscribe,
                &[("contentTopicStr", &content_topic)],
            )
            .await
            .map(|_| ())
        })
    }

    fn publish(
        &self,
        content_topic: String,
        payload: Vec<u8>,
    ) -> BoxFuture<'_, Result<(), BusError>> {
        Box::pin(async move {
            let message = serde_json::json!({
                "contentTopic": content_topic,
                "payload": base64::engine::general_purpose::STANDARD.encode(to_wire(&payload)),
                "ephemeral": false,
            })
            .to_string();
            self.call("send", self.api.send, &[("messageJson", &message)])
                .await
                .map(|_| ())
        })
    }

    fn messages(&self) -> broadcast::Receiver<BusMessage> {
        self.tx.subscribe()
    }
}

/// Addresses of the nodes this process started in local-only mode, so that later nodes can dial
/// earlier ones (the counterpart of p2panda's local discovery in tests).
static LOCAL_NODES: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// Configuration for a node that only talks to other local-only nodes in this process.
pub fn local_config() -> String {
    let entry_nodes = LOCAL_NODES.lock().unwrap().clone();
    serde_json::json!({
        "mode": "Core",
        "preset": "",
        "messagingOverrides": {
            "cluster-id": 42,
            "num-shards-in-network": 1,
            "listen-address": "127.0.0.1",
            "tcp-port": 0,
            "discv5-discovery": false,
            "discv5-udp-port": 0,
            "quic-support": false,
            "log-level": "WARN",
            "entry-node": entry_nodes,
        }
    })
    .to_string()
}

pub async fn register_local(bus: &LibLogosDeliveryBus) {
    match bus.multiaddrs().await {
        Ok(addrs) => LOCAL_NODES.lock().unwrap().extend(addrs),
        Err(error) => warn!("can't read the node's addresses: {error}"),
    }
}

/// Configuration for the public Logos test network, with RLN off.
///
/// The same node configuration loam_core uses by default (a mirror of logos-delivery v0.39's
/// `LogosTestConf`), so a GTK Reflection and a Basecamp one on loam_core meet on one network.
/// With `preset: "logos.test"` RLN is on, and without a membership every send fails.
pub fn default_config() -> String {
    serde_json::json!({
        "mode": "Core",
        "preset": "",
        "clusterId": 2,
        "numShardsInNetwork": 8,
        "maxMessageSize": "150KiB",
        "tcpPort": 0,
        "discv5UdpPort": 0,
        "discv5Discovery": true,
        "enableKadDiscovery": true,
        "reliabilityEnabled": true,
        "entryNodes": [
            "/dns4/node-01.do-ams3.logos.test.status.im/tcp/30303/p2p/16Uiu2HAmQ9X2xDfPG3uL77V9piYDhjq14JhKCtcmNYsTMKNqrKCj",
            "/dns4/node-02.do-ams3.logos.test.status.im/tcp/30303/p2p/16Uiu2HAmB8NYprrfQrgWVzsJtYWkfjsXbmJEGNMG6othXsQ53BwG",
            "/dns4/node-01.gc-us-central1-a.logos.test.status.im/tcp/30303/p2p/16Uiu2HAmF8WtwGPmeGHgYAX2277jHgy5cW9F7zsB8EqUjBZQAZQ3",
            "/dns4/node-02.gc-us-central1-a.logos.test.status.im/tcp/30303/p2p/16Uiu2HAmUuXhUW9bdJpzN1kfDziFiUZo4bszTk66cvr7uuyCHXR7",
            "/dns4/node-01.ac-cn-hongkong-c.logos.test.status.im/tcp/30303/p2p/16Uiu2HAmL3oU95jh1BZHozn3uNhx8HEneirgr8M1jEAapzXGDqRF",
            "/dns4/node-02.ac-cn-hongkong-c.logos.test.status.im/tcp/30303/p2p/16Uiu2HAm28CoBZjpyxsanC8tQpbvZ7bZJnVYuB1EgFzb571qpWsV"
        ]
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_payloads_are_base64_text_and_raw_is_still_read() {
        let frame: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let wire = to_wire(&frame);
        assert!(wire.iter().all(u8::is_ascii));
        assert_eq!(from_wire(wire), frame);
        // A raw binary frame (version byte 0x01, then nonce...) is not base64 text.
        let raw = vec![1u8, 200, 3, 0, 255, 17];
        assert_eq!(from_wire(raw.clone()), raw);
    }
}
