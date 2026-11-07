//! Wire format of the Logos backend (see docs/logos/spec.md).
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use p2panda::{Hash, NetworkId, Topic, VerifyingKey};
use p2panda_core::{Signature, SigningKey};
use serde::{Deserialize, Serialize};

/// Version byte in front of every message on the wire.
pub(crate) const WIRE_VERSION: u8 = 1;

/// Largest operation we send. Logos Delivery messages are limited to about 150 KB.
pub(crate) const MAX_FRAME_LEN: usize = 140 * 1024;

const NONCE_LEN: usize = 24;

/// What a peer sends on a document's content topics, before encryption.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum Frame {
    /// A p2panda operation: CBOR-encoded header plus the raw body.
    #[serde(rename = "op")]
    Operation { header: Bytes, body: Option<Bytes> },

    /// The latest operation we have from each author.
    #[serde(rename = "heads")]
    Heads {
        from: VerifyingKey,
        heads: Vec<(VerifyingKey, u64, Hash)>,
        /// Makes every heads message unique on the wire. Transports that drop repeated
        /// payloads (Loam dedups by frame hash) would otherwise swallow a re-sent heads.
        #[serde(default)]
        nonce: u64,
    },

    /// A signed ephemeral message (presence, cursors).
    #[serde(rename = "eph")]
    Ephemeral(SignedEphemeral),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct SignedEphemeral {
    pub author: VerifyingKey,
    pub timestamp: u64,
    pub body: Bytes,
    pub signature: Signature,
}

impl SignedEphemeral {
    fn signing_bytes(topic: &Topic, author: &VerifyingKey, timestamp: u64, body: &[u8]) -> Vec<u8> {
        p2panda_core::cbor::encode_cbor(&(
            "reflection-eph-v1",
            topic,
            author,
            timestamp,
            Bytes(body.to_vec()),
        ))
        .expect("encodable")
    }

    pub fn new(topic: &Topic, signing_key: &SigningKey, timestamp: u64, body: Vec<u8>) -> Self {
        let author = signing_key.verifying_key();
        let signature = signing_key.sign(&Self::signing_bytes(topic, &author, timestamp, &body));
        Self {
            author,
            timestamp,
            body: Bytes(body),
            signature,
        }
    }

    pub fn verify(&self, topic: &Topic) -> bool {
        let bytes = Self::signing_bytes(topic, &self.author, self.timestamp, &self.body.0);
        self.author.verify(&bytes, &self.signature)
    }
}

/// Per-document keys and content topic names.
///
/// The document id (the topic) is the shared secret of a document: whoever knows it can join.
/// So neither the id nor the content travel in clear on the relay network. The content topics
/// are derived from a hash of the id, and every frame is encrypted with a key derived from it.
#[derive(Clone)]
pub(crate) struct TopicKeys {
    pub ops_topic: String,
    pub ephemeral_topic: String,
    cipher: XChaCha20Poly1305,
    key: [u8; 32],
}

impl TopicKeys {
    pub fn new(network_id: &NetworkId, topic: &Topic) -> Self {
        let derive = |label: &str| {
            let mut input = Vec::new();
            input.extend_from_slice(label.as_bytes());
            input.extend_from_slice(&network_id[..]);
            input.extend_from_slice(topic.as_bytes());
            *Hash::digest(input).as_bytes()
        };

        let name = hex::encode(&derive("reflection-logos-topic-v1")[..16]);
        let key = derive("reflection-logos-key-v1");

        Self {
            ops_topic: format!("/reflection/1/{name}-ops/cbor"),
            ephemeral_topic: format!("/reflection/1/{name}-eph/cbor"),
            cipher: XChaCha20Poly1305::new((&key).into()),
            key,
        }
    }

    /// Encrypt a frame. The nonce is derived from the key and the plaintext, so the same frame
    /// always encrypts to the same bytes and no randomness is needed.
    pub fn seal(&self, frame: &Frame) -> Vec<u8> {
        let plaintext = p2panda_core::cbor::encode_cbor(frame).expect("encodable frame");

        let mut nonce_input = Vec::with_capacity(32 + plaintext.len());
        nonce_input.extend_from_slice(&self.key);
        nonce_input.extend_from_slice(&plaintext);
        let nonce_hash = Hash::digest(nonce_input);
        let nonce = XNonce::from_slice(&nonce_hash.as_bytes()[..NONCE_LEN]);

        let ciphertext = self
            .cipher
            .encrypt(
                nonce,
                Payload {
                    msg: &plaintext,
                    aad: &[WIRE_VERSION],
                },
            )
            .expect("encryption does not fail");

        let mut out = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        out.push(WIRE_VERSION);
        out.extend_from_slice(nonce);
        out.extend_from_slice(&ciphertext);
        out
    }

    pub fn open(&self, bytes: &[u8]) -> Option<Frame> {
        if bytes.len() < 1 + NONCE_LEN || bytes[0] != WIRE_VERSION {
            return None;
        }
        let nonce = XNonce::from_slice(&bytes[1..1 + NONCE_LEN]);
        let plaintext = self
            .cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &bytes[1 + NONCE_LEN..],
                    aad: &[WIRE_VERSION],
                },
            )
            .ok()?;
        p2panda_core::cbor::decode_cbor(&plaintext[..]).ok()
    }
}

/// Bytes, encoded as a CBOR byte string (serde's default for `Vec<u8>` is an array of numbers).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Bytes(pub Vec<u8>);

impl Serialize for Bytes {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Bytes {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Bytes;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a byte string")
            }
            fn visit_bytes<E: serde::de::Error>(self, v: &[u8]) -> Result<Bytes, E> {
                Ok(Bytes(v.to_vec()))
            }
            fn visit_byte_buf<E: serde::de::Error>(self, v: Vec<u8>) -> Result<Bytes, E> {
                Ok(Bytes(v))
            }
        }
        d.deserialize_bytes(Visitor)
    }
}
