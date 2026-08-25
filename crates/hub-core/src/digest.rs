//! Domain-separated SHA-256 content digests.
//!
//! The construction mirrors the discipline used by `scirust-digest` in the
//! SciRust monorepo: a fixed prefix, a length-framed domain, then the data,
//! so chunking can never change the preimage boundary. Domains are namespaced
//! `scirust-hub:*` because digest values are not interchangeable with SciRust
//! monorepo digests; see `docs/adr/0004-execution-model.md`.

use std::fmt;
use std::io::Read;

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const PREFIX: &[u8] = b"scirust-hub-digest:v1\0";
pub const DIGEST_LEN: usize = 32;

/// Domain for artifact blob contents.
pub const DOMAIN_ARTIFACT_BLOB: &[u8] = b"scirust-hub:artifact-blob:v1";
/// Domain for component manifest canonical bytes.
pub const DOMAIN_COMPONENT_MANIFEST: &[u8] = b"scirust-hub:component-manifest:v1";
/// Domain for canonical run parameter JSON.
pub const DOMAIN_RUN_PARAMS: &[u8] = b"scirust-hub:run-params:v1";
/// Domain for captured process output streams.
pub const DOMAIN_CAPTURE: &[u8] = b"scirust-hub:capture:v1";

/// A stable 32-byte content digest with lowercase-hex interchange.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ContentDigest([u8; DIGEST_LEN]);

impl ContentDigest {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; DIGEST_LEN]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; DIGEST_LEN] {
        &self.0
    }

    /// Lowercase hex, the only serialization this type produces.
    #[must_use]
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(DIGEST_LEN * 2);
        for byte in self.0 {
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        out
    }

    /// Accepts 64 hex characters in either case.
    pub fn from_hex(hex: &str) -> Result<Self, ParseDigestError> {
        if hex.len() != DIGEST_LEN * 2 {
            return Err(ParseDigestError);
        }
        let bytes = hex.as_bytes();
        let mut out = [0u8; DIGEST_LEN];
        for (i, slot) in out.iter_mut().enumerate() {
            let hi = hex_value(bytes[i * 2]).ok_or(ParseDigestError)?;
            let lo = hex_value(bytes[i * 2 + 1]).ok_or(ParseDigestError)?;
            *slot = (hi << 4) | lo;
        }
        Ok(Self(out))
    }
}

impl fmt::Display for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for ContentDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ContentDigest({self})")
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("digest must be exactly 64 hexadecimal characters")]
pub struct ParseDigestError;

/// One-shot domain-separated hash of in-memory bytes.
#[must_use]
pub fn hash_bytes(domain: &[u8], data: &[u8]) -> ContentDigest {
    let mut state = DigestState::new(domain);
    state.update(data);
    state.finalize()
}

/// Streaming hash of anything readable, without buffering whole artifacts.
///
/// # Errors
/// Propagates IO errors from the reader.
pub fn hash_reader<R: Read>(domain: &[u8], reader: &mut R) -> std::io::Result<ContentDigest> {
    let mut state = DigestState::new(domain);
    let mut buf = [0u8; 64 * 1024];
    loop {
        match reader.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => state.update(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        }
    }
    Ok(state.finalize())
}

/// Length-framed streaming state; the domain/data boundary is independent of
/// how the caller chunks updates.
pub struct DigestState(Sha256);

impl DigestState {
    #[must_use]
    pub fn new(domain: &[u8]) -> Self {
        let mut state = Sha256::new();
        state.update(PREFIX);
        state.update((domain.len() as u64).to_le_bytes());
        state.update(domain);
        Self(state)
    }

    pub fn update(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }

    #[must_use]
    pub fn finalize(self) -> ContentDigest {
        ContentDigest(self.0.finalize().into())
    }
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_value(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn domains_are_separated() {
        assert_ne!(
            hash_bytes(DOMAIN_ARTIFACT_BLOB, b"same"),
            hash_bytes(DOMAIN_CAPTURE, b"same")
        );
    }

    #[test]
    fn chunking_does_not_change_digest() {
        let data = b"deterministic hub provenance";
        let expected = hash_bytes(DOMAIN_RUN_PARAMS, data);
        let mut state = DigestState::new(DOMAIN_RUN_PARAMS);
        state.update(&data[..5]);
        state.update(&data[5..]);
        assert_eq!(state.finalize(), expected);
    }

    #[test]
    fn reader_matches_one_shot() {
        let data = vec![0x5Au8; 200_000];
        let mut reader = &data[..];
        assert_eq!(
            hash_reader(DOMAIN_ARTIFACT_BLOB, &mut reader).expect("read"),
            hash_bytes(DOMAIN_ARTIFACT_BLOB, &data)
        );
    }

    #[test]
    fn hex_round_trip_is_stable() {
        let digest = hash_bytes(DOMAIN_CAPTURE, b"payload");
        let hex = digest.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit()));
        assert_eq!(ContentDigest::from_hex(&hex).expect("hex"), digest);
        assert_eq!(
            ContentDigest::from_hex(&hex.to_uppercase()).expect("hex"),
            digest
        );
        assert!(ContentDigest::from_hex("xyz").is_err());
    }

    #[test]
    fn known_vector_pins_the_construction() {
        // Pins framing (prefix + length + domain + data) against accidental
        // change; value computed once with this exact construction.
        let d = hash_bytes(b"pin", b"abc");
        assert_eq!(
            d.to_hex(),
            "eebd901a9f381cd53635702755927a740838a78e6826e95fc8ca63ee4e8ec011"
        );
    }
}
