//! Artifact metadata model.

use serde::{Deserialize, Serialize};

use crate::clock::UnixMillis;
use crate::digest::ContentDigest;
use crate::error::CoreError;
use crate::id::{ArtifactId, RunId};

/// Descriptive record for one stored artifact. Contents live in the
/// [`crate::store::ArtifactStore`] keyed by [`ArtifactMeta::digest`];
/// metadata repositories never carry payload bytes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactMeta {
    pub id: ArtifactId,
    /// Stable label within its producing context (`stdout`, `stderr`, or an
    /// ingest-supplied name).
    pub name: String,
    pub media_type: String,
    /// Content digest; also the blob's storage key (content addressing).
    pub digest: ContentDigest,
    pub size: u64,
    pub created_at: UnixMillis,
    /// Set when the artifact was captured from a run's output stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produced_by_run: Option<RunId>,
}

impl ArtifactMeta {
    /// Validates basic shape at construction boundaries.
    ///
    /// # Errors
    /// [`CoreError::Validation`] for empty names or malformed media types.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.name.is_empty() || self.name.len() > 128 {
            return Err(CoreError::Validation(
                "artifact name must be 1..=128 characters".into(),
            ));
        }
        if self.media_type.is_empty()
            || self.media_type.len() > 128
            || self.media_type.chars().any(char::is_control)
        {
            return Err(CoreError::Validation(
                "artifact media_type must be at most 128 printable characters".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest;

    #[test]
    fn validation_rejects_bad_names_and_media_types() {
        let base = |name: &str, media: &str| ArtifactMeta {
            id: ArtifactId::generate(),
            name: name.to_owned(),
            media_type: media.to_owned(),
            digest: digest::hash_bytes(digest::DOMAIN_ARTIFACT_BLOB, b"x"),
            size: 1,
            created_at: 0,
            produced_by_run: None,
        };
        assert!(base("", "text/plain").validate().is_err());
        assert!(base(&"x".repeat(129), "text/plain").validate().is_err());
        assert!(base("ok", "").validate().is_err());
        assert!(base("ok", "text/plain\n").validate().is_err());
        assert!(base("ok", "application/json").validate().is_ok());
    }

    #[test]
    fn meta_round_trips() {
        let meta = ArtifactMeta {
            id: ArtifactId::generate(),
            name: "run-stdout".into(),
            media_type: "text/plain; charset=utf-8".into(),
            digest: digest::hash_bytes(digest::DOMAIN_CAPTURE, b"bytes"),
            size: 5,
            created_at: 42,
            produced_by_run: Some(crate::id::RunId::generate()),
        };
        let encoded = serde_json::to_string(&meta).expect("encode");
        let decoded: ArtifactMeta = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, meta);
    }
}
