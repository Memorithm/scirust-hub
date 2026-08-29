//! Artifact metadata and the storage ports.
//!
//! Metadata (what, when, which digest) is deliberately separate from blob
//! contents; the metadata repository never stores payload bytes.

use crate::artifact::ArtifactMeta;
use crate::digest::ContentDigest;
use crate::error::CoreError;
use crate::id::{ArtifactId, ComponentId, RunId};

/// Repository of registered component manifests, keyed by `(id, version)`.
pub trait ComponentRepository: Send + Sync {
    /// Stores a manifest. Implementations must reject overwriting an existing
    /// `(id, version)` with different content; identical content is a no-op
    /// (idempotent registration).
    ///
    /// Returns `true` when the manifest was newly inserted, `false` when an
    /// identical one already existed.
    ///
    /// # Errors
    /// [`CoreError::ComponentConflict`] on divergent content for the same key.
    fn put(&self, manifest: &crate::component::ComponentManifest) -> Result<bool, CoreError>;

    /// Latest-registered manifest for `id` across versions, if any.
    ///
    /// # Errors
    /// Backend failures only.
    fn latest(
        &self,
        id: &ComponentId,
    ) -> Result<Option<crate::component::ComponentManifest>, CoreError>;

    /// All manifests, deterministically ordered by `(id, version)`.
    ///
    /// # Errors
    /// Backend failures only.
    fn list(&self) -> Result<Vec<crate::component::ComponentManifest>, CoreError>;
}

/// Append/update repository of run records.
pub trait RunRepository: Send + Sync {
    /// # Errors
    /// Backend failures only.
    fn put(&self, record: &crate::run::RunRecord) -> Result<(), CoreError>;

    /// # Errors
    /// Backend failures only.
    fn get(&self, id: &RunId) -> Result<Option<crate::run::RunRecord>, CoreError>;

    /// All runs, deterministically ordered by `(created_at, id)`.
    ///
    /// # Errors
    /// Backend failures only.
    fn list(&self) -> Result<Vec<crate::run::RunRecord>, CoreError>;
}

/// Metadata index for artifacts.
pub trait ArtifactMetadataRepository: Send + Sync {
    /// # Errors
    /// Backend failures only.
    fn put(&self, meta: &ArtifactMeta) -> Result<(), CoreError>;

    /// # Errors
    /// Backend failures only.
    fn get(&self, id: &ArtifactId) -> Result<Option<ArtifactMeta>, CoreError>;

    /// All artifact metadata, deterministically ordered by `(created_at, id)`.
    ///
    /// # Errors
    /// Backend failures only.
    fn list(&self) -> Result<Vec<ArtifactMeta>, CoreError>;
}

/// Repository of workflow records.
pub trait WorkflowRepository: Send + Sync {
    /// Stores a workflow snapshot. Implementations must preserve an already
    /// persisted `cancel_requested_at` when a stale writer supplies `None`.
    /// # Errors
    /// Backend failures only.
    fn put(&self, record: &crate::workflow::WorkflowRecord) -> Result<(), CoreError>;

    /// Atomically records cancellation intent and returns the resulting
    /// record. The timestamp is monotonic: repeated requests keep the first.
    /// # Errors
    /// Backend failures only.
    fn request_cancel(
        &self,
        id: &crate::id::WorkflowId,
        at: crate::clock::UnixMillis,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError>;

    /// # Errors
    /// Backend failures only.
    fn get(
        &self,
        id: &crate::id::WorkflowId,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError>;

    /// All workflows, deterministically ordered by `(created_at, id)`.
    ///
    /// # Errors
    /// Backend failures only.
    fn list(&self) -> Result<Vec<crate::workflow::WorkflowRecord>, CoreError>;
}

/// Content-addressed blob storage. Keys are digests; equal bytes are stored
/// once regardless of how many artifacts reference them.
pub trait ArtifactStore: Send + Sync {
    /// Stores bytes under their digest; returns the digest actually recorded.
    ///
    /// # Errors
    /// [`CoreError::ArtifactTooLarge`] beyond `max_bytes`; backend IO errors
    /// otherwise.
    fn put(&self, bytes: &[u8], max_bytes: u64, domain: &[u8]) -> Result<ContentDigest, CoreError>;

    /// Reads a whole blob back. Blob sizes are capped upstream, so buffering
    /// is bounded.
    ///
    /// # Errors
    /// [`CoreError::ArtifactNotFound`] when the digest is unknown; backend IO
    /// errors otherwise.
    fn read(&self, digest: &ContentDigest) -> Result<Vec<u8>, CoreError>;

    /// Copies a blob to `dest`, creating or truncating the file. Used to
    /// materialize input artifacts into run working directories.
    ///
    /// # Errors
    /// [`CoreError::ArtifactNotFound`] when the digest is unknown; backend IO
    /// errors otherwise.
    fn copy_to_path(&self, digest: &ContentDigest, dest: &std::path::Path)
        -> Result<(), CoreError>;

    #[must_use]
    fn contains(&self, digest: &ContentDigest) -> bool;
}
