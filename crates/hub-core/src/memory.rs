//! In-memory repository implementations.
//!
//! Thread-safe via a single mutex per store; encapsulated behind the
//! repository traits so backends can be swapped (SQLite later) without any
//! domain change.

use std::collections::BTreeMap;
use std::sync::Mutex;

use crate::artifact::ArtifactMeta;
use crate::component::ComponentManifest;
use crate::digest::ContentDigest;
use crate::error::CoreError;
use crate::id::{ArtifactId, ComponentId, RunId};
use crate::run::RunRecord;
use crate::store::{
    ArtifactMetadataRepository, ArtifactStore, ComponentRepository, RunRepository,
    WorkflowRepository,
};
use crate::version::Version;

#[derive(Debug, Default)]
struct Inner {
    manifests: BTreeMap<(ComponentId, Version), ComponentManifest>,
}

/// In-memory component registry backend.
#[derive(Debug, Default)]
pub struct InMemoryComponents(Mutex<Inner>);

impl ComponentRepository for InMemoryComponents {
    fn put(&self, manifest: &ComponentManifest) -> Result<bool, CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        let key = (manifest.id, manifest.version.clone());
        if let Some(existing) = inner.manifests.get(&key) {
            let existing_digest = existing.content_digest()?;
            let new_digest = manifest.content_digest()?;
            if existing_digest == new_digest {
                return Ok(false);
            }
            return Err(CoreError::ComponentConflict {
                id: manifest.id,
                registered: existing_digest.to_string(),
                new: new_digest.to_string(),
            });
        }
        inner.manifests.insert(key, manifest.clone());
        Ok(true)
    }

    fn latest(&self, id: &ComponentId) -> Result<Option<ComponentManifest>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        Ok(inner
            .manifests
            .iter()
            .filter(|((cid, _), _)| cid == id)
            .max_by(|(ka, _), (kb, _)| ka.1.cmp(&kb.1))
            .map(|(_, m)| m.clone()))
    }

    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        Ok(inner.manifests.values().cloned().collect())
    }
}

#[derive(Debug, Default)]
struct RunsInner {
    records: BTreeMap<RunId, RunRecord>,
}

/// In-memory run record backend.
#[derive(Debug, Default)]
pub struct InMemoryRuns(Mutex<RunsInner>);

impl RunRepository for InMemoryRuns {
    fn put(&self, record: &RunRecord) -> Result<(), CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        inner.records.insert(record.id, record.clone());
        Ok(())
    }

    fn get(&self, id: &RunId) -> Result<Option<RunRecord>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        Ok(inner.records.get(id).cloned())
    }

    fn list(&self) -> Result<Vec<RunRecord>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        // Deterministic order independent of map ordering: by creation time,
        // tie-broken by id.
        let mut rows: Vec<&RunRecord> = inner.records.values().collect();
        rows.sort_by_key(|r| (r.created_at, r.id));
        Ok(rows.into_iter().cloned().collect())
    }
}

#[derive(Debug, Default)]
struct ArtifactsInner {
    meta: BTreeMap<ArtifactId, ArtifactMeta>,
}

/// In-memory artifact metadata backend.
#[derive(Debug, Default)]
pub struct InMemoryArtifactMeta(Mutex<ArtifactsInner>);

impl ArtifactMetadataRepository for InMemoryArtifactMeta {
    fn put(&self, meta: &ArtifactMeta) -> Result<(), CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        inner.meta.insert(meta.id, meta.clone());
        Ok(())
    }

    fn get(&self, id: &ArtifactId) -> Result<Option<ArtifactMeta>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        Ok(inner.meta.get(id).cloned())
    }

    fn list(&self) -> Result<Vec<ArtifactMeta>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        let mut rows: Vec<&ArtifactMeta> = inner.meta.values().collect();
        rows.sort_by_key(|m| (m.created_at, m.id));
        Ok(rows.into_iter().cloned().collect())
    }
}

/// Content-addressed file-backed blob store.
///
/// Layout: `<root>/blobs/<first-2-hex>/<hex>`; writes are atomic
/// (temp file + rename). This is durability plumbing, not a security
/// boundary — anyone with filesystem access to `root` can read blobs.
#[derive(Clone, Debug)]
pub struct FileSystemArtifactStore {
    root: std::path::PathBuf,
}

impl FileSystemArtifactStore {
    /// Creates the store root if missing.
    ///
    /// # Errors
    /// [`CoreError::Storage`] when the directory cannot be created.
    pub fn open(root: impl Into<std::path::PathBuf>) -> Result<Self, CoreError> {
        let root = root.into();
        std::fs::create_dir_all(root.join("blobs"))
            .map_err(|e| CoreError::Storage(format!("creating blob dir: {e}")))?;
        Ok(Self { root })
    }

    fn blob_path(&self, digest: &ContentDigest) -> std::path::PathBuf {
        let hex = digest.to_hex();
        self.root.join("blobs").join(&hex[..2]).join(hex)
    }
}

impl ArtifactStore for FileSystemArtifactStore {
    fn put(&self, bytes: &[u8], max_bytes: u64, domain: &[u8]) -> Result<ContentDigest, CoreError> {
        if bytes.len() as u64 > max_bytes {
            return Err(CoreError::ArtifactTooLarge {
                artifact: ArtifactId::generate(),
                size: bytes.len() as u64,
                limit: max_bytes,
            });
        }
        let digest = crate::digest::hash_bytes(domain, bytes);
        let path = self.blob_path(&digest);
        if path.exists() {
            return Ok(digest);
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CoreError::Storage(format!("creating shard dir: {e}")))?;
        }
        let tmp = path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, bytes)
            .map_err(|e| CoreError::Storage(format!("writing temp blob: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| CoreError::Storage(format!("publishing blob: {e}")))?;
        Ok(digest)
    }

    fn read(&self, digest: &ContentDigest) -> Result<Vec<u8>, CoreError> {
        let path = self.blob_path(digest);
        if !path.exists() {
            return Err(CoreError::BlobNotFound {
                hex: digest.to_string(),
            });
        }
        std::fs::read(&path).map_err(|e| CoreError::Storage(format!("reading blob: {e}")))
    }

    fn copy_to_path(
        &self,
        digest: &ContentDigest,
        dest: &std::path::Path,
    ) -> Result<(), CoreError> {
        let src = self.blob_path(digest);
        if !src.exists() {
            return Err(CoreError::BlobNotFound {
                hex: digest.to_string(),
            });
        }
        std::fs::copy(src, dest)
            .map_err(|e| CoreError::Storage(format!("copying artifact to {:?}: {e}", dest)))?;
        Ok(())
    }

    fn contains(&self, digest: &ContentDigest) -> bool {
        self.blob_path(digest).exists()
    }
}

#[derive(Debug, Default)]
struct WorkflowsInner {
    records: BTreeMap<crate::id::WorkflowId, crate::workflow::WorkflowRecord>,
}

/// In-memory workflow backend.
#[derive(Debug, Default)]
pub struct InMemoryWorkflows(Mutex<WorkflowsInner>);

impl WorkflowRepository for InMemoryWorkflows {
    fn put(&self, record: &crate::workflow::WorkflowRecord) -> Result<(), CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        let mut stored = record.clone();
        if let Some(existing) = inner.records.get(&record.id) {
            if stored.cancel_requested_at.is_none() {
                stored.cancel_requested_at = existing.cancel_requested_at;
            }
        }
        inner.records.insert(record.id, stored);
        Ok(())
    }

    fn request_cancel(
        &self,
        id: &crate::id::WorkflowId,
        at: crate::clock::UnixMillis,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        let Some(record) = inner.records.get_mut(id) else {
            return Ok(None);
        };
        if !record.state.is_terminal() {
            record.cancel_requested_at.get_or_insert(at);
        }
        Ok(Some(record.clone()))
    }

    fn get(
        &self,
        id: &crate::id::WorkflowId,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        Ok(inner.records.get(id).cloned())
    }

    fn list(&self) -> Result<Vec<crate::workflow::WorkflowRecord>, CoreError> {
        let inner = self.0.lock().map_err(poison)?;
        let mut rows: Vec<&crate::workflow::WorkflowRecord> = inner.records.values().collect();
        rows.sort_by_key(|r| (r.created_at, r.id));
        Ok(rows.into_iter().cloned().collect())
    }
}

fn poison<T>(_e: T) -> CoreError {
    CoreError::Storage("repository lock poisoned".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::{Capability, CapabilityName, Port};
    use crate::component::{ComponentKind, ComponentName, ExecutionBinding, ProcessBinding};
    use std::collections::BTreeMap;

    fn manifest(id: ComponentId, version: &str, program: &str) -> ComponentManifest {
        ComponentManifest::new_v1(
            id,
            ComponentName::parse("svc").expect("name"),
            Version::parse(version).expect("ver"),
            ComponentKind::parse(ComponentKind::SERVICE).expect("kind"),
            vec![Capability {
                name: CapabilityName::parse("x.y").expect("cap"),
                contract_version: Version::parse("1.0.0").expect("cv"),
                inputs: vec![Port {
                    name: "in".into(),
                    description: String::new(),
                }],
                outputs: vec![],
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: program.into(),
                args: vec![],
                working_dir: None,
                outputs: Vec::new(),
            })),
            None,
            BTreeMap::new(),
        )
        .expect("manifest")
    }

    #[test]
    fn registration_is_idempotent_and_conflict_aware() {
        let store = InMemoryComponents::default();
        let id = ComponentId::generate();
        assert!(store
            .put(&manifest(id, "1.0.0", "/bin/true"))
            .expect("insert"));
        // Identical content: no-op.
        assert!(!store.put(&manifest(id, "1.0.0", "/bin/true")).expect("dup"));
        // Same key, different binding: conflict with digests in the message.
        match store.put(&manifest(id, "1.0.0", "/bin/false")) {
            Err(CoreError::ComponentConflict {
                registered, new, ..
            }) => {
                assert_ne!(registered, new);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        // New version of same component: allowed.
        assert!(store.put(&manifest(id, "2.0.0", "/bin/true")).expect("v2"));
        assert_eq!(store.list().expect("list").len(), 2);
        assert_eq!(
            store
                .latest(&id)
                .expect("latest")
                .expect("present")
                .version
                .as_str(),
            "2.0.0"
        );
    }

    #[test]
    fn runs_are_listed_deterministically() {
        let store = InMemoryRuns::default();
        for t in [30u64, 10, 20] {
            let mut rec = RunRecord::create(
                crate::run::RunSpec {
                    component: ComponentId::generate(),
                    capability: CapabilityName::parse("x.y").expect("c"),
                    parameters: BTreeMap::new(),
                    inputs: vec![],
                    timeout_ms: 1000,
                },
                "n".into(),
                Version::parse("1.0.0").expect("v"),
                Version::parse("1.0.0").expect("v"),
                t,
                &crate::limits::Limits::default(),
            )
            .expect("rec");
            rec.transition(crate::run::RunState::Failed, t + 1)
                .expect("tr");
            store.put(&rec).expect("put");
        }
        let listed = store.list().expect("list");
        let times: Vec<u64> = listed.iter().map(|r| r.created_at).collect();
        assert_eq!(times, vec![10, 20, 30]);
    }

    #[test]
    fn blob_store_is_content_addressed_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("hub-core-test-{}", uuid::Uuid::new_v4()));
        let store = FileSystemArtifactStore::open(&dir).expect("open");
        let data = b"artifact payload";
        let d1 = store
            .put(
                data,
                u64::from(u32::MAX),
                crate::digest::DOMAIN_ARTIFACT_BLOB,
            )
            .expect("put");
        let d2 = store
            .put(
                data,
                u64::from(u32::MAX),
                crate::digest::DOMAIN_ARTIFACT_BLOB,
            )
            .expect("dedup put");
        assert_eq!(d1, d2);
        assert_eq!(store.read(&d1).expect("read"), data);
        assert!(store.contains(&d1));
        let missing = crate::digest::hash_bytes(b"nope", b"missing");
        assert!(store.read(&missing).is_err());
        let dest = dir.join("out.bin");
        store.copy_to_path(&d1, &dest).expect("copy");
        assert_eq!(std::fs::read(&dest).expect("copied"), data);
        drop(store);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn oversize_blob_rejected() {
        let dir = std::env::temp_dir().join(format!("hub-core-test-{}", uuid::Uuid::new_v4()));
        let store = FileSystemArtifactStore::open(&dir).expect("open");
        let big = vec![0u8; 128];
        assert!(matches!(
            store.put(&big, 64, crate::digest::DOMAIN_ARTIFACT_BLOB),
            Err(CoreError::ArtifactTooLarge {
                size: 128,
                limit: 64,
                ..
            })
        ));
        drop(store);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
