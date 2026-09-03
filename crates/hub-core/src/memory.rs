//! In-memory repository implementations.
//!
//! Thread-safe via a single mutex per store; encapsulated behind the
//! repository traits so backends can be swapped (SQLite later) without any
//! domain change.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::sync::Mutex;

use crate::artifact::ArtifactMeta;
use crate::component::ComponentManifest;
use crate::digest::ContentDigest;
use crate::error::CoreError;
use crate::event::{
    artifact_recorded_event, component_registered_event, derive_run_events, derive_workflow_events,
    wall_clock_ms, workflow_cancel_requested_event, InMemoryLifecycleEvents, LifecycleEvent,
    LifecycleEventRepository,
};
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

    fn put_file(
        &self,
        source_path: &std::path::Path,
        max_bytes: u64,
        domain: &[u8],
    ) -> Result<(ContentDigest, u64), CoreError> {
        let source_meta = std::fs::symlink_metadata(source_path).map_err(|e| {
            CoreError::Storage(format!("stating source artifact {source_path:?}: {e}"))
        })?;
        if source_meta.file_type().is_symlink() {
            return Err(CoreError::Storage(format!(
                "refusing symbolic-link artifact source {source_path:?}"
            )));
        }
        if !source_meta.is_file() {
            return Err(CoreError::Storage(format!(
                "artifact source {source_path:?} is not a regular file"
            )));
        }

        let mut source = std::fs::File::open(source_path).map_err(|e| {
            CoreError::Storage(format!("opening source artifact {source_path:?}: {e}"))
        })?;
        if !source
            .metadata()
            .map_err(|e| CoreError::Storage(format!("stating opened artifact: {e}")))?
            .is_file()
        {
            return Err(CoreError::Storage(format!(
                "opened artifact source {source_path:?} is not a regular file"
            )));
        }

        let incoming = self.root.join("incoming");
        std::fs::create_dir_all(&incoming)
            .map_err(|e| CoreError::Storage(format!("creating incoming blob dir: {e}")))?;
        let tmp = incoming.join(format!("{}.tmp", uuid::Uuid::new_v4()));
        let mut staged = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&tmp)
            .map_err(|e| CoreError::Storage(format!("creating staged blob: {e}")))?;

        let result = (|| -> Result<(ContentDigest, u64), CoreError> {
            let mut state = crate::digest::DigestState::new(domain);
            let mut size = 0u64;
            let mut buf = [0u8; 64 * 1024];
            loop {
                let read = match source.read(&mut buf) {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => {
                        return Err(CoreError::Storage(format!(
                            "reading source artifact {source_path:?}: {e}"
                        )))
                    }
                };
                size =
                    size.checked_add(read as u64)
                        .ok_or_else(|| CoreError::ArtifactTooLarge {
                            artifact: ArtifactId::generate(),
                            size: u64::MAX,
                            limit: max_bytes,
                        })?;
                if size > max_bytes {
                    return Err(CoreError::ArtifactTooLarge {
                        artifact: ArtifactId::generate(),
                        size,
                        limit: max_bytes,
                    });
                }
                state.update(&buf[..read]);
                staged
                    .write_all(&buf[..read])
                    .map_err(|e| CoreError::Storage(format!("writing staged blob: {e}")))?;
            }
            staged
                .sync_all()
                .map_err(|e| CoreError::Storage(format!("syncing staged blob: {e}")))?;
            Ok((state.finalize(), size))
        })();

        let (digest, size) = match result {
            Ok(result) => result,
            Err(error) => {
                drop(staged);
                let _ = std::fs::remove_file(&tmp);
                return Err(error);
            }
        };
        drop(staged);

        let destination = self.blob_path(&digest);
        if destination.exists() {
            let _ = std::fs::remove_file(&tmp);
            return Ok((digest, size));
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CoreError::Storage(format!("creating shard dir: {e}")))?;
        }
        if let Err(e) = std::fs::rename(&tmp, &destination) {
            let _ = std::fs::remove_file(&tmp);
            if destination.exists() {
                return Ok((digest, size));
            }
            return Err(CoreError::Storage(format!("publishing streamed blob: {e}")));
        }
        Ok((digest, size))
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

/// Composite in-memory backend used by the daemon's ephemeral mode. It keeps
/// the four metadata repositories and lifecycle chronology coupled behind one
/// object, mirroring the durable SQLite adapter's port surface.
#[derive(Debug, Default)]
pub struct InMemoryHubStore {
    components: InMemoryComponents,
    runs: InMemoryRuns,
    artifacts: InMemoryArtifactMeta,
    workflows: InMemoryWorkflows,
    events: InMemoryLifecycleEvents,
}

impl ComponentRepository for InMemoryHubStore {
    fn put(&self, manifest: &ComponentManifest) -> Result<bool, CoreError> {
        let inserted = ComponentRepository::put(&self.components, manifest)?;
        if inserted {
            self.events
                .record(component_registered_event(manifest, wall_clock_ms()))?;
        }
        Ok(inserted)
    }

    fn latest(&self, id: &ComponentId) -> Result<Option<ComponentManifest>, CoreError> {
        ComponentRepository::latest(&self.components, id)
    }

    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {
        ComponentRepository::list(&self.components)
    }
}

impl RunRepository for InMemoryHubStore {
    fn put(&self, record: &RunRecord) -> Result<(), CoreError> {
        let previous = RunRepository::get(&self.runs, &record.id)?;
        RunRepository::put(&self.runs, record)?;
        for event in derive_run_events(previous.as_ref(), record) {
            self.events.record(event)?;
        }
        Ok(())
    }

    fn get(&self, id: &RunId) -> Result<Option<RunRecord>, CoreError> {
        RunRepository::get(&self.runs, id)
    }

    fn list(&self) -> Result<Vec<RunRecord>, CoreError> {
        RunRepository::list(&self.runs)
    }
}

impl ArtifactMetadataRepository for InMemoryHubStore {
    fn put(&self, meta: &ArtifactMeta) -> Result<(), CoreError> {
        let existed = ArtifactMetadataRepository::get(&self.artifacts, &meta.id)?.is_some();
        ArtifactMetadataRepository::put(&self.artifacts, meta)?;
        if !existed {
            self.events.record(artifact_recorded_event(meta))?;
        }
        Ok(())
    }

    fn get(&self, id: &ArtifactId) -> Result<Option<ArtifactMeta>, CoreError> {
        ArtifactMetadataRepository::get(&self.artifacts, id)
    }

    fn list(&self) -> Result<Vec<ArtifactMeta>, CoreError> {
        ArtifactMetadataRepository::list(&self.artifacts)
    }
}

impl WorkflowRepository for InMemoryHubStore {
    fn put(&self, record: &crate::workflow::WorkflowRecord) -> Result<(), CoreError> {
        let previous = WorkflowRepository::get(&self.workflows, &record.id)?;
        WorkflowRepository::put(&self.workflows, record)?;
        let stored = WorkflowRepository::get(&self.workflows, &record.id)?
            .ok_or_else(|| CoreError::Storage("workflow disappeared after in-memory put".into()))?;
        for event in derive_workflow_events(previous.as_ref(), &stored, wall_clock_ms()) {
            self.events.record(event)?;
        }
        Ok(())
    }

    fn request_cancel(
        &self,
        id: &crate::id::WorkflowId,
        at: crate::clock::UnixMillis,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError> {
        let previous = WorkflowRepository::get(&self.workflows, id)?;
        let updated = WorkflowRepository::request_cancel(&self.workflows, id, at)?;
        if previous
            .and_then(|record| record.cancel_requested_at)
            .is_none()
            && updated
                .as_ref()
                .and_then(|record| record.cancel_requested_at)
                .is_some()
        {
            self.events
                .record(workflow_cancel_requested_event(id.to_string(), at))?;
        }
        Ok(updated)
    }

    fn get(
        &self,
        id: &crate::id::WorkflowId,
    ) -> Result<Option<crate::workflow::WorkflowRecord>, CoreError> {
        WorkflowRepository::get(&self.workflows, id)
    }

    fn list(&self) -> Result<Vec<crate::workflow::WorkflowRecord>, CoreError> {
        WorkflowRepository::list(&self.workflows)
    }
}

impl LifecycleEventRepository for InMemoryHubStore {
    fn list_after(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<Vec<LifecycleEvent>, CoreError> {
        self.events.list_after(after_sequence, limit)
    }

    fn high_water_sequence(&self) -> Result<u64, CoreError> {
        self.events.high_water_sequence()
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
    fn file_blob_store_streams_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("hub-core-file-test-{}", uuid::Uuid::new_v4()));
        let store = FileSystemArtifactStore::open(&dir).expect("open");
        let source = dir.join("source.bin");
        let data = vec![0x5Au8; 200_000];
        std::fs::write(&source, &data).expect("source");
        let (digest, size) = store
            .put_file(&source, 300_000, crate::digest::DOMAIN_ARTIFACT_BLOB)
            .expect("put file");
        assert_eq!(size, data.len() as u64);
        assert_eq!(
            digest,
            crate::digest::hash_bytes(crate::digest::DOMAIN_ARTIFACT_BLOB, &data)
        );
        assert_eq!(store.read(&digest).expect("read"), data);
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[test]
    fn file_blob_store_enforces_limit_while_streaming() {
        let dir = std::env::temp_dir().join(format!("hub-core-file-test-{}", uuid::Uuid::new_v4()));
        let store = FileSystemArtifactStore::open(&dir).expect("open");
        let source = dir.join("oversize.bin");
        std::fs::write(&source, vec![0u8; 128]).expect("source");
        assert!(matches!(
            store.put_file(&source, 64, crate::digest::DOMAIN_ARTIFACT_BLOB),
            Err(CoreError::ArtifactTooLarge { limit: 64, .. })
        ));
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn file_blob_store_rejects_symbolic_links() {
        use std::os::unix::fs::symlink;
        let dir = std::env::temp_dir().join(format!("hub-core-file-test-{}", uuid::Uuid::new_v4()));
        let store = FileSystemArtifactStore::open(&dir).expect("open");
        let source = dir.join("source.bin");
        let link = dir.join("link.bin");
        std::fs::write(&source, b"payload").expect("source");
        symlink(&source, &link).expect("symlink");
        assert!(matches!(
            store.put_file(&link, 1024, crate::digest::DOMAIN_ARTIFACT_BLOB),
            Err(CoreError::Storage(message)) if message.contains("symbolic-link")
        ));
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
