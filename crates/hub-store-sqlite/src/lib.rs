//! # hub-store-sqlite — durable repositories for SciRust Hub
//!
//! Implements the `hub-core` storage ports over a single embedded SQLite
//! file. Design rules:
//!
//! - **Versioned migrations**: `schema_migrations` records applied versions;
//!   every schema change is a new forward-only migration, never a rewrite.
//! - **No dynamic SQL**: all statements are compile-time constants with
//!   bound parameters; user input never reaches SQL text.
//! - **Canonical storage**: domain objects persist as their canonical JSON
//!   (the same serialization digests are computed over), plus projected
//!   columns for ordering and lookup.
//! - Ordering semantics match the in-memory backend exactly: components by
//!   `(id, version)` with lexicographic version comparison, runs and
//!   artifacts by `(created_at, id)`.
//!
//! Durability scope: registries and run/artifact metadata survive restarts.
//! Blob *contents* stay in the file-backed store
//! ([`hub_core::memory::FileSystemArtifactStore`]) under the data directory.

use std::path::Path;
use std::sync::Mutex;

use hub_core::artifact::ArtifactMeta;
use hub_core::component::ComponentManifest;
use hub_core::error::CoreError;
use hub_core::event::{
    artifact_recorded_event, component_registered_event, derive_run_events, derive_workflow_events,
    workflow_cancel_requested_event, LifecycleEntityType, LifecycleEvent, LifecycleEventKind,
    LifecycleEventRepository, NewLifecycleEvent,
};
use hub_core::run::RunRecord;
use hub_core::store::{
    ArtifactMetadataRepository, ComponentRepository, RunRepository, WorkflowRepository,
};
use rusqlite::OptionalExtension as _;

/// Forward-only migrations; index `i` is schema version `i + 1`.
const MIGRATIONS: &[&str] = &[
    // v1: initial schema.
    "CREATE TABLE components (
        id              TEXT    NOT NULL,
        version         TEXT    NOT NULL,
        manifest_json   TEXT    NOT NULL,
        manifest_digest TEXT    NOT NULL,
        registered_at   INTEGER NOT NULL,
        PRIMARY KEY (id, version)
    );
    CREATE TABLE runs (
        id          TEXT PRIMARY KEY,
        created_at  INTEGER NOT NULL,
        final_state TEXT    NOT NULL,
        record_json TEXT    NOT NULL
    );
    CREATE INDEX idx_runs_created ON runs (created_at);
    CREATE TABLE artifact_meta (
        id          TEXT PRIMARY KEY,
        digest      TEXT    NOT NULL,
        created_at  INTEGER NOT NULL,
        meta_json   TEXT    NOT NULL
    );
    CREATE INDEX idx_artifacts_created ON artifact_meta (created_at);",
    // v2: workflow orchestration records.
    "CREATE TABLE workflows (
        id          TEXT PRIMARY KEY,
        created_at  INTEGER NOT NULL,
        state       TEXT    NOT NULL,
        record_json TEXT    NOT NULL
    );
    CREATE INDEX idx_workflows_created ON workflows (created_at);",
    // v3: append-only operational lifecycle chronology.
    "CREATE TABLE lifecycle_events (
        sequence        INTEGER PRIMARY KEY AUTOINCREMENT,
        recorded_at     INTEGER NOT NULL,
        kind            TEXT    NOT NULL,
        entity_type     TEXT    NOT NULL,
        entity_id       TEXT    NOT NULL,
        attributes_json TEXT    NOT NULL
    );
    CREATE INDEX idx_lifecycle_events_entity
        ON lifecycle_events (entity_type, entity_id, sequence);",
];

/// SQLite-backed implementation of all three metadata repository ports.
///
/// The connection sits behind a mutex: `rusqlite::Connection` is not `Sync`,
/// and this control plane is single-node. The lock is an implementation
/// detail hidden behind the port traits and can be replaced by a pool when
/// concurrency demands it.
pub struct SqliteStore {
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteStore {
    /// Opens (creating if needed) a database file and applies pending
    /// migrations transactionally.
    ///
    /// # Errors
    /// [`CoreError::Storage`] on open/migration failure; a failed migration
    /// is rolled back and leaves prior versions untouched.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, CoreError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| CoreError::Storage(format!("creating db dir: {e}")))?;
        }
        let conn = rusqlite::Connection::open(path)
            .map_err(|e| CoreError::Storage(format!("opening sqlite db: {e}")))?;
        Self::initialize(conn)
    }

    /// Ephemeral in-memory database (tests only).
    ///
    /// # Errors
    /// See [`Self::open`].
    #[doc(hidden)]
    pub fn open_in_memory() -> Result<Self, CoreError> {
        let conn = rusqlite::Connection::open_in_memory()
            .map_err(|e| CoreError::Storage(format!("opening sqlite memory db: {e}")))?;
        Self::initialize(conn)
    }

    fn initialize(mut conn: rusqlite::Connection) -> Result<Self, CoreError> {
        // WAL: crash-safe commits with concurrent readers. FULL sync keeps
        // every commit durable at control-plane write rates.
        conn.pragma_update(None, "journal_mode", "WAL")
            .map_err(storage("setting journal_mode"))?;
        conn.pragma_update(None, "synchronous", "FULL")
            .map_err(storage("setting synchronous"))?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (
                version    INTEGER PRIMARY KEY,
                applied_at INTEGER NOT NULL
            );",
        )
        .map_err(storage("creating migrations table"))?;

        let applied: Vec<i64> = {
            let mut stmt = conn
                .prepare("SELECT version FROM schema_migrations ORDER BY version")
                .map_err(storage("reading applied migrations"))?;
            let rows = stmt
                .query_map([], |row| row.get::<_, i64>(0))
                .map_err(storage("reading applied migrations"))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(storage("reading applied migrations"))?
        };

        for (offset, sql) in MIGRATIONS.iter().enumerate() {
            let version = i64::try_from(offset).expect("migration count fits") + 1;
            if applied.contains(&version) {
                continue;
            }
            let tx = conn
                .transaction()
                .map_err(storage("beginning migration transaction"))?;
            tx.execute_batch(sql)
                .map_err(|e| CoreError::Storage(format!("migration v{version} failed: {e}")))?;
            tx.execute(
                "INSERT INTO schema_migrations (version, applied_at) VALUES (?1, ?2)",
                rusqlite::params![version, now_ms()],
            )
            .map_err(storage("recording migration"))?;
            tx.commit()
                .map_err(|e| CoreError::Storage(format!("committing migration v{version}: {e}")))?;
        }
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, rusqlite::Connection>, CoreError> {
        self.conn
            .lock()
            .map_err(|_| CoreError::Storage("sqlite connection lock poisoned".into()))
    }
}

fn now_ms() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(0)
}

fn storage(context: &'static str) -> impl Fn(rusqlite::Error) -> CoreError {
    move |e| CoreError::Storage(format!("{context}: {e}"))
}

fn storage_now_ms() -> u64 {
    u64::try_from(now_ms()).unwrap_or(0)
}

fn append_event_tx(
    tx: &rusqlite::Transaction<'_>,
    event: &NewLifecycleEvent,
) -> Result<LifecycleEvent, CoreError> {
    let attributes_json = serde_json::to_string(&event.attributes)
        .map_err(|e| CoreError::Storage(format!("serializing lifecycle event: {e}")))?;
    tx.execute(
        "INSERT INTO lifecycle_events
         (recorded_at, kind, entity_type, entity_id, attributes_json)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        rusqlite::params![
            event.recorded_at,
            event.kind.as_str(),
            event.entity_type.as_str(),
            event.entity_id,
            attributes_json,
        ],
    )
    .map_err(storage("appending lifecycle event"))?;
    let sequence = u64::try_from(tx.last_insert_rowid())
        .map_err(|_| CoreError::Storage("invalid lifecycle event sequence".into()))?;
    Ok(LifecycleEvent {
        sequence,
        recorded_at: event.recorded_at,
        kind: event.kind,
        entity_type: event.entity_type,
        entity_id: event.entity_id.clone(),
        attributes: event.attributes.clone(),
    })
}

fn decode_event_row(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<(i64, i64, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

impl ComponentRepository for SqliteStore {
    fn put(&self, manifest: &ComponentManifest) -> Result<bool, CoreError> {
        let new_digest = manifest.content_digest()?;
        let json = serde_json::to_string(manifest)
            .map_err(|e| CoreError::Storage(format!("serializing manifest: {e}")))?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning registration"))?;

        let existing: Option<String> = tx
            .query_row(
                "SELECT manifest_digest FROM components WHERE id = ?1 AND version = ?2",
                rusqlite::params![manifest.id.to_string(), manifest.version.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("looking up component"))?;

        if let Some(stored_digest) = existing {
            if stored_digest == new_digest.to_hex() {
                return Ok(false); // identical replay; nothing written
            }
            return Err(CoreError::ComponentConflict {
                id: manifest.id,
                registered: stored_digest,
                new: new_digest.to_hex(),
            });
        }

        tx.execute(
            "INSERT INTO components (id, version, manifest_json, manifest_digest, registered_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                manifest.id.to_string(),
                manifest.version.as_str(),
                json,
                new_digest.to_hex(),
                now_ms()
            ],
        )
        .map_err(storage("inserting component"))?;
        append_event_tx(&tx, &component_registered_event(manifest, storage_now_ms()))?;
        tx.commit().map_err(storage("committing registration"))?;
        Ok(true)
    }

    fn latest(&self, id: &hub_core::ComponentId) -> Result<Option<ComponentManifest>, CoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                // Lexicographic DESC mirrors the in-memory backend's
                // `Version: Ord` semantics exactly.
                "SELECT manifest_json FROM components
                 WHERE id = ?1
                 ORDER BY version DESC
                 LIMIT 1",
                rusqlite::params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading latest component"))?;
        json.map(|j| {
            serde_json::from_str(&j).map_err(|e| {
                CoreError::Storage(format!("stored manifest failed to deserialize: {e}"))
            })
        })
        .transpose()
    }

    fn list(&self) -> Result<Vec<ComponentManifest>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT manifest_json FROM components ORDER BY id, version")
            .map_err(storage("listing components"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage("listing components"))?;
        let mut out = Vec::new();
        for row in rows {
            let json = row.map_err(storage("reading component row"))?;
            out.push(serde_json::from_str(&json).map_err(|e| {
                CoreError::Storage(format!("stored manifest failed to deserialize: {e}"))
            })?);
        }
        Ok(out)
    }
}

impl RunRepository for SqliteStore {
    fn put(&self, record: &RunRecord) -> Result<(), CoreError> {
        let json = serde_json::to_string(record)
            .map_err(|e| CoreError::Storage(format!("serializing run record: {e}")))?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning run upsert"))?;
        let previous_json: Option<String> = tx
            .query_row(
                "SELECT record_json FROM runs WHERE id = ?1",
                rusqlite::params![record.id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading run before upsert"))?;
        let previous = previous_json
            .map(|value| {
                serde_json::from_str::<RunRecord>(&value).map_err(|e| {
                    CoreError::Storage(format!("stored run failed to deserialize: {e}"))
                })
            })
            .transpose()?;

        tx.execute(
            "INSERT INTO runs (id, created_at, final_state, record_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET
                final_state = excluded.final_state,
                record_json = excluded.record_json",
            rusqlite::params![
                record.id.to_string(),
                record.created_at,
                record.state.to_string(),
                json
            ],
        )
        .map_err(storage("upserting run"))?;
        for event in derive_run_events(previous.as_ref(), record) {
            append_event_tx(&tx, &event)?;
        }
        tx.commit().map_err(storage("committing run upsert"))?;
        Ok(())
    }

    fn get(&self, id: &hub_core::RunId) -> Result<Option<RunRecord>, CoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                "SELECT record_json FROM runs WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading run"))?;
        json.map(|j| {
            serde_json::from_str(&j)
                .map_err(|e| CoreError::Storage(format!("stored run failed to deserialize: {e}")))
        })
        .transpose()
    }

    fn list(&self) -> Result<Vec<RunRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT record_json FROM runs ORDER BY created_at, id")
            .map_err(storage("listing runs"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage("listing runs"))?;
        let mut out = Vec::new();
        for row in rows {
            let json = row.map_err(storage("reading run row"))?;
            out.push(serde_json::from_str(&json).map_err(|e| {
                CoreError::Storage(format!("stored run failed to deserialize: {e}"))
            })?);
        }
        Ok(out)
    }
}

impl ArtifactMetadataRepository for SqliteStore {
    fn put(&self, meta: &ArtifactMeta) -> Result<(), CoreError> {
        meta.validate()?;
        let json = serde_json::to_string(meta)
            .map_err(|e| CoreError::Storage(format!("serializing artifact meta: {e}")))?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning artifact insert"))?;
        let existing: Option<String> = tx
            .query_row(
                "SELECT digest FROM artifact_meta WHERE id = ?1",
                rusqlite::params![meta.id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("looking up artifact meta"))?;
        match existing {
            Some(digest) if digest == meta.digest.to_hex() => Ok(()),
            Some(digest) => Err(CoreError::Validation(format!(
                "artifact {} already exists with digest {digest}; refusing divergent metadata",
                meta.id
            ))),
            None => {
                tx.execute(
                    "INSERT INTO artifact_meta (id, digest, created_at, meta_json)
                     VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![
                        meta.id.to_string(),
                        meta.digest.to_hex(),
                        meta.created_at,
                        json
                    ],
                )
                .map_err(storage("inserting artifact meta"))?;
                append_event_tx(&tx, &artifact_recorded_event(meta))?;
                tx.commit().map_err(storage("committing artifact insert"))?;
                Ok(())
            }
        }
    }

    fn get(&self, id: &hub_core::ArtifactId) -> Result<Option<ArtifactMeta>, CoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                "SELECT meta_json FROM artifact_meta WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading artifact meta"))?;
        json.map(|j| {
            serde_json::from_str(&j).map_err(|e| {
                CoreError::Storage(format!("stored artifact failed to deserialize: {e}"))
            })
        })
        .transpose()
    }

    fn list(&self) -> Result<Vec<ArtifactMeta>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT meta_json FROM artifact_meta ORDER BY created_at, id")
            .map_err(storage("listing artifacts"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage("listing artifacts"))?;
        let mut out = Vec::new();
        for row in rows {
            let json = row.map_err(storage("reading artifact row"))?;
            out.push(serde_json::from_str(&json).map_err(|e| {
                CoreError::Storage(format!("stored artifact failed to deserialize: {e}"))
            })?);
        }
        Ok(out)
    }
}

impl WorkflowRepository for SqliteStore {
    fn put(&self, record: &hub_core::workflow::WorkflowRecord) -> Result<(), CoreError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning workflow upsert"))?;
        let existing_json: Option<String> = tx
            .query_row(
                "SELECT record_json FROM workflows WHERE id = ?1",
                rusqlite::params![record.id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading workflow before upsert"))?;
        let previous = existing_json
            .as_deref()
            .map(|json| {
                serde_json::from_str::<hub_core::workflow::WorkflowRecord>(json).map_err(|e| {
                    CoreError::Storage(format!("stored workflow failed to deserialize: {e}"))
                })
            })
            .transpose()?;
        let mut stored = record.clone();
        if stored.cancel_requested_at.is_none() {
            if let Some(existing) = &previous {
                stored.cancel_requested_at = existing.cancel_requested_at;
            }
        }
        let json = serde_json::to_string(&stored)
            .map_err(|e| CoreError::Storage(format!("serializing workflow: {e}")))?;
        tx.execute(
            "INSERT INTO workflows (id, created_at, state, record_json)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(id) DO UPDATE SET
                state = excluded.state,
                record_json = excluded.record_json",
            rusqlite::params![
                stored.id.to_string(),
                stored.created_at,
                format!("{:?}", stored.state),
                json
            ],
        )
        .map_err(storage("upserting workflow"))?;
        for event in derive_workflow_events(previous.as_ref(), &stored, storage_now_ms()) {
            append_event_tx(&tx, &event)?;
        }
        tx.commit().map_err(storage("committing workflow upsert"))?;
        Ok(())
    }

    fn request_cancel(
        &self,
        id: &hub_core::WorkflowId,
        at: hub_core::clock::UnixMillis,
    ) -> Result<Option<hub_core::workflow::WorkflowRecord>, CoreError> {
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning workflow cancellation"))?;
        let json: Option<String> = tx
            .query_row(
                "SELECT record_json FROM workflows WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading workflow for cancellation"))?;
        let Some(json) = json else {
            return Ok(None);
        };
        let mut record: hub_core::workflow::WorkflowRecord =
            serde_json::from_str(&json).map_err(|e| {
                CoreError::Storage(format!("stored workflow failed to deserialize: {e}"))
            })?;
        if !record.state.is_terminal() && record.cancel_requested_at.is_none() {
            record.cancel_requested_at = Some(at);
            let updated = serde_json::to_string(&record)
                .map_err(|e| CoreError::Storage(format!("serializing workflow: {e}")))?;
            tx.execute(
                "UPDATE workflows SET record_json = ?2 WHERE id = ?1",
                rusqlite::params![id.to_string(), updated],
            )
            .map_err(storage("persisting workflow cancellation"))?;
            append_event_tx(&tx, &workflow_cancel_requested_event(id.to_string(), at))?;
        }
        tx.commit()
            .map_err(storage("committing workflow cancellation"))?;
        Ok(Some(record))
    }

    fn get(
        &self,
        id: &hub_core::WorkflowId,
    ) -> Result<Option<hub_core::workflow::WorkflowRecord>, CoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                "SELECT record_json FROM workflows WHERE id = ?1",
                rusqlite::params![id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading workflow"))?;
        json.map(|j| {
            serde_json::from_str(&j).map_err(|e| {
                CoreError::Storage(format!("stored workflow failed to deserialize: {e}"))
            })
        })
        .transpose()
    }

    fn list(&self) -> Result<Vec<hub_core::workflow::WorkflowRecord>, CoreError> {
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare("SELECT record_json FROM workflows ORDER BY created_at, id")
            .map_err(storage("listing workflows"))?;
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(storage("listing workflows"))?;
        let mut out = Vec::new();
        for row in rows {
            let json = row.map_err(storage("reading workflow row"))?;
            out.push(serde_json::from_str(&json).map_err(|e| {
                CoreError::Storage(format!("stored workflow failed to deserialize: {e}"))
            })?);
        }
        Ok(out)
    }
}

impl LifecycleEventRepository for SqliteStore {
    fn list_after(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<Vec<LifecycleEvent>, CoreError> {
        hub_core::event::validate_event_page_limit(limit)?;
        let conn = self.lock()?;
        let mut stmt = conn
            .prepare(
                "SELECT sequence, recorded_at, kind, entity_type, entity_id, attributes_json
                 FROM lifecycle_events
                 WHERE sequence > ?1
                 ORDER BY sequence
                 LIMIT ?2",
            )
            .map_err(storage("preparing lifecycle event query"))?;
        let rows = stmt
            .query_map(rusqlite::params![after_sequence, limit], decode_event_row)
            .map_err(storage("querying lifecycle events"))?;
        let mut events = Vec::new();
        for row in rows {
            let (sequence, recorded_at, kind, entity_type, entity_id, attributes_json) =
                row.map_err(storage("reading lifecycle event row"))?;
            let sequence = u64::try_from(sequence)
                .map_err(|_| CoreError::Storage("negative lifecycle event sequence".into()))?;
            let recorded_at = u64::try_from(recorded_at)
                .map_err(|_| CoreError::Storage("negative lifecycle event timestamp".into()))?;
            let kind = LifecycleEventKind::parse(&kind).ok_or_else(|| {
                CoreError::Storage(format!("unknown lifecycle event kind {kind:?}"))
            })?;
            let entity_type = LifecycleEntityType::parse(&entity_type).ok_or_else(|| {
                CoreError::Storage(format!("unknown lifecycle entity type {entity_type:?}"))
            })?;
            let attributes = serde_json::from_str(&attributes_json).map_err(|e| {
                CoreError::Storage(format!(
                    "stored lifecycle attributes failed to deserialize: {e}"
                ))
            })?;
            events.push(LifecycleEvent {
                sequence,
                recorded_at,
                kind,
                entity_type,
                entity_id,
                attributes,
            });
        }
        Ok(events)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_core::capability::{Capability, CapabilityName};
    use hub_core::component::{ComponentKind, ComponentName, ExecutionBinding, ProcessBinding};
    use hub_core::id::{ArtifactId, ComponentId, RunId};
    use hub_core::run::{RunSpec, RunState};
    use hub_core::Version;
    use std::collections::BTreeMap;

    fn manifest(id: ComponentId, version: &str, program: &str) -> ComponentManifest {
        ComponentManifest::new_v1(
            id,
            ComponentName::parse("svc").expect("name"),
            Version::parse(version).expect("version"),
            ComponentKind::parse(ComponentKind::SERVICE).expect("kind"),
            vec![Capability {
                name: CapabilityName::parse("x.y").expect("capability"),
                contract_version: Version::parse("1.0.0").expect("contract version"),
                inputs: Vec::new(),
                outputs: Vec::new(),
                properties: BTreeMap::new(),
            }],
            Some(ExecutionBinding::Process(ProcessBinding {
                program: program.into(),
                args: Vec::new(),
                working_dir: None,
                outputs: Vec::new(),
            })),
            None,
            BTreeMap::new(),
        )
        .expect("manifest")
    }

    fn run_record(created_at: u64) -> RunRecord {
        RunRecord::create(
            RunSpec {
                component: ComponentId::generate(),
                capability: CapabilityName::parse("x.y").expect("capability"),
                parameters: BTreeMap::new(),
                inputs: Vec::new(),
                timeout_ms: 1_000,
            },
            "svc".into(),
            Version::parse("1.0.0").expect("version"),
            Version::parse("1.0.0").expect("contract version"),
            created_at,
            &hub_core::Limits::default(),
        )
        .expect("record")
    }

    #[test]
    fn migrations_are_idempotent_and_versioned() {
        let dir = std::env::temp_dir().join(format!("hub-sqlite-mig-{}", uuid::Uuid::new_v4()));
        let db = dir.join("nested").join("hub.db");
        {
            let store = SqliteStore::open(&db).expect("first open");
            // Sanity: schema usable right after creation.
            let conn = store.lock().expect("lock");
            let version: i64 = conn
                .query_row(
                    "SELECT COALESCE(MAX(version), 0) FROM schema_migrations",
                    [],
                    |row| row.get(0),
                )
                .expect("migration version");
            assert_eq!(version, MIGRATIONS.len() as i64);
        }
        // Reopening applies nothing new and keeps working.
        let store = SqliteStore::open(&db).expect("reopen");
        assert_eq!(
            ComponentRepository::list(&store).expect("list"),
            Vec::<ComponentManifest>::new()
        );
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn component_registration_matches_in_memory_semantics() {
        let store = SqliteStore::open_in_memory().expect("store");
        let id = ComponentId::generate();

        // New insert -> true.
        assert!(
            ComponentRepository::put(&store, &manifest(id, "1.0.0", "/bin/true")).expect("put")
        );
        // Identical replay -> false, no error.
        assert!(
            !ComponentRepository::put(&store, &manifest(id, "1.0.0", "/bin/true")).expect("replay")
        );
        // Divergent content under same key -> conflict carrying digests.
        match ComponentRepository::put(&store, &manifest(id, "1.0.0", "/bin/false")) {
            Err(CoreError::ComponentConflict {
                registered, new, ..
            }) => {
                assert_ne!(registered, new);
                assert_eq!(registered.len(), 64);
            }
            other => panic!("expected conflict, got {other:?}"),
        }
        // Another version coexists; latest() returns highest version.
        assert!(
            ComponentRepository::put(&store, &manifest(id, "10.0.0", "/bin/true")).expect("v10")
        );
        assert!(ComponentRepository::put(&store, &manifest(id, "2.0.0", "/bin/true")).expect("v2"));
        let latest = ComponentRepository::latest(&store, &id)
            .expect("latest")
            .expect("present");
        // Ordering is lexicographic by documented design (Version docs),
        // matching the in-memory backend byte for byte.
        assert_eq!(latest.version.as_str(), "2.0.0");

        let listed = ComponentRepository::list(&store).expect("list");
        assert_eq!(listed.len(), 3);
        // Deterministic ordering by (id, version).
        let versions: Vec<&str> = listed.iter().map(|m| m.version.as_str()).collect();
        let mut sorted = versions.clone();
        sorted.sort_unstable();
        assert_eq!(versions, sorted);

        // Unknown component -> None.
        assert!(
            ComponentRepository::latest(&store, &ComponentId::generate())
                .expect("missing")
                .is_none()
        );
    }

    #[test]
    fn runs_upsert_and_list_deterministically() {
        let store = SqliteStore::open_in_memory().expect("store");
        let mut late = run_record(30);
        // Legal chain only: created -> validated -> queued.
        late.transition(RunState::Validated, 31)
            .expect("transition");
        late.transition(RunState::Queued, 32).expect("transition");

        for record in [run_record(20), late] {
            RunRepository::put(&store, &record).expect("put");
        }
        // Update the same run (same id) as its state machine progresses.
        let early = run_record(10);
        RunRepository::put(&store, &early).expect("initial put");
        let mut updated = early;
        updated.transition(RunState::Failed, 11).expect("failed");
        RunRepository::put(&store, &updated).expect("upsert");

        let listed = RunRepository::list(&store).expect("list");
        let times: Vec<u64> = listed.iter().map(|r| r.created_at).collect();
        assert_eq!(times, vec![10, 20, 30], "ordered by created_at");
        assert_eq!(
            listed.iter().filter(|r| r.created_at == 10).count(),
            1,
            "upsert replaced, not duplicated"
        );

        let fetched = RunRepository::get(&store, &listed[0].id)
            .expect("get")
            .expect("present");
        assert_eq!(fetched, listed[0]);
        assert_eq!(fetched.state, RunState::Failed);
        assert!(RunRepository::get(&store, &RunId::generate())
            .expect("missing")
            .is_none());
    }

    #[test]
    fn artifact_meta_is_immutable_and_ordered() {
        let store = SqliteStore::open_in_memory().expect("store");
        let meta = |id: ArtifactId, created_at: u64| ArtifactMeta {
            id,
            name: format!("out-{created_at}"),
            media_type: "text/plain".into(),
            digest: hub_core::digest::hash_bytes(
                hub_core::digest::DOMAIN_CAPTURE,
                created_at.to_string().as_bytes(),
            ),
            size: created_at,
            created_at,
            produced_by_run: None,
        };
        for t in [30u64, 10, 20] {
            ArtifactMetadataRepository::put(&store, &meta(ArtifactId::generate(), t)).expect("put");
        }
        let listed = ArtifactMetadataRepository::list(&store).expect("list");
        let times: Vec<u64> = listed.iter().map(|m| m.created_at).collect();
        assert_eq!(times, vec![10, 20, 30]);

        // Identical re-put is accepted (no-op); divergent digest rejected.
        let fixed_id = ArtifactId::generate();
        ArtifactMetadataRepository::put(&store, &meta(fixed_id, 10))
            .expect("first put of fixed id");
        ArtifactMetadataRepository::put(&store, &meta(fixed_id, 10)).expect("identical re-put");
        let mut divergent = meta(fixed_id, 10);
        divergent.digest = hub_core::digest::hash_bytes(hub_core::digest::DOMAIN_CAPTURE, b"other");
        assert!(
            ArtifactMetadataRepository::put(&store, &divergent).is_err(),
            "divergent metadata under one artifact id must be rejected"
        );

        assert!(
            ArtifactMetadataRepository::get(&store, &ArtifactId::generate())
                .expect("missing")
                .is_none()
        );
    }

    #[test]
    fn lifecycle_events_are_cursor_ordered_idempotent_and_durable() {
        let dir = std::env::temp_dir().join(format!("hub-sqlite-events-{}", uuid::Uuid::new_v4()));
        let db = dir.join("hub.db");
        let component_id = ComponentId::generate();
        let run_id;
        {
            let store = SqliteStore::open(&db).expect("open");
            let item = manifest(component_id, "1.0.0", "/bin/true");
            assert!(ComponentRepository::put(&store, &item).expect("component"));
            assert!(!ComponentRepository::put(&store, &item).expect("replay"));

            let mut run = run_record(100);
            run_id = run.id;
            run.transition(RunState::Validated, 101).unwrap();
            run.transition(RunState::Queued, 102).unwrap();
            RunRepository::put(&store, &run).expect("queued run");
            let first = LifecycleEventRepository::list_after(&store, 0, 2).unwrap();
            assert_eq!(first.len(), 2);
            assert_eq!(first[0].sequence, 1);
            assert_eq!(first[0].kind, LifecycleEventKind::ComponentRegistered);
            assert_eq!(first[1].kind, LifecycleEventKind::RunCreated);
        }

        let store = SqliteStore::open(&db).expect("reopen");
        let events = LifecycleEventRepository::list_after(&store, 0, 100).unwrap();
        assert_eq!(events.len(), 4, "component + run created + two transitions");
        assert!(events
            .windows(2)
            .all(|pair| pair[0].sequence < pair[1].sequence));
        assert_eq!(events.last().unwrap().attributes["to"], "queued");
        assert!(events
            .iter()
            .any(|event| event.entity_id == run_id.to_string()));
        assert!(
            LifecycleEventRepository::list_after(&store, events.last().unwrap().sequence, 10)
                .unwrap()
                .is_empty()
        );
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn data_survives_reopen_from_disk() {
        let dir = std::env::temp_dir().join(format!("hub-sqlite-restart-{}", uuid::Uuid::new_v4()));
        let db = dir.join("hub.db");
        let id = ComponentId::generate();
        {
            let store = SqliteStore::open(&db).expect("open");
            ComponentRepository::put(&store, &manifest(id, "1.0.0", "/bin/true")).expect("put");
            let mut record = run_record(7);
            record.transition(RunState::Failed, 9).expect("failed");
            RunRepository::put(&store, &record).expect("put run");
        } // connection dropped here

        let store = SqliteStore::open(&db).expect("reopen");
        let restored = ComponentRepository::latest(&store, &id)
            .expect("latest")
            .expect("component survived");
        assert_eq!(restored.id, id);
        assert_eq!(restored.version.as_str(), "1.0.0");

        let runs = store.list_runs_all();
        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].state, RunState::Failed);
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn workflows_round_trip_and_survive_reopen() {
        let dir = std::env::temp_dir().join(format!("hub-sqlite-wf-{}", uuid::Uuid::new_v4()));
        let db = dir.join("hub.db");
        let id;
        {
            let store = SqliteStore::open(&db).expect("open");
            assert_eq!(
                WorkflowRepository::list(&store).expect("empty"),
                Vec::<hub_core::workflow::WorkflowRecord>::new()
            );
            let mut record = hub_core::workflow::WorkflowRecord::create(
                sample_workflow(),
                hub_core::Version::parse(hub_core::workflow::WORKFLOW_MODEL_VERSION)
                    .expect("model version"),
                42,
            )
            .expect("record");
            id = Some(record.id);
            record
                .transition(hub_core::workflow::WorkflowState::Running, 43)
                .expect("running");
            record.steps.push(hub_core::workflow::StepResult {
                key: "emit".into(),
                run: RunId::generate(),
                state: RunState::Succeeded,
                failure: None,
                attempts: Vec::new(),
            });
            WorkflowRepository::put(&store, &record).expect("put");
        }
        let store = SqliteStore::open(&db).expect("reopen");
        let stored_id = id.expect("assigned in first block");
        let restored = WorkflowRepository::get(&store, &stored_id)
            .expect("get")
            .expect("workflow survived reopen");
        assert_eq!(restored.id, stored_id);
        assert_eq!(restored.state, hub_core::workflow::WorkflowState::Running);
        assert_eq!(restored.steps.len(), 1);
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    fn sample_workflow() -> hub_core::workflow::WorkflowSpec {
        use hub_core::capability::CapabilityName;
        use std::collections::BTreeMap;
        let component = ComponentId::generate();
        hub_core::workflow::WorkflowSpec {
            schema_version: hub_core::workflow::WORKFLOW_SCHEMA_VERSION,
            name: "chain".into(),
            max_concurrency: 1,
            steps: vec![hub_core::workflow::Step {
                key: "emit".into(),
                component,
                capability: CapabilityName::parse("x.y").expect("c"),
                parameters: BTreeMap::new(),
                inputs: BTreeMap::new(),
                timeout_ms: 1_000,
                after: Vec::new(),
                retry: None,
            }],
        }
    }

    impl SqliteStore {
        fn list_runs_all(&self) -> Vec<RunRecord> {
            RunRepository::list(self).expect("list runs")
        }
    }
}
