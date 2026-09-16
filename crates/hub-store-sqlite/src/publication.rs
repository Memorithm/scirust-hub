use std::collections::BTreeMap;
use std::str::FromStr as _;

use hub_core::publication::{
    AuthoritativeStepPublication, PublicationCommit, PublicationFence, PublicationFenceRepository,
    PUBLICATION_FENCE_SCHEMA_VERSION,
};
use hub_core::{ArtifactId, AttemptId, CoreError, WorkflowId};
use rusqlite::OptionalExtension as _;

use crate::{storage, SqliteStore};

impl PublicationFenceRepository for SqliteStore {
    fn advance_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
        attempt: AttemptId,
    ) -> Result<PublicationFence, CoreError> {
        // Validate the step-key/schema boundary before taking a durable write.
        PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt,
            generation: 1,
        }
        .validate()?;

        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning publication fence advance"))?;
        validate_workflow_publishable(&tx, workflow)?;

        let current: Option<i64> = tx
            .query_row(
                "SELECT generation FROM workflow_publication_fences
                 WHERE workflow_id = ?1 AND step_key = ?2",
                rusqlite::params![workflow.to_string(), step_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading publication fence generation"))?;
        let generation = match current {
            None => 1u64,
            Some(value) => u64::try_from(value)
                .map_err(|_| CoreError::Storage("negative publication fence generation".into()))?
                .checked_add(1)
                .ok_or_else(|| CoreError::Storage("publication fence generation overflow".into()))?,
        };
        let generation_i64 = i64::try_from(generation)
            .map_err(|_| CoreError::Storage("publication fence generation exceeds SQLite INTEGER".into()))?;
        tx.execute(
            "INSERT INTO workflow_publication_fences
             (workflow_id, step_key, generation, attempt_id, publication_json)
             VALUES (?1, ?2, ?3, ?4, NULL)
             ON CONFLICT(workflow_id, step_key) DO UPDATE SET
                generation = excluded.generation,
                attempt_id = excluded.attempt_id,
                publication_json = NULL",
            rusqlite::params![
                workflow.to_string(),
                step_key,
                generation_i64,
                attempt.to_string()
            ],
        )
        .map_err(storage("advancing publication fence"))?;
        tx.commit()
            .map_err(storage("committing publication fence advance"))?;

        Ok(PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt,
            generation,
        })
    }

    fn publish_authoritative_outputs(
        &self,
        publication: &AuthoritativeStepPublication,
    ) -> Result<PublicationCommit, CoreError> {
        publication.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction()
            .map_err(storage("beginning authoritative publication"))?;
        validate_workflow_publishable(&tx, publication.fence.workflow)?;

        let current: Option<(i64, String, Option<String>)> = tx
            .query_row(
                "SELECT generation, attempt_id, publication_json
                 FROM workflow_publication_fences
                 WHERE workflow_id = ?1 AND step_key = ?2",
                rusqlite::params![
                    publication.fence.workflow.to_string(),
                    publication.fence.step_key
                ],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .map_err(storage("loading current publication fence"))?;
        let Some((generation, attempt_id, existing_json)) = current else {
            return Err(CoreError::Validation(
                "publication has no issued durable fence".into(),
            ));
        };
        let generation = u64::try_from(generation)
            .map_err(|_| CoreError::Storage("negative publication fence generation".into()))?;
        let attempt = AttemptId::from_str(&attempt_id)
            .map_err(|e| CoreError::Storage(format!("stored publication attempt id is invalid: {e}")))?;
        if generation != publication.fence.generation || attempt != publication.fence.attempt {
            return Err(CoreError::Validation(
                "publication fence is stale or does not match the current attempt".into(),
            ));
        }

        for artifact in publication.outputs.values() {
            let exists: Option<i64> = tx
                .query_row(
                    "SELECT 1 FROM artifact_meta WHERE id = ?1",
                    rusqlite::params![artifact.to_string()],
                    |row| row.get(0),
                )
                .optional()
                .map_err(storage("checking authoritative publication artifact"))?;
            if exists.is_none() {
                return Err(CoreError::ArtifactNotFound(*artifact));
            }
        }

        let json = serde_json::to_string(publication)
            .map_err(|e| CoreError::Storage(format!("serializing authoritative publication: {e}")))?;
        if let Some(existing_json) = existing_json {
            let existing: AuthoritativeStepPublication = serde_json::from_str(&existing_json)
                .map_err(|e| CoreError::Storage(format!("stored authoritative publication is invalid: {e}")))?;
            if existing == *publication {
                tx.commit()
                    .map_err(storage("committing idempotent publication replay"))?;
                return Ok(PublicationCommit::Idempotent);
            }
            return Err(CoreError::Validation(
                "current publication fence already committed different outputs".into(),
            ));
        }

        let changed = tx
            .execute(
                "UPDATE workflow_publication_fences
                 SET publication_json = ?5
                 WHERE workflow_id = ?1 AND step_key = ?2
                   AND generation = ?3 AND attempt_id = ?4
                   AND publication_json IS NULL",
                rusqlite::params![
                    publication.fence.workflow.to_string(),
                    publication.fence.step_key,
                    i64::try_from(publication.fence.generation).map_err(|_| {
                        CoreError::Storage(
                            "publication fence generation exceeds SQLite INTEGER".into(),
                        )
                    })?,
                    publication.fence.attempt.to_string(),
                    json
                ],
            )
            .map_err(storage("publishing authoritative workflow outputs"))?;
        if changed != 1 {
            return Err(CoreError::Validation(
                "publication fence changed before authoritative commit".into(),
            ));
        }
        tx.commit()
            .map_err(storage("committing authoritative publication"))?;
        Ok(PublicationCommit::Published)
    }

    fn current_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<PublicationFence>, CoreError> {
        PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt: AttemptId::generate(),
            generation: 1,
        }
        .validate()?;
        let conn = self.lock()?;
        let row: Option<(i64, String)> = conn
            .query_row(
                "SELECT generation, attempt_id FROM workflow_publication_fences
                 WHERE workflow_id = ?1 AND step_key = ?2",
                rusqlite::params![workflow.to_string(), step_key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(storage("loading current publication fence"))?;
        row.map(|(generation, attempt_id)| {
            Ok(PublicationFence {
                schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
                workflow,
                step_key: step_key.to_owned(),
                attempt: AttemptId::from_str(&attempt_id).map_err(|e| {
                    CoreError::Storage(format!("stored publication attempt id is invalid: {e}"))
                })?,
                generation: u64::try_from(generation).map_err(|_| {
                    CoreError::Storage("negative publication fence generation".into())
                })?,
            })
        })
        .transpose()
    }

    fn authoritative_publication(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<AuthoritativeStepPublication>, CoreError> {
        PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt: AttemptId::generate(),
            generation: 1,
        }
        .validate()?;
        let conn = self.lock()?;
        let json: Option<String> = conn
            .query_row(
                "SELECT publication_json FROM workflow_publication_fences
                 WHERE workflow_id = ?1 AND step_key = ?2",
                rusqlite::params![workflow.to_string(), step_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading authoritative publication"))?
            .flatten();
        json.map(|json| {
            let publication: AuthoritativeStepPublication = serde_json::from_str(&json)
                .map_err(|e| CoreError::Storage(format!("stored authoritative publication is invalid: {e}")))?;
            publication.validate()?;
            Ok(publication)
        })
        .transpose()
    }
}

fn validate_workflow_publishable(
    tx: &rusqlite::Transaction<'_>,
    workflow: WorkflowId,
) -> Result<(), CoreError> {
    let json: Option<String> = tx
        .query_row(
            "SELECT record_json FROM workflows WHERE id = ?1",
            rusqlite::params![workflow.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage("loading workflow for publication fence"))?;
    let Some(json) = json else {
        return Err(CoreError::WorkflowNotFound(workflow));
    };
    let record: hub_core::workflow::WorkflowRecord = serde_json::from_str(&json)
        .map_err(|e| CoreError::Storage(format!("stored workflow failed to deserialize: {e}")))?;
    if record.cancel_requested_at.is_some() || record.state.is_terminal() {
        return Err(CoreError::Validation(format!(
            "workflow {workflow} cannot publish after cancellation or terminal state"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_core::publication::PublicationFenceRepository as _;
    use hub_core::workflow::{WorkflowRecord, WorkflowSpec, WorkflowState};
    use hub_core::Version;

    fn running_workflow(store: &SqliteStore) -> WorkflowRecord {
        let mut record = WorkflowRecord::create(
            WorkflowSpec {
                schema_version: hub_core::WORKFLOW_SCHEMA_VERSION,
                name: "publication-fence-test".into(),
                max_concurrency: 1,
                steps: vec![hub_core::Step {
                    key: "step".into(),
                    component: hub_core::ComponentId::generate(),
                    capability: hub_core::CapabilityName::parse("test.publish").expect("capability"),
                    parameters: BTreeMap::new(),
                    inputs: BTreeMap::new(),
                    timeout_ms: 1_000,
                    after: Vec::new(),
                    retry: None,
                }],
            },
            Version::parse(hub_core::workflow::WORKFLOW_MODEL_VERSION).expect("model version"),
            1,
        )
        .expect("workflow");
        record
            .transition(WorkflowState::Running, 2)
            .expect("running transition");
        hub_core::store::WorkflowRepository::put(store, &record).expect("persist workflow");
        record
    }

    fn artifact(store: &SqliteStore) -> ArtifactId {
        let meta = hub_core::ArtifactMeta {
            id: ArtifactId::generate(),
            name: "result".into(),
            media_type: "application/octet-stream".into(),
            digest: hub_core::digest::hash_bytes(hub_core::digest::DOMAIN_ARTIFACT_BLOB, b"x"),
            size: 1,
            created_at: 3,
            produced_by_run: None,
        };
        hub_core::store::ArtifactMetadataRepository::put(store, &meta).expect("artifact meta");
        meta.id
    }

    #[test]
    fn durable_generation_rejects_stale_attempt_and_survives_reopen() {
        let dir = std::env::temp_dir().join(format!("hub-publication-fence-{}", uuid::Uuid::new_v4()));
        let db = dir.join("hub.db");
        let workflow;
        let first;
        {
            let store = SqliteStore::open(&db).expect("store");
            workflow = running_workflow(&store).id;
            first = store
                .advance_publication_fence(workflow, "step", AttemptId::generate())
                .expect("first");
            assert_eq!(first.generation, 1);
        }
        let store = SqliteStore::open(&db).expect("reopen");
        let second = store
            .advance_publication_fence(workflow, "step", AttemptId::generate())
            .expect("second");
        assert_eq!(second.generation, 2);
        let stale = AuthoritativeStepPublication {
            fence: first,
            outputs: BTreeMap::new(),
        };
        assert!(store.publish_authoritative_outputs(&stale).is_err());
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn publication_is_atomic_idempotent_and_conflict_closed() {
        let store = SqliteStore::open_in_memory().expect("store");
        let workflow = running_workflow(&store).id;
        let fence = store
            .advance_publication_fence(workflow, "step", AttemptId::generate())
            .expect("fence");
        let published = AuthoritativeStepPublication {
            fence: fence.clone(),
            outputs: BTreeMap::from([("stdout".into(), artifact(&store))]),
        };
        assert_eq!(
            store
                .publish_authoritative_outputs(&published)
                .expect("publish"),
            PublicationCommit::Published
        );
        assert_eq!(
            store
                .publish_authoritative_outputs(&published)
                .expect("idempotent replay"),
            PublicationCommit::Idempotent
        );
        let conflicting = AuthoritativeStepPublication {
            fence,
            outputs: BTreeMap::from([("stdout".into(), artifact(&store))]),
        };
        assert!(store.publish_authoritative_outputs(&conflicting).is_err());
    }

    #[test]
    fn cancelled_workflow_cannot_publish() {
        let store = SqliteStore::open_in_memory().expect("store");
        let record = running_workflow(&store);
        let fence = store
            .advance_publication_fence(record.id, "step", AttemptId::generate())
            .expect("fence");
        hub_core::store::WorkflowRepository::request_cancel(&store, &record.id, 4)
            .expect("cancel")
            .expect("workflow");
        let publication = AuthoritativeStepPublication {
            fence,
            outputs: BTreeMap::new(),
        };
        assert!(store.publish_authoritative_outputs(&publication).is_err());
    }
}
