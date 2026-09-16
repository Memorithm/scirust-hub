use std::str::FromStr as _;

use hub_core::publication::{
    AuthoritativeStepPublication, PublicationCommit, PublicationFence, PublicationFenceRepository,
    PUBLICATION_FENCE_SCHEMA_VERSION,
};
use hub_core::{AttemptId, CoreError, WorkflowId};
use rusqlite::{OptionalExtension as _, TransactionBehavior};

use crate::{storage, SqliteStore};

impl PublicationFenceRepository for SqliteStore {
    fn advance_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
        attempt: AttemptId,
    ) -> Result<PublicationFence, CoreError> {
        let candidate = PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt,
            generation: 1,
        };
        candidate.validate()?;

        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage("beginning publication fence advance"))?;
        let record = load_publishable_workflow(&tx, workflow)?;
        validate_current_attempt(&record, step_key, attempt)?;

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
            None => 1,
            Some(value) => u64::try_from(value)
                .map_err(|_| CoreError::Storage("negative publication fence generation".into()))?
                .checked_add(1)
                .ok_or_else(|| CoreError::Storage("publication fence generation overflow".into()))?,
        };
        let generation_i64 = i64::try_from(generation).map_err(|_| {
            CoreError::Storage("publication fence generation exceeds SQLite INTEGER".into())
        })?;

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
            generation,
            ..candidate
        })
    }

    fn publish_authoritative_outputs(
        &self,
        publication: &AuthoritativeStepPublication,
    ) -> Result<PublicationCommit, CoreError> {
        publication.validate()?;
        let mut conn = self.lock()?;
        let tx = conn
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(storage("beginning authoritative publication"))?;
        let record = load_publishable_workflow(&tx, publication.fence.workflow)?;
        validate_current_attempt(
            &record,
            &publication.fence.step_key,
            publication.fence.attempt,
        )?;

        let current: Option<(i64, String, Option<String>)> = tx
            .query_row(
                "SELECT generation, attempt_id, publication_json
                 FROM workflow_publication_fences
                 WHERE workflow_id = ?1 AND step_key = ?2",
                rusqlite::params![
                    publication.fence.workflow.to_string(),
                    &publication.fence.step_key
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
        let attempt = AttemptId::from_str(&attempt_id).map_err(|error| {
            CoreError::Storage(format!("stored publication attempt id is invalid: {error}"))
        })?;
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

        let json = serde_json::to_string(publication).map_err(|error| {
            CoreError::Storage(format!("serializing authoritative publication: {error}"))
        })?;
        if let Some(existing_json) = existing_json {
            let existing: AuthoritativeStepPublication = serde_json::from_str(&existing_json)
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "stored authoritative publication is invalid: {error}"
                    ))
                })?;
            if existing == *publication {
                tx.commit()
                    .map_err(storage("committing idempotent publication replay"))?;
                return Ok(PublicationCommit::Idempotent);
            }
            return Err(CoreError::Validation(
                "current publication fence already committed different outputs".into(),
            ));
        }

        let generation_i64 = i64::try_from(publication.fence.generation).map_err(|_| {
            CoreError::Storage("publication fence generation exceeds SQLite INTEGER".into())
        })?;
        let changed = tx
            .execute(
                "UPDATE workflow_publication_fences
                 SET publication_json = ?5
                 WHERE workflow_id = ?1 AND step_key = ?2
                   AND generation = ?3 AND attempt_id = ?4
                   AND publication_json IS NULL",
                rusqlite::params![
                    publication.fence.workflow.to_string(),
                    &publication.fence.step_key,
                    generation_i64,
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
        let probe = PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt: AttemptId::generate(),
            generation: 1,
        };
        probe.validate()?;
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
                attempt: AttemptId::from_str(&attempt_id).map_err(|error| {
                    CoreError::Storage(format!(
                        "stored publication attempt id is invalid: {error}"
                    ))
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
        let probe = PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt: AttemptId::generate(),
            generation: 1,
        };
        probe.validate()?;
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
                .map_err(|error| {
                    CoreError::Storage(format!(
                        "stored authoritative publication is invalid: {error}"
                    ))
                })?;
            publication.validate()?;
            Ok(publication)
        })
        .transpose()
    }
}

fn load_publishable_workflow(
    tx: &rusqlite::Transaction<'_>,
    workflow: WorkflowId,
) -> Result<hub_core::workflow::WorkflowRecord, CoreError> {
    let json: Option<String> = tx
        .query_row(
            "SELECT record_json FROM workflows WHERE id = ?1",
            rusqlite::params![workflow.to_string()],
            |row| row.get(0),
        )
        .optional()
        .map_err(storage("loading workflow for authoritative publication"))?;
    let Some(json) = json else {
        return Err(CoreError::WorkflowNotFound(workflow));
    };
    let record: hub_core::workflow::WorkflowRecord = serde_json::from_str(&json).map_err(|error| {
        CoreError::Storage(format!("stored workflow failed to deserialize: {error}"))
    })?;
    if record.cancel_requested_at.is_some() || record.state != hub_core::workflow::WorkflowState::Running {
        return Err(CoreError::Validation(format!(
            "workflow {workflow} is not eligible for authoritative publication"
        )));
    }
    Ok(record)
}

fn validate_current_attempt(
    record: &hub_core::workflow::WorkflowRecord,
    step_key: &str,
    attempt: AttemptId,
) -> Result<(), CoreError> {
    let step = record
        .steps
        .iter()
        .find(|result| result.key == step_key)
        .ok_or_else(|| {
            CoreError::Validation(format!(
                "workflow {} has no recorded attempt for step {step_key:?}",
                record.id
            ))
        })?;
    let current = step.attempts.last().ok_or_else(|| {
        CoreError::Validation(format!(
            "workflow {} step {step_key:?} has no attempt history",
            record.id
        ))
    })?;
    if current.id != attempt {
        return Err(CoreError::Validation(format!(
            "attempt {attempt} is not current for workflow {} step {step_key:?}",
            record.id
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use hub_core::publication::PublicationFenceRepository as _;
    use hub_core::run::RunState;
    use hub_core::store::{ArtifactMetadataRepository as _, WorkflowRepository as _};
    use hub_core::workflow::{
        Step, StepAttempt, StepResult, WorkflowRecord, WorkflowSpec, WorkflowState,
    };
    use hub_core::{ArtifactId, CapabilityName, ComponentId, RunId, Version};

    fn running_workflow(
        store: &SqliteStore,
        step_keys: &[&str],
    ) -> (WorkflowRecord, BTreeMap<String, AttemptId>) {
        let steps = step_keys
            .iter()
            .map(|key| Step {
                key: (*key).to_owned(),
                component: ComponentId::generate(),
                capability: CapabilityName::parse("test.publish").expect("capability"),
                parameters: BTreeMap::new(),
                inputs: BTreeMap::new(),
                timeout_ms: 1_000,
                after: Vec::new(),
                retry: None,
            })
            .collect();
        let mut record = WorkflowRecord::create(
            WorkflowSpec {
                schema_version: hub_core::WORKFLOW_SCHEMA_VERSION,
                name: "publication-fence-test".into(),
                max_concurrency: 2,
                steps,
            },
            Version::parse(hub_core::workflow::WORKFLOW_MODEL_VERSION).expect("model version"),
            1,
        )
        .expect("workflow");
        record
            .transition(WorkflowState::Running, 2)
            .expect("running transition");

        let mut attempts = BTreeMap::new();
        for key in step_keys {
            let attempt_id = AttemptId::generate();
            let run_id = RunId::generate();
            attempts.insert((*key).to_owned(), attempt_id);
            record.steps.push(StepResult {
                key: (*key).to_owned(),
                run: run_id,
                state: RunState::Queued,
                failure: None,
                attempts: vec![StepAttempt {
                    id: attempt_id,
                    number: 1,
                    run: run_id,
                    state: RunState::Queued,
                    started_at: None,
                    finished_at: None,
                    failure_category: None,
                    failure: None,
                }],
            });
        }
        record.steps.sort_by(|left, right| left.key.cmp(&right.key));
        WorkflowRepository::put(store, &record).expect("persist workflow");
        (record, attempts)
    }

    fn artifact(store: &SqliteStore, seed: u8) -> ArtifactId {
        let bytes = [seed];
        let meta = hub_core::ArtifactMeta {
            id: ArtifactId::generate(),
            name: format!("result-{seed}"),
            media_type: "application/octet-stream".into(),
            digest: hub_core::digest::hash_bytes(
                hub_core::digest::DOMAIN_ARTIFACT_BLOB,
                &bytes,
            ),
            size: 1,
            created_at: u64::from(seed) + 10,
            produced_by_run: None,
        };
        ArtifactMetadataRepository::put(store, &meta).expect("artifact meta");
        meta.id
    }

    #[test]
    fn generation_survives_reopen_and_stale_attempt_fails_closed() {
        let dir = std::env::temp_dir().join(format!("hub-publication-fence-{}", uuid::Uuid::new_v4()));
        let db = dir.join("hub.db");
        let workflow;
        let first;
        {
            let store = SqliteStore::open(&db).expect("store");
            let (record, attempts) = running_workflow(&store, &["step"]);
            workflow = record.id;
            first = store
                .advance_publication_fence(workflow, "step", attempts["step"])
                .expect("first fence");
            assert_eq!(first.generation, 1);
        }
        let store = SqliteStore::open(&db).expect("reopen");
        let mut record = WorkflowRepository::get(&store, &workflow)
            .expect("get")
            .expect("workflow");
        let second_attempt = AttemptId::generate();
        let second_run = RunId::generate();
        let step = record
            .steps
            .iter_mut()
            .find(|result| result.key == "step")
            .expect("step");
        step.run = second_run;
        step.state = RunState::Queued;
        step.attempts.push(StepAttempt {
            id: second_attempt,
            number: 2,
            run: second_run,
            state: RunState::Queued,
            started_at: None,
            finished_at: None,
            failure_category: None,
            failure: None,
        });
        WorkflowRepository::put(&store, &record).expect("persist retry");
        let second = store
            .advance_publication_fence(workflow, "step", second_attempt)
            .expect("second fence");
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
        let (record, attempts) = running_workflow(&store, &["step"]);
        let fence = store
            .advance_publication_fence(record.id, "step", attempts["step"])
            .expect("fence");
        let published = AuthoritativeStepPublication {
            fence: fence.clone(),
            outputs: BTreeMap::from([("stdout".into(), artifact(&store, 1))]),
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
            outputs: BTreeMap::from([("stdout".into(), artifact(&store, 2))]),
        };
        assert!(store.publish_authoritative_outputs(&conflicting).is_err());
    }

    #[test]
    fn cancellation_blocks_publication_atomically() {
        let store = SqliteStore::open_in_memory().expect("store");
        let (record, attempts) = running_workflow(&store, &["step"]);
        let fence = store
            .advance_publication_fence(record.id, "step", attempts["step"])
            .expect("fence");
        WorkflowRepository::request_cancel(&store, &record.id, 4)
            .expect("cancel")
            .expect("workflow");
        let publication = AuthoritativeStepPublication {
            fence,
            outputs: BTreeMap::new(),
        };
        assert!(store.publish_authoritative_outputs(&publication).is_err());
    }

    #[test]
    fn parallel_steps_have_independent_generations() {
        let store = SqliteStore::open_in_memory().expect("store");
        let (record, attempts) = running_workflow(&store, &["left", "right"]);
        let left = store
            .advance_publication_fence(record.id, "left", attempts["left"])
            .expect("left");
        let right = store
            .advance_publication_fence(record.id, "right", attempts["right"])
            .expect("right");
        assert_eq!(left.generation, 1);
        assert_eq!(right.generation, 1);
        assert_eq!(
            store
                .current_publication_fence(record.id, "left")
                .expect("left read")
                .expect("left fence"),
            left
        );
        assert_eq!(
            store
                .current_publication_fence(record.id, "right")
                .expect("right read")
                .expect("right fence"),
            right
        );
    }
}
