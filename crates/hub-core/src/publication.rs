//! Durable workflow-step publication fencing.
//!
//! This module owns only the generic Hub publication authority needed to
//! prevent a superseded workflow-step attempt from becoming authoritative.
//! Scientific meaning, verdicts, and domain-specific provenance remain in the
//! owning component/research system.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::error::CoreError;
use crate::id::{ArtifactId, AttemptId, WorkflowId};

/// Version of the persisted/interchange publication-fence contract.
pub const PUBLICATION_FENCE_SCHEMA_VERSION: u16 = 1;
/// Hard bound on authoritative output labels carried by one step publication.
pub const MAX_PUBLICATION_OUTPUTS: usize = 256;

/// Authority token for one concrete workflow-step attempt.
///
/// `generation` is monotonically increasing for `(workflow, step_key)`. A
/// publication is authoritative only while all four identity fields still
/// match the repository's current fence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationFence {
    pub schema_version: u16,
    pub workflow: WorkflowId,
    pub step_key: String,
    pub attempt: AttemptId,
    pub generation: u64,
}

impl PublicationFence {
    /// Validates the wire/persistence boundary independently of repository
    /// state.
    ///
    /// # Errors
    /// [`CoreError::Validation`] for unsupported schema, invalid step keys or
    /// zero generation.
    pub fn validate(&self) -> Result<(), CoreError> {
        if self.schema_version != PUBLICATION_FENCE_SCHEMA_VERSION {
            return Err(CoreError::Validation(format!(
                "unsupported publication fence schema_version {}: expected {}",
                self.schema_version, PUBLICATION_FENCE_SCHEMA_VERSION
            )));
        }
        validate_step_key(&self.step_key)?;
        if self.generation == 0 {
            return Err(CoreError::Validation(
                "publication fence generation must be at least 1".into(),
            ));
        }
        Ok(())
    }
}

/// Outputs accepted as authoritative for one fenced workflow-step attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthoritativeStepPublication {
    pub fence: PublicationFence,
    /// Deterministic label -> immutable Hub artifact identity mapping.
    pub outputs: BTreeMap<String, ArtifactId>,
}

impl AuthoritativeStepPublication {
    /// Validates bounded, deterministic publication structure.
    ///
    /// Empty output maps are valid for side-effect-free steps that declare no
    /// artifacts; publication still records which attempt is authoritative.
    pub fn validate(&self) -> Result<(), CoreError> {
        self.fence.validate()?;
        if self.outputs.len() > MAX_PUBLICATION_OUTPUTS {
            return Err(CoreError::Validation(format!(
                "authoritative publication has {} outputs; maximum is {}",
                self.outputs.len(),
                MAX_PUBLICATION_OUTPUTS
            )));
        }
        for name in self.outputs.keys() {
            validate_output_name(name)?;
        }
        Ok(())
    }
}

/// Result of an authoritative publication commit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublicationCommit {
    /// This fence published its output set for the first time.
    Published,
    /// The same current fence replayed a byte-identical logical output map.
    Idempotent,
}

/// Durable authority boundary for workflow-step publications.
///
/// Implementations must serialize updates per `(workflow, step_key)`, persist
/// generations across process restart, and make fence validation plus first
/// authoritative publication one atomic storage transaction. A stale attempt,
/// a conflicting duplicate publication, or publication after workflow
/// cancellation must fail closed.
pub trait PublicationFenceRepository: Send + Sync {
    /// Advances authority to `attempt` and returns its fresh generation.
    ///
    /// # Errors
    /// Workflow/storage/validation errors. The implementation must reject a
    /// cancelled or terminal workflow.
    fn advance_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
        attempt: AttemptId,
    ) -> Result<PublicationFence, CoreError>;

    /// Atomically validates that `publication.fence` is still current and
    /// records its authoritative output map.
    ///
    /// Exact replay by the same current fence is idempotent. Any different
    /// mapping for an already-published fence fails closed.
    fn publish_authoritative_outputs(
        &self,
        publication: &AuthoritativeStepPublication,
    ) -> Result<PublicationCommit, CoreError>;

    /// Returns the current fence for one workflow step, if any.
    fn current_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<PublicationFence>, CoreError>;

    /// Returns the authoritative publication for one workflow step, if any.
    fn authoritative_publication(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<AuthoritativeStepPublication>, CoreError>;
}

#[derive(Clone, Debug)]
struct FenceEntry {
    fence: PublicationFence,
    publication: Option<AuthoritativeStepPublication>,
}

/// In-memory reference implementation used for contract tests and ephemeral
/// callers. It does not claim restart durability; durable deployments use the
/// SQLite implementation.
#[derive(Debug, Default)]
pub struct InMemoryPublicationFences(Mutex<BTreeMap<(WorkflowId, String), FenceEntry>>);

impl PublicationFenceRepository for InMemoryPublicationFences {
    fn advance_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
        attempt: AttemptId,
    ) -> Result<PublicationFence, CoreError> {
        validate_step_key(step_key)?;
        let mut entries = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("publication fence lock poisoned".into()))?;
        let key = (workflow, step_key.to_owned());
        let generation = match entries.get(&key) {
            Some(entry) => entry.fence.generation.checked_add(1).ok_or_else(|| {
                CoreError::Storage("publication fence generation overflow".into())
            })?,
            None => 1,
        };
        let fence = PublicationFence {
            schema_version: PUBLICATION_FENCE_SCHEMA_VERSION,
            workflow,
            step_key: step_key.to_owned(),
            attempt,
            generation,
        };
        entries.insert(
            key,
            FenceEntry {
                fence: fence.clone(),
                publication: None,
            },
        );
        Ok(fence)
    }

    fn publish_authoritative_outputs(
        &self,
        publication: &AuthoritativeStepPublication,
    ) -> Result<PublicationCommit, CoreError> {
        publication.validate()?;
        let mut entries = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("publication fence lock poisoned".into()))?;
        let key = (
            publication.fence.workflow,
            publication.fence.step_key.clone(),
        );
        let entry = entries
            .get_mut(&key)
            .ok_or_else(|| CoreError::Validation("publication has no issued fence".into()))?;
        if entry.fence != publication.fence {
            return Err(CoreError::Validation(
                "publication fence is stale or does not match the current attempt".into(),
            ));
        }
        match &entry.publication {
            None => {
                entry.publication = Some(publication.clone());
                Ok(PublicationCommit::Published)
            }
            Some(existing) if existing == publication => Ok(PublicationCommit::Idempotent),
            Some(_) => Err(CoreError::Validation(
                "current publication fence already committed different outputs".into(),
            )),
        }
    }

    fn current_publication_fence(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<PublicationFence>, CoreError> {
        validate_step_key(step_key)?;
        let entries = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("publication fence lock poisoned".into()))?;
        Ok(entries
            .get(&(workflow, step_key.to_owned()))
            .map(|entry| entry.fence.clone()))
    }

    fn authoritative_publication(
        &self,
        workflow: WorkflowId,
        step_key: &str,
    ) -> Result<Option<AuthoritativeStepPublication>, CoreError> {
        validate_step_key(step_key)?;
        let entries = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("publication fence lock poisoned".into()))?;
        Ok(entries
            .get(&(workflow, step_key.to_owned()))
            .and_then(|entry| entry.publication.clone()))
    }
}

fn validate_step_key(key: &str) -> Result<(), CoreError> {
    let valid = !key.is_empty()
        && key.len() <= 64
        && key.starts_with(|c: char| c.is_ascii_lowercase() || c.is_ascii_digit())
        && key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "publication step key {key:?} must match [a-z0-9][a-z0-9_-]{{0,63}}"
        )))
    }
}

fn validate_output_name(name: &str) -> Result<(), CoreError> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| !c.is_whitespace() && !c.is_control());
    if valid {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "publication output name {name:?} must be 1..=128 characters without whitespace/control characters"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn publication(fence: PublicationFence, artifact: ArtifactId) -> AuthoritativeStepPublication {
        AuthoritativeStepPublication {
            fence,
            outputs: BTreeMap::from([("stdout".into(), artifact)]),
        }
    }

    #[test]
    fn generation_advances_and_stale_attempt_fails_closed() {
        let store = InMemoryPublicationFences::default();
        let workflow = WorkflowId::generate();
        let first = store
            .advance_publication_fence(workflow, "step", AttemptId::generate())
            .expect("first fence");
        let second = store
            .advance_publication_fence(workflow, "step", AttemptId::generate())
            .expect("second fence");
        assert_eq!(first.generation, 1);
        assert_eq!(second.generation, 2);
        assert!(store
            .publish_authoritative_outputs(&publication(first, ArtifactId::generate()))
            .is_err());
    }

    #[test]
    fn identical_replay_is_idempotent_but_conflict_is_rejected() {
        let store = InMemoryPublicationFences::default();
        let fence = store
            .advance_publication_fence(WorkflowId::generate(), "step", AttemptId::generate())
            .expect("fence");
        let first = publication(fence.clone(), ArtifactId::generate());
        assert_eq!(
            store
                .publish_authoritative_outputs(&first)
                .expect("publish"),
            PublicationCommit::Published
        );
        assert_eq!(
            store.publish_authoritative_outputs(&first).expect("replay"),
            PublicationCommit::Idempotent
        );
        let conflicting = publication(fence, ArtifactId::generate());
        assert!(store.publish_authoritative_outputs(&conflicting).is_err());
    }
}
