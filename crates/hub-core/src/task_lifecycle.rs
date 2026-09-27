//! Authoritative lifecycle state for isolated agent tasks.
//!
//! A [`TaskSpec`] declares the requested execution envelope. A [`TaskRecord`]
//! records control-plane lifecycle state for that logical task. It still does
//! not execute code or prove that a sandbox exists.

use serde::{Deserialize, Serialize};

use crate::clock::UnixMillis;
use crate::error::CoreError;
use crate::task::{TaskSpec, WorkspaceMaterializationEvidence};

/// Current durable record schema.
pub const TASK_RECORD_SCHEMA_VERSION: u32 = 2;
const MIN_TASK_RECORD_SCHEMA_VERSION: u32 = 1;

/// Lifecycle of one logical task.
///
/// `Suspended` is non-terminal: resumption returns to `Running`. `Terminated`
/// is reserved for an explicit forced stop/destruction after execution has
/// begun. Ordinary user cancellation uses `Cancelled`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Created,
    Admitted,
    Running,
    Suspended,
    Succeeded,
    Failed,
    Cancelled,
    Terminated,
}

impl TaskState {
    /// Legal forward transitions. Terminal states transition nowhere.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        use TaskState::*;
        matches!(
            (self, next),
            (Created, Admitted)
                | (Created, Failed)
                | (Created, Cancelled)
                | (Admitted, Running)
                | (Admitted, Failed)
                | (Admitted, Cancelled)
                | (Running, Suspended)
                | (Running, Succeeded)
                | (Running, Failed)
                | (Running, Cancelled)
                | (Running, Terminated)
                | (Suspended, Running)
                | (Suspended, Failed)
                | (Suspended, Cancelled)
                | (Suspended, Terminated)
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Succeeded | Self::Failed | Self::Cancelled | Self::Terminated
        )
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Admitted => "admitted",
            Self::Running => "running",
            Self::Suspended => "suspended",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::Terminated => "terminated",
        }
    }
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One retained task-state transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskTransition {
    pub from: TaskState,
    pub to: TaskState,
    pub at: UnixMillis,
}

/// Durable authoritative snapshot of one logical task.
///
/// The immutable `spec` is never rewritten to represent retries or resumed
/// attempts. Later execution-attempt identity belongs to a separate layer.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TaskRecord {
    pub schema_version: u32,
    pub spec: TaskSpec,
    pub state: TaskState,
    pub created_at: UnixMillis,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_evidence: Option<WorkspaceMaterializationEvidence>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admitted_at: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<UnixMillis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub transitions: Vec<TaskTransition>,
}

impl TaskRecord {
    /// Creates a fresh task record in `Created`.
    ///
    /// # Errors
    /// Propagates [`TaskSpec::validate`].
    pub fn create(spec: TaskSpec, now: UnixMillis) -> Result<Self, CoreError> {
        spec.validate()?;
        Ok(Self {
            schema_version: TASK_RECORD_SCHEMA_VERSION,
            spec,
            state: TaskState::Created,
            created_at: now,
            workspace_evidence: None,
            admitted_at: None,
            started_at: None,
            finished_at: None,
            transitions: Vec::new(),
        })
    }

    /// Admits a newly created task only after verified workspace evidence exists.
    ///
    /// # Errors
    /// Validation errors for workspace-evidence mismatch or lifecycle errors.
    pub fn admit(
        &mut self,
        workspace_evidence: WorkspaceMaterializationEvidence,
        now: UnixMillis,
    ) -> Result<(), CoreError> {
        if self.state != TaskState::Created {
            return Err(CoreError::InvalidTaskTransition {
                from: self.state,
                to: TaskState::Admitted,
            });
        }
        workspace_evidence.validate_against(&self.spec.workspace)?;
        if self.workspace_evidence.is_some() {
            return Err(CoreError::Validation(
                "task workspace evidence is already recorded".to_owned(),
            ));
        }
        self.workspace_evidence = Some(workspace_evidence);
        self.apply_transition(TaskState::Admitted, now)
    }

    /// Applies one legal monotonic lifecycle transition after admission.
    ///
    /// `Created -> Admitted` is intentionally unavailable here; callers must
    /// use [`Self::admit`] so durable admission always carries verified
    /// workspace evidence for schema-v2 task records.
    ///
    /// # Errors
    /// Returns [`CoreError::InvalidTaskTransition`] for illegal moves and a
    /// validation error when admission is attempted without workspace evidence
    /// or timestamps would move backwards.
    pub fn transition(&mut self, to: TaskState, now: UnixMillis) -> Result<(), CoreError> {
        if self.state == TaskState::Created && to == TaskState::Admitted {
            return Err(CoreError::Validation(
                "task admission requires verified workspace materialization evidence".to_owned(),
            ));
        }
        self.apply_transition(to, now)
    }

    fn apply_transition(&mut self, to: TaskState, now: UnixMillis) -> Result<(), CoreError> {
        let from = self.state;
        if !from.can_transition_to(to) {
            return Err(CoreError::InvalidTaskTransition { from, to });
        }
        let previous_at = self
            .transitions
            .last()
            .map_or(self.created_at, |transition| transition.at);
        if now < previous_at {
            return Err(CoreError::Validation(format!(
                "task transition timestamp {now} precedes previous lifecycle timestamp {previous_at}"
            )));
        }

        self.state = to;
        if to == TaskState::Admitted {
            self.admitted_at.get_or_insert(now);
        }
        if to == TaskState::Running {
            self.started_at.get_or_insert(now);
        }
        if to.is_terminal() {
            self.finished_at = Some(now);
        }
        self.transitions.push(TaskTransition { from, to, at: now });
        Ok(())
    }

    /// Replays the retained transition chain and verifies that the snapshot is
    /// internally consistent.
    ///
    /// # Errors
    /// Validation errors for schema/spec drift, malformed transition history,
    /// non-monotonic timestamps or inconsistent derived timestamps/state.
    pub fn validate(&self) -> Result<(), CoreError> {
        if !(MIN_TASK_RECORD_SCHEMA_VERSION..=TASK_RECORD_SCHEMA_VERSION)
            .contains(&self.schema_version)
        {
            return Err(CoreError::Validation(format!(
                "unsupported task record schema_version {}; supported range is {MIN_TASK_RECORD_SCHEMA_VERSION}..={TASK_RECORD_SCHEMA_VERSION}",
                self.schema_version
            )));
        }
        self.spec.validate()?;

        let mut replay_state = TaskState::Created;
        let mut previous_at = self.created_at;
        let mut admitted_at = None;
        let mut started_at = None;
        let mut finished_at = None;

        for transition in &self.transitions {
            if transition.from != replay_state || !replay_state.can_transition_to(transition.to) {
                return Err(CoreError::Validation(format!(
                    "invalid retained task transition {:?} -> {:?} while replay state is {:?}",
                    transition.from, transition.to, replay_state
                )));
            }
            if transition.at < previous_at {
                return Err(CoreError::Validation(
                    "task transition timestamps are not monotonic".to_owned(),
                ));
            }
            replay_state = transition.to;
            previous_at = transition.at;
            if replay_state == TaskState::Admitted {
                admitted_at.get_or_insert(transition.at);
            }
            if replay_state == TaskState::Running {
                started_at.get_or_insert(transition.at);
            }
            if replay_state.is_terminal() {
                finished_at = Some(transition.at);
            }
        }

        if replay_state != self.state {
            return Err(CoreError::Validation(format!(
                "task state {:?} does not match replayed state {:?}",
                self.state, replay_state
            )));
        }
        if self.admitted_at != admitted_at
            || self.started_at != started_at
            || self.finished_at != finished_at
        {
            return Err(CoreError::Validation(
                "task lifecycle timestamps do not match retained transitions".to_owned(),
            ));
        }

        if self.schema_version >= 2 {
            match (self.admitted_at, &self.workspace_evidence) {
                (Some(_), Some(evidence)) => evidence.validate_against(&self.spec.workspace)?,
                (Some(_), None) => {
                    return Err(CoreError::Validation(
                        "admitted task is missing verified workspace evidence".to_owned(),
                    ));
                }
                (None, Some(_)) => {
                    return Err(CoreError::Validation(
                        "workspace evidence cannot be retained before task admission".to_owned(),
                    ));
                }
                (None, None) => {}
            }
        } else if self.workspace_evidence.is_some() {
            return Err(CoreError::Validation(
                "legacy task record schema cannot carry workspace evidence".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Validates that an update is an append-only continuation of an existing
/// task snapshot.
///
/// This prevents stale writers from rolling a durable task backwards or
/// rewriting its immutable execution envelope.
///
/// # Errors
/// Validation errors for immutable-field drift or a non-prefix transition
/// history.
pub fn validate_task_snapshot_update(
    previous: &TaskRecord,
    current: &TaskRecord,
) -> Result<(), CoreError> {
    previous.validate()?;
    current.validate()?;
    if previous.schema_version != current.schema_version
        || previous.spec != current.spec
        || previous.created_at != current.created_at
    {
        return Err(CoreError::Validation(
            "task immutable schema/identity/spec/creation time cannot change".to_owned(),
        ));
    }
    match (&previous.workspace_evidence, &current.workspace_evidence) {
        (Some(before), Some(after)) if before != after => {
            return Err(CoreError::Validation(
                "task workspace evidence cannot change after admission".to_owned(),
            ));
        }
        (Some(_), None) => {
            return Err(CoreError::Validation(
                "task workspace evidence cannot be removed".to_owned(),
            ));
        }
        (None, Some(_)) => {
            let new_transitions = current
                .transitions
                .get(previous.transitions.len()..)
                .unwrap_or_default();
            if !new_transitions
                .iter()
                .any(|transition| transition.to == TaskState::Admitted)
            {
                return Err(CoreError::Validation(
                    "workspace evidence may be added only with an admission transition".to_owned(),
                ));
            }
        }
        _ => {}
    }

    if current.transitions.len() < previous.transitions.len()
        || current.transitions[..previous.transitions.len()] != previous.transitions
    {
        return Err(CoreError::Validation(
            "task update must preserve the complete previous transition prefix".to_owned(),
        ));
    }
    if current.transitions.len() == previous.transitions.len() && current != previous {
        return Err(CoreError::Validation(
            "task update without a new transition must be identical".to_owned(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::{
        CapabilitySet, IsolationLevel, NetworkPolicy, ResourceBudget, SandboxRequirements,
        TaskIdentity, WorkspaceMaterializationEvidence, WorkspaceSpec, TASK_SPEC_SCHEMA_VERSION,
    };
    use crate::TaskId;

    fn task_spec() -> TaskSpec {
        let id = TaskId::generate();
        TaskSpec {
            schema_version: TASK_SPEC_SCHEMA_VERSION,
            id,
            identity: TaskIdentity {
                task_id: id,
                principal: format!("task://memorithm/test/{id}"),
            },
            workspace: WorkspaceSpec::default(),
            capabilities: CapabilitySet::default(),
            budget: ResourceBudget::default(),
            sandbox: SandboxRequirements {
                minimum_isolation: IsolationLevel::Process,
                network: NetworkPolicy {
                    default_deny: false,
                    allowed_endpoints: Vec::new(),
                },
                writable_workspace: false,
            },
        }
    }

    fn workspace_evidence(spec: &TaskSpec) -> WorkspaceMaterializationEvidence {
        WorkspaceMaterializationEvidence::new(&spec.workspace, Vec::new()).expect("workspace")
    }

    #[test]
    fn suspend_resume_chain_is_retained() {
        let spec = task_spec();
        let evidence = workspace_evidence(&spec);
        let mut record = TaskRecord::create(spec, 10).expect("task");
        record.admit(evidence, 11).expect("admit");
        for (state, at) in [
            (TaskState::Running, 12),
            (TaskState::Suspended, 13),
            (TaskState::Running, 14),
            (TaskState::Succeeded, 15),
        ] {
            record.transition(state, at).expect("transition");
        }
        assert_eq!(record.state, TaskState::Succeeded);
        assert_eq!(record.admitted_at, Some(11));
        assert_eq!(record.started_at, Some(12));
        assert_eq!(record.finished_at, Some(15));
        assert_eq!(record.transitions.len(), 5);
        record.validate().expect("valid retained lifecycle");
    }

    #[test]
    fn admission_requires_verified_workspace_evidence() {
        let mut record = TaskRecord::create(task_spec(), 10).expect("task");
        assert!(matches!(
            record.transition(TaskState::Admitted, 11),
            Err(CoreError::Validation(message)) if message.contains("workspace materialization")
        ));
        assert_eq!(record.state, TaskState::Created);
        assert!(record.workspace_evidence.is_none());
    }

    #[test]
    fn direct_created_to_running_is_rejected() {
        let mut record = TaskRecord::create(task_spec(), 10).expect("task");
        assert!(matches!(
            record.transition(TaskState::Running, 11),
            Err(CoreError::InvalidTaskTransition { .. })
        ));
        assert_eq!(record.state, TaskState::Created);
    }

    #[test]
    fn terminal_state_is_frozen() {
        let mut record = TaskRecord::create(task_spec(), 10).expect("task");
        record.transition(TaskState::Cancelled, 11).expect("cancel");
        assert!(record.state.is_terminal());
        assert!(record.transition(TaskState::Admitted, 12).is_err());
    }

    #[test]
    fn snapshot_updates_must_be_append_only() {
        let created = TaskRecord::create(task_spec(), 10).expect("task");
        let mut running = created.clone();
        let evidence = workspace_evidence(&running.spec);
        running.admit(evidence, 11).expect("admit");
        running.transition(TaskState::Running, 12).expect("run");
        validate_task_snapshot_update(&created, &running).expect("forward update");

        let mut stale_mutation = created.clone();
        stale_mutation.admitted_at = Some(99);
        assert!(validate_task_snapshot_update(&running, &stale_mutation).is_err());
    }

    #[test]
    fn retained_transition_timestamps_must_be_monotonic() {
        let mut record = TaskRecord::create(task_spec(), 10).expect("task");
        record.transition(TaskState::Admitted, 12).expect("admit");
        assert!(record.transition(TaskState::Running, 11).is_err());
    }
}
