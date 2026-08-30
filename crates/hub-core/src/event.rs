//! Durable lifecycle-event vocabulary.
//!
//! Events are an append-only operational chronology derived from successful
//! authoritative repository mutations. They are not a second state machine:
//! component/run/workflow/artifact records remain the source of truth.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::artifact::ArtifactMeta;
use crate::clock::UnixMillis;
use crate::component::ComponentManifest;
use crate::error::CoreError;
use crate::run::{RunRecord, RunState};
use crate::workflow::{StepAttempt, WorkflowRecord, WorkflowState};

/// Maximum number of lifecycle events returned by one repository read.
pub const MAX_EVENT_PAGE: u32 = 1_000;
/// Default HTTP/CLI event page size.
pub const DEFAULT_EVENT_PAGE: u32 = 100;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventKind {
    ComponentRegistered,
    ArtifactRecorded,
    RunCreated,
    RunStateChanged,
    WorkflowCreated,
    WorkflowStateChanged,
    WorkflowCancelRequested,
    WorkflowAttemptCreated,
    WorkflowAttemptStateChanged,
}

impl LifecycleEventKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ComponentRegistered => "component_registered",
            Self::ArtifactRecorded => "artifact_recorded",
            Self::RunCreated => "run_created",
            Self::RunStateChanged => "run_state_changed",
            Self::WorkflowCreated => "workflow_created",
            Self::WorkflowStateChanged => "workflow_state_changed",
            Self::WorkflowCancelRequested => "workflow_cancel_requested",
            Self::WorkflowAttemptCreated => "workflow_attempt_created",
            Self::WorkflowAttemptStateChanged => "workflow_attempt_state_changed",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "component_registered" => Some(Self::ComponentRegistered),
            "artifact_recorded" => Some(Self::ArtifactRecorded),
            "run_created" => Some(Self::RunCreated),
            "run_state_changed" => Some(Self::RunStateChanged),
            "workflow_created" => Some(Self::WorkflowCreated),
            "workflow_state_changed" => Some(Self::WorkflowStateChanged),
            "workflow_cancel_requested" => Some(Self::WorkflowCancelRequested),
            "workflow_attempt_created" => Some(Self::WorkflowAttemptCreated),
            "workflow_attempt_state_changed" => Some(Self::WorkflowAttemptStateChanged),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEntityType {
    Component,
    Artifact,
    Run,
    Workflow,
}

impl LifecycleEntityType {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Component => "component",
            Self::Artifact => "artifact",
            Self::Run => "run",
            Self::Workflow => "workflow",
        }
    }

    #[must_use]
    pub fn parse(raw: &str) -> Option<Self> {
        match raw {
            "component" => Some(Self::Component),
            "artifact" => Some(Self::Artifact),
            "run" => Some(Self::Run),
            "workflow" => Some(Self::Workflow),
            _ => None,
        }
    }
}

/// Event before a repository assigns its monotonic sequence number.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NewLifecycleEvent {
    pub recorded_at: UnixMillis,
    pub kind: LifecycleEventKind,
    pub entity_type: LifecycleEntityType,
    pub entity_id: String,
    pub attributes: BTreeMap<String, String>,
}

impl NewLifecycleEvent {
    #[must_use]
    pub fn new(
        recorded_at: UnixMillis,
        kind: LifecycleEventKind,
        entity_type: LifecycleEntityType,
        entity_id: impl Into<String>,
        attributes: BTreeMap<String, String>,
    ) -> Self {
        Self {
            recorded_at,
            kind,
            entity_type,
            entity_id: entity_id.into(),
            attributes,
        }
    }
}

/// Persisted append-only lifecycle event. `sequence` is local to one Hub
/// store and strictly increases with successful appends.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEvent {
    pub sequence: u64,
    pub recorded_at: UnixMillis,
    pub kind: LifecycleEventKind,
    pub entity_type: LifecycleEntityType,
    pub entity_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

/// Read port for lifecycle events. Mutations stay private to authoritative
/// repositories so API clients cannot forge audit entries.
pub trait LifecycleEventRepository: Send + Sync {
    /// Returns events with `sequence > after_sequence`, oldest first.
    ///
    /// # Errors
    /// Storage failures or an invalid page limit.
    fn list_after(&self, after_sequence: u64, limit: u32)
        -> Result<Vec<LifecycleEvent>, CoreError>;

    /// Highest committed event sequence, or zero when the log is empty.
    ///
    /// # Errors
    /// Storage failures only.
    fn high_water_sequence(&self) -> Result<u64, CoreError>;
}

/// Standalone in-memory event log used by tests and the ephemeral composite
/// store. `record` is intentionally not part of the public repository port.
#[derive(Debug, Default)]
pub struct InMemoryLifecycleEvents(Mutex<InMemoryEventInner>);

#[derive(Debug, Default)]
struct InMemoryEventInner {
    next_sequence: u64,
    events: Vec<LifecycleEvent>,
}

impl InMemoryLifecycleEvents {
    /// Appends one event and assigns the next sequence.
    ///
    /// # Errors
    /// Lock poisoning or sequence exhaustion.
    pub fn record(&self, event: NewLifecycleEvent) -> Result<LifecycleEvent, CoreError> {
        let mut inner = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("lifecycle event lock poisoned".into()))?;
        inner.next_sequence = inner
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| CoreError::Storage("lifecycle event sequence exhausted".into()))?;
        let stored = LifecycleEvent {
            sequence: inner.next_sequence,
            recorded_at: event.recorded_at,
            kind: event.kind,
            entity_type: event.entity_type,
            entity_id: event.entity_id,
            attributes: event.attributes,
        };
        inner.events.push(stored.clone());
        Ok(stored)
    }
}

impl LifecycleEventRepository for InMemoryLifecycleEvents {
    fn list_after(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<Vec<LifecycleEvent>, CoreError> {
        validate_event_page_limit(limit)?;
        let inner = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("lifecycle event lock poisoned".into()))?;
        Ok(inner
            .events
            .iter()
            .filter(|event| event.sequence > after_sequence)
            .take(limit as usize)
            .cloned()
            .collect())
    }

    fn high_water_sequence(&self) -> Result<u64, CoreError> {
        let inner = self
            .0
            .lock()
            .map_err(|_| CoreError::Storage("lifecycle event lock poisoned".into()))?;
        Ok(inner.next_sequence)
    }
}

/// # Errors
/// Validation error for zero or oversized pages.
pub fn validate_event_page_limit(limit: u32) -> Result<(), CoreError> {
    if (1..=MAX_EVENT_PAGE).contains(&limit) {
        Ok(())
    } else {
        Err(CoreError::Validation(format!(
            "event page limit must be 1..={MAX_EVENT_PAGE}"
        )))
    }
}

#[must_use]
pub fn component_registered_event(
    manifest: &ComponentManifest,
    recorded_at: UnixMillis,
) -> NewLifecycleEvent {
    NewLifecycleEvent::new(
        recorded_at,
        LifecycleEventKind::ComponentRegistered,
        LifecycleEntityType::Component,
        manifest.id.to_string(),
        BTreeMap::from([
            ("name".into(), manifest.name.as_str().to_owned()),
            ("version".into(), manifest.version.as_str().to_owned()),
        ]),
    )
}

#[must_use]
pub fn artifact_recorded_event(meta: &ArtifactMeta) -> NewLifecycleEvent {
    let mut attributes = BTreeMap::from([
        ("name".into(), meta.name.clone()),
        ("media_type".into(), meta.media_type.clone()),
        ("digest".into(), meta.digest.to_hex()),
        ("size".into(), meta.size.to_string()),
    ]);
    if let Some(run) = meta.produced_by_run {
        attributes.insert("produced_by_run".into(), run.to_string());
    }
    NewLifecycleEvent::new(
        meta.created_at,
        LifecycleEventKind::ArtifactRecorded,
        LifecycleEntityType::Artifact,
        meta.id.to_string(),
        attributes,
    )
}

#[must_use]
pub fn derive_run_events(
    previous: Option<&RunRecord>,
    current: &RunRecord,
) -> Vec<NewLifecycleEvent> {
    let mut events = Vec::new();
    if previous.is_none() {
        let mut attributes = BTreeMap::from([
            ("component".into(), current.spec.component.to_string()),
            ("capability".into(), current.spec.capability.to_string()),
            ("state".into(), run_state_name(RunState::Created).into()),
        ]);
        if let Some(origin) = current.reproduced_from {
            attributes.insert("reproduced_from".into(), origin.to_string());
        }
        events.push(NewLifecycleEvent::new(
            current.created_at,
            LifecycleEventKind::RunCreated,
            LifecycleEntityType::Run,
            current.id.to_string(),
            attributes,
        ));
    }

    let previous_transition_count = previous.map_or(0, |record| record.transitions.len());
    for transition in current.transitions.iter().skip(previous_transition_count) {
        events.push(NewLifecycleEvent::new(
            transition.at,
            LifecycleEventKind::RunStateChanged,
            LifecycleEntityType::Run,
            current.id.to_string(),
            BTreeMap::from([
                ("from".into(), run_state_name(transition.from).into()),
                ("to".into(), run_state_name(transition.to).into()),
            ]),
        ));
    }
    events
}

#[must_use]
pub fn derive_workflow_events(
    previous: Option<&WorkflowRecord>,
    current: &WorkflowRecord,
    observed_at: UnixMillis,
) -> Vec<NewLifecycleEvent> {
    let mut events = Vec::new();
    if previous.is_none() {
        events.push(NewLifecycleEvent::new(
            current.created_at,
            LifecycleEventKind::WorkflowCreated,
            LifecycleEntityType::Workflow,
            current.id.to_string(),
            BTreeMap::from([
                ("name".into(), current.spec.name.clone()),
                (
                    "state".into(),
                    workflow_state_name(WorkflowState::Created).into(),
                ),
            ]),
        ));
    }

    let previous_state = previous.map_or(WorkflowState::Created, |record| record.state);
    if current.state != previous_state {
        let at = if current.state == WorkflowState::Running {
            current.started_at.unwrap_or(observed_at)
        } else if current.state.is_terminal() {
            current.finished_at.unwrap_or(observed_at)
        } else {
            observed_at
        };
        events.push(NewLifecycleEvent::new(
            at,
            LifecycleEventKind::WorkflowStateChanged,
            LifecycleEntityType::Workflow,
            current.id.to_string(),
            BTreeMap::from([
                ("from".into(), workflow_state_name(previous_state).into()),
                ("to".into(), workflow_state_name(current.state).into()),
            ]),
        ));
    }

    if previous
        .and_then(|record| record.cancel_requested_at)
        .is_none()
    {
        if let Some(at) = current.cancel_requested_at {
            events.push(workflow_cancel_requested_event(current.id.to_string(), at));
        }
    }

    let previous_attempt_ids: BTreeSet<_> = previous
        .into_iter()
        .flat_map(|record| &record.steps)
        .flat_map(|step| &step.attempts)
        .map(|attempt| attempt.id)
        .collect();
    for step in &current.steps {
        for attempt in &step.attempts {
            let previous_attempt = previous
                .into_iter()
                .flat_map(|record| &record.steps)
                .flat_map(|previous_step| &previous_step.attempts)
                .find(|candidate| candidate.id == attempt.id);
            if !previous_attempt_ids.contains(&attempt.id) {
                events.push(workflow_attempt_created_event(
                    current,
                    &step.key,
                    attempt,
                    observed_at,
                ));
            } else if previous_attempt.is_some_and(|before| before.state != attempt.state) {
                let before = previous_attempt.expect("checked above");
                events.push(workflow_attempt_state_changed_event(
                    current,
                    &step.key,
                    before,
                    attempt,
                    observed_at,
                ));
            }
        }
    }
    events
}

#[must_use]
pub fn workflow_cancel_requested_event(workflow_id: String, at: UnixMillis) -> NewLifecycleEvent {
    NewLifecycleEvent::new(
        at,
        LifecycleEventKind::WorkflowCancelRequested,
        LifecycleEntityType::Workflow,
        workflow_id,
        BTreeMap::new(),
    )
}

fn workflow_attempt_created_event(
    workflow: &WorkflowRecord,
    step_key: &str,
    attempt: &StepAttempt,
    observed_at: UnixMillis,
) -> NewLifecycleEvent {
    NewLifecycleEvent::new(
        attempt.started_at.unwrap_or(observed_at),
        LifecycleEventKind::WorkflowAttemptCreated,
        LifecycleEntityType::Workflow,
        workflow.id.to_string(),
        BTreeMap::from([
            ("step".into(), step_key.to_owned()),
            ("attempt_id".into(), attempt.id.to_string()),
            ("attempt_number".into(), attempt.number.to_string()),
            ("run".into(), attempt.run.to_string()),
            ("state".into(), run_state_name(attempt.state).into()),
        ]),
    )
}

fn workflow_attempt_state_changed_event(
    workflow: &WorkflowRecord,
    step_key: &str,
    before: &StepAttempt,
    after: &StepAttempt,
    observed_at: UnixMillis,
) -> NewLifecycleEvent {
    NewLifecycleEvent::new(
        after
            .finished_at
            .or(after.started_at)
            .unwrap_or(observed_at),
        LifecycleEventKind::WorkflowAttemptStateChanged,
        LifecycleEntityType::Workflow,
        workflow.id.to_string(),
        BTreeMap::from([
            ("step".into(), step_key.to_owned()),
            ("attempt_id".into(), after.id.to_string()),
            ("attempt_number".into(), after.number.to_string()),
            ("run".into(), after.run.to_string()),
            ("from".into(), run_state_name(before.state).into()),
            ("to".into(), run_state_name(after.state).into()),
        ]),
    )
}

const fn run_state_name(state: RunState) -> &'static str {
    match state {
        RunState::Created => "created",
        RunState::Validated => "validated",
        RunState::Queued => "queued",
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Cancelled => "cancelled",
    }
}

const fn workflow_state_name(state: WorkflowState) -> &'static str {
    match state {
        WorkflowState::Created => "created",
        WorkflowState::Running => "running",
        WorkflowState::Succeeded => "succeeded",
        WorkflowState::Failed => "failed",
        WorkflowState::Cancelled => "cancelled",
    }
}

#[must_use]
pub(crate) fn wall_clock_ms() -> UnixMillis {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .unwrap_or(0),
    )
    .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capability::CapabilityName;
    use crate::id::ComponentId;
    use crate::limits::Limits;
    use crate::run::RunSpec;
    use crate::Version;

    #[test]
    fn run_initial_snapshot_expands_embedded_transitions_once() {
        let mut record = RunRecord::create(
            RunSpec {
                component: ComponentId::generate(),
                capability: CapabilityName::parse("demo.echo").unwrap(),
                parameters: BTreeMap::new(),
                inputs: Vec::new(),
                timeout_ms: 100,
            },
            "demo".into(),
            Version::parse("1.0.0").unwrap(),
            Version::parse("1.0.0").unwrap(),
            10,
            &Limits::default(),
        )
        .unwrap();
        record.transition(RunState::Validated, 11).unwrap();
        record.transition(RunState::Queued, 12).unwrap();

        let first = derive_run_events(None, &record);
        assert_eq!(first.len(), 3);
        assert_eq!(first[0].kind, LifecycleEventKind::RunCreated);
        assert_eq!(first[2].attributes["to"], "queued");
        assert!(derive_run_events(Some(&record), &record).is_empty());
    }

    #[test]
    fn event_pages_are_strictly_cursor_based() {
        let store = InMemoryLifecycleEvents::default();
        for at in [10, 20, 30] {
            store
                .record(NewLifecycleEvent::new(
                    at,
                    LifecycleEventKind::RunCreated,
                    LifecycleEntityType::Run,
                    format!("run-{at}"),
                    BTreeMap::new(),
                ))
                .unwrap();
        }
        let first = store.list_after(0, 2).unwrap();
        assert_eq!(
            first.iter().map(|event| event.sequence).collect::<Vec<_>>(),
            vec![1, 2]
        );
        let next = store.list_after(2, 2).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].sequence, 3);
        assert_eq!(store.high_water_sequence().unwrap(), 3);
        assert!(store.list_after(0, 0).is_err());
    }
}
