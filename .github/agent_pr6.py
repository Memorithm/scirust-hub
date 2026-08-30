from pathlib import Path
import re


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    p = Path(path)
    text = p.read_text()
    new, count = re.subn(pattern, replacement, text, count=1, flags=re.S)
    if count != 1:
        raise SystemExit(f"{path}: regex replacement failed: {pattern[:120]!r}")
    p.write_text(new)


# ---------------------------------------------------------------------------
# Core lifecycle-event domain.
# ---------------------------------------------------------------------------
Path("crates/hub-core/src/event.rs").write_text(r'''//! Durable lifecycle-event vocabulary.
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
    fn list_after(
        &self,
        after_sequence: u64,
        limit: u32,
    ) -> Result<Vec<LifecycleEvent>, CoreError>;
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
                ("state".into(), workflow_state_name(WorkflowState::Created).into()),
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

    if previous.and_then(|record| record.cancel_requested_at).is_none() {
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
pub fn workflow_cancel_requested_event(
    workflow_id: String,
    at: UnixMillis,
) -> NewLifecycleEvent {
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
        after.finished_at.or(after.started_at).unwrap_or(observed_at),
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
        assert_eq!(first.iter().map(|event| event.sequence).collect::<Vec<_>>(), vec![1, 2]);
        let next = store.list_after(2, 2).unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(next[0].sequence, 3);
        assert!(store.list_after(0, 0).is_err());
    }
}
''')

replace_once(
    "crates/hub-core/src/lib.rs",
    "pub mod exec;\npub mod id;\n",
    "pub mod event;\npub mod exec;\npub mod id;\n",
)
replace_once(
    "crates/hub-core/src/lib.rs",
    "pub use error::{CoreError, ExecutorFailure};\npub use exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};\n",
    "pub use error::{CoreError, ExecutorFailure};\npub use event::{\n    LifecycleEntityType, LifecycleEvent, LifecycleEventKind, LifecycleEventRepository,\n    NewLifecycleEvent, DEFAULT_EVENT_PAGE, MAX_EVENT_PAGE,\n};\npub use exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};\n",
)
replace_once(
    "crates/hub-core/src/lib.rs",
    "    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,\n    InMemoryWorkflows,\n",
    "    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryHubStore,\n    InMemoryRuns, InMemoryWorkflows,\n",
)

replace_once(
    "crates/hub-core/src/store.rs",
    "/// Content-addressed blob storage. Keys are digests; equal bytes are stored\n",
    r'''/// Append-only lifecycle event read port. Events are generated by successful
/// authoritative repository mutations; callers can read but not forge them.
pub use crate::event::LifecycleEventRepository;

/// Content-addressed blob storage. Keys are digests; equal bytes are stored
''',
)

# Composite ephemeral store: one object implements all metadata ports + events.
replace_once(
    "crates/hub-core/src/memory.rs",
    "use crate::error::CoreError;\n",
    r'''use crate::error::CoreError;
use crate::event::{
    artifact_recorded_event, component_registered_event, derive_run_events,
    derive_workflow_events, wall_clock_ms, workflow_cancel_requested_event,
    InMemoryLifecycleEvents, LifecycleEvent, LifecycleEventRepository,
};
''',
)
replace_once(
    "crates/hub-core/src/memory.rs",
    "fn poison<T>(_e: T) -> CoreError {\n",
    r'''/// Composite in-memory backend used by the daemon's ephemeral mode. It keeps
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
        if previous.and_then(|record| record.cancel_requested_at).is_none()
            && updated.as_ref().and_then(|record| record.cancel_requested_at).is_some()
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
    fn list_after(&self, after_sequence: u64, limit: u32) -> Result<Vec<LifecycleEvent>, CoreError> {
        self.events.list_after(after_sequence, limit)
    }
}

fn poison<T>(_e: T) -> CoreError {
''',
)

# ---------------------------------------------------------------------------
# SQLite: migration v3 + same-transaction event derivation.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    "use hub_core::error::CoreError;\n",
    r'''use hub_core::error::CoreError;
use hub_core::event::{
    artifact_recorded_event, component_registered_event, derive_run_events,
    derive_workflow_events, workflow_cancel_requested_event, LifecycleEntityType, LifecycleEvent,
    LifecycleEventKind, LifecycleEventRepository, NewLifecycleEvent,
};
''',
)
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''    // v2: workflow orchestration records.
    "CREATE TABLE workflows (
        id          TEXT PRIMARY KEY,
        created_at  INTEGER NOT NULL,
        state       TEXT    NOT NULL,
        record_json TEXT    NOT NULL
    );
    CREATE INDEX idx_workflows_created ON workflows (created_at);",
];
''',
    '''    // v2: workflow orchestration records.
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
''',
)

replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''fn storage(context: &'static str) -> impl Fn(rusqlite::Error) -> CoreError {
    move |e| CoreError::Storage(format!("{context}: {e}"))
}

''',
    r'''fn storage(context: &'static str) -> impl Fn(rusqlite::Error) -> CoreError {
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

fn decode_event_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<(i64, i64, String, String, String, String)> {
    Ok((
        row.get(0)?,
        row.get(1)?,
        row.get(2)?,
        row.get(3)?,
        row.get(4)?,
        row.get(5)?,
    ))
}

''',
)

# Component registration: append event before transaction commit.
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''        .map_err(storage("inserting component"))?;
        tx.commit().map_err(storage("committing registration"))?;
        Ok(true)
''',
    '''        .map_err(storage("inserting component"))?;
        append_event_tx(
            &tx,
            &component_registered_event(manifest, storage_now_ms()),
        )?;
        tx.commit().map_err(storage("committing registration"))?;
        Ok(true)
''',
)

run_impl = r'''impl RunRepository for SqliteStore {
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
}'''
regex_once(
    "crates/hub-store-sqlite/src/lib.rs",
    r"impl RunRepository for SqliteStore \{.*?\n\}\n\nimpl ArtifactMetadataRepository for SqliteStore",
    run_impl + "\n\nimpl ArtifactMetadataRepository for SqliteStore",
)

artifact_impl = r'''impl ArtifactMetadataRepository for SqliteStore {
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
}'''
regex_once(
    "crates/hub-store-sqlite/src/lib.rs",
    r"impl ArtifactMetadataRepository for SqliteStore \{.*?\n\}\n\nimpl WorkflowRepository for SqliteStore",
    artifact_impl + "\n\nimpl WorkflowRepository for SqliteStore",
)

workflow_impl = r'''impl WorkflowRepository for SqliteStore {
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
            append_event_tx(
                &tx,
                &workflow_cancel_requested_event(id.to_string(), at),
            )?;
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
            let kind = LifecycleEventKind::parse(&kind)
                .ok_or_else(|| CoreError::Storage(format!("unknown lifecycle event kind {kind:?}")))?;
            let entity_type = LifecycleEntityType::parse(&entity_type).ok_or_else(|| {
                CoreError::Storage(format!("unknown lifecycle entity type {entity_type:?}"))
            })?;
            let attributes = serde_json::from_str(&attributes_json).map_err(|e| {
                CoreError::Storage(format!("stored lifecycle attributes failed to deserialize: {e}"))
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
}'''
regex_once(
    "crates/hub-store-sqlite/src/lib.rs",
    r"impl WorkflowRepository for SqliteStore \{.*?\n\}\n\n#\[cfg\(test\)\]",
    workflow_impl + "\n\n#[cfg(test)]",
)

# SQLite regression: events survive reopen and identical replays do not duplicate.
replace_once(
    "crates/hub-store-sqlite/src/lib.rs",
    '''    #[test]
    fn data_survives_reopen_from_disk() {
''',
    r'''    #[test]
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
        assert!(events.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
        assert_eq!(events.last().unwrap().attributes["to"], "queued");
        assert!(events.iter().any(|event| event.entity_id == run_id.to_string()));
        assert!(LifecycleEventRepository::list_after(&store, events.last().unwrap().sequence, 10)
            .unwrap()
            .is_empty());
        drop(store);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn data_survives_reopen_from_disk() {
''',
)

# ---------------------------------------------------------------------------
# Wire DTOs.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-protocol/src/lib.rs",
    "// ----------------------------------------------------------------------\n// Conversions domain <-> wire\n",
    r'''// ----------------------------------------------------------------------
// Lifecycle events
// ----------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEventDto {
    pub sequence: u64,
    pub recorded_at: u64,
    pub kind: hub_core::LifecycleEventKind,
    pub entity_type: hub_core::LifecycleEntityType,
    pub entity_id: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}

impl From<&hub_core::LifecycleEvent> for LifecycleEventDto {
    fn from(event: &hub_core::LifecycleEvent) -> Self {
        Self {
            sequence: event.sequence,
            recorded_at: event.recorded_at,
            kind: event.kind,
            entity_type: event.entity_type,
            entity_id: event.entity_id.clone(),
            attributes: event.attributes.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleEventListResponse {
    pub events: Vec<LifecycleEventDto>,
    /// Cursor to pass as `after` on the next request. It remains unchanged
    /// when this page is empty.
    pub next_after: u64,
}

// ----------------------------------------------------------------------
// Conversions domain <-> wire
''',
)

# ---------------------------------------------------------------------------
# HTTP API: inject event read port and cursor endpoint.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-api/src/lib.rs",
    "use hub_core::{ArtifactId, ComponentId, ComponentManifest, Orchestrator, RunSpec};\n",
    "use hub_core::{\n    ArtifactId, ComponentId, ComponentManifest, InMemoryLifecycleEvents,\n    LifecycleEventRepository, Orchestrator, RunSpec,\n};\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''pub struct HubState {
    pub orchestrator: Arc<Orchestrator>,
    api_bearer_sha256: Option<[u8; 32]>,
}
''',
    '''pub struct HubState {
    pub orchestrator: Arc<Orchestrator>,
    events: Arc<dyn LifecycleEventRepository>,
    api_bearer_sha256: Option<[u8; 32]>,
}
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''        Self {
            orchestrator,
            api_bearer_sha256: None,
        }
    }

    /// Enables bearer authentication''',
    '''        Self {
            orchestrator,
            events: Arc::new(InMemoryLifecycleEvents::default()),
            api_bearer_sha256: None,
        }
    }

    #[must_use]
    pub fn with_event_repository(mut self, events: Arc<dyn LifecycleEventRepository>) -> Self {
        self.events = events;
        self
    }

    /// Enables bearer authentication''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''        .route("/api/v1/artifacts/{id}", get(get_artifact))
        .route_layer(middleware::from_fn_with_state(
''',
    '''        .route("/api/v1/artifacts/{id}", get(get_artifact))
        .route("/api/v1/events", get(list_lifecycle_events))
        .route_layer(middleware::from_fn_with_state(
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "// ----------------------------------------------------------------------\n// Plumbing\n",
    r'''async fn list_lifecycle_events(
    State(state): State<HubState>,
    Query(query): Query<BTreeMap<String, String>>,
) -> Response {
    let after = match query.get("after") {
        Some(raw) => match raw.parse::<u64>() {
            Ok(value) => value,
            Err(_) => return bad_request("event query parameter 'after' must be an unsigned integer"),
        },
        None => 0,
    };
    let limit = match query.get("limit") {
        Some(raw) => match raw.parse::<u32>() {
            Ok(value) => value,
            Err(_) => return bad_request("event query parameter 'limit' must be an unsigned integer"),
        },
        None => hub_core::DEFAULT_EVENT_PAGE,
    };
    if let Err(error) = hub_core::event::validate_event_page_limit(limit) {
        return core_error(error);
    }
    let events = state.events.clone();
    match joined(tokio::task::spawn_blocking(move || events.list_after(after, limit)).await) {
        Ok(events) => {
            let next_after = events.last().map_or(after, |event| event.sequence);
            Json(proto::LifecycleEventListResponse {
                events: events.iter().map(proto::LifecycleEventDto::from).collect(),
                next_after,
            })
            .into_response()
        }
        Err(response) => response,
    }
}

// ----------------------------------------------------------------------
// Plumbing
''',
)

# API regression before auth regression.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    #[tokio::test]
    async fn bearer_auth_protects_api_but_not_health_endpoints() {
''',
    r'''    #[tokio::test]
    async fn lifecycle_events_endpoint_is_cursor_paginated() {
        let (state, _clock, _dir) = test_state();
        let events = Arc::new(InMemoryLifecycleEvents::default());
        for sequence_hint in [10u64, 20, 30] {
            events
                .record(hub_core::NewLifecycleEvent::new(
                    sequence_hint,
                    hub_core::LifecycleEventKind::RunCreated,
                    hub_core::LifecycleEntityType::Run,
                    format!("run-{sequence_hint}"),
                    BTreeMap::new(),
                ))
                .unwrap();
        }
        let app = router(state.with_event_repository(events));
        let (status, first) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/events?after=0&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{first}");
        assert_eq!(first["events"].as_array().unwrap().len(), 2);
        assert_eq!(first["next_after"], 2);

        let (status, second) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/events?after=2&limit=2")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{second}");
        assert_eq!(second["events"].as_array().unwrap().len(), 1);
        assert_eq!(second["next_after"], 3);

        let (status, error) = send(
            app,
            Request::builder()
                .uri("/api/v1/events?limit=0")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{error}");
    }

    #[tokio::test]
    async fn bearer_auth_protects_api_but_not_health_endpoints() {
''',
)

# ---------------------------------------------------------------------------
# Daemon wiring: SQLite and memory stores both expose the event read port.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''use hub_core::memory::{
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
    InMemoryWorkflows,
};
''',
    '''use hub_core::memory::{FileSystemArtifactStore, InMemoryHubStore};
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    ArtifactMetadataRepository, ComponentRepository, RunRepository, WorkflowRepository,
};
''',
    '''    ArtifactMetadataRepository, ComponentRepository, LifecycleEventRepository, RunRepository,
    WorkflowRepository,
};
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    let orchestrator = match args.store {
        StoreBackend::Sqlite => {
            let db = args.data_dir.join("hub.db");
            let store = Arc::new(SqliteStore::open(&db)?);
            tracing::info!(db = %db.display(), "using durable sqlite stores");
            build_orchestrator(
                store.clone(),
                store.clone(),
                store.clone(),
                store,
                blob_store,
                executor.clone(),
                workdir_root,
            )
        }
        StoreBackend::Memory => {
            tracing::info!("using in-memory stores (state resets on restart)");
            build_orchestrator(
                Arc::new(InMemoryComponents::default()),
                Arc::new(InMemoryRuns::default()),
                Arc::new(InMemoryArtifactMeta::default()),
                Arc::new(InMemoryWorkflows::default()),
                blob_store,
                executor.clone(),
                workdir_root,
            )
        }
    };
''',
    r'''    let (orchestrator, event_store): (Arc<Orchestrator>, Arc<dyn LifecycleEventRepository>) =
        match args.store {
            StoreBackend::Sqlite => {
                let db = args.data_dir.join("hub.db");
                let store = Arc::new(SqliteStore::open(&db)?);
                tracing::info!(db = %db.display(), "using durable sqlite stores");
                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );
                (orchestrator, store)
            }
            StoreBackend::Memory => {
                tracing::info!("using in-memory stores (state resets on restart)");
                let store = Arc::new(InMemoryHubStore::default());
                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );
                (orchestrator, store)
            }
        };
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''        let mut state = HubState::new(orchestrator);
''',
    '''        let mut state = HubState::new(orchestrator).with_event_repository(event_store);
''',
)

# ---------------------------------------------------------------------------
# CLI event cursor reader.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    #[command(subcommand)]
    Workflow(WorkflowCommand),
}
''',
    r'''    #[command(subcommand)]
    Workflow(WorkflowCommand),
    #[command(subcommand)]
    Event(EventCommand),
}

#[derive(Debug, Subcommand)]
enum EventCommand {
    /// Read the append-only lifecycle chronology using a sequence cursor.
    List {
        /// Return events with sequence strictly greater than this cursor.
        #[arg(long, default_value_t = 0)]
        after: u64,
        /// Page size (1..=1000).
        #[arg(long, default_value_t = 100)]
        limit: u32,
    },
}
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''        Command::Artifact(ArtifactCommand::Put {
''',
    r'''        Command::Event(EventCommand::List { after, limit }) => {
            let response = get(url_of(
                args,
                &format!("/api/v1/events?after={after}&limit={limit}"),
            ))?;
            emit(args, &response, |v| {
                let events = v["events"].as_array().cloned().unwrap_or_default();
                if events.is_empty() {
                    println!("no lifecycle events after {after}");
                }
                for event in events {
                    println!(
                        "#{}  {}  {}  {}:{}",
                        event["sequence"],
                        event["recorded_at"],
                        event["kind"].as_str().unwrap_or("?"),
                        event["entity_type"].as_str().unwrap_or("?"),
                        event["entity_id"].as_str().unwrap_or("?"),
                    );
                }
                println!("next_after: {}", v["next_after"]);
            })
        }
        Command::Artifact(ArtifactCommand::Put {
''',
)

# ---------------------------------------------------------------------------
# Read-only MCP introspection gets the same event cursor.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''            "hub.list_artifacts" => self.hub.get_json("/api/v1/artifacts"),
''',
    r'''            "hub.list_events" => {
                let after = arguments.get("after").and_then(Value::as_u64).unwrap_or(0);
                let limit = arguments.get("limit").and_then(Value::as_u64).unwrap_or(100);
                self.hub
                    .get_json(&format!("/api/v1/events?after={after}&limit={limit}"))
            }
            "hub.list_artifacts" => self.hub.get_json("/api/v1/artifacts"),
''',
)
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''            json!({
                "name": "hub.list_artifacts",
''',
    r'''            json!({
                "name": "hub.list_events",
                "description": "Read the Hub append-only lifecycle event stream using a sequence cursor",
                "inputSchema": schema(json!({
                    "after": { "type": "integer", "minimum": 0 },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 1000 }
                }), vec![]),
            }),
            json!({
                "name": "hub.list_artifacts",
''',
)

# ---------------------------------------------------------------------------
# Documentation/changelog.
# ---------------------------------------------------------------------------
Path("docs/adr/0012-lifecycle-event-log.md").write_text(r'''# ADR 0012 — Durable lifecycle event log

Status: accepted

## Decision

SciRust Hub records an append-only, monotonically sequenced lifecycle event
stream for successful authoritative metadata mutations. Events cover component
registration, artifact recording, run creation/state transitions, workflow
creation/state transitions/cancellation, and workflow attempt creation/state
changes.

The event stream is **not** a second state machine. Component manifests, run
records, workflow records and artifact metadata remain authoritative. Events
are derived from those successful writes and exist for operations,
observability, incremental consumers and later metrics.

For SQLite, event rows are inserted in the same transaction as the metadata
mutation that caused them. A committed mutation therefore cannot be separated
from its corresponding lifecycle append by a process crash. Identical component
registration replays and repeated unchanged snapshots produce no duplicate
lifecycle entries.

The in-memory daemon mode uses a composite store exposing the same repository
ports and event chronology; it remains intentionally ephemeral.

## Read contract

`GET /api/v1/events?after=<sequence>&limit=<n>` returns events whose sequence is
strictly greater than the supplied cursor, ordered oldest first. Page size is
bounded to 1..=1000 and the response returns `next_after`. The CLI exposes
`scirust-hub event list`, and the read-only MCP adapter exposes
`hub.list_events`.

Sequences are local to one Hub database. They establish append order, not a
global distributed clock. `recorded_at` is the best domain timestamp available
for the mutation and must not be used to reorder equal/concurrent events.

## Security and privacy

Lifecycle attributes are deliberately small and structured. They contain
identifiers, states, names, versions, digests and sizes; they do not record
environment values, bearer tokens or subprocess stdout/stderr payloads. Access
to `/api/v1/events` follows the same control-plane authentication policy as the
rest of `/api/v1`.
''')

replace_once(
    "CHANGELOG.md",
    "### Added\n\n- Read-only MCP adapter",
    r'''### Added

- Durable append-only lifecycle event log (ADR-0012): SQLite migration v3
  records component, artifact, run, workflow and workflow-attempt lifecycle
  changes in the same transaction as authoritative metadata writes. Cursor
  reads are exposed as `GET /api/v1/events?after=&limit=`, `scirust-hub event
  list`, and read-only MCP tool `hub.list_events`; ephemeral memory mode uses
  an equivalent composite store.

- Read-only MCP adapter''',
)

print("durable lifecycle event log transformations complete")
