from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:80]!r}")
    p.write_text(text.replace(old, new, 1))


def replace_between(path: str, start_marker: str, end_marker: str, replacement: str) -> None:
    p = Path(path)
    text = p.read_text()
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    p.write_text(text[:start] + replacement + text[end:])


# ---------------------------------------------------------------------------
# Typed attempt identity.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/id.rs",
    '''uuid_id! {
    /// Identity of one submitted run.
    RunId
}
uuid_id! {
    /// Handle to artifact metadata; contents live in the artifact store.
    ArtifactId
}''',
    '''uuid_id! {
    /// Identity of one submitted run.
    RunId
}
uuid_id! {
    /// Identity of one workflow step attempt. A retry gets a fresh id.
    AttemptId
}
uuid_id! {
    /// Handle to artifact metadata; contents live in the artifact store.
    ArtifactId
}''',
)

replace_once(
    "crates/hub-core/src/lib.rs",
    "pub use id::{ArtifactId, ComponentId, RunId, WorkflowId};",
    "pub use id::{ArtifactId, AttemptId, ComponentId, RunId, WorkflowId};",
)
replace_once(
    "crates/hub-core/src/lib.rs",
    '''pub use workflow::{
    InputSource, Step, StepResult, WorkflowRecord, WorkflowSpec, WorkflowState,
    WORKFLOW_SCHEMA_VERSION,
};''',
    '''pub use workflow::{
    AttemptFailureCategory, InputSource, RetryPolicy, Step, StepAttempt, StepResult,
    WorkflowRecord, WorkflowSpec, WorkflowState, WORKFLOW_SCHEMA_VERSION,
};''',
)

# ---------------------------------------------------------------------------
# Workflow model: explicit retry policy, attempt history, cancellation intent.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''//! Scope honesty (see ADR-0006): this is **sequential, single-node**
//! orchestration. Steps run one at a time in topological order; a failed or
//! cancelled step fails the workflow immediately (fail-fast). No parallel
//! scheduling, retries or distribution yet.''',
    '''//! Scope honesty (see ADR-0006): scheduling is still **sequential,
//! single-node** in this module revision. Retries and workflow cancellation
//! are explicit and persisted; parallel and distributed scheduling remain
//! separate later layers.''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    'pub const WORKFLOW_MODEL_VERSION: &str = "1.0.0";',
    'pub const WORKFLOW_MODEL_VERSION: &str = "1.1.0";',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''/// One unit of work inside a workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {''',
    '''/// Stable categories used by retry policy and attempt provenance.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptFailureCategory {
    TimedOut,
    StartFailure,
    NonZeroExit,
    Signaled,
    MissingRequiredOutput,
    /// Explicit cancellation is terminal and is never retryable.
    Cancelled,
    /// Hub/executor/storage errors are not safely replayable automatically.
    ExecutionError,
}

impl AttemptFailureCategory {
    #[must_use]
    pub const fn may_retry(self) -> bool {
        !matches!(self, Self::Cancelled | Self::ExecutionError)
    }
}

/// Explicit per-step retry policy. No policy means exactly one attempt,
/// preserving the pre-retry semantics.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Total attempts including the first execution.
    pub max_attempts: u32,
    /// Fixed delay between attempts. `None` and `Some(0)` both mean no delay.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backoff_ms: Option<u64>,
    /// Only these observed failure categories may be retried.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub retry_on: BTreeSet<AttemptFailureCategory>,
}

impl RetryPolicy {
    pub const MAX_ATTEMPTS: u32 = 32;
    pub const MAX_BACKOFF_MS: u64 = 3_600_000;

    fn validate(&self, key: &str) -> Result<(), CoreError> {
        if self.max_attempts == 0 || self.max_attempts > Self::MAX_ATTEMPTS {
            return Err(CoreError::InvalidRunSpec(format!(
                "step {key:?} retry max_attempts must be 1..={}",
                Self::MAX_ATTEMPTS
            )));
        }
        if self.backoff_ms.unwrap_or(0) > Self::MAX_BACKOFF_MS {
            return Err(CoreError::InvalidRunSpec(format!(
                "step {key:?} retry backoff_ms exceeds {}",
                Self::MAX_BACKOFF_MS
            )));
        }
        if self.max_attempts > 1 && self.retry_on.is_empty() {
            return Err(CoreError::InvalidRunSpec(format!(
                "step {key:?} retry_on must be non-empty when max_attempts > 1"
            )));
        }
        if let Some(category) = self.retry_on.iter().find(|category| !category.may_retry()) {
            return Err(CoreError::InvalidRunSpec(format!(
                "step {key:?} failure category {category:?} is not retryable"
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn allows(&self, category: AttemptFailureCategory) -> bool {
        category.may_retry() && self.retry_on.contains(&category)
    }
}

/// One unit of work inside a workflow.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Step {''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
}''',
    '''    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub after: Vec<String>,
    /// Optional explicit retry policy. Omitted = one attempt, no retry.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<RetryPolicy>,
}''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''            if step.timeout_ms == 0 {
                return Err(CoreError::InvalidRunSpec(format!(
                    "step {:?} timeout_ms must be at least 1",
                    step.key
                )));
            }
            for dep in &step.after {''',
    '''            if step.timeout_ms == 0 {
                return Err(CoreError::InvalidRunSpec(format!(
                    "step {:?} timeout_ms must be at least 1",
                    step.key
                )));
            }
            if let Some(retry) = &step.retry {
                retry.validate(&step.key)?;
            }
            for dep in &step.after {''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''/// Outcome of one executed step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub key: String,
    pub run: RunId,
    pub state: crate::run::RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}''',
    '''/// Persisted provenance for one concrete attempt of a workflow step.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepAttempt {
    pub id: crate::id::AttemptId,
    /// One-based attempt number within the step.
    pub number: u32,
    pub run: RunId,
    pub state: crate::run::RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<AttemptFailureCategory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

/// Outcome of one workflow step. `run/state/failure` mirror the latest
/// attempt for backwards-compatible readers; `attempts` is authoritative.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepResult {
    pub key: String,
    pub run: RunId,
    pub state: crate::run::RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<StepAttempt>,
}''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepResult>,''',
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<crate::clock::UnixMillis>,
    /// Monotonic persisted cancellation intent. Once set it must never be
    /// cleared by a concurrent scheduler write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_requested_at: Option<crate::clock::UnixMillis>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepResult>,''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''            started_at: None,
            finished_at: None,
            steps: Vec::new(),''',
    '''            started_at: None,
            finished_at: None,
            cancel_requested_at: None,
            steps: Vec::new(),''',
)

# ---------------------------------------------------------------------------
# Repository contract: cancellation intent is an atomic, monotonic mutation.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/store.rs",
    '''pub trait WorkflowRepository: Send + Sync {
    /// # Errors
    /// Backend failures only.
    fn put(&self, record: &crate::workflow::WorkflowRecord) -> Result<(), CoreError>;

    /// # Errors''',
    '''pub trait WorkflowRepository: Send + Sync {
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

    /// # Errors''',
)

replace_once(
    "crates/hub-core/src/memory.rs",
    '''impl WorkflowRepository for InMemoryWorkflows {
    fn put(&self, record: &crate::workflow::WorkflowRecord) -> Result<(), CoreError> {
        let mut inner = self.0.lock().map_err(poison)?;
        inner.records.insert(record.id, record.clone());
        Ok(())
    }

    fn get(''',
    '''impl WorkflowRepository for InMemoryWorkflows {
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

    fn get(''',
)

# SQLite keeps the cancellation-intent mutation in the same connection lock
# as normal upserts, so a stale scheduler write cannot clear it.
replace_between(
    "crates/hub-store-sqlite/src/lib.rs",
    "impl WorkflowRepository for SqliteStore {",
    "#[cfg(test)]\nmod tests {",
    r'''impl WorkflowRepository for SqliteStore {
    fn put(&self, record: &hub_core::workflow::WorkflowRecord) -> Result<(), CoreError> {
        let conn = self.lock()?;
        let existing_json: Option<String> = conn
            .query_row(
                "SELECT record_json FROM workflows WHERE id = ?1",
                rusqlite::params![record.id.to_string()],
                |row| row.get(0),
            )
            .optional()
            .map_err(storage("loading workflow before upsert"))?;
        let mut stored = record.clone();
        if let Some(existing_json) = existing_json {
            let existing: hub_core::workflow::WorkflowRecord = serde_json::from_str(&existing_json)
                .map_err(|e| CoreError::Storage(format!("stored workflow failed to deserialize: {e}")))?;
            if stored.cancel_requested_at.is_none() {
                stored.cancel_requested_at = existing.cancel_requested_at;
            }
        }
        let json = serde_json::to_string(&stored)
            .map_err(|e| CoreError::Storage(format!("serializing workflow: {e}")))?;
        conn.execute(
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
        Ok(())
    }

    fn request_cancel(
        &self,
        id: &hub_core::WorkflowId,
        at: hub_core::clock::UnixMillis,
    ) -> Result<Option<hub_core::workflow::WorkflowRecord>, CoreError> {
        let conn = self.lock()?;
        let json: Option<String> = conn
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
        let mut record: hub_core::workflow::WorkflowRecord = serde_json::from_str(&json)
            .map_err(|e| CoreError::Storage(format!("stored workflow failed to deserialize: {e}")))?;
        if !record.state.is_terminal() && record.cancel_requested_at.is_none() {
            record.cancel_requested_at = Some(at);
            let updated = serde_json::to_string(&record)
                .map_err(|e| CoreError::Storage(format!("serializing workflow: {e}")))?;
            conn.execute(
                "UPDATE workflows SET record_json = ?2 WHERE id = ?1",
                rusqlite::params![id.to_string(), updated],
            )
            .map_err(storage("persisting workflow cancellation"))?;
        }
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

''',
)

# ---------------------------------------------------------------------------
# Orchestrator: active workflow cancellation + retry loop.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "use crate::id::{ArtifactId, ComponentId, RunId};",
    "use crate::id::{ArtifactId, AttemptId, ComponentId, RunId, WorkflowId};",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    workdir_root: PathBuf,
    active_cancels: Mutex<BTreeMap<RunId, CancelToken>>,
}''',
    '''    workdir_root: PathBuf,
    active_cancels: Mutex<BTreeMap<RunId, CancelToken>>,
    active_workflow_cancels: Mutex<BTreeMap<WorkflowId, CancelToken>>,
    active_workflow_runs: Mutex<BTreeMap<WorkflowId, RunId>>,
}''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            workdir_root: workdir_root.into(),
            active_cancels: Mutex::new(BTreeMap::new()),
        }''',
    '''            workdir_root: workdir_root.into(),
            active_cancels: Mutex::new(BTreeMap::new()),
            active_workflow_cancels: Mutex::new(BTreeMap::new()),
            active_workflow_runs: Mutex::new(BTreeMap::new()),
        }''',
)

workflow_engine = r'''    /// Executes a created workflow in deterministic dependency order. A step
    /// may perform multiple attempts only when it carries an explicit retry
    /// policy. Workflow cancellation intent is persisted before any active
    /// run is signalled.
    ///
    /// # Errors
    /// [`CoreError::WorkflowNotFound`] / [`CoreError::WorkflowNotExecutable`]
    /// / storage failures. Per-step execution failures become terminal
    /// workflow records rather than escaping as scheduler errors.
    #[instrument(skip_all, fields(workflow = %workflow_id))]
    pub fn execute_workflow(
        &self,
        workflow_id: WorkflowId,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        let token = CancelToken::new();
        {
            use std::collections::btree_map::Entry;
            let mut active = self
                .active_workflow_cancels
                .lock()
                .map_err(|_| CoreError::Storage("workflow cancellation map lock poisoned".into()))?;
            match active.entry(workflow_id) {
                Entry::Occupied(_) => {
                    let current = self
                        .workflows
                        .get(&workflow_id)?
                        .map_or(crate::workflow::WorkflowState::Running, |record| record.state);
                    return Err(CoreError::WorkflowNotExecutable {
                        workflow: workflow_id,
                        current,
                    });
                }
                Entry::Vacant(slot) => {
                    slot.insert(token.clone());
                }
            }
        }

        let result = self.execute_workflow_inner(workflow_id, &token);
        self.active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .remove(&workflow_id);
        self.active_workflow_cancels
            .lock()
            .map_err(|_| CoreError::Storage("workflow cancellation map lock poisoned".into()))?
            .remove(&workflow_id);
        result
    }

    fn execute_workflow_inner(
        &self,
        workflow_id: WorkflowId,
        token: &CancelToken,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        let mut record = self
            .workflows
            .get(&workflow_id)?
            .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
        if record.state != crate::workflow::WorkflowState::Created {
            return Err(CoreError::WorkflowNotExecutable {
                workflow: workflow_id,
                current: record.state,
            });
        }
        self.refresh_cancel_intent(&mut record)?;
        if token.is_cancelled() || record.cancel_requested_at.is_some() {
            return self.finish_workflow_cancelled(&mut record, "cancelled before execution".into());
        }

        let started_at = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Running, started_at)?;
        self.workflows.put(&record)?;

        let order = record.spec.topo_keys()?;
        for key in &order {
            self.refresh_cancel_intent(&mut record)?;
            if token.is_cancelled() || record.cancel_requested_at.is_some() {
                return self.finish_workflow_cancelled(&mut record, "workflow cancellation requested".into());
            }

            let Some(step) = record.spec.steps.iter().find(|s| &s.key == key).cloned() else {
                return self.finish_workflow_failed(
                    &mut record,
                    format!("step {key:?} disappeared from validated workflow"),
                );
            };

            let resolved = match self.resolve_workflow_inputs(&record, &step)? {
                Ok(resolved) => resolved,
                Err(message) => return self.finish_workflow_failed(&mut record, message),
            };
            let run_spec = crate::workflow::WorkflowSpec::step_run_spec(&step, &resolved);
            let max_attempts = step.retry.as_ref().map_or(1, |policy| policy.max_attempts);
            let mut attempt_number = 1u32;

            loop {
                self.refresh_cancel_intent(&mut record)?;
                if token.is_cancelled() || record.cancel_requested_at.is_some() {
                    return self.finish_workflow_cancelled(
                        &mut record,
                        "workflow cancellation requested".into(),
                    );
                }

                let submitted = match self.submit_run(run_spec.clone()) {
                    Ok(submitted) => submitted,
                    Err(error) => return self.finish_workflow_failed(&mut record, error.to_string()),
                };
                let attempt = crate::workflow::StepAttempt {
                    id: AttemptId::generate(),
                    number: attempt_number,
                    run: submitted.id,
                    state: submitted.state,
                    started_at: submitted.started_at,
                    finished_at: submitted.finished_at,
                    failure_category: None,
                    failure: None,
                };
                Self::record_attempt(&mut record, key, attempt);
                self.workflows.put(&record)?;

                self.active_workflow_runs
                    .lock()
                    .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
                    .insert(workflow_id, submitted.id);

                // Close the race between persisting the queued attempt and
                // publishing it as the active run.
                self.refresh_cancel_intent(&mut record)?;
                if token.is_cancelled() || record.cancel_requested_at.is_some() {
                    let _ = self.cancel_run(submitted.id);
                }

                let executed = self.execute_run(submitted.id);
                self.active_workflow_runs
                    .lock()
                    .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
                    .remove(&workflow_id);

                let finished = match executed {
                    Ok(finished) => finished,
                    Err(error) => {
                        let category = crate::workflow::AttemptFailureCategory::ExecutionError;
                        Self::finish_recorded_attempt(
                            &mut record,
                            key,
                            submitted.id,
                            self.runs.get(&submitted.id)?.as_ref(),
                            Some(category),
                            Some(error.to_string()),
                        );
                        self.workflows.put(&record)?;
                        return self.finish_workflow_failed(&mut record, error.to_string());
                    }
                };

                let category = classify_attempt_failure(&finished);
                let failure = finished
                    .outcome
                    .as_ref()
                    .and_then(|outcome| outcome.failure.clone());
                Self::finish_recorded_attempt(
                    &mut record,
                    key,
                    submitted.id,
                    Some(&finished),
                    category,
                    failure.clone(),
                );
                self.workflows.put(&record)?;

                self.refresh_cancel_intent(&mut record)?;
                if token.is_cancelled()
                    || record.cancel_requested_at.is_some()
                    || finished.state == RunState::Cancelled
                {
                    return self.finish_workflow_cancelled(
                        &mut record,
                        failure.unwrap_or_else(|| "workflow cancellation requested".into()),
                    );
                }
                if finished.state == RunState::Succeeded {
                    break;
                }

                let observed = category.unwrap_or(crate::workflow::AttemptFailureCategory::ExecutionError);
                let retry = step.retry.as_ref().is_some_and(|policy| {
                    attempt_number < max_attempts && policy.allows(observed)
                });
                if !retry {
                    let message = format!(
                        "step {key:?} attempt {attempt_number} ended in state {}{}",
                        finished.state,
                        failure
                            .as_ref()
                            .map(|value| format!(": {value}"))
                            .unwrap_or_default()
                    );
                    return self.finish_workflow_failed(&mut record, message);
                }

                let backoff_ms = step
                    .retry
                    .as_ref()
                    .and_then(|policy| policy.backoff_ms)
                    .unwrap_or(0);
                if !self.wait_retry_backoff(workflow_id, token, backoff_ms)? {
                    self.refresh_cancel_intent(&mut record)?;
                    return self.finish_workflow_cancelled(
                        &mut record,
                        "workflow cancellation requested during retry backoff".into(),
                    );
                }
                attempt_number += 1;
            }
        }

        let now = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Succeeded, now)?;
        self.workflows.put(&record)?;
        info!(workflow = %workflow_id, "workflow succeeded");
        Ok(record)
    }

    fn resolve_workflow_inputs(
        &self,
        record: &crate::workflow::WorkflowRecord,
        step: &crate::workflow::Step,
    ) -> Result<Result<BTreeMap<String, ArtifactId>, String>, CoreError> {
        let mut resolved = BTreeMap::new();
        for (input_name, source) in &step.inputs {
            match source {
                crate::workflow::InputSource::Artifact { artifact } => {
                    if self.artifacts_meta.get(artifact)?.is_some() {
                        resolved.insert(input_name.clone(), *artifact);
                    } else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: artifact {} not found",
                            step.key, artifact
                        )));
                    }
                }
                crate::workflow::InputSource::FromStep { key: dep, output } => {
                    let produced = record.steps.iter().find(|result| &result.key == dep);
                    let Some(dep_run) = produced.map(|result| result.run) else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: dependency {dep:?} has not run",
                            step.key
                        )));
                    };
                    let dep_record = self.runs.get(&dep_run)?;
                    let artifact = dep_record.as_ref().and_then(|run| {
                        run.outcome.as_ref().and_then(|outcome| {
                            outcome
                                .outputs
                                .iter()
                                .find(|candidate| &candidate.name == output)
                                .map(|candidate| candidate.artifact)
                        })
                    });
                    if let Some(artifact) = artifact {
                        resolved.insert(input_name.clone(), artifact);
                    } else {
                        return Ok(Err(format!(
                            "step {:?} input {input_name:?}: step {dep:?} produced no output named {output:?}",
                            step.key
                        )));
                    }
                }
            }
        }
        Ok(Ok(resolved))
    }

    fn record_attempt(
        record: &mut crate::workflow::WorkflowRecord,
        key: &str,
        attempt: crate::workflow::StepAttempt,
    ) {
        if let Some(result) = record.steps.iter_mut().find(|result| result.key == key) {
            result.run = attempt.run;
            result.state = attempt.state;
            result.failure = attempt.failure.clone();
            result.attempts.push(attempt);
        } else {
            record.steps.push(crate::workflow::StepResult {
                key: key.to_owned(),
                run: attempt.run,
                state: attempt.state,
                failure: attempt.failure.clone(),
                attempts: vec![attempt],
            });
        }
    }

    fn finish_recorded_attempt(
        record: &mut crate::workflow::WorkflowRecord,
        key: &str,
        run_id: RunId,
        run: Option<&RunRecord>,
        category: Option<crate::workflow::AttemptFailureCategory>,
        failure: Option<String>,
    ) {
        let Some(result) = record.steps.iter_mut().find(|result| result.key == key) else {
            return;
        };
        let state = run.map_or(RunState::Failed, |run| run.state);
        result.run = run_id;
        result.state = state;
        result.failure = failure.clone();
        if let Some(attempt) = result.attempts.iter_mut().find(|attempt| attempt.run == run_id) {
            attempt.state = state;
            attempt.started_at = run.and_then(|run| run.started_at);
            attempt.finished_at = run.and_then(|run| run.finished_at);
            attempt.failure_category = category;
            attempt.failure = failure;
        }
    }

    fn refresh_cancel_intent(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
    ) -> Result<(), CoreError> {
        if let Some(persisted) = self.workflows.get(&record.id)? {
            if record.cancel_requested_at.is_none() {
                record.cancel_requested_at = persisted.cancel_requested_at;
            }
        }
        Ok(())
    }

    fn wait_retry_backoff(
        &self,
        workflow_id: WorkflowId,
        token: &CancelToken,
        backoff_ms: u64,
    ) -> Result<bool, CoreError> {
        if backoff_ms == 0 {
            return Ok(!token.is_cancelled()
                && self
                    .workflows
                    .get(&workflow_id)?
                    .is_some_and(|record| record.cancel_requested_at.is_none()));
        }
        let started = std::time::Instant::now();
        let delay = std::time::Duration::from_millis(backoff_ms);
        loop {
            if token.is_cancelled()
                || self
                    .workflows
                    .get(&workflow_id)?
                    .is_some_and(|record| record.cancel_requested_at.is_some())
            {
                return Ok(false);
            }
            let elapsed = started.elapsed();
            if elapsed >= delay {
                return Ok(true);
            }
            std::thread::sleep((delay - elapsed).min(std::time::Duration::from_millis(25)));
        }
    }

    fn finish_workflow_failed(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
        message: String,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        self.refresh_cancel_intent(record)?;
        if record.cancel_requested_at.is_some() {
            return self.finish_workflow_cancelled(record, message);
        }
        let now = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Failed, now)?;
        record.failure = Some(message.clone());
        self.workflows.put(record)?;
        warn!(workflow = %record.id, %message, "workflow failed");
        Ok(record.clone())
    }

    fn finish_workflow_cancelled(
        &self,
        record: &mut crate::workflow::WorkflowRecord,
        message: String,
    ) -> Result<crate::workflow::WorkflowRecord, CoreError> {
        self.refresh_cancel_intent(record)?;
        record.cancel_requested_at.get_or_insert_with(|| self.clock.now_ms());
        if !record.state.is_terminal() {
            record.transition(crate::workflow::WorkflowState::Cancelled, self.clock.now_ms())?;
        }
        record.failure = Some(message.clone());
        self.workflows.put(record)?;
        info!(workflow = %record.id, %message, "workflow cancelled");
        Ok(record.clone())
    }

    /// Persists workflow cancellation intent before signalling the currently
    /// running attempt. Returns whether a live workflow execution was
    /// signalled in this process.
    pub fn cancel_workflow(&self, workflow_id: WorkflowId) -> Result<bool, CoreError> {
        let now = self.clock.now_ms();
        let mut record = self
            .workflows
            .request_cancel(&workflow_id, now)?
            .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
        if record.state.is_terminal() {
            return Ok(false);
        }

        let active_token = self
            .active_workflow_cancels
            .lock()
            .map_err(|_| CoreError::Storage("workflow cancellation map lock poisoned".into()))?
            .get(&workflow_id)
            .cloned();
        if let Some(token) = &active_token {
            token.cancel();
        }

        let active_run = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .get(&workflow_id)
            .copied();
        if let Some(run_id) = active_run {
            let _ = self.cancel_run(run_id)?;
        }

        if active_token.is_none() {
            if let Some(run_id) = latest_nonterminal_attempt_run(&record, self) ? {
                let _ = self.cancel_run(run_id)?;
            }
            record = self
                .workflows
                .get(&workflow_id)?
                .ok_or(CoreError::WorkflowNotFound(workflow_id))?;
            return self
                .finish_workflow_cancelled(&mut record, "workflow cancellation requested".into())
                .map(|_| false);
        }
        Ok(true)
    }

    /// Reconciles cancellation intent left behind by a daemon restart. Any
    /// recorded non-terminal attempt is terminalized before the workflow.
    /// Returns the number of workflows reconciled.
    pub fn recover_workflow_cancellations(&self) -> Result<usize, CoreError> {
        let mut recovered = 0usize;
        for mut record in self.workflows.list()? {
            if record.state.is_terminal() || record.cancel_requested_at.is_none() {
                continue;
            }
            if let Some(run_id) = latest_nonterminal_attempt_run(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }
            self.finish_workflow_cancelled(
                &mut record,
                "workflow cancellation recovered after restart".into(),
            )?;
            recovered += 1;
        }
        Ok(recovered)
    }

'''
replace_between(
    "crates/hub-core/src/orchestrator.rs",
    "    /// Executes a created workflow sequentially in dependency order",
    "    /// Reproduces a recorded run:",
    workflow_engine,
)

# Free helpers live immediately before the existing orchestrator test module.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "#[cfg(test)]\nmod tests {",
    r'''fn classify_attempt_failure(
    run: &RunRecord,
) -> Option<crate::workflow::AttemptFailureCategory> {
    if run.state == RunState::Succeeded {
        return None;
    }
    let Some(outcome) = &run.outcome else {
        return Some(crate::workflow::AttemptFailureCategory::ExecutionError);
    };
    if run.state == RunState::Cancelled || outcome.cancelled {
        return Some(crate::workflow::AttemptFailureCategory::Cancelled);
    }
    if outcome.timed_out {
        return Some(crate::workflow::AttemptFailureCategory::TimedOut);
    }
    if outcome
        .failure
        .as_deref()
        .is_some_and(|failure| failure.starts_with("failed to start:"))
    {
        return Some(crate::workflow::AttemptFailureCategory::StartFailure);
    }
    if outcome
        .failure
        .as_deref()
        .is_some_and(|failure| failure.starts_with("required output(s) not produced:"))
    {
        return Some(crate::workflow::AttemptFailureCategory::MissingRequiredOutput);
    }
    if outcome.exit_code.is_some_and(|code| code != 0) {
        return Some(crate::workflow::AttemptFailureCategory::NonZeroExit);
    }
    if outcome.signal.is_some() {
        return Some(crate::workflow::AttemptFailureCategory::Signaled);
    }
    Some(crate::workflow::AttemptFailureCategory::ExecutionError)
}

fn latest_nonterminal_attempt_run(
    record: &crate::workflow::WorkflowRecord,
    orchestrator: &Orchestrator,
) -> Result<Option<RunId>, CoreError> {
    for result in record.steps.iter().rev() {
        for attempt in result.attempts.iter().rev() {
            if orchestrator
                .runs
                .get(&attempt.run)?
                .is_some_and(|run| !run.state.is_terminal())
            {
                return Ok(Some(attempt.run));
            }
        }
    }
    Ok(None)
}

#[cfg(test)]
mod tests {''',
)

# ---------------------------------------------------------------------------
# Protocol/API/CLI expose the new state without moving business logic.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepResultDto>,''',
    '''    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_requested_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub steps: Vec<StepResultDto>,''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''pub struct StepResultDto {
    pub key: String,
    pub run: RunId,
    pub state: RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListResponse {''',
    '''pub struct StepResultDto {
    pub key: String,
    pub run: RunId,
    pub state: RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attempts: Vec<StepAttemptDto>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepAttemptDto {
    pub id: hub_core::AttemptId,
    pub number: u32,
    pub run: RunId,
    pub state: RunState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_category: Option<hub_core::AttemptFailureCategory>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelWorkflowResponse {
    pub workflow_id: hub_core::WorkflowId,
    pub signalled_active_execution: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowListResponse {''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''            finished_at: w.finished_at,
            steps: w
                .steps
                .iter()
                .map(|sr| StepResultDto {
                    key: sr.key.clone(),
                    run: sr.run,
                    state: sr.state,
                    failure: sr.failure.clone(),
                })
                .collect(),''',
    '''            finished_at: w.finished_at,
            cancel_requested_at: w.cancel_requested_at,
            steps: w
                .steps
                .iter()
                .map(|sr| StepResultDto {
                    key: sr.key.clone(),
                    run: sr.run,
                    state: sr.state,
                    failure: sr.failure.clone(),
                    attempts: sr
                        .attempts
                        .iter()
                        .map(|attempt| StepAttemptDto {
                            id: attempt.id,
                            number: attempt.number,
                            run: attempt.run,
                            state: attempt.state,
                            started_at: attempt.started_at,
                            finished_at: attempt.finished_at,
                            failure_category: attempt.failure_category,
                            failure: attempt.failure.clone(),
                        })
                        .collect(),
                })
                .collect(),''',
)

replace_once(
    "crates/hub-api/src/lib.rs",
    '''        .route("/api/v1/workflows/{id}", get(get_workflow))
        .route("/api/v1/workflows/{id}/executions", post(execute_workflow))''',
    '''        .route("/api/v1/workflows/{id}", get(get_workflow))
        .route("/api/v1/workflows/{id}/cancel", post(cancel_workflow))
        .route("/api/v1/workflows/{id}/executions", post(execute_workflow))''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''/// Executes a created workflow sequentially; returns the final record with
/// per-step provenance.
async fn execute_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {''',
    '''async fn cancel_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::WorkflowId>(&id) else {
        return not_found("workflow", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.cancel_workflow(parsed)).await) {
        Ok(signalled) => Json(proto::CancelWorkflowResponse {
            workflow_id: parsed,
            signalled_active_execution: signalled,
        })
        .into_response(),
        Err(response) => response,
    }
}

/// Executes a created workflow and waits for its terminal record.
async fn execute_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {''',
)

replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    /// Execute a created workflow sequentially and wait.
    Run {
        id: String,
    },
    List,''',
    '''    /// Execute a created workflow and wait.
    Run {
        id: String,
    },
    /// Persist cancellation intent and stop the active attempt, if any.
    Cancel {
        id: String,
    },
    List,''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''        Command::Workflow(WorkflowCommand::List) => {''',
    '''        Command::Workflow(WorkflowCommand::Cancel { id }) => {
            let response = post_empty(url_of(args, &format!("/api/v1/workflows/{id}/cancel")))?;
            emit(args, &response, |v| {
                println!(
                    "workflow {}: signalled_active_execution={}",
                    v["workflow_id"], v["signalled_active_execution"]
                );
            })
        }
        Command::Workflow(WorkflowCommand::List) => {''',
)

# Daemon startup reconciles a persisted cancellation intent left by a crash.
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    tracing::info!(
        %listen,
        data_dir = %args.data_dir.display(),
        executor = orchestrator.executor_backend_id(),
        "scirust-hubd starting"
    );''',
    '''    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(recovered_cancellations, "reconciled workflow cancellations after restart");
    }

    tracing::info!(
        %listen,
        data_dir = %args.data_dir.display(),
        executor = orchestrator.executor_backend_id(),
        "scirust-hubd starting"
    );''',
)

# ---------------------------------------------------------------------------
# Deterministic scheduler tests.
# ---------------------------------------------------------------------------
Path("crates/hub-core/tests").mkdir(parents=True, exist_ok=True)
Path("crates/hub-core/tests/workflow_retry_cancel.rs").write_text(r'''use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hub_core::store::WorkflowRepository as _;
use hub_core::{
    AttemptFailureCategory, Capability, CapabilityName, ComponentId, ComponentKind,
    ComponentManifest, ComponentName, ExecutionBinding, ExecutionOutcome, ExecutionRequest,
    Executor, ExecutorFailure, FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents,
    InMemoryRuns, InMemoryWorkflows, Limits, Orchestrator, ProcessBinding, RetryPolicy, Step,
    Version, WorkflowSpec, WorkflowState, WORKFLOW_SCHEMA_VERSION,
};

#[derive(Default)]
struct ScriptedExecutor {
    outcomes: Mutex<VecDeque<ExecutionOutcome>>,
    calls: AtomicUsize,
}

impl ScriptedExecutor {
    fn new(outcomes: Vec<ExecutionOutcome>) -> Self {
        Self {
            outcomes: Mutex::new(outcomes.into()),
            calls: AtomicUsize::new(0),
        }
    }
}

impl Executor for ScriptedExecutor {
    fn backend_id(&self) -> &str {
        "scripted"
    }

    fn execute(
        &self,
        _request: &ExecutionRequest,
        cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if cancel.is_cancelled() {
            return Ok(cancelled());
        }
        Ok(self
            .outcomes
            .lock()
            .expect("script lock")
            .pop_front()
            .unwrap_or_else(success))
    }
}

struct BlockingExecutor {
    started: Arc<AtomicBool>,
}

impl Executor for BlockingExecutor {
    fn backend_id(&self) -> &str {
        "blocking"
    }

    fn execute(
        &self,
        _request: &ExecutionRequest,
        cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        self.started.store(true, Ordering::SeqCst);
        while !cancel.is_cancelled() {
            thread::sleep(Duration::from_millis(2));
        }
        Ok(cancelled())
    }
}

fn success() -> ExecutionOutcome {
    ExecutionOutcome {
        exit_code: Some(0),
        signal: None,
        timed_out: false,
        cancelled: false,
        start_error: None,
        duration_ms: 1,
        stdout: Vec::new(),
        stdout_truncated: false,
        stderr: Vec::new(),
        stderr_truncated: false,
    }
}

fn failed(code: i32) -> ExecutionOutcome {
    ExecutionOutcome {
        exit_code: Some(code),
        signal: None,
        timed_out: false,
        cancelled: false,
        start_error: None,
        duration_ms: 1,
        stdout: Vec::new(),
        stdout_truncated: false,
        stderr: b"failed".to_vec(),
        stderr_truncated: false,
    }
}

fn cancelled() -> ExecutionOutcome {
    ExecutionOutcome {
        exit_code: None,
        signal: None,
        timed_out: false,
        cancelled: true,
        start_error: None,
        duration_ms: 1,
        stdout: Vec::new(),
        stdout_truncated: false,
        stderr: Vec::new(),
        stderr_truncated: false,
    }
}

struct Fixture {
    orch: Arc<Orchestrator>,
    workflows: Arc<InMemoryWorkflows>,
    component: ComponentId,
    root: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture(executor: Arc<dyn Executor>) -> Fixture {
    let root = std::env::temp_dir().join(format!("hub-workflow-policy-{}", uuid::Uuid::new_v4()));
    let components = Arc::new(InMemoryComponents::default());
    let workflows = Arc::new(InMemoryWorkflows::default());
    let component = ComponentId::generate();
    let manifest = ComponentManifest::new_v1(
        component,
        ComponentName::parse("workflow-test").expect("name"),
        Version::parse("1.0.0").expect("version"),
        ComponentKind::parse(ComponentKind::TOOL).expect("kind"),
        vec![Capability {
            name: CapabilityName::parse("test.run").expect("capability"),
            contract_version: Version::parse("1.0.0").expect("contract"),
            inputs: Vec::new(),
            outputs: Vec::new(),
            properties: BTreeMap::new(),
        }],
        Some(ExecutionBinding::Process(ProcessBinding {
            program: "unused-by-test-executor".into(),
            args: Vec::new(),
            working_dir: None,
            outputs: Vec::new(),
        })),
        None,
        BTreeMap::new(),
    )
    .expect("manifest");
    components.put(&manifest).expect("register in fixture");
    let orch = Arc::new(Orchestrator::new(
        Arc::new(hub_core::ManualClock::starting_at(1_000)),
        components,
        Arc::new(InMemoryRuns::default()),
        Arc::new(InMemoryArtifactMeta::default()),
        workflows.clone(),
        FileSystemArtifactStore::open(root.join("blobs")).expect("blobs"),
        executor,
        Limits::default(),
        root.join("runs"),
    ));
    Fixture {
        orch,
        workflows,
        component,
        root,
    }
}

fn step(component: ComponentId, retry: Option<RetryPolicy>) -> Step {
    Step {
        key: "only".into(),
        component,
        capability: CapabilityName::parse("test.run").expect("capability"),
        parameters: BTreeMap::new(),
        inputs: BTreeMap::new(),
        timeout_ms: 5_000,
        after: Vec::new(),
        retry,
    }
}

fn policy(max_attempts: u32) -> RetryPolicy {
    RetryPolicy {
        max_attempts,
        backoff_ms: None,
        retry_on: BTreeSet::from([AttemptFailureCategory::NonZeroExit]),
    }
}

fn submit(fixture: &Fixture, retry: Option<RetryPolicy>) -> hub_core::WorkflowRecord {
    fixture
        .orch
        .submit_workflow(WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "policy-test".into(),
            steps: vec![step(fixture.component, retry)],
        })
        .expect("submit workflow")
}

#[test]
fn retry_succeeds_with_distinct_attempt_and_run_identity() {
    let executor = Arc::new(ScriptedExecutor::new(vec![failed(7), success()]));
    let fixture = fixture(executor.clone());
    let workflow = submit(&fixture, Some(policy(2)));
    let finished = fixture
        .orch
        .execute_workflow(workflow.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Succeeded);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 2);
    let attempts = &finished.steps[0].attempts;
    assert_eq!(attempts.len(), 2);
    assert_ne!(attempts[0].id, attempts[1].id);
    assert_ne!(attempts[0].run, attempts[1].run);
    assert_eq!(attempts[0].failure_category, Some(AttemptFailureCategory::NonZeroExit));
    assert_eq!(attempts[1].state, hub_core::RunState::Succeeded);
}

#[test]
fn retry_exhaustion_persists_every_attempt() {
    let executor = Arc::new(ScriptedExecutor::new(vec![failed(2), failed(3), failed(4)]));
    let fixture = fixture(executor.clone());
    let workflow = submit(&fixture, Some(policy(3)));
    let finished = fixture
        .orch
        .execute_workflow(workflow.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Failed);
    assert_eq!(executor.calls.load(Ordering::SeqCst), 3);
    assert_eq!(finished.steps[0].attempts.len(), 3);
    assert!(finished.steps[0]
        .attempts
        .iter()
        .all(|attempt| attempt.failure_category == Some(AttemptFailureCategory::NonZeroExit)));
}

#[test]
fn cancelling_created_workflow_prevents_any_step_from_starting() {
    let executor = Arc::new(ScriptedExecutor::default());
    let fixture = fixture(executor.clone());
    let workflow = submit(&fixture, None);
    assert!(!fixture.orch.cancel_workflow(workflow.id).expect("cancel"));
    let stored = fixture.orch.workflow(&workflow.id).expect("stored workflow");
    assert_eq!(stored.state, WorkflowState::Cancelled);
    assert!(stored.cancel_requested_at.is_some());
    assert_eq!(executor.calls.load(Ordering::SeqCst), 0);
    assert!(fixture.orch.execute_workflow(workflow.id).is_err());
}

#[test]
fn cancelling_running_workflow_signals_the_active_attempt() {
    let started = Arc::new(AtomicBool::new(false));
    let fixture = fixture(Arc::new(BlockingExecutor {
        started: started.clone(),
    }));
    let workflow = submit(&fixture, None);
    let orch = fixture.orch.clone();
    let id = workflow.id;
    let handle = thread::spawn(move || orch.execute_workflow(id).expect("workflow thread"));
    for _ in 0..2_000 {
        if started.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(started.load(Ordering::SeqCst), "executor never started");
    assert!(fixture.orch.cancel_workflow(id).expect("cancel running"));
    let finished = handle.join().expect("join workflow");
    assert_eq!(finished.state, WorkflowState::Cancelled);
    assert_eq!(finished.steps.len(), 1);
    assert_eq!(finished.steps[0].attempts.len(), 1);
    assert_eq!(finished.steps[0].attempts[0].state, hub_core::RunState::Cancelled);
    assert_eq!(
        finished.steps[0].attempts[0].failure_category,
        Some(AttemptFailureCategory::Cancelled)
    );
}

#[test]
fn restart_recovery_terminalizes_persisted_cancellation_intent() {
    let fixture = fixture(Arc::new(ScriptedExecutor::default()));
    let workflow = submit(&fixture, None);
    let mut running = workflow.clone();
    running
        .transition(WorkflowState::Running, 1_100)
        .expect("running transition");
    fixture.workflows.put(&running).expect("persist running");
    fixture
        .workflows
        .request_cancel(&running.id, 1_200)
        .expect("persist cancellation");

    assert_eq!(fixture.orch.recover_workflow_cancellations().expect("recover"), 1);
    let recovered = fixture.orch.workflow(&running.id).expect("recovered record");
    assert_eq!(recovered.state, WorkflowState::Cancelled);
    assert_eq!(recovered.cancel_requested_at, Some(1_200));
}

#[test]
fn retry_policy_requires_explicit_retryable_categories() {
    let fixture = fixture(Arc::new(ScriptedExecutor::default()));
    let invalid = RetryPolicy {
        max_attempts: 2,
        backoff_ms: None,
        retry_on: BTreeSet::new(),
    };
    let result = fixture.orch.submit_workflow(WorkflowSpec {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        name: "invalid-policy".into(),
        steps: vec![step(fixture.component, Some(invalid))],
    });
    assert!(result.is_err());
}
''')

# Update old in-tree workflow test literals to include the additive field.
for path in ["crates/hub-core/src/workflow.rs", "crates/hub-core/src/orchestrator.rs"]:
    p = Path(path)
    text = p.read_text()
    # Struct literals in existing tests use `after` as the final Step field.
    text = text.replace("after: vec![],\n                }", "after: vec![],\n                    retry: None,\n                }")
    text = text.replace("after: vec![],\n            }", "after: vec![],\n                retry: None,\n            }")
    text = text.replace("after: Vec::new(),\n                }", "after: Vec::new(),\n                    retry: None,\n                }")
    text = text.replace("after: Vec::new(),\n            }", "after: Vec::new(),\n                retry: None,\n            }")
    p.write_text(text)

# Update all repository Step literals conservatively via rust-aware-ish textual
# insertion only where `after` is immediately followed by the struct close.
for p in Path(".").rglob("*.rs"):
    text = p.read_text()
    original = text
    text = text.replace("after: vec![],\n        }", "after: vec![],\n            retry: None,\n        }")
    text = text.replace("after: Vec::new(),\n        }", "after: Vec::new(),\n            retry: None,\n        }")
    if text != original:
        p.write_text(text)

print("PR1 source transformations complete")
