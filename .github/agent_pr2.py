from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:100]!r}")
    p.write_text(text.replace(old, new, 1))


def replace_between(path: str, start_marker: str, end_marker: str, replacement: str) -> None:
    p = Path(path)
    text = p.read_text()
    start = text.index(start_marker)
    end = text.index(end_marker, start)
    p.write_text(text[:start] + replacement + text[end:])


# ---------------------------------------------------------------------------
# Workflow contract: explicit bounded concurrency, backwards-default = 1.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''//! Scope honesty (see ADR-0006): scheduling is still **sequential,
//! single-node** in this module revision. Retries and workflow cancellation
//! are explicit and persisted; parallel and distributed scheduling remain
//! separate later layers.''',
    '''//! Scope honesty (see ADR-0006): scheduling is bounded-parallel on one
//! Hub node. Ready nodes are selected deterministically; retries and
//! cancellation remain persisted. Executor location is still local here.''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    'pub const WORKFLOW_MODEL_VERSION: &str = "1.1.0";',
    'pub const WORKFLOW_MODEL_VERSION: &str = "1.2.0";\n\n/// Hard safety bound for one workflow scheduler.\npub const MAX_WORKFLOW_CONCURRENCY: u16 = 64;',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''pub struct WorkflowSpec {
    pub schema_version: u16,
    /// Human-readable label (not an identifier).
    pub name: String,
    pub steps: Vec<Step>,
}''',
    '''pub struct WorkflowSpec {
    pub schema_version: u16,
    /// Human-readable label (not an identifier).
    pub name: String,
    /// Maximum concurrently active DAG nodes. Missing in older JSON means 1,
    /// preserving the former sequential behaviour.
    #[serde(default = "default_workflow_concurrency")]
    pub max_concurrency: u16,
    pub steps: Vec<Step>,
}

const fn default_workflow_concurrency() -> u16 {
    1
}''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''        if self.name.is_empty() || self.name.len() > 128 || self.name.contains('\\0') {
            return Err(CoreError::InvalidRunSpec(
                "workflow name must be 1..=128 characters".into(),
            ));
        }
        let limits = DagLimits::default();''',
    '''        if self.name.is_empty() || self.name.len() > 128 || self.name.contains('\\0') {
            return Err(CoreError::InvalidRunSpec(
                "workflow name must be 1..=128 characters".into(),
            ));
        }
        if self.max_concurrency == 0 || self.max_concurrency > MAX_WORKFLOW_CONCURRENCY {
            return Err(CoreError::InvalidRunSpec(format!(
                "workflow max_concurrency must be 1..={MAX_WORKFLOW_CONCURRENCY}"
            )));
        }
        let limits = DagLimits::default();''',
)
replace_once(
    "crates/hub-core/src/workflow.rs",
    '''    /// Deterministic execution order.
    ///
    /// # Errors''',
    '''    /// Effective bounded parallelism for the scheduler.
    #[must_use]
    pub const fn concurrency_limit(&self) -> usize {
        self.max_concurrency as usize
    }

    /// Direct dependency set for each step, combining explicit `after`
    /// barriers and data-flow dependencies. Keys and dependency sets are
    /// ordered, which makes the scheduler's ready-set choice deterministic.
    #[must_use]
    pub fn dependencies(&self) -> BTreeMap<String, BTreeSet<String>> {
        self.steps
            .iter()
            .map(|step| {
                let mut deps: BTreeSet<String> = step.after.iter().cloned().collect();
                for source in step.inputs.values() {
                    if let InputSource::FromStep { key, .. } = source {
                        deps.insert(key.clone());
                    }
                }
                (step.key.clone(), deps)
            })
            .collect()
    }

    /// Deterministic execution order.
    ///
    /// # Errors''',
)

# Export the concurrency bound for operators/protocol users.
replace_once(
    "crates/hub-core/src/lib.rs",
    '''    AttemptFailureCategory, InputSource, RetryPolicy, Step, StepAttempt, StepResult,
    WorkflowRecord, WorkflowSpec, WorkflowState, WORKFLOW_SCHEMA_VERSION,
};''',
    '''    AttemptFailureCategory, InputSource, RetryPolicy, Step, StepAttempt, StepResult,
    WorkflowRecord, WorkflowSpec, WorkflowState, MAX_WORKFLOW_CONCURRENCY,
    WORKFLOW_SCHEMA_VERSION,
};''',
)

# ---------------------------------------------------------------------------
# Scheduler internals.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "use std::collections::BTreeMap;",
    "use std::collections::{BTreeMap, BTreeSet};",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "use std::sync::{Arc, Mutex};",
    "use std::sync::{mpsc, Arc, Mutex};",
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    "active_workflow_runs: Mutex<BTreeMap<WorkflowId, RunId>>",
    "active_workflow_runs: Mutex<BTreeMap<WorkflowId, BTreeSet<RunId>>>",
)

parallel_engine = r'''    fn execute_workflow_inner(
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

        let spec = record.spec.clone();
        let concurrency = spec.concurrency_limit();
        let dependencies = spec.dependencies();
        let shared_record = Mutex::new(record);
        let mut pending: BTreeSet<String> = spec.steps.iter().map(|step| step.key.clone()).collect();
        let mut completed = BTreeSet::new();
        let mut in_flight = BTreeSet::new();
        let mut first_failure: Option<String> = None;
        let mut cancellation_reason: Option<String> = None;
        let (tx, rx) = mpsc::channel::<(String, Result<ParallelStepTerminal, CoreError>)>();

        std::thread::scope(|scope| -> Result<(), CoreError> {
            loop {
                let persisted_cancel = {
                    let mut current = lock_workflow_record(&shared_record)?;
                    self.refresh_cancel_intent(&mut current)?;
                    current.cancel_requested_at.is_some()
                };
                if persisted_cancel {
                    token.cancel();
                    cancellation_reason
                        .get_or_insert_with(|| "workflow cancellation requested".into());
                    self.cancel_active_workflow_runs(workflow_id)?;
                }

                while first_failure.is_none()
                    && cancellation_reason.is_none()
                    && !token.is_cancelled()
                    && in_flight.len() < concurrency
                {
                    let next = pending
                        .iter()
                        .find(|key| {
                            dependencies
                                .get(*key)
                                .is_some_and(|deps| deps.iter().all(|dep| completed.contains(dep)))
                        })
                        .cloned();
                    let Some(key) = next else {
                        break;
                    };
                    let Some(step) = spec.steps.iter().find(|step| step.key == key).cloned() else {
                        first_failure = Some(format!(
                            "step {key:?} disappeared from validated workflow"
                        ));
                        break;
                    };
                    let resolved = {
                        let current = lock_workflow_record(&shared_record)?;
                        self.resolve_workflow_inputs(&current, &step)?
                    };
                    let resolved = match resolved {
                        Ok(resolved) => resolved,
                        Err(message) => {
                            first_failure = Some(message);
                            break;
                        }
                    };

                    pending.remove(&key);
                    in_flight.insert(key.clone());
                    let sender = tx.clone();
                    let worker_key = key.clone();
                    let worker_token = token.clone();
                    let shared = &shared_record;
                    scope.spawn(move || {
                        let result = self.execute_parallel_step(
                            workflow_id,
                            &step,
                            resolved,
                            &worker_token,
                            shared,
                        );
                        let _ = sender.send((worker_key, result));
                    });
                }

                if first_failure.is_some() {
                    token.cancel();
                    self.cancel_active_workflow_runs(workflow_id)?;
                } else if cancellation_reason.is_some() || token.is_cancelled() {
                    self.cancel_active_workflow_runs(workflow_id)?;
                }

                if in_flight.is_empty() {
                    if pending.is_empty()
                        || first_failure.is_some()
                        || cancellation_reason.is_some()
                        || token.is_cancelled()
                    {
                        break;
                    }
                    return Err(CoreError::Storage(
                        "workflow scheduler stalled with no ready or in-flight step".into(),
                    ));
                }

                let (key, result) = rx.recv().map_err(|_| {
                    CoreError::Storage("workflow scheduler completion channel closed".into())
                })?;
                in_flight.remove(&key);
                match result {
                    Ok(ParallelStepTerminal::Succeeded) => {
                        completed.insert(key);
                    }
                    Ok(ParallelStepTerminal::Failed(message)) => {
                        if first_failure.is_none() {
                            first_failure = Some(message);
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                    Ok(ParallelStepTerminal::Cancelled(message)) => {
                        // A sibling cancelled by fail-fast must not overwrite
                        // the original failure. Otherwise cancellation is the
                        // workflow's terminal cause.
                        if first_failure.is_none() && cancellation_reason.is_none() {
                            cancellation_reason = Some(message);
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                    Err(error) => {
                        if first_failure.is_none() {
                            first_failure = Some(error.to_string());
                            token.cancel();
                            self.cancel_active_workflow_runs(workflow_id)?;
                        }
                    }
                }
            }
            Ok(())
        })?;

        let mut record = shared_record.into_inner().map_err(|_| {
            CoreError::Storage("workflow record lock poisoned after scheduler completion".into())
        })?;
        self.refresh_cancel_intent(&mut record)?;
        if record.cancel_requested_at.is_some() || cancellation_reason.is_some() {
            return self.finish_workflow_cancelled(
                &mut record,
                cancellation_reason.unwrap_or_else(|| "workflow cancellation requested".into()),
            );
        }
        if let Some(message) = first_failure {
            return self.finish_workflow_failed(&mut record, message);
        }
        if completed.len() != spec.steps.len() {
            return self.finish_workflow_failed(
                &mut record,
                format!(
                    "workflow scheduler completed {} of {} steps",
                    completed.len(),
                    spec.steps.len()
                ),
            );
        }

        let now = self.clock.now_ms();
        record.transition(crate::workflow::WorkflowState::Succeeded, now)?;
        self.workflows.put(&record)?;
        info!(workflow = %workflow_id, concurrency, "workflow succeeded");
        Ok(record)
    }

    fn execute_parallel_step(
        &self,
        workflow_id: WorkflowId,
        step: &crate::workflow::Step,
        resolved: BTreeMap<String, ArtifactId>,
        token: &CancelToken,
        shared_record: &Mutex<crate::workflow::WorkflowRecord>,
    ) -> Result<ParallelStepTerminal, CoreError> {
        let run_spec = crate::workflow::WorkflowSpec::step_run_spec(step, &resolved);
        let max_attempts = step.retry.as_ref().map_or(1, |policy| policy.max_attempts);
        let mut attempt_number = 1u32;

        loop {
            if token.is_cancelled() || self.workflow_cancel_requested(workflow_id)? {
                return Ok(ParallelStepTerminal::Cancelled(
                    "workflow cancellation requested".into(),
                ));
            }

            let submitted = self.submit_run(run_spec.clone())?;
            {
                let mut record = lock_workflow_record(shared_record)?;
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
                Self::record_attempt(&mut record, &step.key, attempt);
                self.workflows.put(&record)?;
            }

            self.register_active_workflow_run(workflow_id, submitted.id)?;
            if token.is_cancelled() || self.workflow_cancel_requested(workflow_id)? {
                let _ = self.cancel_run(submitted.id)?;
            }

            let executed = self.execute_run(submitted.id);
            self.unregister_active_workflow_run(workflow_id, submitted.id)?;

            let finished = match executed {
                Ok(finished) => finished,
                Err(error) => {
                    let current_run = self.runs.get(&submitted.id)?;
                    let mut record = lock_workflow_record(shared_record)?;
                    Self::finish_recorded_attempt(
                        &mut record,
                        &step.key,
                        submitted.id,
                        current_run.as_ref(),
                        Some(crate::workflow::AttemptFailureCategory::ExecutionError),
                        Some(error.to_string()),
                    );
                    self.workflows.put(&record)?;
                    return Err(error);
                }
            };

            let category = classify_attempt_failure(&finished);
            let failure = finished
                .outcome
                .as_ref()
                .and_then(|outcome| outcome.failure.clone());
            {
                let mut record = lock_workflow_record(shared_record)?;
                Self::finish_recorded_attempt(
                    &mut record,
                    &step.key,
                    submitted.id,
                    Some(&finished),
                    category,
                    failure.clone(),
                );
                self.workflows.put(&record)?;
            }

            if self.workflow_cancel_requested(workflow_id)?
                || (token.is_cancelled() && finished.state == RunState::Cancelled)
                || finished.state == RunState::Cancelled
            {
                return Ok(ParallelStepTerminal::Cancelled(
                    failure.unwrap_or_else(|| "workflow cancellation requested".into()),
                ));
            }
            if finished.state == RunState::Succeeded {
                return Ok(ParallelStepTerminal::Succeeded);
            }

            let observed =
                category.unwrap_or(crate::workflow::AttemptFailureCategory::ExecutionError);
            let retry = step
                .retry
                .as_ref()
                .is_some_and(|policy| attempt_number < max_attempts && policy.allows(observed));
            if !retry {
                return Ok(ParallelStepTerminal::Failed(format!(
                    "step {:?} attempt {attempt_number} ended in state {}{}",
                    step.key,
                    finished.state,
                    failure
                        .as_ref()
                        .map(|value| format!(": {value}"))
                        .unwrap_or_default()
                )));
            }

            let backoff_ms = step
                .retry
                .as_ref()
                .and_then(|policy| policy.backoff_ms)
                .unwrap_or(0);
            if !self.wait_retry_backoff(workflow_id, token, backoff_ms)? {
                return Ok(ParallelStepTerminal::Cancelled(
                    "workflow cancellation requested during retry backoff".into(),
                ));
            }
            attempt_number += 1;
        }
    }

    fn workflow_cancel_requested(&self, workflow_id: WorkflowId) -> Result<bool, CoreError> {
        Ok(self
            .workflows
            .get(&workflow_id)?
            .is_some_and(|record| record.cancel_requested_at.is_some()))
    }

    fn register_active_workflow_run(
        &self,
        workflow_id: WorkflowId,
        run_id: RunId,
    ) -> Result<(), CoreError> {
        self.active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .entry(workflow_id)
            .or_default()
            .insert(run_id);
        Ok(())
    }

    fn unregister_active_workflow_run(
        &self,
        workflow_id: WorkflowId,
        run_id: RunId,
    ) -> Result<(), CoreError> {
        let mut active = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?;
        let empty = if let Some(runs) = active.get_mut(&workflow_id) {
            runs.remove(&run_id);
            runs.is_empty()
        } else {
            false
        };
        if empty {
            active.remove(&workflow_id);
        }
        Ok(())
    }

    fn cancel_active_workflow_runs(&self, workflow_id: WorkflowId) -> Result<usize, CoreError> {
        let runs = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .get(&workflow_id)
            .cloned()
            .unwrap_or_default();
        let mut signalled = 0usize;
        for run_id in runs {
            if self.cancel_run(run_id)? {
                signalled += 1;
            }
        }
        Ok(signalled)
    }

'''
replace_between(
    "crates/hub-core/src/orchestrator.rs",
    "    fn execute_workflow_inner(",
    "    fn resolve_workflow_inputs(",
    parallel_engine,
)

# Sort StepResult rows by key so persistence remains deterministic even when
# worker start timing differs after the ready-set choice.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            record.steps.push(crate::workflow::StepResult {
                key: key.to_owned(),
                run: attempt.run,
                state: attempt.state,
                failure: attempt.failure.clone(),
                attempts: vec![attempt],
            });
        }
    }''',
    '''            record.steps.push(crate::workflow::StepResult {
                key: key.to_owned(),
                run: attempt.run,
                state: attempt.state,
                failure: attempt.failure.clone(),
                attempts: vec![attempt],
            });
            record.steps.sort_by(|left, right| left.key.cmp(&right.key));
        }
    }''',
)

# Replace single-run cancellation fanout with a set fanout.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        let active_run = self
            .active_workflow_runs
            .lock()
            .map_err(|_| CoreError::Storage("active workflow run map lock poisoned".into()))?
            .get(&workflow_id)
            .copied();
        if let Some(run_id) = active_run {
            let _ = self.cancel_run(run_id)?;
        }
''',
    '''        let signalled_runs = self.cancel_active_workflow_runs(workflow_id)?;
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        if active_token.is_none() {
            if let Some(run_id) = latest_nonterminal_attempt_run(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }''',
    '''        if active_token.is_none() {
            for run_id in nonterminal_attempt_runs(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        }
        Ok(true)
    }

    /// Reconciles cancellation intent''',
    '''        }
        Ok(active_token.is_some() || signalled_runs > 0)
    }

    /// Reconciles cancellation intent''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            if let Some(run_id) = latest_nonterminal_attempt_run(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }
            self.finish_workflow_cancelled(''',
    '''            for run_id in nonterminal_attempt_runs(&record, self)? {
                let _ = self.cancel_run(run_id)?;
            }
            self.finish_workflow_cancelled(''',
)

# Add deterministic fail-closed recovery for workflows interrupted without a
# persisted cancellation request. This avoids duplicate replay after restart.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        Ok(recovered)
    }

    /// Reproduces a recorded run:''',
    r'''        Ok(recovered)
    }

    /// Reconciles workflows left `running` by a daemon crash. Local process
    /// attempts cannot be reattached safely after restart, so Hub fails them
    /// closed instead of replaying side effects. Completed attempts remain
    /// untouched and fully queryable.
    pub fn recover_interrupted_workflows(&self) -> Result<usize, CoreError> {
        let mut recovered = 0usize;
        for mut record in self.workflows.list()? {
            if record.state != crate::workflow::WorkflowState::Running
                || record.cancel_requested_at.is_some()
            {
                continue;
            }
            let message =
                "workflow execution interrupted by daemon restart; local attempts are not replayed automatically"
                    .to_owned();
            let now = self.clock.now_ms();
            for result in &mut record.steps {
                for attempt in &mut result.attempts {
                    let Some(mut run) = self.runs.get(&attempt.run)? else {
                        continue;
                    };
                    if run.state.is_terminal() {
                        continue;
                    }
                    run.transition(RunState::Failed, now)?;
                    self.runs.put(&run)?;
                    attempt.state = RunState::Failed;
                    attempt.finished_at = Some(now);
                    attempt.failure_category =
                        Some(crate::workflow::AttemptFailureCategory::ExecutionError);
                    attempt.failure = Some(message.clone());
                    if result.run == attempt.run {
                        result.state = RunState::Failed;
                        result.failure = Some(message.clone());
                    }
                }
            }
            record.transition(crate::workflow::WorkflowState::Failed, now)?;
            record.failure = Some(message.clone());
            self.workflows.put(&record)?;
            warn!(workflow = %record.id, %message, "recovered interrupted workflow");
            recovered += 1;
        }
        Ok(recovered)
    }

    /// Reproduces a recorded run:''',
)

# Replace helper with all nonterminal attempts and define scheduler-local types.
replace_between(
    "crates/hub-core/src/orchestrator.rs",
    "fn latest_nonterminal_attempt_run(",
    "#[cfg(test)]\nmod tests {",
    r'''fn nonterminal_attempt_runs(
    record: &crate::workflow::WorkflowRecord,
    orchestrator: &Orchestrator,
) -> Result<BTreeSet<RunId>, CoreError> {
    let mut runs = BTreeSet::new();
    for result in &record.steps {
        for attempt in &result.attempts {
            if orchestrator
                .runs
                .get(&attempt.run)?
                .is_some_and(|run| !run.state.is_terminal())
            {
                runs.insert(attempt.run);
            }
        }
    }
    Ok(runs)
}

enum ParallelStepTerminal {
    Succeeded,
    Failed(String),
    Cancelled(String),
}

fn lock_workflow_record(
    record: &Mutex<crate::workflow::WorkflowRecord>,
) -> Result<std::sync::MutexGuard<'_, crate::workflow::WorkflowRecord>, CoreError> {
    record
        .lock()
        .map_err(|_| CoreError::Storage("workflow record lock poisoned".into()))
}

#[cfg(test)]
mod tests {''',
)

# Daemon startup recovery: cancellation intent first, then crash leftovers.
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(recovered_cancellations, "reconciled workflow cancellations after restart");
    }

    tracing::info!(''',
    '''    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(recovered_cancellations, "reconciled workflow cancellations after restart");
    }
    let recovered_interruptions = orchestrator.recover_interrupted_workflows()?;
    if recovered_interruptions > 0 {
        tracing::warn!(
            recovered_interruptions,
            "failed closed workflows interrupted by the previous daemon lifetime"
        );
    }

    tracing::info!(''',
)

# Architecture decision update.
Path("docs/adr/0008-bounded-parallel-workflow-scheduler.md").write_text(r'''# ADR-0008: Bounded parallel workflow scheduler

Status: accepted
Date: 2026-08-29

## Context

Hub's first workflow engine executed one topological node at a time. The
cancellation/retry layer made attempts explicit and restart-observable, but
independent DAG nodes still could not overlap.

## Decision

- `WorkflowSpec.max_concurrency` is explicit and bounded to `1..=64`.
  Missing values deserialize as `1`, preserving existing workflow timing.
- The scheduler owns a lexicographically ordered ready set. A node becomes
  ready only after every explicit and data-flow dependency succeeds.
- Up to `max_concurrency` ready nodes execute on scoped worker threads through
  the existing synchronous `Executor` port. Executor location is not part of
  scheduling semantics.
- The persisted workflow record remains one coherent document. Worker writes
  are serialized through one record mutex; `StepResult` rows are sorted by key
  before persistence so concurrency does not leak timing into record order.
- Fail-fast remains the workflow policy: the first terminal step failure stops
  admission of new nodes and actively cancels every in-flight sibling. A user
  cancellation is distinguished by its persisted cancellation intent.
- Retries remain per-step and retain fresh attempt/run identities. Retry
  backoff occupies only that step's worker and does not block unrelated nodes.
- On daemon restart, persisted cancellation intent is reconciled first.
  A remaining `running` workflow is failed closed: local process attempts are
  not replayed automatically because Hub cannot prove whether a pre-crash
  process performed external side effects. This is recovery without duplicate
  execution, not transparent process reattachment.

## Consequences

- Parallelism is opt-in; old workflow JSON remains sequential.
- SQLite continues to serialize metadata writes behind its connection mutex,
  while execution itself can overlap.
- A later remote executor may reuse the same ready-set and attempt semantics;
  it must not fork scheduler policy.
- Transparent continuation of an in-flight local subprocess is deliberately
  out of scope until process identity/lease semantics make it safe.
''')

# ---------------------------------------------------------------------------
# Deterministic parallel scheduler tests.
# ---------------------------------------------------------------------------
Path("crates/hub-core/tests/parallel_dag.rs").write_text(r'''use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hub_core::store::ComponentRepository as _;
use hub_core::{
    Capability, CapabilityName, ComponentId, ComponentKind, ComponentManifest, ComponentName,
    ExecutionBinding, ExecutionOutcome, ExecutionRequest, Executor, ExecutorFailure,
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
    InMemoryWorkflows, Limits, Orchestrator, ProcessBinding, Step, Version, WorkflowSpec,
    WorkflowState, WORKFLOW_SCHEMA_VERSION,
};

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

#[derive(Default)]
struct TrackingExecutor {
    active: AtomicUsize,
    max_active: AtomicUsize,
    starts: Mutex<Vec<String>>,
    finishes: Mutex<Vec<String>>,
}

impl Executor for TrackingExecutor {
    fn backend_id(&self) -> &str {
        "tracking"
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        self.starts
            .lock()
            .expect("starts lock")
            .push(request.program.clone());
        for _ in 0..20 {
            if cancel.is_cancelled() {
                self.active.fetch_sub(1, Ordering::SeqCst);
                return Ok(cancelled());
            }
            thread::sleep(Duration::from_millis(2));
        }
        self.finishes
            .lock()
            .expect("finishes lock")
            .push(request.program.clone());
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(success())
    }
}

struct QueueCancellationExecutor {
    first_started: Arc<AtomicBool>,
    starts: Arc<Mutex<Vec<String>>>,
}

impl Executor for QueueCancellationExecutor {
    fn backend_id(&self) -> &str {
        "queue-cancellation"
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &hub_core::CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        self.starts
            .lock()
            .expect("starts lock")
            .push(request.program.clone());
        if request.program == "a" {
            self.first_started.store(true, Ordering::SeqCst);
            while !cancel.is_cancelled() {
                thread::sleep(Duration::from_millis(2));
            }
            return Ok(cancelled());
        }
        Ok(success())
    }
}

struct Fixture {
    orch: Arc<Orchestrator>,
    components: Arc<InMemoryComponents>,
    root: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn fixture(executor: Arc<dyn Executor>) -> Fixture {
    let root = std::env::temp_dir().join(format!("hub-parallel-dag-{}", uuid::Uuid::new_v4()));
    let components = Arc::new(InMemoryComponents::default());
    let orch = Arc::new(Orchestrator::new(
        Arc::new(hub_core::ManualClock::starting_at(5_000)),
        components.clone(),
        Arc::new(InMemoryRuns::default()),
        Arc::new(InMemoryArtifactMeta::default()),
        Arc::new(InMemoryWorkflows::default()),
        FileSystemArtifactStore::open(root.join("blobs")).expect("blobs"),
        executor,
        Limits::default(),
        root.join("runs"),
    ));
    Fixture {
        orch,
        components,
        root,
    }
}

fn component(fixture: &Fixture, program: &str) -> ComponentId {
    let id = ComponentId::generate();
    let manifest = ComponentManifest::new_v1(
        id,
        ComponentName::parse(&format!("component-{program}")).expect("name"),
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
            program: program.into(),
            args: Vec::new(),
            working_dir: None,
            outputs: Vec::new(),
        })),
        None,
        BTreeMap::new(),
    )
    .expect("manifest");
    fixture.components.put(&manifest).expect("register");
    id
}

fn step(key: &str, component: ComponentId, after: &[&str]) -> Step {
    Step {
        key: key.into(),
        component,
        capability: CapabilityName::parse("test.run").expect("capability"),
        parameters: BTreeMap::new(),
        inputs: BTreeMap::new(),
        timeout_ms: 5_000,
        after: after.iter().map(|value| (*value).to_owned()).collect(),
        retry: None,
    }
}

fn submit(
    fixture: &Fixture,
    max_concurrency: u16,
    steps: Vec<Step>,
) -> hub_core::WorkflowRecord {
    fixture
        .orch
        .submit_workflow(WorkflowSpec {
            schema_version: WORKFLOW_SCHEMA_VERSION,
            name: "parallel-test".into(),
            max_concurrency,
            steps,
        })
        .expect("submit")
}

#[test]
fn independent_nodes_execute_in_parallel() {
    let executor = Arc::new(TrackingExecutor::default());
    let fixture = fixture(executor.clone());
    let a = component(&fixture, "a");
    let b = component(&fixture, "b");
    let workflow = submit(&fixture, 2, vec![step("a", a, &[]), step("b", b, &[])]);

    let finished = fixture
        .orch
        .execute_workflow(workflow.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Succeeded);
    assert_eq!(executor.max_active.load(Ordering::SeqCst), 2);
    assert_eq!(
        *executor.starts.lock().expect("starts"),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(
        finished
            .steps
            .iter()
            .map(|result| result.key.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b"]
    );
}

#[test]
fn dependency_barrier_prevents_early_start() {
    let executor = Arc::new(TrackingExecutor::default());
    let fixture = fixture(executor.clone());
    let a = component(&fixture, "a");
    let b = component(&fixture, "b");
    let workflow = submit(
        &fixture,
        2,
        vec![step("a", a, &[]), step("b", b, &["a"])],
    );

    let finished = fixture
        .orch
        .execute_workflow(workflow.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Succeeded);
    assert_eq!(executor.max_active.load(Ordering::SeqCst), 1);
    assert_eq!(
        *executor.starts.lock().expect("starts"),
        vec!["a".to_owned(), "b".to_owned()]
    );
    assert_eq!(
        *executor.finishes.lock().expect("finishes"),
        vec!["a".to_owned(), "b".to_owned()]
    );
}

#[test]
fn cancellation_does_not_admit_queued_ready_node() {
    let first_started = Arc::new(AtomicBool::new(false));
    let starts = Arc::new(Mutex::new(Vec::new()));
    let fixture = fixture(Arc::new(QueueCancellationExecutor {
        first_started: first_started.clone(),
        starts: starts.clone(),
    }));
    let a = component(&fixture, "a");
    let b = component(&fixture, "b");
    let workflow = submit(&fixture, 1, vec![step("a", a, &[]), step("b", b, &[])]);
    let id = workflow.id;
    let orch = fixture.orch.clone();
    let handle = thread::spawn(move || orch.execute_workflow(id).expect("workflow thread"));
    for _ in 0..2_000 {
        if first_started.load(Ordering::SeqCst) {
            break;
        }
        thread::sleep(Duration::from_millis(1));
    }
    assert!(first_started.load(Ordering::SeqCst), "first node never started");
    assert!(fixture.orch.cancel_workflow(id).expect("cancel"));
    let finished = handle.join().expect("join");
    assert_eq!(finished.state, WorkflowState::Cancelled);
    assert_eq!(*starts.lock().expect("starts"), vec!["a".to_owned()]);
}

#[test]
fn concurrency_zero_is_rejected() {
    let fixture = fixture(Arc::new(TrackingExecutor::default()));
    let a = component(&fixture, "a");
    let result = fixture.orch.submit_workflow(WorkflowSpec {
        schema_version: WORKFLOW_SCHEMA_VERSION,
        name: "invalid-concurrency".into(),
        max_concurrency: 0,
        steps: vec![step("a", a, &[])],
    });
    assert!(result.is_err());
}
''')

# Add `max_concurrency: 1` to the known existing WorkflowSpec test literals.
# The compiler gate below intentionally catches any missed literal.
for path in [
    "crates/hub-core/src/workflow.rs",
    "crates/hub-core/src/orchestrator.rs",
    "crates/hub-core/tests/workflow_retry_cancel.rs",
    "crates/hub-store-sqlite/src/lib.rs",
]:
    p = Path(path)
    text = p.read_text()
    original = text
    # Existing literals consistently place `name` immediately before `steps`.
    import re
    text = re.sub(
        r'(name:\s*[^\n]+,\n)(\s*)(steps:)',
        lambda m: m.group(1) + m.group(2) + 'max_concurrency: 1,\n' + m.group(2) + m.group(3),
        text,
    )
    if text != original:
        p.write_text(text)

print("PR2 bounded parallel scheduler transformations complete")
