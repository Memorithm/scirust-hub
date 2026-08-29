use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use hub_core::store::{ComponentRepository as _, WorkflowRepository as _};
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
    assert_eq!(
        attempts[0].failure_category,
        Some(AttemptFailureCategory::NonZeroExit)
    );
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
    let stored = fixture
        .orch
        .workflow(&workflow.id)
        .expect("stored workflow");
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
    assert_eq!(
        finished.steps[0].attempts[0].state,
        hub_core::RunState::Cancelled
    );
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

    assert_eq!(
        fixture
            .orch
            .recover_workflow_cancellations()
            .expect("recover"),
        1
    );
    let recovered = fixture
        .orch
        .workflow(&running.id)
        .expect("recovered record");
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
