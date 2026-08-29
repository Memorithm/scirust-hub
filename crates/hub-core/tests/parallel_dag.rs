use std::collections::BTreeMap;
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

fn submit(fixture: &Fixture, max_concurrency: u16, steps: Vec<Step>) -> hub_core::WorkflowRecord {
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
    let mut starts = executor.starts.lock().expect("starts").clone();
    starts.sort();
    assert_eq!(starts, vec!["a".to_owned(), "b".to_owned()]);
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
    let workflow = submit(&fixture, 2, vec![step("a", a, &[]), step("b", b, &["a"])]);

    let finished = fixture
        .orch
        .execute_workflow(workflow.id)
        .expect("execute workflow");
    assert_eq!(finished.state, WorkflowState::Succeeded);
    assert_eq!(executor.max_active.load(Ordering::SeqCst), 1);
    let mut starts = executor.starts.lock().expect("starts").clone();
    starts.sort();
    assert_eq!(starts, vec!["a".to_owned(), "b".to_owned()]);
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
    assert!(
        first_started.load(Ordering::SeqCst),
        "first node never started"
    );
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
