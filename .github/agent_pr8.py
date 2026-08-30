from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:180]!r}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# Executor report: preserve per-invocation backend/worker provenance without
# contaminating the wire-level ExecutionOutcome used by remote workers.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-core/src/exec.rs",
    '''impl ExecutionOutcome {
    /// Success means a clean zero exit without timeout, cancellation or
    /// start failure.
    #[must_use]
    pub fn exited_cleanly(&self) -> bool {
        !self.timed_out
            && !self.cancelled
            && self.start_error.is_none()
            && self.exit_code == Some(0)
    }
}

/// A backend capable of executing [`ExecutionRequest`]s.
''',
    '''impl ExecutionOutcome {
    /// Success means a clean zero exit without timeout, cancellation or
    /// start failure.
    #[must_use]
    pub fn exited_cleanly(&self) -> bool {
        !self.timed_out
            && !self.cancelled
            && self.start_error.is_none()
            && self.exit_code == Some(0)
    }
}

/// One executor observation plus the concrete backend target that produced it.
///
/// Most executors use their stable [`Executor::backend_id`]. Placement-aware
/// executors override [`Executor::execute_report`] so provenance can identify
/// the worker chosen for one invocation without mutable global "last worker"
/// state that would race under parallel workflows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionReport {
    pub outcome: ExecutionOutcome,
    pub backend_id: String,
}

/// A backend capable of executing [`ExecutionRequest`]s.
''',
)
replace_once(
    "crates/hub-core/src/exec.rs",
    '''    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure>;
}
''',
    '''    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure>;

    /// Executes one request and reports the concrete backend target used for
    /// this invocation. Placement-aware executors override this method.
    ///
    /// # Errors
    /// Same contract as [`Self::execute`].
    fn execute_report(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionReport, ExecutorFailure> {
        self.execute(request, cancel).map(|outcome| ExecutionReport {
            outcome,
            backend_id: self.backend_id().to_owned(),
        })
    }
}
''',
)

# Orchestrator consumes the execution report so the selected worker is durable
# provenance rather than transient scheduler state.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    'use crate::exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};',
    'use crate::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        let outcome = self.run_process(&mut record, &binding, &workdir, token)?;
        let finished_at = self.clock.now_ms();
''',
    '''        let report = self.run_process(&mut record, &binding, &workdir, token)?;
        let outcome = report.outcome;
        let finished_at = self.clock.now_ms();
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''            executor_backend: self.executor.backend_id().to_owned(),
''',
    '''            executor_backend: report.backend_id,
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    ) -> Result<ExecutionOutcome, CoreError> {
        let request = match self.build_request(&record.spec, binding, workdir) {
''',
    '''    ) -> Result<ExecutionReport, CoreError> {
        let request = match self.build_request(&record.spec, binding, workdir) {
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''        match self.executor.execute(&request, token) {
            Ok(outcome) => Ok(outcome),
            Err(crate::error::ExecutorFailure::TimedOut { timeout_ms }) => {
                // The backend enforced the timeout; surface it as a timed-out
                // outcome so provenance stays complete instead of erroring.
                Ok(ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: true,
                    cancelled: false,
                    start_error: None,
                    duration_ms: timeout_ms,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                })
            }
            Err(crate::error::ExecutorFailure::Cancelled) => Ok(ExecutionOutcome {
                exit_code: None,
                signal: None,
                timed_out: false,
                cancelled: true,
                start_error: None,
                duration_ms: 0,
                stdout: Vec::new(),
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            }),
''',
    '''        match self.executor.execute_report(&request, token) {
            Ok(report) => Ok(report),
            Err(crate::error::ExecutorFailure::TimedOut { timeout_ms }) => {
                // The backend enforced the timeout; surface it as a timed-out
                // outcome so provenance stays complete instead of erroring.
                Ok(ExecutionReport {
                    backend_id: self.executor.backend_id().to_owned(),
                    outcome: ExecutionOutcome {
                        exit_code: None,
                        signal: None,
                        timed_out: true,
                        cancelled: false,
                        start_error: None,
                        duration_ms: timeout_ms,
                        stdout: Vec::new(),
                        stdout_truncated: false,
                        stderr: Vec::new(),
                        stderr_truncated: false,
                    },
                })
            }
            Err(crate::error::ExecutorFailure::Cancelled) => Ok(ExecutionReport {
                backend_id: self.executor.backend_id().to_owned(),
                outcome: ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: true,
                    start_error: None,
                    duration_ms: 0,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                },
            }),
''',
)

# Add a focused provenance regression by making one test executor report a
# per-invocation target distinct from its global backend id.
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    struct TestHub {
        orch: Orchestrator,
''',
    '''    struct ReportedExecutor;

    impl Executor for ReportedExecutor {
        fn backend_id(&self) -> &str {
            "remote-pool"
        }

        fn execute(
            &self,
            _request: &ExecutionRequest,
            _cancel: &CancelToken,
        ) -> Result<ExecutionOutcome, crate::error::ExecutorFailure> {
            Ok(ExecutionOutcome {
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
            })
        }

        fn execute_report(
            &self,
            request: &ExecutionRequest,
            cancel: &CancelToken,
        ) -> Result<ExecutionReport, crate::error::ExecutorFailure> {
            self.execute(request, cancel).map(|outcome| ExecutionReport {
                outcome,
                backend_id: "remote:worker-a@http://worker-a".into(),
            })
        }
    }

    struct TestHub {
        orch: Orchestrator,
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    fn echo_manifest(id: ComponentId) -> ComponentManifest {
''',
    '''    fn hub_with_executor(executor: Arc<dyn Executor>) -> TestHub {
        let dir = std::env::temp_dir().join(format!("hub-orch-report-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        let artifacts = Arc::new(InMemoryArtifactMeta::default());
        let orch = Orchestrator::new(
            Arc::new(ManualClock::starting_at(2_000)),
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            artifacts.clone(),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.join("blobs")).expect("blobs"),
            executor,
            Limits::default(),
            dir.join("workdirs"),
        );
        TestHub { orch, artifacts, dir }
    }

    fn echo_manifest(id: ComponentId) -> ComponentManifest {
''',
)
replace_once(
    "crates/hub-core/src/orchestrator.rs",
    '''    #[test]
    fn registration_is_idempotent_and_conflicts_are_surfaced() {
''',
    '''    #[test]
    fn per_invocation_executor_target_is_recorded_in_run_provenance() {
        let hub = hub_with_executor(Arc::new(ReportedExecutor));
        let manifest = echo_manifest(ComponentId::generate());
        hub.orch.register_component(manifest.clone()).expect("register");
        let submitted = hub
            .orch
            .submit_run(RunSpec {
                component: manifest.id,
                capability: CapabilityName::parse("demo.echo").expect("cap"),
                parameters: BTreeMap::new(),
                inputs: Vec::new(),
                timeout_ms: 1_000,
            })
            .expect("submit");
        let finished = hub.orch.execute_run(submitted.id).expect("execute");
        assert_eq!(
            finished.outcome.expect("outcome").executor_backend,
            "remote:worker-a@http://worker-a"
        );
    }

    #[test]
    fn registration_is_idempotent_and_conflicts_are_surfaced() {
''',
)

# ---------------------------------------------------------------------------
# Remote executor discovery helpers reused by the pool before any dispatch.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-executor/src/remote.rs",
    '''    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint)
    }
''',
    '''    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn discover_eligible(&self) -> Result<WorkerDescriptor, String> {
        let descriptor = self.describe().map_err(|error| match error {
            RemoteCallError::Authorization => "authorization refused".to_owned(),
            other => format!("unavailable: {other}"),
        })?;
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {
            return Err(format!(
                "protocol {} unsupported; expected {}",
                descriptor.protocol_version, WORKER_PROTOCOL_VERSION
            ));
        }
        if !descriptor
            .capabilities
            .iter()
            .any(|capability| capability == PROCESS_EXECUTION_CAPABILITY)
        {
            return Err(format!(
                "worker {} lacks capability {}",
                descriptor.worker_id, PROCESS_EXECUTION_CAPABILITY
            ));
        }
        let descriptor_limit = usize::try_from(descriptor.max_payload_bytes).unwrap_or(usize::MAX);
        if self.max_payload_bytes > descriptor_limit {
            return Err(format!(
                "worker payload limit {} is below configured client limit {}",
                descriptor.max_payload_bytes, self.max_payload_bytes
            ));
        }
        Ok(descriptor)
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint)
    }
''',
)

# ---------------------------------------------------------------------------
# Configured multi-worker pool. Discovery is pre-dispatch only. Once a worker
# is selected there is deliberately no automatic failover because a lost
# lease-create response is ambiguous and replaying on another worker could
# duplicate side effects.
# ---------------------------------------------------------------------------
Path("crates/hub-executor/src/pool.rs").write_text(r'''//! Deterministic configured-worker placement over authenticated remote executors.
//!
//! Discovery happens before dispatch. Unavailable/incompatible workers are
//! skipped while no lease exists. After a target is selected, execution stays
//! pinned to it: an ambiguous lease-create/transport failure is never retried
//! on another worker because the first worker may already be executing.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use hub_core::error::ExecutorFailure;
use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};

use crate::RemoteExecutor;

#[derive(Debug)]
pub struct RemotePoolExecutor {
    workers: Vec<RemoteExecutor>,
    in_flight: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl RemotePoolExecutor {
    /// Builds a pool from two or more configured endpoints sharing one worker
    /// bearer credential. Endpoint strings must be unique.
    pub fn new(endpoints: Vec<String>, token: impl Into<String>) -> Result<Self, String> {
        if endpoints.len() < 2 {
            return Err("remote worker pool requires at least two endpoints".into());
        }
        let token = token.into();
        if token.is_empty() {
            return Err("remote worker bearer token must not be empty".into());
        }
        let mut seen = BTreeMap::<String, ()>::new();
        let mut workers = Vec::with_capacity(endpoints.len());
        for endpoint in endpoints {
            let normalized = endpoint.trim_end_matches('/').to_owned();
            if seen.insert(normalized.clone(), ()).is_some() {
                return Err(format!("duplicate remote worker endpoint {normalized:?}"));
            }
            workers.push(RemoteExecutor::new(normalized, token.clone())?);
        }
        Ok(Self {
            workers,
            in_flight: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn select(&self) -> Result<(RemoteExecutor, String, InFlightGuard), String> {
        let mut eligible = Vec::new();
        let mut diagnostics = Vec::new();
        let mut identities = BTreeMap::<String, String>::new();
        for worker in &self.workers {
            match worker.discover_eligible() {
                Ok(descriptor) => {
                    if let Some(previous_endpoint) = identities
                        .insert(descriptor.worker_id.clone(), worker.endpoint().to_owned())
                    {
                        return Err(format!(
                            "duplicate worker identity {:?} advertised by {:?} and {:?}",
                            descriptor.worker_id,
                            previous_endpoint,
                            worker.endpoint()
                        ));
                    }
                    eligible.push((worker.clone(), descriptor.worker_id));
                }
                Err(error) => diagnostics.push(format!("{}: {error}", worker.endpoint())),
            }
        }
        if eligible.is_empty() {
            return Err(if diagnostics.is_empty() {
                "no eligible remote workers".into()
            } else {
                format!("no eligible remote workers: {}", diagnostics.join("; "))
            });
        }

        let mut loads = self
            .in_flight
            .lock()
            .map_err(|_| "remote worker pool load map lock poisoned".to_owned())?;
        eligible.sort_by(|(left_worker, left_id), (right_worker, right_id)| {
            let left_load = loads.get(left_worker.endpoint()).copied().unwrap_or(0);
            let right_load = loads.get(right_worker.endpoint()).copied().unwrap_or(0);
            (left_load, left_id, left_worker.endpoint()).cmp(&(
                right_load,
                right_id,
                right_worker.endpoint(),
            ))
        });
        let (worker, worker_id) = eligible.remove(0);
        let key = worker.endpoint().to_owned();
        let count = loads.entry(key.clone()).or_insert(0);
        *count = count.saturating_add(1);
        drop(loads);
        let guard = InFlightGuard {
            key,
            loads: self.in_flight.clone(),
        };
        Ok((worker, worker_id, guard))
    }

    fn failure_report(&self, started: Instant, reason: String) -> ExecutionReport {
        ExecutionReport {
            backend_id: self.backend_id().to_owned(),
            outcome: ExecutionOutcome {
                exit_code: None,
                signal: None,
                timed_out: false,
                cancelled: false,
                start_error: Some(reason),
                duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                stdout: Vec::new(),
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            },
        }
    }
}

struct InFlightGuard {
    key: String,
    loads: Arc<Mutex<BTreeMap<String, u64>>>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if let Ok(mut loads) = self.loads.lock() {
            if let Some(count) = loads.get_mut(&self.key) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    loads.remove(&self.key);
                }
            }
        }
    }
}

impl Executor for RemotePoolExecutor {
    fn backend_id(&self) -> &str {
        "remote-pool"
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        self.execute_report(request, cancel).map(|report| report.outcome)
    }

    fn execute_report(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionReport, ExecutorFailure> {
        let started = Instant::now();
        if cancel.is_cancelled() {
            return Ok(ExecutionReport {
                backend_id: self.backend_id().to_owned(),
                outcome: ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: true,
                    start_error: None,
                    duration_ms: 0,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                },
            });
        }
        let (worker, worker_id, _guard) = match self.select() {
            Ok(selection) => selection,
            Err(error) => return Ok(self.failure_report(started, error)),
        };
        let backend_id = format!("remote:{worker_id}@{}", worker.endpoint());
        worker
            .execute(request, cancel)
            .map(|outcome| ExecutionReport { outcome, backend_id })
    }
}
''')

# Export the pool.
replace_once(
    "crates/hub-executor/src/lib.rs",
    '''//! - [`RemoteExecutor`]: authenticated lease-based execution on a worker.

pub mod remote;
pub mod worker;

pub use remote::RemoteExecutor;
''',
    '''//! - [`RemoteExecutor`]: authenticated lease-based execution on one worker.
//! - [`RemotePoolExecutor`]: deterministic pre-dispatch placement across a
//!   configured set of workers, with no unsafe post-dispatch failover.

pub mod pool;
pub mod remote;
pub mod worker;

pub use pool::RemotePoolExecutor;
pub use remote::RemoteExecutor;
''',
)

# ---------------------------------------------------------------------------
# Real-worker integration tests for concurrent placement and identity safety.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-executor/tests/remote_worker.rs",
    '''use hub_executor::RemoteExecutor;
''',
    '''use hub_executor::{RemoteExecutor, RemotePoolExecutor};
''',
)
replace_once(
    "crates/hub-executor/tests/remote_worker.rs",
    '''async fn start_worker(token: &str) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let root = temp_dir("worker");
    let service = WorkerService::new("worker-e2e", token, root.clone(), 8 * 1024 * 1024)
        .expect("worker service");
''',
    '''async fn start_worker(token: &str) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    start_named_worker("worker-e2e", token).await
}

async fn start_named_worker(
    worker_id: &str,
    token: &str,
) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let root = temp_dir(worker_id);
    let service = WorkerService::new(worker_id, token, root.clone(), 8 * 1024 * 1024)
        .expect("worker service");
''',
)
replace_once(
    "crates/hub-executor/tests/remote_worker.rs",
    '''#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_executor_transports_inputs_and_materializes_outputs() {
''',
    '''fn slow_request(workdir: PathBuf) -> ExecutionRequest {
    let mut request = request(workdir);
    request.args = vec![
        "-c".into(),
        "sleep 0.5; cat inputs/source > outputs/result".into(),
    ];
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pool_places_concurrent_runs_on_distinct_workers() {
    let (endpoint_a, root_a, task_a) = start_named_worker("worker-a", "secret").await;
    let (endpoint_b, root_b, task_b) = start_named_worker("worker-b", "secret").await;
    // Reverse endpoint order deliberately: lexical worker identity is the
    // deterministic zero-load tie-break, not configuration order.
    let pool = std::sync::Arc::new(
        RemotePoolExecutor::new(vec![endpoint_b, endpoint_a], "secret").expect("pool"),
    );
    let local_a = temp_dir("pool-a");
    let local_b = temp_dir("pool-b");

    let first_pool = pool.clone();
    let first_dir = local_a.clone();
    let first = tokio::task::spawn_blocking(move || {
        first_pool.execute_report(&slow_request(first_dir), &CancelToken::new())
    });
    tokio::time::sleep(Duration::from_millis(75)).await;
    let second_pool = pool.clone();
    let second_dir = local_b.clone();
    let second = tokio::task::spawn_blocking(move || {
        second_pool.execute_report(&slow_request(second_dir), &CancelToken::new())
    });

    let first = first.await.expect("join first").expect("first report");
    let second = second.await.expect("join second").expect("second report");
    assert!(first.outcome.exited_cleanly(), "{:?}", first.outcome);
    assert!(second.outcome.exited_cleanly(), "{:?}", second.outcome);
    let targets = std::collections::BTreeSet::from([first.backend_id, second.backend_id]);
    assert_eq!(targets.len(), 2, "concurrent placements must spread at zero/one load");
    assert!(targets.iter().any(|target| target.contains("worker-a@")));
    assert!(targets.iter().any(|target| target.contains("worker-b@")));

    task_a.abort();
    task_b.abort();
    for path in [local_a, local_b, root_a, root_b] {
        let _ = fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pool_rejects_duplicate_worker_identity_before_dispatch() {
    let (endpoint_a, root_a, task_a) = start_named_worker("worker-dup", "secret").await;
    let (endpoint_b, root_b, task_b) = start_named_worker("worker-dup", "secret").await;
    let pool = RemotePoolExecutor::new(vec![endpoint_a, endpoint_b], "secret").expect("pool");
    let local = temp_dir("dup-id");
    let local_for_run = local.clone();
    let report = tokio::task::spawn_blocking(move || {
        pool.execute_report(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("report");
    assert!(report
        .outcome
        .start_error
        .as_deref()
        .is_some_and(|error| error.contains("duplicate worker identity")));
    assert!(!local.join("outputs/result").exists());

    task_a.abort();
    task_b.abort();
    for path in [local, root_a, root_b] {
        let _ = fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_executor_transports_inputs_and_materializes_outputs() {
''',
)

# ---------------------------------------------------------------------------
# Daemon configuration: preserve one-worker mode; repeated/comma-separated
# URLs activate the configured pool with the same environment-only credential.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''use hub_executor::{ProcessExecutor, RemoteExecutor};
''',
    '''use hub_executor::{ProcessExecutor, RemoteExecutor, RemotePoolExecutor};
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_URL")]
    remote_worker_url: Option<String>,
''',
    '''    /// One or more worker URLs. Repeat the flag or comma-separate the
    /// environment value to enable deterministic multi-worker placement.
    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_URL", value_delimiter = ',')]
    remote_worker_url: Vec<String>,
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''fn run(args: Args) -> Result<(), DaemonError> {
''',
    '''fn build_executor(
    backend: ExecutorBackend,
    remote_worker_urls: Vec<String>,
    remote_worker_token: Option<String>,
) -> Result<Arc<dyn Executor>, DaemonError> {
    match backend {
        ExecutorBackend::Process => Ok(Arc::new(ProcessExecutor::new())),
        ExecutorBackend::Remote => {
            if remote_worker_urls.is_empty() {
                return Err(DaemonError::ExecutorConfig(
                    "at least one --remote-worker-url is required with --executor remote".into(),
                ));
            }
            let token = remote_worker_token.ok_or_else(|| {
                DaemonError::ExecutorConfig(
                    "--remote-worker-token is required with --executor remote".into(),
                )
            })?;
            if remote_worker_urls.len() == 1 {
                let url = remote_worker_urls.into_iter().next().expect("checked length");
                Ok(Arc::new(
                    RemoteExecutor::new(url, token).map_err(DaemonError::ExecutorConfig)?,
                ))
            } else {
                Ok(Arc::new(
                    RemotePoolExecutor::new(remote_worker_urls, token)
                        .map_err(DaemonError::ExecutorConfig)?,
                ))
            }
        }
    }
}

fn run(args: Args) -> Result<(), DaemonError> {
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    let executor: Arc<dyn Executor> = match args.executor {
        ExecutorBackend::Process => Arc::new(ProcessExecutor::new()),
        ExecutorBackend::Remote => {
            let url = args.remote_worker_url.ok_or_else(|| {
                DaemonError::ExecutorConfig(
                    "--remote-worker-url is required with --executor remote".into(),
                )
            })?;
            let token = args.remote_worker_token.ok_or_else(|| {
                DaemonError::ExecutorConfig(
                    "--remote-worker-token is required with --executor remote".into(),
                )
            })?;
            Arc::new(RemoteExecutor::new(url, token).map_err(DaemonError::ExecutorConfig)?)
        }
    };
''',
    '''    let executor = build_executor(
        args.executor,
        args.remote_worker_url,
        args.remote_worker_token,
    )?;
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
''',
    '''    #[test]
    fn repeated_remote_worker_urls_select_pool_configuration() {
        let single = build_executor(
            ExecutorBackend::Remote,
            vec!["http://worker-a:8488".into()],
            Some("secret".into()),
        )
        .expect("single");
        assert_eq!(single.backend_id(), "remote:http://worker-a:8488");

        let pool = build_executor(
            ExecutorBackend::Remote,
            vec![
                "http://worker-a:8488".into(),
                "http://worker-b:8488".into(),
            ],
            Some("secret".into()),
        )
        .expect("pool");
        assert_eq!(pool.backend_id(), "remote-pool");
    }

    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
''',
)

# ---------------------------------------------------------------------------
# Documentation: precise claims only. Static configured pool is not a dynamic
# registry or resource-aware scheduler.
# ---------------------------------------------------------------------------
replace_once(
    "README.md",
    '''The current remote backend targets one configured worker endpoint; a
multi-worker placement scheduler is not claimed.
''',
    '''A single configured URL retains the original direct `RemoteExecutor`.
Repeating `--remote-worker-url` (or comma-separating
`SCIRUST_HUB_REMOTE_WORKER_URL`) enables a configured worker pool. The pool
queries every worker descriptor before dispatch, skips unavailable or
incompatible endpoints, rejects duplicate worker identities, and selects the
lowest local in-flight count with worker-id/endpoint tie-breaking. Once selected,
a run stays pinned to that worker; an ambiguous post-dispatch transport failure
is never replayed elsewhere. All configured workers currently share the same
environment-only worker bearer token.
''',
)
replace_once(
    "README.md",
    '''- The remote executor targets one configured worker endpoint; there is no
  multi-worker capability-aware placement scheduler yet.
''',
    '''- Multi-worker execution is a configured pool with descriptor discovery
  and deterministic local-load placement; there is not yet a dynamic worker
  registration/expiry service or resource-aware global scheduler.
''',
)
replace_once(
    "README.md",
    '''control-plane authentication and lifecycle-event cursor durability.
''',
    '''control-plane authentication, lifecycle-event cursor durability and
configured multi-worker placement/identity safety.
''',
)

replace_once(
    "CHANGELOG.md",
    '''### Added

''',
    '''### Added

- Configured multi-worker remote placement: repeating `--remote-worker-url`
  (or comma-separating `SCIRUST_HUB_REMOTE_WORKER_URL`) discovers compatible
  worker descriptors before dispatch and deterministically selects the lowest
  Hub-local in-flight target. Duplicate worker identities fail closed; once a
  target is selected there is no ambiguous post-dispatch failover. Per-run
  provenance records the concrete selected worker target while single-worker
  remote mode remains compatible.

''',
)

Path("docs/adr/0014-configured-multi-worker-placement.md").write_text(r'''# ADR 0014 — Configured multi-worker placement

Status: accepted

## Context

The first remote execution substrate targeted one configured worker endpoint.
That proved transport, lease identity, liveness, cancellation and duplicate
attempt/result semantics, but independent parallel workflow steps could not be
spread across workers.

A naive retry/load-balancer wrapper would be unsafe: if a lease-create request
reaches worker A but the response is lost, replaying the execution on worker B
can duplicate externally visible side effects.

## Decision

`--executor remote` accepts one or more configured worker URLs. One URL keeps
the original direct `RemoteExecutor`. Two or more URLs construct a
`RemotePoolExecutor` using the same worker bearer credential.

Before every dispatch the pool queries each worker descriptor and keeps only
workers that are reachable, authorized, protocol-compatible, advertise
`process.execute.v1`, and satisfy the configured transport-size contract.
Worker ids must be unique across eligible endpoints; duplicate identity fails
closed before any lease is created.

Placement is deterministic within one Hub process: choose the eligible endpoint
with the lowest current Hub-local in-flight count, then worker id and endpoint
as lexical tie-breakers. The local count is only a placement hint; it is not a
claim about global cluster load.

After selection, the invocation is pinned to that endpoint and the existing
lease protocol remains authoritative. The pool does **not** fail over an
ambiguous lease creation or active lease to a second worker.

The executor port gains an `ExecutionReport` wrapper with a default
implementation. Placement-aware executors override it to return the concrete
selected target. `RunOutcome.executor_backend` therefore records e.g.
`remote:worker-a@http://...` instead of only the generic pool identity. This is
per-invocation data, so no shared mutable "last worker" field is introduced.

## Security and limits

All configured workers currently share one `SCIRUST_HUB_REMOTE_WORKER_TOKEN`.
The pool does not weaken the existing transport boundary: bearer credentials
still require trusted TLS/tunnel protection on untrusted networks, and workers
remain process executors rather than OS sandboxes.

This is **not** dynamic worker registration, expiry/heartbeating at the pool
level, global capacity accounting, resource labels, or capability-aware
placement beyond the existing process-execution protocol capability. Those are
separate future scheduler features.
''')

# Reconcile report through the now-implemented configured pool while keeping
# the remaining dynamic-registry gap explicit.
report = Path("docs/AUTONOMOUS_IMPLEMENTATION_REPORT.md")
text = report.read_text()
text = text.replace(
    "Reconciled through: `main` @ `f7de2829af40f8f54bfc17cf5dfec573e4c3cbcf`",
    "Reconciled through: configured multi-worker placement working branch based on `main` @ `9a2f12ab91af9f551f5faa4efdd9b9c468fa7d66`",
)
text = text.replace(
    "This is a real remote execution substrate but **not yet a multi-worker\ncapability-aware placement scheduler**. One daemon remote backend is configured\nagainst one worker endpoint.",
    "The remote substrate now also supports a configured multi-worker pool with\npre-dispatch descriptor discovery, duplicate-identity rejection and deterministic\nHub-local least-in-flight placement. It is **not yet a dynamic worker registry or\nresource-aware global scheduler**; configured endpoints and one shared worker\ncredential remain operator supplied.",
)
text = text.replace(
    "- Remote execution currently targets one configured worker endpoint; there is\n  no trusted multi-worker scheduler/placement policy.",
    "- Multi-worker placement uses an operator-configured endpoint pool and Hub-local\n  in-flight counts; dynamic registration, expiry and global resource accounting do\n  not exist yet.",
)
text = text.replace(
    "- A multi-worker registry/placement scheduler is not implemented.",
    "- Dynamic worker registration/expiry and resource-aware global placement are\n  not implemented; the current multi-worker pool is statically configured.",
)
text = text.replace(
    "The next largest execution-scale gap is multi-worker discovery/placement. The\ncurrent remote backend deliberately targets one configured worker endpoint.\nAny expansion should preserve the existing `Executor` authority, lease/result\nidempotency and fail-closed evidence model rather than hiding distributed state\nbehind a best-effort load balancer. Capability/resource matching, worker\nregistration expiry and deterministic placement evidence should be designed as\nan explicit scheduler contract before implementation.",
    "After configured multi-worker placement, the remaining execution-scale gap is\ndynamic worker registration/expiry plus resource-aware global placement. Any\nfuture registry must preserve the current no-ambiguous-failover rule and record\nplacement evidence rather than turning worker discovery into a best-effort load\nbalancer. Worker liveness leases, resource/capability labels and deterministic\nselection policy should be explicit durable contracts before automatic cluster\nmembership is introduced.",
)
report.write_text(text)

print("configured multi-worker placement transformations complete")
