from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:180]!r}")
    p.write_text(text.replace(old, new, 1))

replace_once(
    "crates/hub-executor/src/remote.rs",
    '''use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};''',
    '''use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};''',
)

replace_once(
    "crates/hub-executor/src/remote.rs",
    '''impl Executor for RemoteExecutor {
    fn backend_id(&self) -> &str {
        &self.backend_id
    }

    fn execute(''',
    '''impl Executor for RemoteExecutor {
    fn backend_id(&self) -> &str {
        &self.backend_id
    }

    fn execute_report(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionReport, ExecutorFailure> {
        let descriptor = match self.discover_eligible() {
            Ok(descriptor) => descriptor,
            Err(_) => {
                return self.execute(request, cancel).map(|outcome| ExecutionReport {
                    outcome,
                    backend_id: self.backend_id.clone(),
                });
            }
        };
        let worker_id = descriptor.worker_id;
        let backend_id = format!("remote:{worker_id}@{}", self.endpoint);
        self.clone()
            .with_expected_worker_id(worker_id)
            .execute(request, cancel)
            .map(|outcome| ExecutionReport {
                outcome,
                backend_id,
            })
    }

    fn execute(''',
)

replace_once(
    "crates/hub-executor/tests/remote_worker.rs",
    '''    let outcome = tokio::task::spawn_blocking(move || {
        exec.execute(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(outcome.exited_cleanly(), "{outcome:?}");
''',
    '''    let report = tokio::task::spawn_blocking(move || {
        exec.execute_report(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(report.outcome.exited_cleanly(), "{:?}", report.outcome);
    assert!(report.backend_id.contains("remote:worker-e2e@"));
''',
)

replace_once(
    "docs/adr/0014-configured-multi-worker-placement.md",
    '''The executor port gains an `ExecutionReport` wrapper with a default
implementation. Placement-aware executors override it to return the concrete
selected target. `RunOutcome.executor_backend` therefore records e.g.
`remote:worker-a@http://...` instead of only the generic pool identity. This is
per-invocation data, so no shared mutable "last worker" field is introduced.
''',
    '''The executor port gains an `ExecutionReport` wrapper with a default
implementation. Remote executors override it to return the concrete observed
worker target. `RunOutcome.executor_backend` therefore records e.g.
`remote:worker-a@http://...` for both direct and pooled remote execution once a
worker descriptor has been authenticated. Failures that occur before a worker
can be identified retain only the configured endpoint. This is per-invocation
data, so no shared mutable "last worker" field is introduced.
''',
)

print("single-worker remote provenance reporting applied")
