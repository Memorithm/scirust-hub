//! Executor backends implementing the [`Executor`] port from `hub-core`.
//!
//! - [`ProcessExecutor`]: supervised local subprocess execution with hard
//!   output caps, wall-clock timeouts, cooperative cancellation and an
//!   explicitly constructed environment. On Unix each execution is isolated
//!   into its own process group so ordinary descendants are terminated with
//!   the group on timeout/cancellation. **This is resource control, not a
//!   security sandbox**: children run with the Hub's OS privileges.
//! - [`MockExecutor`]: scripted deterministic outcomes for tests.
//! - [`RemoteExecutor`]: authenticated lease-based execution on one worker.
//! - [`RemotePoolExecutor`]: deterministic pre-dispatch placement across a
//!   configured set of workers, with no unsafe post-dispatch failover.

mod execution_guard;
pub mod local_capacity;
pub mod pool;
pub mod remote;
pub mod worker;
pub mod workspace;

pub use local_capacity::LocalCapacityExecutor;
pub use pool::RemotePoolExecutor;
pub use remote::RemoteExecutor;
pub use workspace::{
    digest_materialized_checkout, GitWorkspaceMaterializer, MaterializedWorkspace,
    WorkspaceSourceMap,
};

use std::collections::VecDeque;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use execution_guard::{
    collect_captured_streams, confirm_group_termination_after_parent_exit, spawn_capped_reader,
    termination_confirmation_deadline,
};

use hub_core::error::ExecutorFailure;
use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};
use hub_core::{IsolationLevel, ResourceEnforcement, SandboxBackendDescriptor};

/// Poll interval while waiting on the child.
const POLL_INTERVAL_MS: u64 = 10;

/// Local subprocess backend. See crate docs for scope honesty.
#[derive(Clone, Copy, Debug)]
pub struct ProcessExecutor;

impl ProcessExecutor {
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl Default for ProcessExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for ProcessExecutor {
    fn backend_id(&self) -> &str {
        "process"
    }

    fn backend_descriptor(&self) -> SandboxBackendDescriptor {
        SandboxBackendDescriptor {
            backend_id: self.backend_id().to_owned(),
            isolation: IsolationLevel::Process,
            isolation_qualified: false,
            isolation_evidence_id: None,
            enforces_network_policy: false,
            enforces_default_deny_network: false,
            enforces_workspace_write_policy: false,
            resources: ResourceEnforcement {
                wall_clock_ms: true,
                ..ResourceEnforcement::default()
            },
            capabilities: Default::default(),
        }
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        let started = Instant::now();
        let mut command = Command::new(&request.program);
        command
            .args(&request.args)
            .current_dir(&request.working_dir)
            .env_clear()
            .envs(request.env.iter())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        // On Unix the direct child becomes leader of a fresh process group.
        // Descendants inherit that group unless they deliberately detach, so
        // timeout/cancellation can terminate the ordinary execution subtree
        // without signalling the Hub's own process group.
        #[cfg(unix)]
        command.process_group(0);

        // Spawn failure (missing program, permission denied, bad cwd) is an
        // observable outcome recorded in provenance, not a backend panic.
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(io_error) => {
                return Ok(ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: false,
                    start_error: Some(sanitized_spawn_error(request, &io_error)),
                    duration_ms: ms_since(started),
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                });
            }
        };

        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();
        let stdout_handle = spawn_capped_reader(stdout_pipe, request.max_capture_bytes_per_stream);
        let stderr_handle = spawn_capped_reader(stderr_pipe, request.max_capture_bytes_per_stream);

        let deadline = started
            .checked_add(Duration::from_millis(request.timeout_ms))
            .unwrap_or_else(|| {
                started
                    .checked_add(Duration::from_secs(u64::MAX >> 2))
                    .expect("sane")
            });

        let mut timed_out = false;
        let mut cancelled = false;
        let mut terminal_failure = None;
        let mut parent_exited_normally = false;
        let status = loop {
            match child.try_wait() {
                Err(io_error) => {
                    terminal_failure = Some(ExecutorFailure::Backend {
                        reason: format!("wait failed: {io_error}"),
                    });
                    let _ = terminate_supervised_process(&mut child);
                    break None;
                }
                Ok(Some(status)) => {
                    parent_exited_normally = true;
                    break Some(status);
                }
                Ok(None) => {}
            }
            if cancel.is_cancelled() {
                cancelled = true;
                match terminate_supervised_process(&mut child) {
                    Ok(status) => break Some(status),
                    Err(error) => {
                        terminal_failure = Some(error);
                        break None;
                    }
                }
            }
            if Instant::now() >= deadline {
                timed_out = true;
                match terminate_supervised_process(&mut child) {
                    Ok(status) => break Some(status),
                    Err(error) => {
                        terminal_failure = Some(error);
                        break None;
                    }
                }
            }
            thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        };

        // A parent can exit while an ordinary descendant keeps the inherited
        // stdout/stderr descriptors open. Kill and confirm the process group
        // before awaiting readers so capture cannot outlive its own deadline.
        if parent_exited_normally {
            if let Err(error) = confirm_group_termination_after_parent_exit(
                child.id(),
                termination_confirmation_deadline(),
            ) {
                terminal_failure = Some(error);
            }
        }

        let captured = collect_captured_streams(stdout_handle, stderr_handle);
        if let Some(error) = terminal_failure {
            return Err(error);
        }
        let (stdout, stderr) = captured?;

        Ok(ExecutionOutcome {
            exit_code: status.as_ref().and_then(|s| s.code()),
            signal: signal_of(status.as_ref()),
            timed_out,
            cancelled,
            start_error: None,
            duration_ms: ms_since(started),
            stdout: stdout.bytes,
            stdout_truncated: stdout.truncated,
            stderr: stderr.bytes,
            stderr_truncated: stderr.truncated,
        })
    }
}

fn sanitized_spawn_error(request: &ExecutionRequest, io_error: &std::io::Error) -> String {
    let executable = std::path::Path::new(&request.program)
        .file_name()
        .filter(|name| !name.is_empty())
        .map_or_else(
            || "<unidentified>".to_owned(),
            |name| name.to_string_lossy().into_owned(),
        );
    let environment_names: Vec<&str> = request.env.keys().map(String::as_str).collect();
    format!(
        "spawn failed for executable {executable:?}; environment names: {environment_names:?}; OS error: {io_error}"
    )
}

fn terminate_supervised_process(
    child: &mut Child,
) -> Result<std::process::ExitStatus, ExecutorFailure> {
    let child_id = child.id();
    #[cfg(unix)]
    let termination = {
        use nix::errno::Errno;
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;

        let result = i32::try_from(child.id())
            .map_err(std::io::Error::other)
            .and_then(|pid| match killpg(Pid::from_raw(pid), Signal::SIGKILL) {
                Ok(()) | Err(Errno::ESRCH) => Ok(()),
                Err(error) => Err(std::io::Error::from_raw_os_error(error as i32)),
            });
        if result.is_err() {
            // Best-effort cleanup does not prove the whole group terminated.
            let _ = child.kill();
            let _ = child.try_wait();
        }
        result
    };
    #[cfg(not(unix))]
    let termination = child.kill();
    let status = confirm_termination(termination, || child.wait())?;
    confirm_group_termination_after_parent_exit(child_id, termination_confirmation_deadline())?;
    Ok(status)
}

fn confirm_termination(
    termination: std::io::Result<()>,
    reap: impl FnOnce() -> std::io::Result<std::process::ExitStatus>,
) -> Result<std::process::ExitStatus, ExecutorFailure> {
    termination.map_err(|error| ExecutorFailure::Backend {
        reason: format!("termination unconfirmed: {error}"),
    })?;
    reap().map_err(|error| ExecutorFailure::Backend {
        reason: format!("reaping unconfirmed: {error}"),
    })
}

fn ms_since(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

#[cfg(unix)]
fn signal_of(status: Option<&std::process::ExitStatus>) -> Option<i32> {
    use std::os::unix::process::ExitStatusExt as _;
    status.and_then(|s| s.signal())
}

#[cfg(not(unix))]
fn signal_of(_status: Option<&std::process::ExitStatus>) -> Option<i32> {
    None
}

/// Deterministic scripted executor for tests: pops queued outcomes, falling
/// back to a clean empty success when the script runs dry.
#[derive(Clone, Debug, Default)]
pub struct MockExecutor {
    script: Arc<Mutex<VecDeque<ExecutionOutcome>>>,
}

impl MockExecutor {
    #[must_use]
    pub fn new(script: Vec<ExecutionOutcome>) -> Self {
        Self {
            script: Arc::new(Mutex::new(script.into())),
        }
    }

    /// A clean outcome with the given stdout.
    #[must_use]
    pub fn success(stdout: &[u8]) -> ExecutionOutcome {
        ExecutionOutcome {
            exit_code: Some(0),
            signal: None,
            timed_out: false,
            cancelled: false,
            start_error: None,
            duration_ms: 1,
            stdout: stdout.to_vec(),
            stdout_truncated: false,
            stderr: Vec::new(),
            stderr_truncated: false,
        }
    }

    /// A failed outcome (non-zero exit) with the given stderr.
    #[must_use]
    pub fn failure(exit_code: i32, stderr: &[u8]) -> ExecutionOutcome {
        ExecutionOutcome {
            exit_code: Some(exit_code),
            signal: None,
            timed_out: false,
            cancelled: false,
            start_error: None,
            duration_ms: 1,
            stdout: Vec::new(),
            stdout_truncated: false,
            stderr: stderr.to_vec(),
            stderr_truncated: false,
        }
    }
}

impl Executor for MockExecutor {
    fn backend_id(&self) -> &str {
        "mock"
    }

    fn execute(
        &self,
        _request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        if cancel.is_cancelled() {
            return Ok(cancelled_outcome());
        }
        let next = self
            .script
            .lock()
            .map(|mut q| q.pop_front())
            .unwrap_or(None);
        Ok(next.unwrap_or_else(|| MockExecutor::success(b"")))
    }
}

/// The canonical cancelled outcome used by backends and tests alike.
#[must_use]
pub fn cancelled_outcome() -> ExecutionOutcome {
    ExecutionOutcome {
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
    }
}

#[cfg(test)]
mod tests {
    //! These tests exercise real processes using standard tools resolved from
    //! PATH; they stay hermetic (no network, no GPU).

    use super::*;

    fn task_for_process_budget() -> hub_core::TaskSpec {
        let id = hub_core::TaskId::generate();
        hub_core::TaskSpec {
            schema_version: hub_core::TASK_SPEC_SCHEMA_VERSION,
            id,
            identity: hub_core::TaskIdentity {
                task_id: id,
                principal: format!("task://memorithm/executor/{id}"),
            },
            workspace: hub_core::WorkspaceSpec::default(),
            capabilities: hub_core::CapabilitySet::default(),
            budget: hub_core::ResourceBudget {
                wall_clock_ms: Some(30_000),
                ..hub_core::ResourceBudget::default()
            },
            sandbox: hub_core::SandboxRequirements {
                minimum_isolation: hub_core::IsolationLevel::Process,
                network: hub_core::NetworkPolicy {
                    default_deny: false,
                    allowed_endpoints: Vec::new(),
                },
                writable_workspace: true,
            },
        }
    }

    #[test]
    fn task_aware_process_execution_admits_only_enforceable_dimensions() {
        let exec = ProcessExecutor::new();
        let task = task_for_process_budget();
        let outcome = exec
            .execute_task_report(
                &task,
                &base_request("echo", &["task-aware"]),
                &CancelToken::new(),
            )
            .expect("wall-clock-only task is admissible");
        assert!(outcome.execution.outcome.exited_cleanly());
        assert_eq!(
            outcome.admitted_backend.resources,
            hub_core::ResourceEnforcement {
                wall_clock_ms: true,
                ..hub_core::ResourceEnforcement::default()
            }
        );

        let mut memory_task = task;
        memory_task.budget.memory_bytes = Some(1024);
        let error = exec
            .execute_task_report(&memory_task, &base_request("x", &[]), &CancelToken::new())
            .expect_err("memory budget must fail before process dispatch");
        assert!(matches!(
            error,
            ExecutorFailure::Backend { reason } if reason.contains("memory_bytes")
        ));
    }

    #[test]
    fn task_aware_process_execution_rejects_read_only_workspace_policy() {
        let exec = ProcessExecutor::new();
        let mut task = task_for_process_budget();
        task.sandbox.writable_workspace = false;
        let error = exec
            .execute_task_report(&task, &base_request("x", &[]), &CancelToken::new())
            .expect_err("plain process cannot enforce read-only workspace");
        assert!(matches!(
            error,
            ExecutorFailure::Backend { reason } if reason.contains("read-only workspace")
        ));
    }

    #[test]
    fn termination_failure_does_not_wait_or_report_completion() {
        let result = confirm_termination(
            Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "kill denied",
            )),
            || panic!("must not block waiting after unsuccessful termination"),
        );
        assert!(
            matches!(result, Err(ExecutorFailure::Backend { reason }) if reason.contains("termination unconfirmed"))
        );
    }

    #[test]
    fn reap_failure_does_not_report_completion() {
        let result = confirm_termination(Ok(()), || Err(std::io::Error::other("wait failed")));
        assert!(
            matches!(result, Err(ExecutorFailure::Backend { reason }) if reason.contains("reaping unconfirmed"))
        );
    }
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn base_request(program: &str, args: &[&str]) -> ExecutionRequest {
        ExecutionRequest {
            program: if program == "x" {
                // Literal placeholder for backends that never spawn.
                "x".to_owned()
            } else {
                resolve(program)
            },
            args: args.iter().map(|s| (*s).to_owned()).collect(),
            working_dir: std::env::temp_dir(),
            env: BTreeMap::from([("PATH".to_owned(), path_env())]),
            timeout_ms: 30_000,
            max_capture_bytes_per_stream: 1024 * 1024,
        }
    }

    fn path_env() -> String {
        std::env::var("PATH").unwrap_or_else(|_| "/usr/bin:/bin".to_owned())
    }

    /// Resolve a tool name against PATH like a shell would, returning an
    /// absolute path so tests do not depend on cwd.
    fn resolve(tool: &str) -> String {
        for dir in std::env::split_paths(&path_env()) {
            let candidate = dir.join(tool);
            if candidate.is_file() {
                return candidate.display().to_string();
            }
        }
        panic!("test tool {tool:?} not found on PATH");
    }

    fn temp_workdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("hub-exec-{tag}-{}", uuid_like()));
        std::fs::create_dir_all(&dir).expect("mkdir");
        dir
    }

    fn uuid_like() -> String {
        format!(
            "{:x}{:x}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            std::process::id()
        )
    }

    #[test]
    fn captures_stdout_and_exit_code() {
        let exec = ProcessExecutor::new();
        let outcome = exec
            .execute(
                &base_request("echo", &["hello", "hub"]),
                &CancelToken::new(),
            )
            .expect("execute");
        assert!(outcome.exited_cleanly());
        assert_eq!(String::from_utf8_lossy(&outcome.stdout), "hello hub\n");
        assert!(outcome.stderr.is_empty());
    }

    #[test]
    fn argv_is_passed_verbatim_without_shell_interpretation() {
        let exec = ProcessExecutor::new();
        // If this ever goes through a shell, `$HOME` would expand.
        let outcome = exec
            .execute(
                &base_request("echo", &["$HOME ; rm -rf /"]),
                &CancelToken::new(),
            )
            .expect("execute");
        assert!(outcome.exited_cleanly());
        assert_eq!(
            String::from_utf8_lossy(&outcome.stdout),
            "$HOME ; rm -rf /\n"
        );
    }

    #[test]
    fn environment_is_exactly_what_the_request_specifies() {
        let exec = ProcessExecutor::new();
        let workdir = temp_workdir("env");
        let mut request = base_request("env", &["-0"]);
        request.working_dir = workdir.clone();
        request.env = BTreeMap::from([
            ("PATH".to_owned(), path_env()),
            ("HUB_TEST_MARKER".to_owned(), "present".to_owned()),
        ]);
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(outcome.exited_cleanly());
        let text = String::from_utf8_lossy(&outcome.stdout);
        assert!(text.contains("HUB_TEST_MARKER=present"));
        // Nothing else leaks in: HOME/SHELL/etc. are absent.
        assert!(!text.contains("\0HOME="));
        drop(request);
        let _ = std::fs::remove_dir_all(workdir);
    }

    #[test]
    fn working_directory_is_applied() {
        let exec = ProcessExecutor::new();
        let workdir = temp_workdir("cwd");
        let mut request = base_request("pwd", &[]);
        request.working_dir = workdir.clone();
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(outcome.exited_cleanly());
        let reported = PathBuf::from(String::from_utf8_lossy(&outcome.stdout).trim());
        assert_eq!(
            reported.canonicalize().expect("canonicalize"),
            workdir.canonicalize().expect("canonicalize")
        );
        let _ = std::fs::remove_dir_all(workdir);
    }

    #[test]
    fn oversized_output_is_truncated_but_drained() {
        let exec = ProcessExecutor::new();
        let mut request = base_request(
            "dd",
            &[
                "if=/dev/zero",
                "bs=1024",
                "count=64", // 64 KiB total
            ],
        );
        request.max_capture_bytes_per_stream = 4 * 1024;
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(outcome.exited_cleanly());
        assert!(outcome.stdout_truncated);
        assert_eq!(outcome.stdout.len(), 4 * 1024);
    }

    #[test]
    fn timeout_kills_the_child_and_is_reported() {
        let exec = ProcessExecutor::new();
        let mut request = base_request("sleep", &["30"]);
        request.timeout_ms = 200;
        let started = Instant::now();
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(outcome.timed_out);
        assert!(!outcome.exited_cleanly());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn cancellation_stops_a_running_child() {
        let exec = ProcessExecutor::new();
        let mut request = base_request("sleep", &["30"]);
        request.timeout_ms = 60_000;
        let cancel = CancelToken::new();
        let cancel_for_thread = cancel.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(150));
            cancel_for_thread.cancel();
        });
        let started = Instant::now();
        let outcome = exec.execute(&request, &cancel).expect("run");
        assert!(outcome.cancelled);
        assert!(!outcome.exited_cleanly());
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_kills_ordinary_descendants_in_process_group() {
        use nix::errno::Errno;
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::{getpgid, Pid};

        let exec = ProcessExecutor::new();
        let workdir = temp_workdir("cancel-group");
        let pid_file = workdir.join("pids");
        let mut request = base_request("sh", &[]);
        request.working_dir = workdir.clone();
        request.args = vec![
            "-c".to_owned(),
            r#"sleep 30 & printf '%s %s\n' "$$" "$!" > "$1"; wait"#.to_owned(),
            "hub-process-group-test".to_owned(),
            pid_file.display().to_string(),
        ];
        request.timeout_ms = 60_000;

        let cancel = CancelToken::new();
        let watcher_cancel = cancel.clone();
        let watcher_pid_file = pid_file.clone();
        let watcher = thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Ok(text) = std::fs::read_to_string(&watcher_pid_file) {
                    let mut fields = text.split_whitespace();
                    if let (Some(parent), Some(descendant)) = (fields.next(), fields.next()) {
                        let parent: i32 = parent.parse().expect("parent pid");
                        let descendant: i32 = descendant.parse().expect("descendant pid");
                        assert!(parent > 1 && descendant > 1);
                        assert_eq!(
                            getpgid(Some(Pid::from_raw(descendant))).expect("descendant pgid"),
                            Pid::from_raw(parent),
                            "descendant must inherit the executor-created process group"
                        );
                        watcher_cancel.cancel();
                        return parent;
                    }
                }
                assert!(
                    Instant::now() < deadline,
                    "child did not publish process-group membership"
                );
                thread::sleep(Duration::from_millis(10));
            }
        });

        let started = Instant::now();
        let outcome = exec.execute(&request, &cancel).expect("run");
        let process_group = watcher.join().expect("watcher");
        assert!(outcome.cancelled);
        assert!(started.elapsed() < Duration::from_secs(5));

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match killpg(Pid::from_raw(process_group), Option::<Signal>::None) {
                Err(Errno::ESRCH) => break,
                Ok(()) => {}
                Err(error) => panic!("checking execution process group: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "execution process group survived cancellation"
            );
            thread::sleep(Duration::from_millis(10));
        }
        let _ = std::fs::remove_dir_all(workdir);
    }

    #[cfg(unix)]
    #[test]
    fn parent_exit_cleans_descendants_before_bounded_pipe_drain() {
        let exec = ProcessExecutor::new();
        let mut request = base_request("sh", &[]);
        request.args = vec![
            "-c".to_owned(),
            "sleep 30 & printf 'parent-done\\n'".to_owned(),
        ];
        request.timeout_ms = 10_000;

        let started = Instant::now();
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");

        assert!(outcome.exited_cleanly());
        assert_eq!(outcome.stdout, b"parent-done\n");
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn missing_program_is_an_observed_start_error() {
        let exec = ProcessExecutor::new();
        let mut request = base_request("echo", &[]);
        request.program = "/nonexistent/hub-no-such-binary".to_owned();
        request.args = vec!["ARG_SECRET_CANARY".to_owned()];
        request
            .env
            .insert("HUB_SECRET_NAME".to_owned(), "ENV_SECRET_CANARY".to_owned());
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(!outcome.exited_cleanly());
        let start_error = outcome.start_error.expect("start error");
        assert!(start_error.contains("hub-no-such-binary"));
        assert!(start_error.contains("HUB_SECRET_NAME"));
        assert!(start_error.contains("OS error"));
        assert!(!start_error.contains("ARG_SECRET_CANARY"));
        assert!(!start_error.contains("ENV_SECRET_CANARY"));
        assert!(!start_error.contains("/nonexistent"));
        assert!(outcome.exit_code.is_none());
    }

    #[test]
    fn nonzero_exit_is_an_outcome_not_a_backend_error() {
        let exec = ProcessExecutor::new();
        let outcome = exec
            .execute(&base_request("false", &[]), &CancelToken::new())
            .expect("execute");
        assert_eq!(outcome.exit_code, Some(1));
        assert!(!outcome.exited_cleanly());
    }

    #[test]
    fn mock_executor_follows_script_then_defaults_to_success() {
        let exec = MockExecutor::new(vec![
            MockExecutor::failure(3, b"boom"),
            MockExecutor::success(b"second"),
        ]);
        let cancel = CancelToken::new();
        let first = exec
            .execute(&base_request("x", &[]), &cancel)
            .expect("first");
        assert_eq!(first.exit_code, Some(3));
        let second = exec
            .execute(&base_request("x", &[]), &cancel)
            .expect("second");
        assert_eq!(second.stdout, b"second");
        let third = exec
            .execute(&base_request("x", &[]), &cancel)
            .expect("third");
        assert!(third.exited_cleanly());
    }

    #[test]
    fn mock_executor_honours_cancel_token() {
        let exec = MockExecutor::default();
        let cancel = CancelToken::new();
        cancel.cancel();
        let outcome = exec.execute(&base_request("x", &[]), &cancel).expect("run");
        assert!(outcome.cancelled);
    }
}
