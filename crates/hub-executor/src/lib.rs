//! Executor backends implementing the [`Executor`](hub_core::Executor) port.
//!
//! - [`ProcessExecutor`]: supervised local subprocess execution with hard
//!   output caps, wall-clock timeouts, cooperative cancellation and an
//!   explicitly constructed environment. **This is resource control, not a
//!   security sandbox**: children run with the Hub's OS privileges.
//! - [`MockExecutor`]: scripted deterministic outcomes for tests.

use std::collections::VecDeque;
use std::io::Read;
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use hub_core::error::ExecutorFailure;
use hub_core::exec::{
    CancelToken, ExecutionOutcome, ExecutionRequest, Executor,
};

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

/// Result of one capped stream reader thread.
struct CapturedStream {
    bytes: Vec<u8>,
    truncated: bool,
}

impl Executor for ProcessExecutor {
    fn backend_id(&self) -> &str {
        "process"
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

        // Spawn failure (missing program, permission denied, bad cwd) is an
        // observable outcome recorded in provenance, not a backend panic.
        let Ok(mut child) = command.spawn() else {
            return Ok(ExecutionOutcome {
                exit_code: None,
                signal: None,
                timed_out: false,
                cancelled: false,
                start_error: Some(format!("spawn failed: {command:?}")),
                duration_ms: ms_since(started),
                stdout: Vec::new(),
                stdout_truncated: false,
                stderr: Vec::new(),
                stderr_truncated: false,
            });
        };

        let stdout_pipe = child.stdout.take();
        let stderr_pipe = child.stderr.take();
        let stdout_handle =
            spawn_capped_reader(stdout_pipe, request.max_capture_bytes_per_stream);
        let stderr_handle =
            spawn_capped_reader(stderr_pipe, request.max_capture_bytes_per_stream);

        let deadline = started
            .checked_add(Duration::from_millis(request.timeout_ms))
            .unwrap_or_else(|| started.checked_add(Duration::from_secs(u64::MAX >> 2)).expect("sane"));

        let mut timed_out = false;
        let mut cancelled = false;
        let status = loop {
            match child.try_wait() {
                Err(io_error) => {
                    return Err(ExecutorFailure::Backend {
                        reason: format!("wait failed: {io_error}"),
                    });
                },
                Ok(Some(status)) => break Some(status),
                Ok(None) => {},
            }
            if cancel.is_cancelled() {
                cancelled = true;
                let _ = child.kill();
                break child.wait().ok();
            }
            if Instant::now() >= deadline {
                timed_out = true;
                let _ = child.kill();
                break child.wait().ok();
            }
            thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        };

        let stdout = stdout_handle
            .join()
            .unwrap_or_else(|_| CapturedStream { bytes: Vec::new(), truncated: false });
        let stderr = stderr_handle
            .join()
            .unwrap_or_else(|_| CapturedStream { bytes: Vec::new(), truncated: false });

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

/// Drains one pipe to EOF, keeping at most `cap` bytes (excess is discarded
/// but still drained so the child never blocks on a full pipe).
fn spawn_capped_reader<R>(pipe: Option<R>, cap: usize) -> thread::JoinHandle<CapturedStream>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut captured = CapturedStream {
            bytes: Vec::new(),
            truncated: false,
        };
        let Some(mut pipe) = pipe else {
            return captured;
        };
        let mut buf = [0u8; 8192];
        loop {
            match pipe.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => {
                    if captured.bytes.len() < cap {
                        let remaining = cap - captured.bytes.len();
                        let take = n.min(remaining);
                        captured.bytes.extend_from_slice(&buf[..take]);
                        if take < n {
                            captured.truncated = true;
                        }
                    } else {
                        captured.truncated = true;
                    }
                    // Continue draining regardless of truncation state.
                },
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                // Reader-side errors (broken pipe during kill) end capture.
                Err(_) => break,
            }
        }
        captured
    })
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
            .execute(&base_request("echo", &["hello", "hub"]), &CancelToken::new())
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
            .execute(&base_request("echo", &["$HOME ; rm -rf /"]), &CancelToken::new())
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
        let mut request = base_request("dd", &[
            "if=/dev/zero",
            "bs=1024",
            "count=64", // 64 KiB total
        ]);
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

    #[test]
    fn missing_program_is_an_observed_start_error() {
        let exec = ProcessExecutor::new();
        let mut request = base_request("echo", &[]);
        request.program = "/nonexistent/hub-no-such-binary".to_owned();
        let outcome = exec.execute(&request, &CancelToken::new()).expect("run");
        assert!(!outcome.exited_cleanly());
        assert!(outcome.start_error.is_some());
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
        let first = exec.execute(&base_request("x", &[]), &cancel).expect("first");
        assert_eq!(first.exit_code, Some(3));
        let second = exec.execute(&base_request("x", &[]), &cancel).expect("second");
        assert_eq!(second.stdout, b"second");
        let third = exec.execute(&base_request("x", &[]), &cancel).expect("third");
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
