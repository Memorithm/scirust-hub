//! Shared guards for local process lifetime and output capture.
//!
//! These helpers deliberately fail closed when termination or pipe drainage
//! cannot be confirmed. They provide resource control only; they are not a
//! security sandbox.

use std::io::Read;
#[cfg(unix)]
use std::os::unix::process::CommandExt as _;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use hub_core::error::ExecutorFailure;

const GROUP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(1);
const PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const GROUP_POLL_INTERVAL: Duration = Duration::from_millis(10);

pub(crate) struct GuardedCommandOutput {
    pub(crate) status: ExitStatus,
    pub(crate) timed_out: bool,
    pub(crate) stdout: CapturedStream,
    pub(crate) stderr: CapturedStream,
}

/// Execute a preconstructed command under the existing process/capture guard.
/// The caller retains argv, environment and sandbox authority.
pub(crate) fn execute_guarded_command(
    command: &mut Command,
    deadline: Instant,
    capture_limit: usize,
) -> Result<GuardedCommandOutput, ExecutorFailure> {
    if Instant::now() >= deadline {
        return Err(ExecutorFailure::Backend {
            reason: "command deadline exhausted before spawn".into(),
        });
    }
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    #[cfg(unix)]
    command.process_group(0);
    let mut child = command.spawn().map_err(|error| ExecutorFailure::Backend {
        reason: format!("guarded command spawn failed: {error}"),
    })?;
    let stdout = spawn_capped_reader(child.stdout.take(), capture_limit);
    let stderr = spawn_capped_reader(child.stderr.take(), capture_limit);
    let mut timed_out = false;
    let terminal = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                break confirm_group_termination_after_parent_exit(
                    child.id(),
                    termination_confirmation_deadline(),
                )
                .map(|()| status)
            }
            Ok(None) if Instant::now() < deadline => thread::sleep(GROUP_POLL_INTERVAL),
            Ok(None) => {
                timed_out = true;
                break terminate_group_and_reap(&mut child);
            }
            Err(error) => {
                let _ = terminate_group_and_reap(&mut child);
                break Err(ExecutorFailure::Backend {
                    reason: format!("guarded command wait failed: {error}"),
                });
            }
        }
    };
    let captured = collect_captured_streams(stdout, stderr);
    let status = terminal?;
    let (stdout, stderr) = captured?;
    Ok(GuardedCommandOutput {
        status,
        timed_out,
        stdout,
        stderr,
    })
}

/// Signal the group, poll the direct child's reap and confirm group absence
/// under one deadline. Never perform an unbounded Child::wait after SIGKILL.
pub(crate) fn terminate_group_and_reap(child: &mut Child) -> Result<ExitStatus, ExecutorFailure> {
    let deadline = termination_confirmation_deadline();
    #[cfg(unix)]
    {
        use nix::{
            errno::Errno,
            sys::signal::{killpg, Signal},
            unistd::Pid,
        };
        let pid = i32::try_from(child.id())
            .map(Pid::from_raw)
            .map_err(|error| ExecutorFailure::Backend {
                reason: format!("invalid process-group identifier: {error}"),
            })?;
        match killpg(pid, Signal::SIGKILL) {
            Ok(()) | Err(Errno::ESRCH) => {}
            Err(error) => {
                return Err(ExecutorFailure::Backend {
                    reason: format!("termination unconfirmed: {error}"),
                })
            }
        }
    }
    #[cfg(not(unix))]
    child.kill().map_err(|error| ExecutorFailure::Backend {
        reason: format!("termination unconfirmed: {error}"),
    })?;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() < deadline => thread::sleep(GROUP_POLL_INTERVAL),
            Ok(None) => {
                return Err(ExecutorFailure::Backend {
                    reason: "reaping unconfirmed before deadline".into(),
                })
            }
            Err(error) => {
                return Err(ExecutorFailure::Backend {
                    reason: format!("reaping unconfirmed: {error}"),
                })
            }
        }
    };
    confirm_group_termination_after_parent_exit(child.id(), deadline)?;
    Ok(status)
}

#[derive(Debug)]
pub(crate) struct CapturedStream {
    pub(crate) bytes: Vec<u8>,
    pub(crate) truncated: bool,
}

pub(crate) struct CapturedPipe {
    receiver: Receiver<Result<CapturedStream, String>>,
}

pub(crate) fn termination_confirmation_deadline() -> Instant {
    Instant::now()
        .checked_add(GROUP_CONFIRM_TIMEOUT)
        .unwrap_or_else(Instant::now)
}

/// Kill any ordinary descendants that retained the process group's pipes and
/// confirm that the group no longer exists before capture is collected.
pub(crate) fn confirm_group_termination_after_parent_exit(
    child_id: u32,
    deadline: Instant,
) -> Result<(), ExecutorFailure> {
    #[cfg(unix)]
    {
        use nix::errno::Errno;
        use nix::sys::signal::{killpg, Signal};
        use nix::unistd::Pid;

        let process_group = i32::try_from(child_id)
            .map(Pid::from_raw)
            .map_err(|error| ExecutorFailure::Backend {
                reason: format!("invalid process-group identifier: {error}"),
            })?;

        match killpg(process_group, Signal::SIGKILL) {
            Ok(()) => {}
            Err(Errno::ESRCH) => return Ok(()),
            Err(error) => {
                return Err(ExecutorFailure::Backend {
                    reason: format!("process-group cleanup unconfirmed: {error}"),
                });
            }
        }

        loop {
            match killpg(process_group, Option::<Signal>::None) {
                Err(Errno::ESRCH) => return Ok(()),
                Ok(()) => {}
                Err(error) => {
                    return Err(ExecutorFailure::Backend {
                        reason: format!("process-group termination probe failed: {error}"),
                    });
                }
            }
            if Instant::now() >= deadline {
                return Err(ExecutorFailure::Backend {
                    reason: "process-group termination unconfirmed before deadline".to_owned(),
                });
            }
            thread::sleep(GROUP_POLL_INTERVAL);
        }
    }

    #[cfg(not(unix))]
    {
        let _ = (child_id, deadline);
        Ok(())
    }
}

/// Drains one pipe to EOF, keeping at most `cap` bytes. Excess is discarded
/// but still drained so the child cannot block on a full pipe.
pub(crate) fn spawn_capped_reader<R>(pipe: Option<R>, cap: usize) -> CapturedPipe
where
    R: Read + Send + 'static,
{
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut captured = CapturedStream {
            bytes: Vec::new(),
            truncated: false,
        };
        let result = if let Some(mut pipe) = pipe {
            let mut buf = [0_u8; 8192];
            loop {
                match pipe.read(&mut buf) {
                    Ok(0) => break Ok(captured),
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
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(error) => break Err(error.to_string()),
                }
            }
        } else {
            Ok(captured)
        };
        let _ = sender.send(result);
    });
    CapturedPipe { receiver }
}

pub(crate) fn collect_captured_streams(
    stdout: CapturedPipe,
    stderr: CapturedPipe,
) -> Result<(CapturedStream, CapturedStream), ExecutorFailure> {
    let deadline = Instant::now()
        .checked_add(PIPE_DRAIN_TIMEOUT)
        .unwrap_or_else(Instant::now);
    let stdout = stdout.receive_before(deadline, "stdout")?;
    let stderr = stderr.receive_before(deadline, "stderr")?;
    Ok((stdout, stderr))
}

impl CapturedPipe {
    fn receive_before(
        self,
        deadline: Instant,
        stream_name: &str,
    ) -> Result<CapturedStream, ExecutorFailure> {
        let remaining = deadline.saturating_duration_since(Instant::now());
        match self.receiver.recv_timeout(remaining) {
            Ok(Ok(captured)) => Ok(captured),
            Ok(Err(error)) => Err(ExecutorFailure::Backend {
                reason: format!("{stream_name} capture incomplete: {error}"),
            }),
            Err(RecvTimeoutError::Timeout) => Err(ExecutorFailure::Backend {
                reason: format!("{stream_name} capture incomplete before drain deadline"),
            }),
            Err(RecvTimeoutError::Disconnected) => Err(ExecutorFailure::Backend {
                reason: format!("{stream_name} capture incomplete: reader disconnected"),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn guarded_command_times_out_without_unbounded_wait() {
        let mut command = Command::new("sh");
        command.args(["-c", "exec sleep 30"]);
        let started = Instant::now();
        let output =
            execute_guarded_command(&mut command, started + Duration::from_millis(20), 64).unwrap();
        assert!(output.timed_out);
        assert!(!output.status.success());
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn detached_retained_pipe_fails_closed_with_bounded_drain() {
        let ready = std::env::temp_dir().join(format!(
            "hub-guard-ready-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut command = Command::new("sh");
        // Barrier after setsid, otherwise group cleanup can correctly kill the
        // descendant before it detaches and the fixture exercises no open pipe.
        // Trusted finite fixture; the detached pipe holder exits on its own.
        command
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("GUARD_READY", &ready);
        command.args(["-c", "setsid sh -c 'printf ready > \"$GUARD_READY\"; exec sleep 3' & while [ ! -e \"$GUARD_READY\" ]; do sleep 0.01; done; printf fixture; exit 0"]);
        let started = Instant::now();
        let result = execute_guarded_command(&mut command, started + Duration::from_secs(5), 64);
        assert!(started.elapsed() < Duration::from_secs(3));
        assert!(
            matches!(result, Err(ExecutorFailure::Backend { reason }) if reason.contains("capture incomplete"))
        );
        std::fs::remove_file(ready).unwrap();
    }

    #[test]
    fn capture_wait_is_bounded_and_reports_incomplete_stream() {
        let (sender, receiver) = mpsc::sync_channel(1);
        let capture = CapturedPipe { receiver };
        let started = Instant::now();
        let error = capture
            .receive_before(Instant::now() + Duration::from_millis(20), "stdout")
            .expect_err("open sender must not look like a complete capture");
        drop(sender);

        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(matches!(
            error,
            ExecutorFailure::Backend { reason }
                if reason.contains("stdout capture incomplete before drain deadline")
        ));
    }
}
