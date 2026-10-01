//! Shared guards for local process lifetime and output capture.
//!
//! These helpers deliberately fail closed when termination or pipe drainage
//! cannot be confirmed. They provide resource control only; they are not a
//! security sandbox.

use std::io::Read;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};

use hub_core::error::ExecutorFailure;

const GROUP_CONFIRM_TIMEOUT: Duration = Duration::from_secs(1);
const PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(1);
const GROUP_POLL_INTERVAL: Duration = Duration::from_millis(10);

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
