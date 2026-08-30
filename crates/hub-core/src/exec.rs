//! The executor port and its data contracts.
//!
//! The port is synchronous by design (see `docs/adr/0004-execution-model.md`):
//! subprocess supervision with deadlines maps naturally onto blocking calls,
//! the domain stays runtime-free, and async callers offload via
//! `spawn_blocking`. Future remote backends can wrap their own IO internally.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::ExecutorFailure;

/// Cooperative cancellation flag shared between the caller and executor.
#[derive(Clone, Debug, Default)]
pub struct CancelToken(Arc<AtomicBool>);

impl CancelToken {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests cancellation; executors observe this between polls.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Fully resolved, structured execution request. No shell, ever: `program`
/// plus argv are passed to the OS directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionRequest {
    pub program: String,
    pub args: Vec<String>,
    pub working_dir: std::path::PathBuf,
    /// Explicit environment for the child; nothing else is inherited.
    pub env: BTreeMapEnv,
    pub timeout_ms: u64,
    /// Hard cap per captured stream; excess bytes are truncated and flagged.
    pub max_capture_bytes_per_stream: usize,
}

/// Wrapper newtype only to keep the request's env map ordered in debug output
/// without leaking BTreeMap into public signatures elsewhere.
pub type BTreeMapEnv = std::collections::BTreeMap<String, String>;

/// What an executor observed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionOutcome {
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub cancelled: bool,
    /// Set when the program could not be started at all (spawn failure);
    /// provenance records this verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_error: Option<String>,
    pub duration_ms: u64,
    pub stdout: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr: Vec<u8>,
    pub stderr_truncated: bool,
}

impl ExecutionOutcome {
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
pub trait Executor: Send + Sync {
    /// Stable identifier recorded in run provenance.
    fn backend_id(&self) -> &str;

    /// Executes one request to completion (success, failure, timeout or
    /// cancellation). Implementations must respect `cancel` between polls and
    /// enforce the requested timeout themselves.
    ///
    /// # Errors
    /// [`ExecutorFailure`] distinguishes timeout/cancel/start/backend errors;
    /// a non-zero exit code is *not* an error here — it is observable through
    /// the outcome.
    fn execute(
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
        self.execute(request, cancel)
            .map(|outcome| ExecutionReport {
                outcome,
                backend_id: self.backend_id().to_owned(),
            })
    }
}
