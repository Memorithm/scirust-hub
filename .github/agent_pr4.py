from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


Path("crates/hub-protocol/src/distributed.rs").write_text(r'''//! Versioned wire contract for the first remote process-execution substrate.
//!
//! The protocol deliberately transports immutable file bytes and workdir-
//! relative paths. It never assumes that Hub and a worker share a filesystem.

use std::collections::BTreeMap;

use hub_core::exec::ExecutionOutcome;
use serde::{Deserialize, Serialize};

/// Wire version for Hub <-> worker execution leases.
pub const WORKER_PROTOCOL_VERSION: u16 = 1;
/// Capability required by the v1 remote process executor.
pub const PROCESS_EXECUTION_CAPABILITY: &str = "process.execute.v1";
/// Token used inside argv/env values for paths rooted in the transported workdir.
pub const WORKDIR_TOKEN: &str = "{hub-workdir}";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerDescriptor {
    pub protocol_version: u16,
    pub worker_id: String,
    pub capabilities: Vec<String>,
    pub max_payload_bytes: u64,
    pub heartbeat_interval_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteFile {
    /// UTF-8, workdir-relative path. Receivers must reject absolute paths and
    /// parent traversal before materialization.
    pub path: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteExecutionRequest {
    pub program: String,
    pub args: Vec<String>,
    pub env: BTreeMap<String, String>,
    pub timeout_ms: u64,
    pub max_capture_bytes_per_stream: usize,
    pub files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCreateRequest {
    pub protocol_version: u16,
    /// Stable for retries of one executor invocation. Reusing this id with a
    /// different payload is a protocol conflict.
    pub attempt_id: String,
    pub execution: RemoteExecutionRequest,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseCreateResponse {
    pub lease_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub state: LeaseState,
    pub expires_at_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Queued,
    Running,
    Completed,
    Cancelled,
    Failed,
}

impl LeaseState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Cancelled | Self::Failed)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteExecutionResult {
    pub outcome: ExecutionOutcome,
    /// Files present in the worker workdir after execution. The Hub-side
    /// remote executor materializes them back into its local run workdir;
    /// normal Hub output ingestion remains authoritative afterwards.
    pub files: Vec<RemoteFile>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseStatusResponse {
    pub lease_id: String,
    pub attempt_id: String,
    pub worker_id: String,
    pub state: LeaseState,
    pub last_heartbeat_ms: u64,
    pub expires_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<RemoteExecutionResult>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerErrorResponse {
    pub error: String,
}
''')

replace_once(
    "crates/hub-protocol/src/lib.rs",
    "use std::collections::BTreeMap;\n",
    "pub mod distributed;\n\nuse std::collections::BTreeMap;\n",
)

Path("crates/hub-executor/src/remote.rs").write_text(r'''//! Authenticated remote executor using the versioned worker lease protocol.
//!
//! This backend preserves the existing [`hub_core::exec::Executor`] boundary:
//! Hub still materializes immutable inputs and ingests outputs/provenance. The
//! backend only transports the run-local workdir to a worker and materializes
//! returned files back into that same local workdir.

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hub_core::error::ExecutorFailure;
use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionRequest, Executor};
use hub_protocol::distributed::{
    LeaseCreateRequest, LeaseCreateResponse, LeaseState, LeaseStatusResponse,
    PROCESS_EXECUTION_CAPABILITY, RemoteExecutionRequest, RemoteFile, WORKDIR_TOKEN,
    WORKER_PROTOCOL_VERSION, WorkerDescriptor,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

const DEFAULT_POLL_MS: u64 = 100;
const DEFAULT_LOST_AFTER_MS: u64 = 3_000;
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RemoteExecutor {
    endpoint: String,
    token: String,
    backend_id: String,
    poll_interval: Duration,
    lost_after: Duration,
    max_payload_bytes: usize,
}

impl RemoteExecutor {
    /// Creates a remote executor. The bearer token must be non-empty. Plain
    /// HTTP is suitable only for loopback/private trusted tunnels; production
    /// deployments should terminate TLS in front of the worker endpoint.
    pub fn new(endpoint: impl Into<String>, token: impl Into<String>) -> Result<Self, String> {
        let endpoint = endpoint.into().trim_end_matches('/').to_owned();
        let token = token.into();
        if !(endpoint.starts_with("http://") || endpoint.starts_with("https://")) {
            return Err("remote worker URL must start with http:// or https://".into());
        }
        if token.is_empty() {
            return Err("remote worker bearer token must not be empty".into());
        }
        Ok(Self {
            backend_id: format!("remote:{endpoint}"),
            endpoint,
            token,
            poll_interval: Duration::from_millis(DEFAULT_POLL_MS),
            lost_after: Duration::from_millis(DEFAULT_LOST_AFTER_MS),
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
        })
    }

    #[must_use]
    pub fn with_max_payload_bytes(mut self, value: usize) -> Self {
        self.max_payload_bytes = value.max(1);
        self
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.endpoint)
    }

    fn authorized(&self, request: ureq::Request) -> ureq::Request {
        request.set("Authorization", &format!("Bearer {}", self.token))
    }

    fn get_json<T: DeserializeOwned>(&self, path: &str) -> Result<T, RemoteCallError> {
        decode_response(self.authorized(ureq::get(&self.url(path))).call())
    }

    fn post_json<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, RemoteCallError> {
        let value = serde_json::to_value(body)
            .map_err(|e| RemoteCallError::Decode(format!("encoding request: {e}")))?;
        decode_response(self.authorized(ureq::post(&self.url(path))).send_json(value))
    }

    fn post_empty(&self, path: &str) -> Result<(), RemoteCallError> {
        match self.authorized(ureq::post(&self.url(path))).call() {
            Ok(_) => Ok(()),
            Err(error) => Err(classify_ureq(error)),
        }
    }

    fn describe(&self) -> Result<WorkerDescriptor, RemoteCallError> {
        self.get_json("/v1/worker")
    }

    fn build_remote_request(
        &self,
        request: &ExecutionRequest,
    ) -> Result<RemoteExecutionRequest, String> {
        if !request.working_dir.is_dir() {
            return Err(format!(
                "local execution workdir {:?} does not exist",
                request.working_dir
            ));
        }
        let files = collect_files(&request.working_dir, self.max_payload_bytes)?;
        let args = request
            .args
            .iter()
            .map(|value| encode_workdir_path(value, &request.working_dir))
            .collect();
        let env = request
            .env
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    encode_workdir_path(value, &request.working_dir),
                )
            })
            .collect();
        Ok(RemoteExecutionRequest {
            program: request.program.clone(),
            args,
            env,
            timeout_ms: request.timeout_ms,
            max_capture_bytes_per_stream: request.max_capture_bytes_per_stream,
            files,
        })
    }

    fn materialize_result_files(
        &self,
        root: &Path,
        files: &[RemoteFile],
    ) -> Result<(), String> {
        let mut total = 0usize;
        for file in files {
            total = total
                .checked_add(file.bytes.len())
                .ok_or_else(|| "remote result payload size overflow".to_owned())?;
            if total > self.max_payload_bytes {
                return Err(format!(
                    "remote result files exceed {} byte transport limit",
                    self.max_payload_bytes
                ));
            }
            let relative = checked_relative_path(&file.path)?;
            let destination = root.join(relative);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent)
                    .map_err(|e| format!("creating returned file parent: {e}"))?;
            }
            fs::write(&destination, &file.bytes)
                .map_err(|e| format!("materializing returned file {:?}: {e}", file.path))?;
        }
        Ok(())
    }

    fn remote_failure(&self, started: Instant, reason: impl Into<String>) -> ExecutionOutcome {
        ExecutionOutcome {
            exit_code: None,
            signal: None,
            timed_out: false,
            cancelled: false,
            start_error: Some(reason.into()),
            duration_ms: elapsed_ms(started),
            stdout: Vec::new(),
            stdout_truncated: false,
            stderr: Vec::new(),
            stderr_truncated: false,
        }
    }
}

impl Executor for RemoteExecutor {
    fn backend_id(&self) -> &str {
        &self.backend_id
    }

    fn execute(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionOutcome, ExecutorFailure> {
        let started = Instant::now();
        if cancel.is_cancelled() {
            return Ok(cancelled_outcome(started));
        }

        let descriptor = match self.describe() {
            Ok(descriptor) => descriptor,
            Err(RemoteCallError::Authorization) => {
                return Ok(self.remote_failure(started, "remote worker authorization refused"));
            }
            Err(error) => {
                return Ok(self.remote_failure(
                    started,
                    format!("remote worker unavailable: {error}"),
                ));
            }
        };
        if descriptor.protocol_version != WORKER_PROTOCOL_VERSION {
            return Ok(self.remote_failure(
                started,
                format!(
                    "remote worker protocol {} unsupported; expected {}",
                    descriptor.protocol_version, WORKER_PROTOCOL_VERSION
                ),
            ));
        }
        if !descriptor
            .capabilities
            .iter()
            .any(|capability| capability == PROCESS_EXECUTION_CAPABILITY)
        {
            return Ok(self.remote_failure(
                started,
                format!(
                    "remote worker {} lacks capability {}",
                    descriptor.worker_id, PROCESS_EXECUTION_CAPABILITY
                ),
            ));
        }
        let descriptor_limit = usize::try_from(descriptor.max_payload_bytes).unwrap_or(usize::MAX);
        if self.max_payload_bytes > descriptor_limit {
            return Ok(self.remote_failure(
                started,
                format!(
                    "remote worker payload limit {} is below configured client limit {}",
                    descriptor.max_payload_bytes, self.max_payload_bytes
                ),
            ));
        }

        let execution = match self.build_remote_request(request) {
            Ok(execution) => execution,
            Err(error) => return Ok(self.remote_failure(started, error)),
        };
        let attempt_id = uuid::Uuid::new_v4().to_string();
        let lease_request = LeaseCreateRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            attempt_id: attempt_id.clone(),
            execution,
        };
        let lease: LeaseCreateResponse = match self.post_json("/v1/leases", &lease_request) {
            Ok(lease) => lease,
            Err(RemoteCallError::Authorization) => {
                return Ok(self.remote_failure(started, "remote worker authorization refused"));
            }
            Err(error) => {
                return Ok(self.remote_failure(
                    started,
                    format!("remote worker could not create lease: {error}"),
                ));
            }
        };
        if lease.attempt_id != attempt_id || lease.worker_id != descriptor.worker_id {
            return Ok(self.remote_failure(
                started,
                "remote worker returned mismatched lease identity",
            ));
        }

        let lease_path = format!("/v1/leases/{}", lease.lease_id);
        let cancel_path = format!("{lease_path}/cancel");
        let mut last_contact = Instant::now();
        loop {
            if cancel.is_cancelled() {
                let _ = self.post_empty(&cancel_path);
                return Ok(cancelled_outcome(started));
            }
            if now_ms() > lease.expires_at_ms {
                return Ok(self.remote_failure(started, "remote execution lease expired"));
            }

            match self.get_json::<LeaseStatusResponse>(&lease_path) {
                Ok(status) => {
                    last_contact = Instant::now();
                    if status.lease_id != lease.lease_id
                        || status.attempt_id != attempt_id
                        || status.worker_id != descriptor.worker_id
                    {
                        return Ok(self.remote_failure(
                            started,
                            "remote worker returned mismatched status identity",
                        ));
                    }
                    if now_ms().saturating_sub(status.last_heartbeat_ms)
                        > u64::try_from(self.lost_after.as_millis()).unwrap_or(u64::MAX)
                        && !status.state.is_terminal()
                    {
                        return Ok(self.remote_failure(
                            started,
                            "remote worker heartbeat became stale",
                        ));
                    }
                    match status.state {
                        LeaseState::Queued | LeaseState::Running => {}
                        LeaseState::Completed | LeaseState::Cancelled => {
                            let Some(result) = status.result else {
                                return Ok(self.remote_failure(
                                    started,
                                    "remote terminal lease omitted execution result",
                                ));
                            };
                            if let Err(error) = self.materialize_result_files(
                                &request.working_dir,
                                &result.files,
                            ) {
                                return Ok(self.remote_failure(started, error));
                            }
                            return Ok(result.outcome);
                        }
                        LeaseState::Failed => {
                            return Ok(self.remote_failure(
                                started,
                                status
                                    .error
                                    .unwrap_or_else(|| "remote worker lease failed".into()),
                            ));
                        }
                    }
                }
                Err(RemoteCallError::Authorization) => {
                    return Ok(self.remote_failure(started, "remote worker authorization refused"));
                }
                Err(_) if last_contact.elapsed() < self.lost_after => {}
                Err(error) => {
                    return Ok(self.remote_failure(
                        started,
                        format!("remote worker lost while lease was active: {error}"),
                    ));
                }
            }
            std::thread::sleep(self.poll_interval);
        }
    }
}

#[derive(Debug)]
enum RemoteCallError {
    Authorization,
    Status(u16, String),
    Transport(String),
    Decode(String),
}

impl std::fmt::Display for RemoteCallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Authorization => f.write_str("authorization refused"),
            Self::Status(status, body) => write!(f, "HTTP {status}: {body}"),
            Self::Transport(reason) => f.write_str(reason),
            Self::Decode(reason) => f.write_str(reason),
        }
    }
}

fn decode_response<T: DeserializeOwned>(
    response: Result<ureq::Response, ureq::Error>,
) -> Result<T, RemoteCallError> {
    match response {
        Ok(response) => response
            .into_json()
            .map_err(|e| RemoteCallError::Decode(format!("decoding response: {e}"))),
        Err(error) => Err(classify_ureq(error)),
    }
}

fn classify_ureq(error: ureq::Error) -> RemoteCallError {
    match error {
        ureq::Error::Status(401 | 403, _) => RemoteCallError::Authorization,
        ureq::Error::Status(status, response) => {
            let body = response.into_string().unwrap_or_default();
            RemoteCallError::Status(status, body)
        }
        ureq::Error::Transport(error) => RemoteCallError::Transport(error.to_string()),
    }
}

fn collect_files(root: &Path, limit: usize) -> Result<Vec<RemoteFile>, String> {
    let mut paths = Vec::new();
    collect_paths(root, root, &mut paths)?;
    paths.sort();
    let mut total = 0usize;
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(&path).map_err(|e| format!("reading transport file {path:?}: {e}"))?;
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| "remote request payload size overflow".to_owned())?;
        if total > limit {
            return Err(format!("remote request files exceed {limit} byte transport limit"));
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| "transport path escaped workdir".to_owned())?;
        files.push(RemoteFile {
            path: relative.to_string_lossy().into_owned(),
            bytes,
        });
    }
    Ok(files)
}

fn collect_paths(root: &Path, current: &Path, paths: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(current).map_err(|e| format!("reading workdir {current:?}: {e}"))? {
        let entry = entry.map_err(|e| format!("reading workdir entry: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("reading workdir file type: {e}"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "remote transport refuses symlink {:?}",
                entry.path().strip_prefix(root).unwrap_or(entry.path().as_path())
            ));
        }
        if file_type.is_dir() {
            collect_paths(root, &entry.path(), paths)?;
        } else if file_type.is_file() {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn checked_relative_path(raw: &str) -> Result<PathBuf, String> {
    let path = Path::new(raw);
    if raw.is_empty() || path.is_absolute() {
        return Err(format!("invalid remote relative path {raw:?}"));
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(format!("unsafe remote relative path {raw:?}"));
        }
    }
    Ok(path.to_path_buf())
}

fn encode_workdir_path(value: &str, root: &Path) -> String {
    let candidate = Path::new(value);
    if candidate.is_absolute() && candidate.starts_with(root) {
        if let Ok(relative) = candidate.strip_prefix(root) {
            if relative.as_os_str().is_empty() {
                return WORKDIR_TOKEN.to_owned();
            }
            return format!("{WORKDIR_TOKEN}/{}", relative.to_string_lossy());
        }
    }
    value.to_owned()
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

fn cancelled_outcome(started: Instant) -> ExecutionOutcome {
    ExecutionOutcome {
        exit_code: None,
        signal: None,
        timed_out: false,
        cancelled: true,
        start_error: None,
        duration_ms: elapsed_ms(started),
        stdout: Vec::new(),
        stdout_truncated: false,
        stderr: Vec::new(),
        stderr_truncated: false,
    }
}
''')

Path("crates/hub-executor/src/worker.rs").write_text(r'''//! In-memory authenticated worker service for remote process execution.
//!
//! Leases and results are intentionally ephemeral in v1: a worker restart is
//! detected by the Hub remote executor and fails the affected run closed. Hub
//! remains the durable authority for run/provenance state.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Component, Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Path as AxumPath, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hub_core::exec::{CancelToken, ExecutionRequest, Executor};
use hub_protocol::distributed::{
    LeaseCreateRequest, LeaseCreateResponse, LeaseState, LeaseStatusResponse,
    PROCESS_EXECUTION_CAPABILITY, RemoteExecutionResult, RemoteFile, WORKDIR_TOKEN,
    WORKER_PROTOCOL_VERSION, WorkerDescriptor, WorkerErrorResponse,
};

use crate::ProcessExecutor;

const HEARTBEAT_INTERVAL_MS: u64 = 250;
const LEASE_GRACE_MS: u64 = 30_000;

#[derive(Clone)]
pub struct WorkerService {
    inner: Arc<WorkerInner>,
}

struct WorkerInner {
    worker_id: String,
    token: String,
    work_root: PathBuf,
    max_payload_bytes: usize,
    leases: Mutex<LeaseBook>,
}

#[derive(Default)]
struct LeaseBook {
    leases: BTreeMap<String, LeaseEntry>,
    attempts: BTreeMap<String, String>,
}

#[derive(Clone)]
struct LeaseEntry {
    attempt_id: String,
    execution: hub_protocol::distributed::RemoteExecutionRequest,
    state: LeaseState,
    last_heartbeat_ms: u64,
    expires_at_ms: u64,
    result: Option<RemoteExecutionResult>,
    error: Option<String>,
    cancel: CancelToken,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CompletionDisposition {
    Stored,
    Duplicate,
}

impl WorkerService {
    pub fn new(
        worker_id: impl Into<String>,
        token: impl Into<String>,
        work_root: PathBuf,
        max_payload_bytes: usize,
    ) -> Result<Self, String> {
        let worker_id = worker_id.into();
        let token = token.into();
        if worker_id.trim().is_empty() {
            return Err("worker id must not be empty".into());
        }
        if token.is_empty() {
            return Err("worker bearer token must not be empty".into());
        }
        if max_payload_bytes == 0 {
            return Err("worker max payload bytes must be greater than zero".into());
        }
        fs::create_dir_all(&work_root)
            .map_err(|e| format!("creating worker work root {work_root:?}: {e}"))?;
        Ok(Self {
            inner: Arc::new(WorkerInner {
                worker_id,
                token,
                work_root,
                max_payload_bytes,
                leases: Mutex::new(LeaseBook::default()),
            }),
        })
    }

    #[must_use]
    pub fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor {
            protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: self.inner.worker_id.clone(),
            capabilities: vec![PROCESS_EXECUTION_CAPABILITY.to_owned()],
            max_payload_bytes: self.inner.max_payload_bytes as u64,
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
        }
    }

    fn authorize(&self, headers: &HeaderMap) -> bool {
        headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| value == self.inner.token)
    }

    fn reserve_lease(
        &self,
        request: &LeaseCreateRequest,
    ) -> Result<LeaseCreateResponse, ReserveError> {
        if request.protocol_version != WORKER_PROTOCOL_VERSION {
            return Err(ReserveError::BadRequest(format!(
                "unsupported worker protocol {}; expected {}",
                request.protocol_version, WORKER_PROTOCOL_VERSION
            )));
        }
        if request.attempt_id.trim().is_empty() {
            return Err(ReserveError::BadRequest("attempt id must not be empty".into()));
        }
        let input_bytes = request
            .execution
            .files
            .iter()
            .try_fold(0usize, |total, file| total.checked_add(file.bytes.len()))
            .ok_or_else(|| ReserveError::BadRequest("payload size overflow".into()))?;
        if input_bytes > self.inner.max_payload_bytes {
            return Err(ReserveError::PayloadTooLarge);
        }
        for file in &request.execution.files {
            checked_relative_path(&file.path).map_err(ReserveError::BadRequest)?;
        }

        let mut book = self
            .inner
            .leases
            .lock()
            .map_err(|_| ReserveError::Internal("lease book lock poisoned".into()))?;
        if let Some(existing_id) = book.attempts.get(&request.attempt_id).cloned() {
            let existing = book
                .leases
                .get(&existing_id)
                .ok_or_else(|| ReserveError::Internal("attempt index drift".into()))?;
            if existing.execution != request.execution {
                return Err(ReserveError::Conflict(
                    "attempt id was reused with a different execution payload".into(),
                ));
            }
            return Ok(self.create_response(&existing_id, existing));
        }

        let lease_id = uuid::Uuid::new_v4().to_string();
        let now = now_ms();
        let expires_at_ms = now
            .saturating_add(request.execution.timeout_ms)
            .saturating_add(LEASE_GRACE_MS);
        let entry = LeaseEntry {
            attempt_id: request.attempt_id.clone(),
            execution: request.execution.clone(),
            state: LeaseState::Queued,
            last_heartbeat_ms: now,
            expires_at_ms,
            result: None,
            error: None,
            cancel: CancelToken::new(),
        };
        let response = self.create_response(&lease_id, &entry);
        book.attempts
            .insert(request.attempt_id.clone(), lease_id.clone());
        book.leases.insert(lease_id, entry);
        Ok(response)
    }

    fn create_response(&self, lease_id: &str, entry: &LeaseEntry) -> LeaseCreateResponse {
        LeaseCreateResponse {
            lease_id: lease_id.to_owned(),
            attempt_id: entry.attempt_id.clone(),
            worker_id: self.inner.worker_id.clone(),
            state: entry.state,
            expires_at_ms: entry.expires_at_ms,
        }
    }

    fn status(&self, lease_id: &str) -> Result<Option<LeaseStatusResponse>, String> {
        let book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        Ok(book.leases.get(lease_id).map(|entry| LeaseStatusResponse {
            lease_id: lease_id.to_owned(),
            attempt_id: entry.attempt_id.clone(),
            worker_id: self.inner.worker_id.clone(),
            state: entry.state,
            last_heartbeat_ms: entry.last_heartbeat_ms,
            expires_at_ms: entry.expires_at_ms,
            result: entry.result.clone(),
            error: entry.error.clone(),
        }))
    }

    fn cancel(&self, lease_id: &str) -> Result<bool, String> {
        let book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        let Some(entry) = book.leases.get(lease_id) else {
            return Ok(false);
        };
        entry.cancel.cancel();
        Ok(true)
    }

    fn heartbeat(&self, lease_id: &str) -> Result<bool, String> {
        let mut book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        let Some(entry) = book.leases.get_mut(lease_id) else {
            return Ok(false);
        };
        if entry.state.is_terminal() {
            return Ok(false);
        }
        entry.last_heartbeat_ms = now_ms();
        Ok(true)
    }

    fn mark_running(&self, lease_id: &str) -> Result<(hub_protocol::distributed::RemoteExecutionRequest, CancelToken), String> {
        let mut book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        let entry = book
            .leases
            .get_mut(lease_id)
            .ok_or_else(|| "lease disappeared before execution".to_owned())?;
        entry.state = LeaseState::Running;
        entry.last_heartbeat_ms = now_ms();
        Ok((entry.execution.clone(), entry.cancel.clone()))
    }

    fn complete_result(
        &self,
        lease_id: &str,
        result: RemoteExecutionResult,
    ) -> Result<CompletionDisposition, String> {
        let mut book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        let entry = book
            .leases
            .get_mut(lease_id)
            .ok_or_else(|| "lease disappeared before result commit".to_owned())?;
        if let Some(existing) = &entry.result {
            if existing == &result {
                return Ok(CompletionDisposition::Duplicate);
            }
            return Err("conflicting duplicate remote result".into());
        }
        entry.state = if result.outcome.cancelled {
            LeaseState::Cancelled
        } else {
            LeaseState::Completed
        };
        entry.last_heartbeat_ms = now_ms();
        entry.result = Some(result);
        entry.error = None;
        Ok(CompletionDisposition::Stored)
    }

    fn fail_lease(&self, lease_id: &str, error: String) {
        if let Ok(mut book) = self.inner.leases.lock() {
            if let Some(entry) = book.leases.get_mut(lease_id) {
                if !entry.state.is_terminal() {
                    entry.state = LeaseState::Failed;
                    entry.last_heartbeat_ms = now_ms();
                    entry.error = Some(error);
                }
            }
        }
    }

    async fn run_lease(self, lease_id: String) {
        let (execution, cancel) = match self.mark_running(&lease_id) {
            Ok(value) => value,
            Err(error) => {
                self.fail_lease(&lease_id, error);
                return;
            }
        };
        let root = self.inner.work_root.join(&lease_id);
        if let Err(error) = prepare_workdir(&root, &execution.files, self.inner.max_payload_bytes) {
            self.fail_lease(&lease_id, error);
            return;
        }
        let args = execution
            .args
            .iter()
            .map(|value| expand_workdir_path(value, &root))
            .collect();
        let env = execution
            .env
            .iter()
            .map(|(key, value)| (key.clone(), expand_workdir_path(value, &root)))
            .collect();
        let request = ExecutionRequest {
            program: execution.program,
            args,
            working_dir: root.clone(),
            env,
            timeout_ms: execution.timeout_ms,
            max_capture_bytes_per_stream: execution.max_capture_bytes_per_stream,
        };

        let heartbeat_service = self.clone();
        let heartbeat_lease = lease_id.clone();
        let heartbeat = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(HEARTBEAT_INTERVAL_MS)).await;
                match heartbeat_service.heartbeat(&heartbeat_lease) {
                    Ok(true) => {}
                    _ => break,
                }
            }
        });

        let outcome = tokio::task::spawn_blocking(move || ProcessExecutor::new().execute(&request, &cancel)).await;
        heartbeat.abort();
        let outcome = match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                self.fail_lease(&lease_id, format!("worker executor backend failed: {error}"));
                let _ = fs::remove_dir_all(&root);
                return;
            }
            Err(error) => {
                self.fail_lease(&lease_id, format!("worker execution task failed: {error}"));
                let _ = fs::remove_dir_all(&root);
                return;
            }
        };
        let files = match collect_files(&root, self.inner.max_payload_bytes) {
            Ok(files) => files,
            Err(error) => {
                self.fail_lease(&lease_id, error);
                let _ = fs::remove_dir_all(&root);
                return;
            }
        };
        let result = RemoteExecutionResult { outcome, files };
        if let Err(error) = self.complete_result(&lease_id, result) {
            self.fail_lease(&lease_id, error);
        }
        let _ = fs::remove_dir_all(root);
    }
}

pub fn router(service: WorkerService) -> Router {
    let max_body = service.inner.max_payload_bytes.saturating_mul(2).max(1024 * 1024);
    Router::new()
        .route("/v1/worker", get(describe_worker))
        .route("/v1/leases", post(create_lease))
        .route("/v1/leases/{lease_id}", get(get_lease))
        .route("/v1/leases/{lease_id}/cancel", post(cancel_lease))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(service)
}

pub async fn serve(listener: tokio::net::TcpListener, service: WorkerService) -> std::io::Result<()> {
    axum::serve(listener, router(service)).await
}

async fn describe_worker(State(service): State<WorkerService>, headers: HeaderMap) -> Response {
    if !service.authorize(&headers) {
        return unauthorized();
    }
    Json(service.descriptor()).into_response()
}

async fn create_lease(
    State(service): State<WorkerService>,
    headers: HeaderMap,
    body: Result<Json<LeaseCreateRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    if !service.authorize(&headers) {
        return unauthorized();
    }
    let Json(request) = match body {
        Ok(body) => body,
        Err(error) => return worker_error(StatusCode::BAD_REQUEST, format!("invalid lease body: {error}")),
    };
    let response = match service.reserve_lease(&request) {
        Ok(response) => response,
        Err(ReserveError::BadRequest(error)) => return worker_error(StatusCode::BAD_REQUEST, error),
        Err(ReserveError::Conflict(error)) => return worker_error(StatusCode::CONFLICT, error),
        Err(ReserveError::PayloadTooLarge) => {
            return worker_error(StatusCode::PAYLOAD_TOO_LARGE, "remote payload exceeds worker limit")
        }
        Err(ReserveError::Internal(error)) => return worker_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    };
    if response.state == LeaseState::Queued {
        let lease_id = response.lease_id.clone();
        let runner = service.clone();
        tokio::spawn(async move { runner.run_lease(lease_id).await });
    }
    (StatusCode::ACCEPTED, Json(response)).into_response()
}

async fn get_lease(
    State(service): State<WorkerService>,
    headers: HeaderMap,
    AxumPath(lease_id): AxumPath<String>,
) -> Response {
    if !service.authorize(&headers) {
        return unauthorized();
    }
    match service.status(&lease_id) {
        Ok(Some(status)) => Json(status).into_response(),
        Ok(None) => worker_error(StatusCode::NOT_FOUND, "lease not found"),
        Err(error) => worker_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

async fn cancel_lease(
    State(service): State<WorkerService>,
    headers: HeaderMap,
    AxumPath(lease_id): AxumPath<String>,
) -> Response {
    if !service.authorize(&headers) {
        return unauthorized();
    }
    match service.cancel(&lease_id) {
        Ok(true) => StatusCode::ACCEPTED.into_response(),
        Ok(false) => worker_error(StatusCode::NOT_FOUND, "lease not found"),
        Err(error) => worker_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

fn unauthorized() -> Response {
    worker_error(StatusCode::UNAUTHORIZED, "worker authorization refused")
}

fn worker_error(status: StatusCode, error: impl Into<String>) -> Response {
    (status, Json(WorkerErrorResponse { error: error.into() })).into_response()
}

#[derive(Debug)]
enum ReserveError {
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge,
    Internal(String),
}

fn prepare_workdir(root: &Path, files: &[RemoteFile], limit: usize) -> Result<(), String> {
    if root.exists() {
        fs::remove_dir_all(root).map_err(|e| format!("cleaning worker workdir: {e}"))?;
    }
    fs::create_dir_all(root).map_err(|e| format!("creating worker workdir: {e}"))?;
    let mut total = 0usize;
    for file in files {
        total = total
            .checked_add(file.bytes.len())
            .ok_or_else(|| "remote payload size overflow".to_owned())?;
        if total > limit {
            return Err(format!("remote payload exceeds {limit} byte worker limit"));
        }
        let relative = checked_relative_path(&file.path)?;
        let destination = root.join(relative);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent).map_err(|e| format!("creating worker input parent: {e}"))?;
        }
        fs::write(destination, &file.bytes).map_err(|e| format!("writing worker input: {e}"))?;
    }
    Ok(())
}

fn collect_files(root: &Path, limit: usize) -> Result<Vec<RemoteFile>, String> {
    let mut paths = Vec::new();
    collect_paths(root, root, &mut paths)?;
    paths.sort();
    let mut total = 0usize;
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = fs::read(&path).map_err(|e| format!("reading worker result file: {e}"))?;
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| "worker result payload size overflow".to_owned())?;
        if total > limit {
            return Err(format!("worker result files exceed {limit} byte transport limit"));
        }
        let relative = path
            .strip_prefix(root)
            .map_err(|_| "worker result path escaped workdir".to_owned())?;
        files.push(RemoteFile {
            path: relative.to_string_lossy().into_owned(),
            bytes,
        });
    }
    Ok(files)
}

fn collect_paths(root: &Path, current: &Path, paths: &mut Vec<PathBuf>) -> Result<(), String> {
    for entry in fs::read_dir(current).map_err(|e| format!("reading worker workdir: {e}"))? {
        let entry = entry.map_err(|e| format!("reading worker directory entry: {e}"))?;
        let kind = entry.file_type().map_err(|e| format!("reading worker file type: {e}"))?;
        if kind.is_symlink() {
            return Err(format!(
                "worker refuses result symlink {:?}",
                entry.path().strip_prefix(root).unwrap_or(entry.path().as_path())
            ));
        }
        if kind.is_dir() {
            collect_paths(root, &entry.path(), paths)?;
        } else if kind.is_file() {
            paths.push(entry.path());
        }
    }
    Ok(())
}

fn checked_relative_path(raw: &str) -> Result<PathBuf, String> {
    let path = Path::new(raw);
    if raw.is_empty() || path.is_absolute() {
        return Err(format!("invalid workdir-relative path {raw:?}"));
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            return Err(format!("unsafe workdir-relative path {raw:?}"));
        }
    }
    Ok(path.to_path_buf())
}

fn expand_workdir_path(value: &str, root: &Path) -> String {
    if value == WORKDIR_TOKEN {
        return root.display().to_string();
    }
    if let Some(relative) = value.strip_prefix(&format!("{WORKDIR_TOKEN}/")) {
        return root.join(relative).display().to_string();
    }
    value.to_owned()
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| u64::try_from(duration.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hub_core::exec::ExecutionOutcome;
    use hub_protocol::distributed::RemoteExecutionRequest;

    fn service() -> WorkerService {
        let root = std::env::temp_dir().join(format!("hub-worker-unit-{}", uuid::Uuid::new_v4()));
        WorkerService::new("worker-test", "secret", root, 1024 * 1024).expect("service")
    }

    fn request(attempt: &str) -> LeaseCreateRequest {
        LeaseCreateRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            attempt_id: attempt.into(),
            execution: RemoteExecutionRequest {
                program: "/bin/true".into(),
                args: Vec::new(),
                env: BTreeMap::new(),
                timeout_ms: 1_000,
                max_capture_bytes_per_stream: 1024,
                files: Vec::new(),
            },
        }
    }

    fn result(stdout: &[u8]) -> RemoteExecutionResult {
        RemoteExecutionResult {
            outcome: ExecutionOutcome {
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
            },
            files: Vec::new(),
        }
    }

    #[test]
    fn duplicate_attempt_is_idempotent_but_payload_drift_conflicts() {
        let service = service();
        let first = service.reserve_lease(&request("attempt-1")).expect("reserve");
        let replay = service.reserve_lease(&request("attempt-1")).expect("replay");
        assert_eq!(first.lease_id, replay.lease_id);

        let mut changed = request("attempt-1");
        changed.execution.args.push("different".into());
        assert!(matches!(
            service.reserve_lease(&changed),
            Err(ReserveError::Conflict(_))
        ));
    }

    #[test]
    fn duplicate_remote_result_is_idempotent_and_conflicting_result_is_rejected() {
        let service = service();
        let lease = service.reserve_lease(&request("attempt-2")).expect("reserve");
        let first = result(b"same");
        assert_eq!(
            service.complete_result(&lease.lease_id, first.clone()).expect("first"),
            CompletionDisposition::Stored
        );
        assert_eq!(
            service.complete_result(&lease.lease_id, first).expect("duplicate"),
            CompletionDisposition::Duplicate
        );
        assert!(service.complete_result(&lease.lease_id, result(b"different")).is_err());
    }

    #[test]
    fn traversal_paths_are_rejected() {
        assert!(checked_relative_path("../escape").is_err());
        assert!(checked_relative_path("/absolute").is_err());
        assert!(checked_relative_path("inputs/data").is_ok());
    }
}
''')

replace_once(
    "crates/hub-executor/src/lib.rs",
    "//! - [`MockExecutor`]: scripted deterministic outcomes for tests.\n\n",
    "//! - [`MockExecutor`]: scripted deterministic outcomes for tests.\n//! - [`RemoteExecutor`]: authenticated lease-based execution on a worker.\n\n"
    "pub mod remote;\npub mod worker;\n\npub use remote::RemoteExecutor;\n\n",
)

replace_once(
    "crates/hub-executor/Cargo.toml",
    "[dependencies]\nhub-core = { workspace = true }\n",
    "[dependencies]\nhub-core = { workspace = true }\nhub-protocol = { workspace = true }\nserde = { workspace = true }\nserde_json = { workspace = true }\nuuid = { workspace = true }\naxum = { workspace = true }\ntokio = { workspace = true }\nureq = { workspace = true }\n",
)

Path("crates/hub-executor/tests/remote_worker.rs").write_text(r'''use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::time::Duration;

use hub_core::exec::{CancelToken, ExecutionRequest, Executor};
use hub_executor::worker::{serve, WorkerService};
use hub_executor::RemoteExecutor;

fn temp_dir(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hub-remote-{tag}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).expect("mkdir");
    path
}

async fn start_worker(token: &str) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("addr");
    let root = temp_dir("worker");
    let service = WorkerService::new("worker-e2e", token, root.clone(), 8 * 1024 * 1024)
        .expect("worker service");
    let task = tokio::spawn(async move {
        serve(listener, service).await.expect("serve worker");
    });
    (format!("http://{address}"), root, task)
}

fn request(workdir: PathBuf) -> ExecutionRequest {
    fs::create_dir_all(workdir.join("inputs")).expect("inputs");
    fs::create_dir_all(workdir.join("outputs")).expect("outputs");
    fs::write(workdir.join("inputs/source"), b"remote-payload\n").expect("input");
    ExecutionRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "cat inputs/source > outputs/result".into()],
        working_dir: workdir,
        env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        timeout_ms: 5_000,
        max_capture_bytes_per_stream: 1024 * 1024,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_executor_transports_inputs_and_materializes_outputs() {
    let (endpoint, worker_root, task) = start_worker("secret").await;
    let local = temp_dir("local");
    let exec = RemoteExecutor::new(endpoint, "secret")
        .expect("remote")
        .with_max_payload_bytes(8 * 1024 * 1024);
    let local_for_run = local.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        exec.execute(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(outcome.exited_cleanly(), "{outcome:?}");
    assert_eq!(fs::read(local.join("outputs/result")).expect("result"), b"remote-payload\n");
    task.abort();
    let _ = fs::remove_dir_all(local);
    let _ = fs::remove_dir_all(worker_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_refuses_bad_auth_without_executing() {
    let (endpoint, worker_root, task) = start_worker("secret").await;
    let local = temp_dir("auth");
    let exec = RemoteExecutor::new(endpoint, "wrong-secret")
        .expect("remote")
        .with_max_payload_bytes(8 * 1024 * 1024);
    let local_for_run = local.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        exec.execute(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(outcome.start_error.as_deref().is_some_and(|error| error.contains("authorization refused")));
    assert!(!local.join("outputs/result").exists());
    task.abort();
    let _ = fs::remove_dir_all(local);
    let _ = fs::remove_dir_all(worker_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_worker_fails_closed_as_observed_run_outcome() {
    let probe = StdTcpListener::bind("127.0.0.1:0").expect("probe");
    let address = probe.local_addr().expect("addr");
    drop(probe);
    let local = temp_dir("lost");
    let exec = RemoteExecutor::new(format!("http://{address}"), "secret").expect("remote");
    let local_for_run = local.clone();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            exec.execute(&request(local_for_run), &CancelToken::new())
        }),
    )
    .await
    .expect("must fail promptly")
    .expect("join")
    .expect("executor");
    assert!(outcome.start_error.as_deref().is_some_and(|error| error.contains("unavailable")));
    assert!(!outcome.exited_cleanly());
    let _ = fs::remove_dir_all(local);
}
''')

Path("apps/scirust-hub-worker/Cargo.toml").parent.mkdir(parents=True, exist_ok=True)
Path("apps/scirust-hub-worker/Cargo.toml").write_text(r'''[package]
name = "scirust-hub-worker"
description = "Authenticated remote execution worker for SciRust Hub"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
publish.workspace = true
repository.workspace = true

[dependencies]
hub-executor = { workspace = true }
tokio = { workspace = true }
clap = { workspace = true }
thiserror = { workspace = true }

[lints]
workspace = true

[[bin]]
name = "scirust-hub-worker"
path = "src/main.rs"
''')

Path("apps/scirust-hub-worker/src").mkdir(parents=True, exist_ok=True)
Path("apps/scirust-hub-worker/src/main.rs").write_text(r'''//! `scirust-hub-worker` — authenticated remote process execution worker.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use hub_executor::worker::WorkerService;

const DEFAULT_LISTEN: &str = "127.0.0.1:8488";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(name = "scirust-hub-worker", about = "SciRust Hub remote execution worker", version)]
struct Args {
    #[arg(long, env = "SCIRUST_HUB_WORKER_LISTEN", default_value = DEFAULT_LISTEN)]
    listen: String,
    #[arg(long, env = "SCIRUST_HUB_WORKER_ID", default_value = "worker-1")]
    worker_id: String,
    /// Shared bearer token. Required; never logged.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TOKEN")]
    token: String,
    #[arg(long, env = "SCIRUST_HUB_WORKER_DATA_DIR", default_value = "scirust-hub-worker-data")]
    data_dir: PathBuf,
    #[arg(long, env = "SCIRUST_HUB_WORKER_MAX_PAYLOAD_BYTES", default_value_t = DEFAULT_MAX_PAYLOAD_BYTES)]
    max_payload_bytes: usize,
}

#[derive(Debug, thiserror::Error)]
enum WorkerError {
    #[error("invalid listen address {0:?}")]
    Listen(String),
    #[error("invalid worker configuration: {0}")]
    Configuration(String),
    #[error("worker runtime failed: {0}")]
    Runtime(String),
    #[error("worker server failed: {0}")]
    Serve(std::io::Error),
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(args) {
        eprintln!("scirust-hub-worker: {error}");
        std::process::exit(1);
    }
}

fn run(args: Args) -> Result<(), WorkerError> {
    let listen: SocketAddr = args
        .listen
        .parse()
        .map_err(|_| WorkerError::Listen(args.listen.clone()))?;
    let service = WorkerService::new(
        args.worker_id,
        args.token,
        args.data_dir,
        args.max_payload_bytes,
    )
    .map_err(WorkerError::Configuration)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| WorkerError::Runtime(error.to_string()))?;
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(WorkerError::Serve)?;
        hub_executor::worker::serve(listener, service)
            .await
            .map_err(WorkerError::Serve)
    })
}
''')

replace_once(
    "Cargo.toml",
    '    "apps/scirust-hubd",\n',
    '    "apps/scirust-hubd",\n    "apps/scirust-hub-worker",\n',
)

# Wire optional remote execution into the daemon while keeping local process
# execution as the default and preserving one orchestrator path.
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "use hub_core::clock::SystemClock;\n",
    "use hub_core::clock::SystemClock;\nuse hub_core::exec::Executor;\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "use hub_executor::ProcessExecutor;\n",
    "use hub_executor::{ProcessExecutor, RemoteExecutor};\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "    store: StoreBackend,\n}\n\n#[derive(Debug, Clone, Copy, clap::ValueEnum)]\nenum StoreBackend {\n",
    r'''    store: StoreBackend,
    /// Execution backend. Remote mode requires worker URL + bearer token.
    #[arg(
        long,
        env = "SCIRUST_HUB_EXECUTOR",
        value_enum,
        default_value_t = ExecutorBackend::Process
    )]
    executor: ExecutorBackend,
    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_URL")]
    remote_worker_url: Option<String>,
    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_TOKEN")]
    remote_worker_token: Option<String>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ExecutorBackend {
    Process,
    Remote,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum StoreBackend {
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "    #[error(\"opening persistent store: {0}\")]\n    Store(String),\n}\n",
    "    #[error(\"opening persistent store: {0}\")]\n    Store(String),\n    #[error(\"invalid execution configuration: {0}\")]\n    ExecutorConfig(String),\n}\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "    blob_store: FileSystemArtifactStore,\n    workdir_root: PathBuf,\n) -> Arc<Orchestrator> {\n",
    "    blob_store: FileSystemArtifactStore,\n    executor: Arc<dyn Executor>,\n    workdir_root: PathBuf,\n) -> Arc<Orchestrator> {\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "        blob_store,\n        Arc::new(ProcessExecutor::new()),\n        Limits::default(),\n",
    "        blob_store,\n        executor,\n        Limits::default(),\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "    // One store instance serves all three repository ports; `Arc` is\n    // coerced separately per port.\n    let orchestrator = match args.store {\n",
    r'''    let executor: Arc<dyn Executor> = match args.executor {
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

    // One store instance serves all repository ports; `Arc` is coerced
    // separately per port.
    let orchestrator = match args.store {
''',
)
# Both build_orchestrator calls have the same tail.
text = Path("apps/scirust-hubd/src/main.rs").read_text()
old = "                blob_store,\n                workdir_root,\n"
count = text.count(old)
if count != 2:
    raise SystemExit(f"daemon: expected two orchestrator call tails, found {count}")
text = text.replace(old, "                blob_store,\n                executor.clone(),\n                workdir_root,\n")
Path("apps/scirust-hubd/src/main.rs").write_text(text)

Path("docs/adr/0010-distributed-executor-v1.md").write_text(r'''# ADR 0010 — Distributed executor v1

Status: accepted

## Decision

Hub's first distributed execution substrate remains behind the existing
`hub_core::exec::Executor` port. `RemoteExecutor` transports a run-local
workdir to an authenticated `scirust-hub-worker`, leases one process execution,
polls worker liveness, and materializes the returned workdir files locally.
The normal Hub orchestrator then performs the same output validation, artifact
ingestion and provenance recording used by the local process executor.

The wire contract is versioned independently (`WORKER_PROTOCOL_VERSION = 1`).
Every attempt has a stable `attempt_id`; lease creation is idempotent for an
identical retry and conflicts if the same attempt id carries a different
payload. Result commit is likewise idempotent for byte-identical duplicate
results and rejects conflicting duplicates.

The worker advertises identity, protocol version, capabilities, transport size
limit and heartbeat interval. The Hub rejects incompatible workers, stale
heartbeats, expired leases, mismatched attempt/lease/worker identities and
unsafe returned paths. Input and output files are transported as bytes with
workdir-relative paths; no shared filesystem is assumed.

## Failure semantics

Worker unavailability, authentication refusal, protocol mismatch, lease loss,
stale heartbeat and unsafe transport data become observed failed execution
outcomes. They do not create successful provenance. Cancellation is forwarded
to the active worker lease and remains represented by the existing Hub run
state machine.

Worker lease state is intentionally ephemeral in v1. Hub remains the durable
authority. A worker restart therefore causes an active remote attempt to fail
closed; later scheduler policy may choose an explicit retry with a new attempt.

## Security boundary

The v1 worker requires a bearer token and never logs it. The worker executes the
absolute program path supplied by an authenticated Hub using the same
`ProcessExecutor` resource controls. It is **not a sandbox** and bearer auth is
**not transport encryption**. Plain HTTP must therefore be limited to loopback
or a trusted private/tunneled network; production Internet exposure requires a
TLS/authenticated transport boundary in front of the worker.

## Deliberate non-goals

- no generic job queue or web-service platform;
- no shared filesystem requirement;
- no silent retry after worker loss;
- no claim that bearer auth is equivalent to mTLS;
- no distributed artifact cache yet;
- no capability-aware multi-worker scheduler yet.
''')

print("distributed executor v1 transformations complete")
