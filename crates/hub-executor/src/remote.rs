//! Authenticated remote executor using the versioned worker lease protocol.
//!
//! This backend preserves the existing [`hub_core::exec::Executor`] boundary:
//! Hub still materializes immutable inputs and ingests outputs/provenance. The
//! backend only transports the run-local workdir to a worker and materializes
//! returned files back into that same local workdir.

use std::fs;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use hub_core::error::ExecutorFailure;
use hub_core::exec::{CancelToken, ExecutionOutcome, ExecutionReport, ExecutionRequest, Executor};
use hub_protocol::distributed::{
    LeaseCreateRequest, LeaseCreateResponse, LeaseState, LeaseStatusResponse,
    RemoteExecutionRequest, RemoteFile, WorkerDescriptor, PROCESS_EXECUTION_CAPABILITY,
    WORKDIR_TOKEN, WORKER_PROTOCOL_VERSION,
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
    expected_worker_id: Option<String>,
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
            expected_worker_id: None,
        })
    }

    #[must_use]
    pub fn with_max_payload_bytes(mut self, value: usize) -> Self {
        self.max_payload_bytes = value.max(1);
        self
    }

    pub(crate) fn with_expected_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.expected_worker_id = Some(worker_id.into());
        self
    }

    pub(crate) fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub(crate) fn discover_eligible(&self) -> Result<WorkerDescriptor, String> {
        let descriptor = self.describe().map_err(|error| match error {
            RemoteCallError::Authorization => "authorization refused".to_owned(),
            other => format!("unavailable: {other}"),
        })?;
        if descriptor.worker_id.trim().is_empty() {
            return Err("worker identity is empty".into());
        }
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
        decode_response(
            self.authorized(ureq::post(&self.url(path)))
                .send_json(value),
        )
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
        let directories = collect_directories(&request.working_dir)?;
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
            directories,
            files,
        })
    }

    fn materialize_result_files(&self, root: &Path, files: &[RemoteFile]) -> Result<(), String> {
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

    fn execute_report(
        &self,
        request: &ExecutionRequest,
        cancel: &CancelToken,
    ) -> Result<ExecutionReport, ExecutorFailure> {
        let descriptor = match self.discover_eligible() {
            Ok(descriptor) => descriptor,
            Err(_) => {
                return self
                    .execute(request, cancel)
                    .map(|outcome| ExecutionReport {
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
                return Ok(
                    self.remote_failure(started, format!("remote worker unavailable: {error}"))
                );
            }
        };
        if descriptor.worker_id.trim().is_empty() {
            return Ok(self.remote_failure(started, "remote worker identity is empty"));
        }
        if let Some(expected) = &self.expected_worker_id {
            if descriptor.worker_id != *expected {
                return Ok(self.remote_failure(
                    started,
                    format!(
                        "remote worker identity changed before lease dispatch: expected {expected:?}, found {:?}",
                        descriptor.worker_id
                    ),
                ));
            }
        }
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
            return Ok(
                self.remote_failure(started, "remote worker returned mismatched lease identity")
            );
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
                        return Ok(
                            self.remote_failure(started, "remote worker heartbeat became stale")
                        );
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
                            if let Err(error) =
                                self.materialize_result_files(&request.working_dir, &result.files)
                            {
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

fn collect_directories(root: &Path) -> Result<Vec<String>, String> {
    let mut directories = Vec::new();
    collect_directory_paths(root, root, &mut directories)?;
    directories.sort();
    Ok(directories)
}

fn collect_directory_paths(
    root: &Path,
    current: &Path,
    directories: &mut Vec<String>,
) -> Result<(), String> {
    for entry in fs::read_dir(current).map_err(|e| format!("reading workdir {current:?}: {e}"))? {
        let entry = entry.map_err(|e| format!("reading workdir entry: {e}"))?;
        let file_type = entry
            .file_type()
            .map_err(|e| format!("reading workdir file type: {e}"))?;
        if file_type.is_symlink() {
            return Err(format!(
                "remote transport refuses symlink {:?}",
                entry
                    .path()
                    .strip_prefix(root)
                    .unwrap_or(entry.path().as_path())
            ));
        }
        if file_type.is_dir() {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .map_err(|_| "transport directory escaped workdir".to_owned())?;
            directories.push(relative.to_string_lossy().into_owned());
            collect_directory_paths(root, &path, directories)?;
        }
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
        let bytes = fs::read(&path).map_err(|e| format!("reading transport file {path:?}: {e}"))?;
        total = total
            .checked_add(bytes.len())
            .ok_or_else(|| "remote request payload size overflow".to_owned())?;
        if total > limit {
            return Err(format!(
                "remote request files exceed {limit} byte transport limit"
            ));
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
                entry
                    .path()
                    .strip_prefix(root)
                    .unwrap_or(entry.path().as_path())
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
