//! In-memory authenticated worker service for remote process execution.
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
    RemoteExecutionResult, RemoteFile, WorkerDescriptor, WorkerErrorResponse,
    PROCESS_EXECUTION_CAPABILITY, WORKDIR_TOKEN, WORKER_PROTOCOL_VERSION,
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
            return Err(ReserveError::BadRequest(
                "attempt id must not be empty".into(),
            ));
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

    fn mark_running(
        &self,
        lease_id: &str,
    ) -> Result<
        (
            hub_protocol::distributed::RemoteExecutionRequest,
            CancelToken,
        ),
        String,
    > {
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
        if let Err(error) = prepare_workdir(
            &root,
            &execution.directories,
            &execution.files,
            self.inner.max_payload_bytes,
        ) {
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

        let outcome =
            tokio::task::spawn_blocking(move || ProcessExecutor::new().execute(&request, &cancel))
                .await;
        heartbeat.abort();
        let outcome = match outcome {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(error)) => {
                self.fail_lease(
                    &lease_id,
                    format!("worker executor backend failed: {error}"),
                );
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
    let max_body = service
        .inner
        .max_payload_bytes
        .saturating_mul(2)
        .max(1024 * 1024);
    Router::new()
        .route("/v1/worker", get(describe_worker))
        .route("/v1/leases", post(create_lease))
        .route("/v1/leases/{lease_id}", get(get_lease))
        .route("/v1/leases/{lease_id}/cancel", post(cancel_lease))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(service)
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    service: WorkerService,
) -> std::io::Result<()> {
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
        Err(error) => {
            return worker_error(
                StatusCode::BAD_REQUEST,
                format!("invalid lease body: {error}"),
            )
        }
    };
    let response = match service.reserve_lease(&request) {
        Ok(response) => response,
        Err(ReserveError::BadRequest(error)) => {
            return worker_error(StatusCode::BAD_REQUEST, error)
        }
        Err(ReserveError::Conflict(error)) => return worker_error(StatusCode::CONFLICT, error),
        Err(ReserveError::PayloadTooLarge) => {
            return worker_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "remote payload exceeds worker limit",
            )
        }
        Err(ReserveError::Internal(error)) => {
            return worker_error(StatusCode::INTERNAL_SERVER_ERROR, error)
        }
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
    (
        status,
        Json(WorkerErrorResponse {
            error: error.into(),
        }),
    )
        .into_response()
}

#[derive(Debug)]
enum ReserveError {
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge,
    Internal(String),
}

fn prepare_workdir(
    root: &Path,
    directories: &[String],
    files: &[RemoteFile],
    limit: usize,
) -> Result<(), String> {
    if root.exists() {
        fs::remove_dir_all(root).map_err(|e| format!("cleaning worker workdir: {e}"))?;
    }
    fs::create_dir_all(root).map_err(|e| format!("creating worker workdir: {e}"))?;
    for directory in directories {
        let relative = checked_relative_path(directory)?;
        fs::create_dir_all(root.join(relative))
            .map_err(|e| format!("creating worker workdir directory {directory:?}: {e}"))?;
    }
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
            return Err(format!(
                "worker result files exceed {limit} byte transport limit"
            ));
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
        let kind = entry
            .file_type()
            .map_err(|e| format!("reading worker file type: {e}"))?;
        if kind.is_symlink() {
            return Err(format!(
                "worker refuses result symlink {:?}",
                entry
                    .path()
                    .strip_prefix(root)
                    .unwrap_or(entry.path().as_path())
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
                directories: Vec::new(),
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
        let first = service
            .reserve_lease(&request("attempt-1"))
            .expect("reserve");
        let replay = service
            .reserve_lease(&request("attempt-1"))
            .expect("replay");
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
        let lease = service
            .reserve_lease(&request("attempt-2"))
            .expect("reserve");
        let first = result(b"same");
        assert_eq!(
            service
                .complete_result(&lease.lease_id, first.clone())
                .expect("first"),
            CompletionDisposition::Stored
        );
        assert_eq!(
            service
                .complete_result(&lease.lease_id, first)
                .expect("duplicate"),
            CompletionDisposition::Duplicate
        );
        assert!(service
            .complete_result(&lease.lease_id, result(b"different"))
            .is_err());
    }

    #[test]
    fn traversal_paths_are_rejected() {
        assert!(checked_relative_path("../escape").is_err());
        assert!(checked_relative_path("/absolute").is_err());
        assert!(checked_relative_path("inputs/data").is_ok());
    }
}
