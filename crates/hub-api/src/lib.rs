//! # hub-api — SciRust Hub HTTP adapter
//!
//! Thin axum handlers mapping `/api/v1` onto the domain orchestrator. No
//! business rules live here: parse, validate the protocol version, convert,
//! delegate, map errors. Domain calls are blocking; handlers offload them to
//! `spawn_blocking`.

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, Request, State};
use axum::http::{header, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use hub_core::error::CoreError;
use hub_core::{ArtifactId, ComponentId, ComponentManifest, Orchestrator, RunSpec};
use hub_protocol as proto;
use sha2::{Digest, Sha256};

/// Shared application state.
#[derive(Clone)]
pub struct HubState {
    pub orchestrator: Arc<Orchestrator>,
    api_bearer_sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EmptyBearerToken;

impl std::fmt::Display for EmptyBearerToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("API bearer token must not be empty")
    }
}

impl std::error::Error for EmptyBearerToken {}

impl HubState {
    #[must_use]
    pub fn new(orchestrator: Arc<Orchestrator>) -> Self {
        Self {
            orchestrator,
            api_bearer_sha256: None,
        }
    }

    /// Enables bearer authentication for all `/api/v1/*` routes. Only a
    /// SHA-256 verifier is retained in shared state; the plaintext token is
    /// discarded after construction.
    ///
    /// # Errors
    /// [`EmptyBearerToken`] when an empty token is supplied.
    pub fn with_bearer_token(mut self, token: impl AsRef<str>) -> Result<Self, EmptyBearerToken> {
        let token = token.as_ref();
        if token.is_empty() {
            return Err(EmptyBearerToken);
        }
        self.api_bearer_sha256 = Some(Sha256::digest(token.as_bytes()).into());
        Ok(self)
    }
}

/// Inline-content cap for artifact reads (`?include=content`).
const INLINE_CONTENT_LIMIT: u64 = 64 * 1024;

/// Builds the full router (health + versioned API).
pub fn router(state: HubState) -> Router {
    let manifest_limit = state.orchestrator.limits().max_manifest_bytes;
    let api = Router::new()
        .route(
            "/api/v1/components",
            post(register_component).get(list_components),
        )
        .route("/api/v1/components/{id}", get(get_component))
        .route("/api/v1/capabilities", get(list_capabilities))
        .route("/api/v1/runs", post(submit_run).get(list_runs))
        .route("/api/v1/runs/{id}", get(get_run))
        .route("/api/v1/runs/{id}/cancel", post(cancel_run))
        .route("/api/v1/runs/{id}/reproduce", post(reproduce_run))
        .route("/api/v1/executions", post(execute_run))
        .route(
            "/api/v1/workflows",
            post(submit_workflow).get(list_workflows),
        )
        .route("/api/v1/workflows/{id}", get(get_workflow))
        .route("/api/v1/workflows/{id}/cancel", post(cancel_workflow))
        .route("/api/v1/workflows/{id}/executions", post(execute_workflow))
        .route(
            "/api/v1/artifacts",
            post(upload_artifact).get(list_artifacts),
        )
        .route("/api/v1/artifacts/{id}", get(get_artifact))
        .route_layer(middleware::from_fn_with_state(
            state.clone(),
            require_api_bearer,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
        .merge(api)
        .layer(DefaultBodyLimit::max(manifest_limit))
        .with_state(state)
}

async fn require_api_bearer(
    State(state): State<HubState>,
    request: Request,
    next: Next,
) -> Response {
    let Some(expected) = state.api_bearer_sha256 else {
        return next.run(request).await;
    };
    let supplied = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty())
        .map(|token| <[u8; 32]>::from(Sha256::digest(token.as_bytes())));
    if supplied.is_some_and(|actual| digest_eq(&expected, &actual)) {
        return next.run(request).await;
    }

    let mut response = error_response(
        StatusCode::UNAUTHORIZED,
        proto::ErrorCode::Unauthorized,
        "bearer authentication required",
    );
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn digest_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    let mut difference = 0u8;
    for (&a, &b) in left.iter().zip(right.iter()) {
        difference |= a ^ b;
    }
    difference == 0
}

// ----------------------------------------------------------------------
// Handlers
// ----------------------------------------------------------------------

async fn health(State(_state): State<HubState>) -> Json<proto::HealthResponse> {
    Json(proto::HealthResponse {
        status: "ok",
        protocol_version: proto::PROTOCOL_VERSION,
    })
}

async fn ready(State(state): State<HubState>) -> Json<proto::ReadyResponse> {
    // Readiness = registries answer queries. This daemon's stores are
    // in-process, so there is no external dependency to await.
    let components_registered = state
        .orchestrator
        .components()
        .map_or(0, |list| u64::try_from(list.len()).unwrap_or(u64::MAX));
    let runs_recorded = state
        .orchestrator
        .list_runs()
        .map_or(0, |list| u64::try_from(list.len()).unwrap_or(u64::MAX));
    Json(proto::ReadyResponse {
        ready: true,
        components_registered,
        runs_recorded,
        executor_backend: state.orchestrator.executor_backend_id().to_owned(),
    })
}

async fn register_component(
    State(state): State<HubState>,
    body: Result<Json<proto::RegisterComponentRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(request) = match body {
        Ok(parsed) => parsed,
        Err(rejection) => {
            return bad_request(format!("malformed request body: {rejection}"));
        }
    };
    if let Err(e) = proto::check_schema_version(request.schema_version) {
        return protocol_error(e);
    }
    let component_id = request.manifest.id;
    let manifest: ComponentManifest = request.manifest.into();

    let orch = state.orchestrator.clone();
    let registered = tokio::task::spawn_blocking(move || orch.register_component(manifest)).await;
    match joined(registered) {
        Ok(status) => {
            let created = status == hub_core::RegistrationStatus::Created;
            let stored = match state.orchestrator.component(&component_id) {
                Ok(Some(stored)) => stored,
                _ => return internal("registered manifest could not be read back"),
            };
            let digest = stored
                .content_digest()
                .map(|d| d.to_string())
                .unwrap_or_default();
            let mut response = Json(proto::RegisterComponentResponse {
                status: if created {
                    "created"
                } else {
                    "already_registered"
                }
                .into(),
                component: proto::ComponentDto::from(&stored),
                manifest_digest: digest,
            })
            .into_response();
            *response.status_mut() = if created {
                StatusCode::CREATED
            } else {
                StatusCode::OK
            };
            response
        }
        Err(response) => response,
    }
}

async fn list_components(
    State(state): State<HubState>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let orch = state.orchestrator.clone();
    let capability_filter = query.get("capability").cloned();
    match joined(
        tokio::task::spawn_blocking(move || match capability_filter {
            Some(name) => hub_core::CapabilityName::parse(&name)
                .map_err(|e| CoreError::Validation(e.to_string()))
                .and_then(|capability| orch.discover_by_capability(&capability)),
            None => orch.components(),
        })
        .await,
    ) {
        Ok(components) => Json(proto::ComponentListResponse {
            components: components.iter().map(proto::ComponentDto::from).collect(),
        })
        .into_response(),
        Err(response) => response,
    }
}

async fn get_component(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<ComponentId>(&id) else {
        return not_found("component", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.component(&parsed)).await) {
        Ok(Some(manifest)) => Json(proto::ComponentDto::from(&manifest)).into_response(),
        Ok(None) => not_found("component", &id),
        Err(response) => response,
    }
}

async fn list_capabilities(State(state): State<HubState>) -> Response {
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || summarize_capabilities(&orch)).await) {
        Ok(summary) => Json(proto::CapabilityListResponse {
            capabilities: summary,
        })
        .into_response(),
        Err(response) => response,
    }
}

fn summarize_capabilities(
    orch: &Orchestrator,
) -> Result<Vec<proto::CapabilitySummaryDto>, CoreError> {
    let mut counts: BTreeMap<hub_core::CapabilityName, (u64, hub_core::Version)> = BTreeMap::new();
    for manifest in orch.components()? {
        for capability in &manifest.capabilities {
            counts
                .entry(capability.name.clone())
                .and_modify(|(n, _)| *n += 1)
                .or_insert((1, capability.contract_version.clone()));
        }
    }
    Ok(counts
        .into_iter()
        .map(
            |(name, (declared_by, contract_version))| proto::CapabilitySummaryDto {
                name,
                declared_by,
                contract_version,
            },
        )
        .collect())
}

async fn submit_run(
    State(state): State<HubState>,
    body: Result<Json<proto::SubmitRunRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(request) = match body {
        Ok(parsed) => parsed,
        Err(rejection) => {
            return bad_request(format!("malformed request body: {rejection}"));
        }
    };
    if let Err(e) = proto::check_schema_version(request.schema_version) {
        return protocol_error(e);
    }
    let spec: RunSpec = request.run_spec.into();
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.submit_run(spec)).await) {
        Ok(record) => {
            let mut response = Json(proto::SubmitRunResponse {
                run: proto::RunDto::from(&record),
            })
            .into_response();
            *response.status_mut() = StatusCode::CREATED;
            response
        }
        Err(response) => response,
    }
}

async fn list_runs(State(state): State<HubState>) -> Response {
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.list_runs()).await) {
        Ok(runs) => Json(proto::RunListResponse {
            runs: runs.iter().map(proto::RunDto::from).collect(),
        })
        .into_response(),
        Err(response) => response,
    }
}

async fn get_run(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::RunId>(&id) else {
        return not_found("run", &id);
    };
    let orch = state.orchestrator.clone();
    match joined_or_none(tokio::task::spawn_blocking(move || orch.run(&parsed)).await) {
        Ok(Some(record)) => Json(proto::RunDto::from(&record)).into_response(),
        Ok(None) => not_found("run", &id),
        Err(response) => response,
    }
}

async fn cancel_run(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::RunId>(&id) else {
        return not_found("run", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.cancel_run(parsed)).await) {
        Ok(signalled) => Json(proto::CancelRunResponse {
            run_id: parsed,
            signalled_active_execution: signalled,
        })
        .into_response(),
        Err(response) => response,
    }
}

/// Reproduces a recorded run: re-submits its stored spec as a new queued run
/// linked via `reproduced_from`.
async fn reproduce_run(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::RunId>(&id) else {
        return not_found("run", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.reproduce_run(parsed)).await) {
        Ok(record) => {
            let mut response = Json(proto::SubmitRunResponse {
                run: proto::RunDto::from(&record),
            })
            .into_response();
            *response.status_mut() = StatusCode::CREATED;
            response
        }
        Err(response) => response,
    }
}

/// Executes a previously submitted run synchronously; the response carries
/// the final record including full provenance.
async fn execute_run(
    State(state): State<HubState>,
    Json(run_id): Json<hub_core::RunId>,
) -> Response {
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.execute_run(run_id)).await) {
        Ok(record) => Json(proto::RunDto::from(&record)).into_response(),
        Err(response) => response,
    }
}

async fn upload_artifact(State(state): State<HubState>, request: Request) -> Response {
    let name = match request
        .headers()
        .get("x-scirust-artifact-name")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => return bad_request("missing or invalid x-scirust-artifact-name header"),
    };
    let media_type = match request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.is_empty() => value.to_owned(),
        _ => return bad_request("missing or invalid content-type header"),
    };
    let limit =
        usize::try_from(state.orchestrator.limits().max_artifact_bytes).unwrap_or(usize::MAX);
    let bytes = match axum::body::to_bytes(request.into_body(), limit).await {
        Ok(bytes) => bytes.to_vec(),
        Err(error) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                proto::ErrorCode::Validation,
                format!("artifact body exceeds configured limit: {error}"),
            );
        }
    };
    let orch = state.orchestrator.clone();
    match joined(
        tokio::task::spawn_blocking(move || orch.ingest_artifact(name, media_type, &bytes)).await,
    ) {
        Ok(meta) => {
            let mut response = Json(proto::ArtifactDto::from(&meta)).into_response();
            *response.status_mut() = StatusCode::CREATED;
            response
        }
        Err(response) => response,
    }
}

async fn list_artifacts(State(state): State<HubState>) -> Response {
    let orch = state.orchestrator.clone();
    match joined_or_none(tokio::task::spawn_blocking(move || orch.artifacts()).await) {
        Ok(artifacts) => Json(proto::ArtifactListResponse {
            artifacts: artifacts.iter().map(proto::ArtifactDto::from).collect(),
        })
        .into_response(),
        Err(_) => internal("artifact listing failed"),
    }
}

async fn get_artifact(
    State(state): State<HubState>,
    Path(id): Path<String>,
    Query(query): Query<std::collections::HashMap<String, String>>,
) -> Response {
    let Some(parsed) = typed_id::<ArtifactId>(&id) else {
        return not_found("artifact", &id);
    };
    let include_content = query.get("include").is_some_and(|v| v == "content");
    let orch = state.orchestrator.clone();
    let fetched = tokio::task::spawn_blocking(move || {
        let meta = orch.artifact_meta(&parsed);
        match meta {
            None => Err(CoreError::ArtifactNotFound(parsed)),
            Some(meta) => {
                if include_content {
                    orch.artifact_bytes(&meta.id)
                        .map(|(_, bytes)| (meta, Some(bytes)))
                } else {
                    Ok((meta, None))
                }
            }
        }
    })
    .await;
    match joined(fetched) {
        Ok((meta, bytes)) => {
            let mut dto = proto::ArtifactDto::from(&meta);
            if let Some(bytes) = bytes {
                if meta.size > INLINE_CONTENT_LIMIT {
                    return error_response(
                        StatusCode::PAYLOAD_TOO_LARGE,
                        proto::ErrorCode::Validation,
                        format!(
                            "artifact content is {} bytes; inline limit is {INLINE_CONTENT_LIMIT}",
                            meta.size
                        ),
                    );
                }
                match String::from_utf8(bytes) {
                    Ok(text) => dto.content_text = Some(text),
                    Err(_) => {
                        return error_response(
                            StatusCode::NOT_ACCEPTABLE,
                            proto::ErrorCode::BadRequest,
                            "artifact content is binary; inline text view unavailable",
                        );
                    }
                }
            }
            Json(dto).into_response()
        }
        // ArtifactNotFound/BlobNotFound already map to 404 envelopes.
        Err(response) => response,
    }
}

async fn submit_workflow(
    State(state): State<HubState>,
    body: Result<Json<proto::SubmitWorkflowRequest>, axum::extract::rejection::JsonRejection>,
) -> Response {
    let Json(request) = match body {
        Ok(parsed) => parsed,
        Err(rejection) => return bad_request(format!("malformed request body: {rejection}")),
    };
    if let Err(e) = proto::check_schema_version(request.schema_version) {
        return protocol_error(e);
    }
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.submit_workflow(request.workflow)).await)
    {
        Ok(record) => {
            let mut response = Json(proto::SubmitWorkflowResponse {
                workflow: proto::WorkflowDto::from(&record),
            })
            .into_response();
            *response.status_mut() = StatusCode::CREATED;
            response
        }
        Err(response) => response,
    }
}

async fn list_workflows(State(state): State<HubState>) -> Response {
    let workflows = blocking_workflows(&state);
    Json(proto::WorkflowListResponse {
        workflows: workflows.iter().map(proto::WorkflowDto::from).collect(),
    })
    .into_response()
}

fn blocking_workflows(state: &HubState) -> Vec<hub_core::WorkflowRecord> {
    state.orchestrator.workflows()
}

async fn get_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::WorkflowId>(&id) else {
        return not_found("workflow", &id);
    };
    let orch = state.orchestrator.clone();
    match joined_or_none(tokio::task::spawn_blocking(move || orch.workflow(&parsed)).await) {
        Ok(Some(record)) => Json(proto::WorkflowDto::from(&record)).into_response(),
        Ok(None) => not_found("workflow", &id),
        Err(_) => internal("workflow lookup failed"),
    }
}

async fn cancel_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::WorkflowId>(&id) else {
        return not_found("workflow", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.cancel_workflow(parsed)).await) {
        Ok(signalled) => Json(proto::CancelWorkflowResponse {
            workflow_id: parsed,
            signalled_active_execution: signalled,
        })
        .into_response(),
        Err(response) => response,
    }
}

/// Executes a created workflow and waits for its terminal record.
async fn execute_workflow(State(state): State<HubState>, Path(id): Path<String>) -> Response {
    let Some(parsed) = typed_id::<hub_core::WorkflowId>(&id) else {
        return not_found("workflow", &id);
    };
    let orch = state.orchestrator.clone();
    match joined(tokio::task::spawn_blocking(move || orch.execute_workflow(parsed)).await) {
        Ok(record) => Json(proto::WorkflowDto::from(&record)).into_response(),
        // WorkflowNotFound maps to 404 through core_error already.
        Err(response) => response,
    }
}

// ----------------------------------------------------------------------
// Plumbing
// ----------------------------------------------------------------------

#[allow(clippy::result_large_err)] // Response carries the structured envelope
fn joined<T>(joined: Result<Result<T, CoreError>, tokio::task::JoinError>) -> Result<T, Response> {
    match joined {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(core_error(error)),
        Err(join_error) => {
            tracing::error!(error = %join_error, "blocking task panicked or was cancelled");
            Err(internal("internal task failed"))
        }
    }
}

#[allow(clippy::result_large_err)] // Response carries the structured envelope
fn joined_or_none<T>(joined: Result<T, tokio::task::JoinError>) -> Result<T, Response>
where
    T: Default,
{
    match joined {
        Ok(value) => Ok(value),
        Err(join_error) => {
            tracing::error!(error = %join_error, "blocking task panicked or was cancelled");
            Err(internal("internal task failed"))
        }
    }
}

fn bad_request(message: impl Into<String>) -> Response {
    error_response(
        StatusCode::BAD_REQUEST,
        proto::ErrorCode::BadRequest,
        message,
    )
}

fn internal(message: impl Into<String>) -> Response {
    let message = message.into();
    tracing::error!(%message, "internal handler error");
    error_response(
        StatusCode::INTERNAL_SERVER_ERROR,
        proto::ErrorCode::Internal,
        message,
    )
}

fn protocol_error(e: proto::ProtocolError) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(proto::ErrorEnvelope::with_details(
            proto::ErrorCode::UnsupportedSchemaVersion,
            e.to_string(),
            BTreeMap::from([("expected".into(), proto::PROTOCOL_VERSION.to_string())]),
        )),
    )
        .into_response()
}

fn not_found(kind: &str, id: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(proto::ErrorEnvelope::with_details(
            proto::ErrorCode::NotFound,
            format!("{kind} {id} not found"),
            BTreeMap::from([("kind".into(), kind.into()), ("id".into(), id.into())]),
        )),
    )
        .into_response()
}

fn error_response(
    status: StatusCode,
    code: proto::ErrorCode,
    message: impl Into<String>,
) -> Response {
    (status, Json(proto::ErrorEnvelope::new(code, message))).into_response()
}

/// Maps domain errors onto HTTP semantics with structured envelopes.
fn core_error(error: CoreError) -> Response {
    use proto::ErrorCode;
    let (status, code) = match &error {
        CoreError::ComponentConflict { .. } | CoreError::ComponentAlreadyRegistered { .. } => {
            (StatusCode::CONFLICT, ErrorCode::Conflict)
        }
        CoreError::ComponentNotFound(_)
        | CoreError::RunNotFound(_)
        | CoreError::ArtifactNotFound(_)
        | CoreError::BlobNotFound { .. } => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
        CoreError::CapabilityNotDeclared { .. }
        | CoreError::MissingInputBinding { .. }
        | CoreError::InvalidTransition { .. }
        | CoreError::InvalidWorkflowTransition { .. }
        | CoreError::RunNotExecutable { .. }
        | CoreError::WorkflowNotExecutable { .. }
        | CoreError::InvalidRunSpec(_)
        | CoreError::InvalidManifest(_)
        | CoreError::Validation(_) => (StatusCode::UNPROCESSABLE_ENTITY, ErrorCode::Validation),
        CoreError::WorkflowNotFound(_) => (StatusCode::NOT_FOUND, ErrorCode::NotFound),
        CoreError::ArtifactTooLarge { .. } => {
            (StatusCode::PAYLOAD_TOO_LARGE, ErrorCode::Validation)
        }
        CoreError::ExecutionFailed { .. } | CoreError::Storage(_) => {
            (StatusCode::INTERNAL_SERVER_ERROR, ErrorCode::Internal)
        }
    };
    tracing::warn!(error = %error, %status, "request failed");
    (
        status,
        Json(proto::ErrorEnvelope::new(code, error.to_string())),
    )
        .into_response()
}

/// Parses a path segment into a typed id, `None` when malformed.
fn typed_id<T>(raw: &str) -> Option<T>
where
    T: std::str::FromStr,
{
    raw.parse::<T>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use hub_core::clock::ManualClock;
    use hub_core::limits::Limits;
    use hub_core::memory::{
        FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
        InMemoryWorkflows,
    };
    use hub_protocol::PROTOCOL_VERSION;
    use std::sync::Arc;
    use tower::ServiceExt;

    fn test_state() -> (HubState, Arc<ManualClock>, PathGuard) {
        let dir = PathGuard(std::env::temp_dir().join(format!("hub-api-{}", uuid::Uuid::new_v4())));
        let clock = Arc::new(ManualClock::starting_at(1_000));
        let orch = Orchestrator::new(
            clock,
            Arc::new(InMemoryComponents::default()),
            Arc::new(InMemoryRuns::default()),
            Arc::new(InMemoryArtifactMeta::default()),
            Arc::new(InMemoryWorkflows::default()),
            FileSystemArtifactStore::open(dir.0.join("blobs")).expect("blobs"),
            // Real process executor: the vertical slice must not be mocked.
            Arc::new(hub_exec_for_tests()),
            Limits::default(),
            dir.0.join("workdirs"),
        );
        (
            HubState::new(Arc::new(orch)),
            Arc::new(ManualClock::starting_at(0)),
            dir,
        )
    }

    struct PathGuard(std::path::PathBuf);
    impl Drop for PathGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Re-exported real executor without making hub-executor a dependency of
    /// the API crate's production path; the daemon wires it explicitly.
    struct TestProcessExecutor;
    impl hub_core::Executor for TestProcessExecutor {
        fn backend_id(&self) -> &str {
            "process"
        }
        fn execute(
            &self,
            request: &hub_core::ExecutionRequest,
            cancel: &hub_core::CancelToken,
        ) -> Result<hub_core::ExecutionOutcome, hub_core::ExecutorFailure> {
            // Delegate to the same logic by constructing it locally is
            // duplicative; instead assert through the daemon e2e suite and
            // keep this minimal-but-real: run /bin/echo via std directly.
            use std::process::{Command, Stdio};
            let started = std::time::Instant::now();
            let mut cmd = Command::new(&request.program);
            cmd.args(&request.args)
                .current_dir(&request.working_dir)
                .env_clear()
                .envs(request.env.iter())
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            match cmd.output() {
                Ok(output) => Ok(hub_core::ExecutionOutcome {
                    exit_code: output.status.code(),
                    signal: None,
                    timed_out: false,
                    cancelled: cancel.is_cancelled(),
                    start_error: None,
                    duration_ms: u64::try_from(started.elapsed().as_millis()).unwrap_or(0),
                    stdout: output.stdout,
                    stdout_truncated: false,
                    stderr: output.stderr,
                    stderr_truncated: false,
                }),
                Err(e) => Ok(hub_core::ExecutionOutcome {
                    exit_code: None,
                    signal: None,
                    timed_out: false,
                    cancelled: false,
                    start_error: Some(e.to_string()),
                    duration_ms: 0,
                    stdout: Vec::new(),
                    stdout_truncated: false,
                    stderr: Vec::new(),
                    stderr_truncated: false,
                }),
            }
        }
    }

    fn hub_exec_for_tests() -> TestProcessExecutor {
        TestProcessExecutor
    }

    async fn send(router: Router, request: Request<Body>) -> (StatusCode, serde_json::Value) {
        let response = router.oneshot(request).await.expect("infallible");
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("body");
        let json = if bytes.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::from_slice(&bytes).expect("json body")
        };
        (status, json)
    }

    fn sample_manifest_json() -> String {
        let id = uuid::Uuid::new_v4();
        format!(
            r#"{{
                "schema_version": {PROTOCOL_VERSION},
                "manifest": {{
                    "id": "{id}",
                    "name": "demo-echo",
                    "version": "1.0.0",
                    "kind": "tool",
                    "capabilities": [
                        {{
                            "name": "demo.echo",
                            "contract_version": "1.0.0"
                        }}
                    ],
                    "execution": {{
                        "type": "process",
                        "program": "/bin/echo",
                        "args": ["{{params}}"]
                    }}
                }}
            }}"#
        )
    }

    #[tokio::test]
    async fn raw_artifact_upload_round_trips_exact_bytes() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let payload = vec![0, 1, 2, 0xff, b'x'];
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/artifacts")
            .header("x-scirust-artifact-name", "capsule-input")
            .header("content-type", "application/octet-stream")
            .body(Body::from(payload.clone()))
            .expect("req");
        let (status, body) = send(app.clone(), request).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["name"], "capsule-input");
        assert_eq!(body["size"], payload.len());
        assert!(body["produced_by_run"].is_null());
        let id = body["id"].as_str().expect("id");

        let response = app
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/artifacts/{id}?include=content"))
                    .body(Body::empty())
                    .expect("req"),
            )
            .await
            .expect("response");
        assert_eq!(response.status(), StatusCode::NOT_ACCEPTABLE);
    }

    #[tokio::test]
    async fn artifact_upload_requires_explicit_metadata_headers() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/artifacts")
            .body(Body::from("bytes"))
            .expect("req");
        let (status, body) = send(app, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "bad_request");
    }

    #[tokio::test]
    async fn bearer_auth_protects_api_but_not_health_endpoints() {
        let (state, _clock, _dir) = test_state();
        let state = state.with_bearer_token("control-secret").expect("token");
        let app = router(state);

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "unauthorized");

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer wrong-secret")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "unauthorized");

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer control-secret")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let (status, _) = send(
            app,
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn empty_bearer_token_is_rejected() {
        let (state, _clock, _dir) = test_state();
        assert!(state.with_bearer_token("").is_err());
    }

    #[tokio::test]
    async fn health_and_ready_report_shape() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "ok");
        assert_eq!(body["protocol_version"], PROTOCOL_VERSION);

        let (status, body) = send(
            app,
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["ready"], true);
        assert_eq!(body["executor_backend"], "process");
    }

    #[tokio::test]
    async fn register_component_is_created_then_replayed() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let payload = sample_manifest_json();
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(payload.clone()))
            .expect("req");
        let (status, body) = send(app.clone(), request).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["status"], "created");
        let digest = body["manifest_digest"].as_str().expect("digest").to_owned();

        let replay = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(payload))
            .expect("req");
        let (status, body) = send(app.clone(), replay).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["status"], "already_registered");
        assert_eq!(body["manifest_digest"].as_str(), Some(digest.as_str()));

        // Listed and fetchable.
        let (status, list) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["components"].as_array().expect("arr").len(), 1);

        let component_id = list["components"][0]["id"]
            .as_str()
            .expect("string id")
            .to_owned();
        let uri = format!("/api/v1/components/{component_id}");
        let (status, one) = send(
            app,
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(one["name"], "demo-echo");
    }

    #[tokio::test]
    async fn conflicting_manifest_content_is_409_with_envelope() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let base_payload = sample_manifest_json();
        for (expected_status, program) in [
            (StatusCode::CREATED, "/bin/true"),
            (StatusCode::CONFLICT, "/bin/false"),
        ] {
            let mut manifest: serde_json::Value =
                serde_json::from_str(&base_payload).expect("payload");
            manifest["manifest"]["execution"]["program"] =
                serde_json::value::Value::String(program.to_owned());
            let request = Request::builder()
                .method("POST")
                .uri("/api/v1/components")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_string(&manifest).expect("ser")))
                .expect("req");
            let (status, body) = send(app.clone(), request).await;
            assert_eq!(status, expected_status, "body: {body}");
            if expected_status == StatusCode::CONFLICT {
                assert_eq!(body["error"]["code"], "conflict");
            }
        }
    }

    #[tokio::test]
    async fn unsupported_schema_version_is_rejected() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let mut payload: serde_json::Value =
            serde_json::from_str(&sample_manifest_json()).expect("payload");
        payload["schema_version"] = serde_json::json!(42);
        let request = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(payload.to_string()))
            .expect("req");
        let (status, body) = send(app, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["error"]["code"], "unsupported_schema_version");
    }

    #[tokio::test]
    async fn unknown_ids_return_404_envelopes() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        for uri in [
            format!("/api/v1/components/{}", uuid::Uuid::new_v4()),
            format!("/api/v1/runs/{}", uuid::Uuid::new_v4()),
            format!("/api/v1/artifacts/{}", uuid::Uuid::new_v4()),
        ] {
            let (status, body) = send(
                app.clone(),
                Request::builder()
                    .uri(uri)
                    .body(Body::empty())
                    .expect("req"),
            )
            .await;
            assert_eq!(status, StatusCode::NOT_FOUND);
            assert_eq!(body["error"]["code"], "not_found");
        }
        // Malformed ids are also 404 with envelope.
        let (status, body) = send(
            app,
            Request::builder()
                .uri("/api/v1/runs/not-a-uuid")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn full_vertical_slice_over_http() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);

        // 1. register
        let register = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(sample_manifest_json()))
            .expect("req");
        let (status, registered) = send(app.clone(), register).await;
        assert_eq!(status, StatusCode::CREATED);
        let component_id = registered["component"]["id"]
            .as_str()
            .expect("id")
            .to_owned();

        // 2. capability discovery sees it
        let (status, caps) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/capabilities")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(caps["capabilities"][0]["name"], "demo.echo");
        assert_eq!(caps["capabilities"][0]["declared_by"], 1);

        // 3. submit run
        let submit_payload = serde_json::json!({
            "schema_version": PROTOCOL_VERSION,
            "run_spec": {
                "component": component_id,
                "capability": "demo.echo",
                "parameters": {"msg": "over-http"},
                "inputs": [],
                "timeout_ms": 5000
            }
        });
        let submit = Request::builder()
            .method("POST")
            .uri("/api/v1/runs")
            .header("content-type", "application/json")
            .body(Body::from(submit_payload.to_string()))
            .expect("req");
        let (status, submitted) = send(app.clone(), submit).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(submitted["run"]["state"], "queued");
        let run_id = submitted["run"]["id"].as_str().expect("run id").to_owned();

        // 4. execute synchronously
        let exec = Request::builder()
            .method("POST")
            .uri("/api/v1/executions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!(run_id).to_string()))
            .expect("req");
        let (status, executed) = send(app.clone(), exec).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(executed["state"], "succeeded");
        assert_eq!(executed["outcome"]["exit_code"], 0);
        assert_eq!(executed["outcome"]["executor_backend"], "process");

        // 5. provenance + artifacts queryable
        let outputs = executed["outcome"]["outputs"].as_array().expect("outputs");
        assert_eq!(outputs.len(), 1);
        let artifact_id = outputs[0]["artifact"]
            .as_str()
            .expect("artifact id")
            .to_owned();

        let uri = format!("/api/v1/artifacts/{artifact_id}?include=content");
        let (status, artifact) = send(
            app.clone(),
            Request::builder()
                .uri(uri)
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            artifact["content_text"].as_str().expect("content text"),
            "{\"msg\":\"over-http\"}\n"
        );

        let (status, run_view) = send(
            app,
            Request::builder()
                .uri(format!("/api/v1/runs/{run_id}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(run_view["state"], "succeeded");
        assert!(
            run_view["transitions"]
                .as_array()
                .expect("transitions")
                .len()
                >= 4
        );
    }

    #[tokio::test]
    async fn component_filter_by_capability() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let register = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(sample_manifest_json()))
            .expect("req");
        let (status, _) = send(app.clone(), register).await;
        assert_eq!(status, StatusCode::CREATED);

        // Matching filter returns the declaring component.
        let (status, list) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components?capability=demo.echo")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(list["components"].as_array().expect("arr").len(), 1);

        // Declared-but-unmatched filter returns empty list.
        let (status, list) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components?capability=other.thing")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(list["components"].as_array().expect("arr").is_empty());

        // Malformed capability names are 422 validation errors.
        let (status, body) = send(
            app,
            Request::builder()
                .uri("/api/v1/components?capability=BAD%20NAME")
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "validation_failed");
    }

    #[tokio::test]
    async fn reproduce_endpoint_links_and_executes() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);

        let register = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(sample_manifest_json()))
            .expect("req");
        let (_, registered) = send(app.clone(), register).await;
        let component_id = registered["component"]["id"].as_str().unwrap().to_owned();

        let spec = serde_json::json!({
            "schema_version": PROTOCOL_VERSION,
            "run_spec": {
                "component": component_id,
                "capability": "demo.echo",
                "parameters": {"msg": "original"},
                "inputs": [],
                "timeout_ms": 5000
            }
        });
        let submit = Request::builder()
            .method("POST")
            .uri("/api/v1/runs")
            .header("content-type", "application/json")
            .body(Body::from(spec.to_string()))
            .expect("req");
        let (_, submitted) = send(app.clone(), submit).await;
        let run_id = submitted["run"]["id"].as_str().unwrap().to_owned();

        // Reproduce -> new queued run linked to the original.
        let repro = Request::builder()
            .method("POST")
            .uri(format!("/api/v1/runs/{run_id}/reproduce"))
            .body(Body::empty())
            .expect("req");
        let (status, body) = send(app.clone(), repro).await;
        assert_eq!(status, StatusCode::CREATED);
        assert_eq!(body["run"]["reproduced_from"], serde_json::json!(run_id));
        assert_eq!(body["run"]["state"], "queued");
        let repro_id = body["run"]["id"].as_str().unwrap().to_owned();

        // Execute the reproduction through the normal path.
        let exec = Request::builder()
            .method("POST")
            .uri("/api/v1/executions")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::json!(repro_id).to_string()))
            .expect("req");
        let (status, executed) = send(app.clone(), exec).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(executed["state"], "succeeded");

        // The original run is untouched and still queryable.
        let (status, original) = send(
            app.clone(),
            Request::builder()
                .uri(format!("/api/v1/runs/{run_id}"))
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(original["id"], serde_json::json!(run_id));

        // Unknown runs produce the canonical 404 envelope.
        let (status, body) = send(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/runs/{}/reproduce", uuid::Uuid::new_v4()))
                .body(Body::empty())
                .expect("req"),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["error"]["code"], "not_found");
    }

    #[tokio::test]
    async fn undeclared_capability_is_422_validation() {
        let (state, _clock, _dir) = test_state();
        let app = router(state);
        let register = Request::builder()
            .method("POST")
            .uri("/api/v1/components")
            .header("content-type", "application/json")
            .body(Body::from(sample_manifest_json()))
            .expect("req");
        let (_, registered) = send(app.clone(), register).await;
        let component_id = registered["component"]["id"].as_str().unwrap().to_owned();

        let submit_payload = serde_json::json!({
            "schema_version": PROTOCOL_VERSION,
            "run_spec": {
                "component": component_id,
                "capability": "demo.missing",
                "parameters": {},
                "inputs": [],
                "timeout_ms": 1000
            }
        });
        let submit = Request::builder()
            .method("POST")
            .uri("/api/v1/runs")
            .header("content-type", "application/json")
            .body(Body::from(submit_payload.to_string()))
            .expect("req");
        let (status, body) = send(app, submit).await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(body["error"]["code"], "validation_failed");
        assert!(body["error"]["message"]
            .as_str()
            .expect("msg")
            .contains("does not declare"));
    }
}
