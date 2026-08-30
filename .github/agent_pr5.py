from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:120]!r}")
    p.write_text(text.replace(old, new, 1))


# ---------------------------------------------------------------------------
# Protocol: structured 401 error code.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-protocol/src/lib.rs",
    "    BadRequest,\n    UnsupportedSchemaVersion,\n",
    "    BadRequest,\n    Unauthorized,\n    UnsupportedSchemaVersion,\n",
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '            ErrorCode::BadRequest => "bad_request",\n            ErrorCode::UnsupportedSchemaVersion => "unsupported_schema_version",\n',
    '            ErrorCode::BadRequest => "bad_request",\n            ErrorCode::Unauthorized => "unauthorized",\n            ErrorCode::UnsupportedSchemaVersion => "unsupported_schema_version",\n',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '            "bad_request" => Ok(ErrorCode::BadRequest),\n            "unsupported_schema_version" => Ok(ErrorCode::UnsupportedSchemaVersion),\n',
    '            "bad_request" => Ok(ErrorCode::BadRequest),\n            "unauthorized" => Ok(ErrorCode::Unauthorized),\n            "unsupported_schema_version" => Ok(ErrorCode::UnsupportedSchemaVersion),\n',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '                    "bad_request",\n                    "unsupported_schema_version",\n',
    '                    "bad_request",\n                    "unauthorized",\n                    "unsupported_schema_version",\n',
)

# ---------------------------------------------------------------------------
# HTTP API: protect /api/v1 with optional bearer auth; health/ready stay open.
# Store only SHA-256(token), never the plaintext token, in shared app state.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-api/Cargo.toml",
    "tracing = { workspace = true }\n",
    "tracing = { workspace = true }\nsha2 = { workspace = true }\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "use axum::http::{header, StatusCode};\nuse axum::response::{IntoResponse, Response};\n",
    "use axum::http::{header, HeaderValue, StatusCode};\nuse axum::middleware::{self, Next};\nuse axum::response::{IntoResponse, Response};\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "use hub_protocol as proto;\n",
    "use hub_protocol as proto;\nuse sha2::{Digest, Sha256};\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''pub struct HubState {
    pub orchestrator: Arc<Orchestrator>,
}

impl HubState {
    #[must_use]
    pub fn new(orchestrator: Arc<Orchestrator>) -> Self {
        Self { orchestrator }
    }
}
''',
    r'''pub struct HubState {
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
''',
)

# Replace the router construction so middleware is scoped only to /api/v1.
old_router = r'''pub fn router(state: HubState) -> Router {
    let manifest_limit = state.orchestrator.limits().max_manifest_bytes;
    Router::new()
        .route("/health", get(health))
        .route("/ready", get(ready))
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
        .layer(DefaultBodyLimit::max(manifest_limit))
        .with_state(state)
}
'''
new_router = r'''pub fn router(state: HubState) -> Router {
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
'''
replace_once("crates/hub-api/src/lib.rs", old_router, new_router)

# Add API auth regression before health tests.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    #[tokio::test]
    async fn health_and_ready_report_shape() {
''',
    r'''    #[tokio::test]
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
''',
)

# ---------------------------------------------------------------------------
# Daemon: token only from environment, and non-loopback bind fails closed
# unless authentication is configured.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "    #[error(\"invalid execution configuration: {0}\")]\n    ExecutorConfig(String),\n",
    "    #[error(\"invalid execution configuration: {0}\")]\n    ExecutorConfig(String),\n    #[error(\"invalid control-plane security configuration: {0}\")]\n    SecurityConfig(String),\n",
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
        address: args.listen.clone(),
        source,
    })?;

''',
    r'''    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
        address: args.listen.clone(),
        source,
    })?;
    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    validate_control_plane_security(listen, api_token.as_deref())?;
    if !listen.ip().is_loopback() {
        tracing::warn!(
            %listen,
            "control plane is non-loopback: bearer auth is enabled, but HTTP is plaintext; terminate TLS at a trusted boundary"
        );
    }

''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    runtime.block_on(async move {
        let app = hub_api::router(HubState::new(orchestrator));
        let listener = tokio::net::TcpListener::bind(listen)
''',
    r'''    runtime.block_on(async move {
        let mut state = HubState::new(orchestrator);
        if let Some(token) = api_token {
            state = state
                .with_bearer_token(token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        }
        let app = hub_api::router(state);
        let listener = tokio::net::TcpListener::bind(listen)
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    "async fn shutdown_signal() {\n",
    r'''fn validate_control_plane_security(
    listen: SocketAddr,
    api_token: Option<&str>,
) -> Result<(), DaemonError> {
    if !listen.ip().is_loopback() && api_token.is_none() {
        return Err(DaemonError::SecurityConfig(format!(
            "refusing unauthenticated non-loopback bind {listen}; set SCIRUST_HUB_TOKEN"
        )));
    }
    Ok(())
}

async fn shutdown_signal() {
''',
)
# append daemon config tests at EOF
p = Path("apps/scirust-hubd/src/main.rs")
text = p.read_text()
text += r'''

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
        let listen: SocketAddr = "127.0.0.1:8477".parse().unwrap();
        assert!(validate_control_plane_security(listen, None).is_ok());
    }

    #[test]
    fn non_loopback_bind_requires_control_plane_token() {
        let listen: SocketAddr = "0.0.0.0:8477".parse().unwrap();
        assert!(validate_control_plane_security(listen, None).is_err());
        assert!(validate_control_plane_security(listen, Some("secret")).is_ok());
    }
}
'''
p.write_text(text)

# ---------------------------------------------------------------------------
# CLI: automatically attach SCIRUST_HUB_TOKEN to every request. Keeping the
# token environment-only avoids exposing it in the process command line.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''fn url_of(args: &Args, path: &str) -> String {
    format!("{}{path}", args.url)
}

''',
    r'''fn url_of(args: &Args, path: &str) -> String {
    format!("{}{path}", args.url)
}

fn authorized(request: ureq::Request) -> ureq::Request {
    match std::env::var("SCIRUST_HUB_TOKEN") {
        Ok(token) if !token.is_empty() => {
            request.set("Authorization", &format!("Bearer {token}"))
        }
        _ => request,
    }
}

''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    ureq::get(&path_url)
        .call()
''',
    '''    authorized(ureq::get(&path_url))
        .call()
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    ureq::post(&path_url)
        .call()
''',
    '''    authorized(ureq::post(&path_url))
        .call()
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    ureq::post(path_url)
        .set("x-scirust-artifact-name", name)
''',
    '''    authorized(ureq::post(path_url))
        .set("x-scirust-artifact-name", name)
''',
)
replace_once(
    "apps/scirust-hub/src/main.rs",
    '''    request
        .send_json(payload)
''',
    '''    authorized(request)
        .send_json(payload)
''',
)

# ---------------------------------------------------------------------------
# MCP HTTP client: same environment-only token convention.
# ---------------------------------------------------------------------------
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''pub struct HttpHub {
    base_url: String,
}
''',
    '''pub struct HttpHub {
    base_url: String,
    bearer_token: Option<String>,
}
''',
)
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''        Self {
            base_url: base_url.into(),
        }
    }
}
''',
    r'''        Self {
            base_url: base_url.into(),
            bearer_token: std::env::var("SCIRUST_HUB_TOKEN")
                .ok()
                .filter(|token| !token.is_empty()),
        }
    }

    fn authorized(&self, request: ureq::Request) -> ureq::Request {
        match &self.bearer_token {
            Some(token) => request.set("Authorization", &format!("Bearer {token}")),
            None => request,
        }
    }
}
''',
)
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''        ureq::get(&format!("{}{path}", self.base_url))
            .call()
''',
    '''        self.authorized(ureq::get(&format!("{}{path}", self.base_url)))
            .call()
''',
)
replace_once(
    "crates/hub-mcp/src/lib.rs",
    '''        ureq::post(&format!("{}{path}", self.base_url))
            .send_json(body.clone())
''',
    '''        self.authorized(ureq::post(&format!("{}{path}", self.base_url)))
            .send_json(body.clone())
''',
)

# ---------------------------------------------------------------------------
# Security documentation and architecture decision.
# ---------------------------------------------------------------------------
Path("docs/adr/0011-control-plane-auth.md").write_text(r'''# ADR 0011 — Control-plane bearer authentication

Status: accepted

## Decision

`/api/v1/*` supports static bearer authentication configured only through the
`SCIRUST_HUB_TOKEN` environment variable. `/health` and `/ready` remain
unauthenticated so supervisors can probe process health without holding control
plane credentials.

The daemon refuses any non-loopback listen address unless a non-empty control
plane token is configured. Loopback remains unauthenticated by default for
backward-compatible local development. When a token is configured, it protects
the API even on loopback.

The HTTP adapter retains only `SHA-256(token)` in shared state. Incoming bearer
values are hashed and their fixed-size digests are compared without an early
exit. The CLI and read-only MCP adapter automatically attach
`SCIRUST_HUB_TOKEN` when it is present. Tokens are not accepted as command-line
arguments and are never intentionally logged.

## Boundary

Bearer authentication establishes possession of a shared secret; it does not
provide confidentiality or peer identity equivalent to TLS/mTLS. The daemon
therefore emits a warning for authenticated non-loopback plaintext HTTP.
Production network exposure must terminate TLS at a trusted reverse proxy,
service mesh or tunnel until native TLS/mTLS is deliberately implemented.

This change is authentication, not authorization. All authenticated API callers
retain the same control-plane permissions. Fine-grained principals/roles are a
future protocol decision.
''')

Path("SECURITY.md").write_text(r'''# Security policy

## Honest threat model

SciRust Hub executes processes. It provides resource control and execution
hygiene, not a sandbox:

- Process execution uses structured argv (no implicit shell construction).
- Child environments are constructed from scratch; only explicitly selected
  values are passed, and provenance records environment variable names rather
  than secret values.
- Captured output streams are bounded and truncation is recorded.
- Executions have wall-clock timeouts and cooperative cancellation.
- Working directories are per-run beneath the configured data directory.
- Remote worker transport rejects absolute/parent-traversal paths and does not
  assume a shared filesystem.

**A subprocess is not a sandbox.** Local and remote worker children execute with
the OS privileges of their respective daemon/worker process. They may access
resources permitted to that OS identity unless deployment-level isolation
prevents it.

## Control-plane authentication

`/api/v1/*` can be protected with a static bearer token supplied only through
`SCIRUST_HUB_TOKEN`. The daemon refuses a non-loopback listen address when that
token is absent. The CLI and the read-only MCP adapter automatically attach the
same environment variable when configured. `/health` and `/ready` intentionally
remain unauthenticated for supervisor probes.

The API stores only a SHA-256 verifier for the bearer token in shared state and
does not intentionally log token contents. The remote worker separately
requires `SCIRUST_HUB_WORKER_TOKEN`; these credentials serve different trust
boundaries and should not be reused.

Bearer authentication over HTTP **does not encrypt traffic** and does not
provide mTLS-style peer identity. Plain HTTP should remain on loopback or a
trusted private/tunneled network. Production exposure beyond that boundary must
terminate TLS at a trusted reverse proxy, service mesh or tunnel until native
TLS/mTLS support exists.

Authentication is currently coarse-grained: possession of the Hub token grants
access to the complete `/api/v1` surface. There are no per-principal roles yet.

## Additional boundaries

- Registration is metadata-only; registering a manifest never executes it.
- HTTP bodies are size-limited; manifests are version-checked and validated at
  domain construction time.
- Input artifact names are validated path components; blobs are
  content-addressed and written atomically.
- SciCapsule format/trust/extraction ownership remains in SciCapsule/SciRust;
  Hub validates only the published integration contract.

## Reporting

Report vulnerabilities privately to the maintainers via GitHub security
advisories for `Memorithm/scirust-hub` rather than public issues.
''')

print("control-plane authentication transformations complete")
