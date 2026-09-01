from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    data = p.read_text()
    count = data.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one match, found {count}: {old[:120]!r}")
    p.write_text(data.replace(old, new, 1))


# hub-api dependency and public auth surface.
replace_once(
    "crates/hub-api/Cargo.toml",
    "sha2 = { workspace = true }\n",
    "sha2 = { workspace = true }\nthiserror = { workspace = true }\n",
)

replace_once(
    "crates/hub-api/src/lib.rs",
    "mod metrics;\n",
    "mod auth;\nmod metrics;\n\npub use auth::{\n    validate_static_principals, AuthenticatedPrincipal, AuthorizationConfigError,\n    ControlPlanePermission, PrincipalRole, StaticPrincipal, AUTHORIZATION_VERSION,\n};\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "use axum::http::{header, HeaderValue, StatusCode};\n",
    "use axum::http::{header, HeaderValue, Method, StatusCode};\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "use sha2::{Digest, Sha256};\n",
    "",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "    api_bearer_sha256: Option<[u8; 32]>,\n",
    "    authorization: Option<auth::AuthorizationState>,\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "            api_bearer_sha256: None,\n",
    "            authorization: None,\n",
)
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    /// Enables bearer authentication for protected control-plane routes
    /// (`/api/v1/*` and `/metrics`). Only a SHA-256 verifier is retained in
    /// shared state; the plaintext token is
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
''',
    '''    /// Enables the legacy single bearer token as a full-control principal.
    /// Only a SHA-256 verifier is retained; the plaintext token is discarded
    /// after construction.
    ///
    /// # Errors
    /// [`EmptyBearerToken`] when an empty token is supplied.
    pub fn with_bearer_token(mut self, token: impl AsRef<str>) -> Result<Self, EmptyBearerToken> {
        let token = token.as_ref();
        if token.is_empty() {
            return Err(EmptyBearerToken);
        }
        self.authorization = Some(auth::AuthorizationState::legacy(token));
        Ok(self)
    }

    /// Enables versioned static principal authorization for protected routes.
    ///
    /// # Errors
    /// [`AuthorizationConfigError`] when the principal set is empty or contains
    /// duplicate identities or bearer credentials.
    pub fn with_static_principals(
        mut self,
        principals: Vec<StaticPrincipal>,
    ) -> Result<Self, AuthorizationConfigError> {
        self.authorization = Some(auth::AuthorizationState::new(principals)?);
        Ok(self)
    }
''',
)
replace_once(
    "crates/hub-api/src/lib.rs",
    "            require_api_bearer,\n",
    "            require_api_authorization,\n",
)

middleware_start = Path("crates/hub-api/src/lib.rs").read_text().index("async fn require_api_bearer(\n")
middleware_end_marker = "\n// ----------------------------------------------------------------------\n// Handlers\n"
lib_data = Path("crates/hub-api/src/lib.rs").read_text()
middleware_end = lib_data.index(middleware_end_marker, middleware_start)
new_middleware = r'''async fn require_api_authorization(
    State(state): State<HubState>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(authorization) = state.authorization.as_ref() else {
        return next.run(request).await;
    };
    let token = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .filter(|value| !value.is_empty());
    let Some(principal) = token.and_then(|token| authorization.authenticate(token)) else {
        let mut response = error_response(
            StatusCode::UNAUTHORIZED,
            proto::ErrorCode::Unauthorized,
            "bearer authentication required",
        );
        response
            .headers_mut()
            .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        return response;
    };

    let permission = if matches!(*request.method(), Method::GET | Method::HEAD) {
        ControlPlanePermission::Read
    } else {
        ControlPlanePermission::Control
    };
    if !principal.role().allows(permission) {
        return error_response(
            StatusCode::FORBIDDEN,
            proto::ErrorCode::Forbidden,
            "authenticated principal is not authorized for this control-plane action",
        );
    }

    let principal_id = principal.id().to_owned();
    let method = request.method().clone();
    let path = request.uri().path().to_owned();
    request.extensions_mut().insert(principal);
    let response = next.run(request).await;
    if permission == ControlPlanePermission::Control && response.status().is_success() {
        tracing::info!(
            principal_id = %principal_id,
            %method,
            %path,
            "authorized control-plane mutation completed"
        );
    }
    response
}
'''
lib_data = lib_data[:middleware_start] + new_middleware + lib_data[middleware_end:]
Path("crates/hub-api/src/lib.rs").write_text(lib_data)

# Add authorization regression tests next to the legacy bearer test.
replace_once(
    "crates/hub-api/src/lib.rs",
    '''    #[test]
    fn empty_bearer_token_is_rejected() {
''',
    '''    #[tokio::test]
    async fn static_principals_separate_read_and_control_permissions() {
        let (state, _clock, _dir) = test_state();
        let state = state
            .with_static_principals(vec![
                StaticPrincipal::new("auditor", PrincipalRole::ReadOnly, "read-secret")
                    .expect("reader"),
                StaticPrincipal::new("operator", PrincipalRole::Control, "control-secret")
                    .expect("controller"),
            ])
            .expect("authorization");
        let app = router(state);

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer read-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");

        let metrics = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/metrics")
                    .header("authorization", "Bearer read-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(metrics.status(), StatusCode::OK);

        for spoofed_header in [None, Some("operator") ] {
            let mut request = Request::builder()
                .method("POST")
                .uri("/api/v1/components")
                .header("authorization", "Bearer read-secret");
            if let Some(value) = spoofed_header {
                request = request.header("x-scirust-hub-principal", value);
            }
            let (status, body) = send(app.clone(), request.body(Body::empty()).unwrap()).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
            assert_eq!(body["error"]["code"], "forbidden");
        }

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri("/api/v1/components")
                .header("authorization", "Bearer control-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");

        let (status, body) = send(
            app.clone(),
            Request::builder()
                .uri("/api/v1/components")
                .header("authorization", "Bearer wrong-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        assert_eq!(body["error"]["code"], "unauthorized");

        let (status, _) = send(
            app,
            Request::builder()
                .uri("/ready")
                .body(Body::empty())
                .unwrap(),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[test]
    fn empty_bearer_token_is_rejected() {
''',
)

# Protocol additive forbidden error code.
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''    Unauthorized,
    UnsupportedSchemaVersion,
''',
    '''    Unauthorized,
    Forbidden,
    UnsupportedSchemaVersion,
''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::UnsupportedSchemaVersion => "unsupported_schema_version",
''',
    '''            ErrorCode::Unauthorized => "unauthorized",
            ErrorCode::Forbidden => "forbidden",
            ErrorCode::UnsupportedSchemaVersion => "unsupported_schema_version",
''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''            "unauthorized" => Ok(ErrorCode::Unauthorized),
            "unsupported_schema_version" => Ok(ErrorCode::UnsupportedSchemaVersion),
''',
    '''            "unauthorized" => Ok(ErrorCode::Unauthorized),
            "forbidden" => Ok(ErrorCode::Forbidden),
            "unsupported_schema_version" => Ok(ErrorCode::UnsupportedSchemaVersion),
''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''                    "unauthorized",
                    "unsupported_schema_version",
''',
    '''                    "unauthorized",
                    "forbidden",
                    "unsupported_schema_version",
''',
)
replace_once(
    "crates/hub-protocol/src/lib.rs",
    '''    #[test]
    fn schema_version_gate_rejects_other_versions() {
''',
    '''    #[test]
    fn forbidden_error_code_is_additive_in_protocol_v1() {
        assert_eq!(PROTOCOL_VERSION, 1);
        let envelope = ErrorEnvelope::new(ErrorCode::Forbidden, "not authorized");
        let json = serde_json::to_string(&envelope).expect("serialize");
        assert!(json.contains("\\\"forbidden\\\""));
        let decoded: ErrorEnvelope = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(decoded.error.code, ErrorCode::Forbidden);
    }

    #[test]
    fn schema_version_gate_rejects_other_versions() {
''',
)

# Daemon versioned multi-principal configuration.
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''use hub_api::HubState;
''',
    '''use hub_api::{
    validate_static_principals, HubState, PrincipalRole, StaticPrincipal, AUTHORIZATION_VERSION,
};
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''const REMOTE_WORKER_TOKENS_JSON_ENV: &str = "SCIRUST_HUB_REMOTE_WORKER_TOKENS_JSON";
''',
    '''const REMOTE_WORKER_TOKENS_JSON_ENV: &str = "SCIRUST_HUB_REMOTE_WORKER_TOKENS_JSON";
const CONTROL_PLANE_PRINCIPALS_JSON_ENV: &str = "SCIRUST_HUB_PRINCIPALS_JSON";
''',
)

run_anchor = '''    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    validate_control_plane_security(listen, api_token.as_deref())?;
'''
run_replace = '''    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let principals_json = std::env::var(CONTROL_PLANE_PRINCIPALS_JSON_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    if api_token.is_some() && principals_json.is_some() {
        return Err(DaemonError::SecurityConfig(format!(
            "SCIRUST_HUB_TOKEN and {CONTROL_PLANE_PRINCIPALS_JSON_ENV} are mutually exclusive"
        )));
    }
    let static_principals = parse_control_plane_principals(principals_json.as_deref())?;
    let authentication_enabled = api_token.is_some() || static_principals.is_some();
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    validate_control_plane_security(listen, authentication_enabled)?;
'''
replace_once("apps/scirust-hubd/src/main.rs", run_anchor, run_replace)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''            "control plane is non-loopback: bearer auth is enabled, but HTTP is plaintext; enable native TLS or terminate TLS at a trusted boundary"
''',
    '''            "control plane is non-loopback: authentication is enabled, but HTTP is plaintext; enable native TLS or terminate TLS at a trusted boundary"
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''        if let Some(token) = api_token {
            state = state
                .with_bearer_token(token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        }
''',
    '''        if let Some(principals) = static_principals {
            state = state
                .with_static_principals(principals)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        } else if let Some(token) = api_token {
            state = state
                .with_bearer_token(token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        }
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''fn validate_control_plane_security(
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
''',
    '''fn validate_control_plane_security(
    listen: SocketAddr,
    authentication_enabled: bool,
) -> Result<(), DaemonError> {
    if !listen.ip().is_loopback() && !authentication_enabled {
        return Err(DaemonError::SecurityConfig(format!(
            "refusing unauthenticated non-loopback bind {listen}; set SCIRUST_HUB_TOKEN or {CONTROL_PLANE_PRINCIPALS_JSON_ENV}"
        )));
    }
    Ok(())
}

fn parse_control_plane_principals(
    raw: Option<&str>,
) -> Result<Option<Vec<StaticPrincipal>>, DaemonError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        DaemonError::SecurityConfig(format!(
            "invalid {CONTROL_PLANE_PRINCIPALS_JSON_ENV} JSON: {error}"
        ))
    })?;
    let object = value.as_object().ok_or_else(|| {
        DaemonError::SecurityConfig(format!(
            "{CONTROL_PLANE_PRINCIPALS_JSON_ENV} must be a JSON object"
        ))
    })?;
    for key in object.keys() {
        if !matches!(key.as_str(), "schema_version" | "principals") {
            return Err(DaemonError::SecurityConfig(format!(
                "unknown {CONTROL_PLANE_PRINCIPALS_JSON_ENV} field {key:?}"
            )));
        }
    }
    let version = object
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            DaemonError::SecurityConfig(format!(
                "{CONTROL_PLANE_PRINCIPALS_JSON_ENV}.schema_version must be an integer"
            ))
        })?;
    if version != u64::from(AUTHORIZATION_VERSION) {
        return Err(DaemonError::SecurityConfig(format!(
            "unsupported {CONTROL_PLANE_PRINCIPALS_JSON_ENV} schema_version {version}; expected {AUTHORIZATION_VERSION}"
        )));
    }
    let entries = object
        .get("principals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            DaemonError::SecurityConfig(format!(
                "{CONTROL_PLANE_PRINCIPALS_JSON_ENV}.principals must be an array"
            ))
        })?;
    if entries.is_empty() {
        return Err(DaemonError::SecurityConfig(format!(
            "{CONTROL_PLANE_PRINCIPALS_JSON_ENV}.principals must not be empty"
        )));
    }

    let mut principals = Vec::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        let entry = entry.as_object().ok_or_else(|| {
            DaemonError::SecurityConfig(format!(
                "{CONTROL_PLANE_PRINCIPALS_JSON_ENV}.principals[{index}] must be an object"
            ))
        })?;
        for key in entry.keys() {
            if !matches!(key.as_str(), "id" | "role" | "token") {
                return Err(DaemonError::SecurityConfig(format!(
                    "unknown principal field {key:?} at index {index}"
                )));
            }
        }
        let id = entry.get("id").and_then(serde_json::Value::as_str).ok_or_else(|| {
            DaemonError::SecurityConfig(format!("principal id must be a string at index {index}"))
        })?;
        let role = entry
            .get("role")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "principal role must be a string at index {index}"
                ))
            })?;
        let role = match role {
            "read_only" => PrincipalRole::ReadOnly,
            "control" => PrincipalRole::Control,
            other => {
                return Err(DaemonError::SecurityConfig(format!(
                    "unknown principal role {other:?} at index {index}"
                )));
            }
        };
        let token = entry
            .get("token")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "principal token must be a string at index {index}"
                ))
            })?;
        principals.push(
            StaticPrincipal::new(id, role, token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?,
        );
    }
    validate_static_principals(&principals)
        .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
    Ok(Some(principals))
}
''',
)

# Update daemon tests and add versioned-principal coverage.
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''        assert!(validate_control_plane_security(listen, None).is_ok());
''',
    '''        assert!(validate_control_plane_security(listen, false).is_ok());
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''        assert!(validate_control_plane_security(listen, None).is_err());
        assert!(validate_control_plane_security(listen, Some("secret")).is_ok());
''',
    '''        assert!(validate_control_plane_security(listen, false).is_err());
        assert!(validate_control_plane_security(listen, true).is_ok());
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
''',
    '''    #[test]
    fn versioned_static_principal_configuration_is_strict_and_secret_safe() {
        let principals = parse_control_plane_principals(Some(
            r#"{"schema_version":1,"principals":[{"id":"auditor","role":"read_only","token":"read-secret"},{"id":"operator","role":"control","token":"control-secret"}]}"#,
        ))
        .expect("parse")
        .expect("configured");
        assert_eq!(principals.len(), 2);
        assert_eq!(principals[0].id(), "auditor");
        assert_eq!(principals[0].role(), PrincipalRole::ReadOnly);
        let rendered = format!("{principals:?}");
        assert!(!rendered.contains("read-secret"));
        assert!(!rendered.contains("control-secret"));

        for raw in [
            r#"{"schema_version":2,"principals":[{"id":"a","role":"read_only","token":"x"}]}"#,
            r#"{"schema_version":1,"principals":[]}"#,
            r#"{"schema_version":1,"principals":[{"id":"a","role":"unknown","token":"x"}]}"#,
            r#"{"schema_version":1,"principals":[{"id":"a","role":"read_only","token":"same"},{"id":"b","role":"control","token":"same"}]}"#,
            r#"{"schema_version":1,"principals":[{"id":"a","role":"read_only","token":"x","extra":true}]}"#,
        ] {
            let error = parse_control_plane_principals(Some(raw)).expect_err("invalid config");
            assert!(!error.to_string().contains("read-secret"));
            assert!(!error.to_string().contains("control-secret"));
        }
    }

    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
''',
)
