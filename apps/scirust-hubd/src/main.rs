//! # scirust-hubd — the SciRust Hub daemon
//!
//! Boot order (each step fails fast with a typed error):
//! 1. parse and validate configuration;
//! 2. initialize tracing;
//! 3. initialize stores (in-memory registries + file-backed blobs);
//! 4. wire the orchestrator with the process executor;
//! 5. serve `/api/v1` until SIGINT/SIGTERM;
//! 6. shut down gracefully.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;

use clap::Parser;
use hub_api::{AuthPermission, HubState, StaticPrincipal};
use hub_core::clock::SystemClock;
use hub_core::exec::Executor;
use hub_core::limits::Limits;
use hub_core::memory::{FileSystemArtifactStore, InMemoryHubStore};
use hub_core::store::{
    ArtifactMetadataRepository, ComponentRepository, LifecycleEventRepository, RunRepository,
    WorkflowRepository,
};
use hub_core::Orchestrator;
use hub_executor::{ProcessExecutor, RemoteExecutor, RemotePoolExecutor};
use hub_store_sqlite::SqliteStore;

/// Default TCP listen address.
const DEFAULT_LISTEN: &str = "127.0.0.1:8477";
const REMOTE_WORKER_TOKENS_JSON_ENV: &str = "SCIRUST_HUB_REMOTE_WORKER_TOKENS_JSON";
const PRINCIPALS_JSON_ENV: &str = "SCIRUST_HUB_PRINCIPALS_JSON";

#[derive(Debug, clap::Parser)]
#[command(
    name = "scirust-hubd",
    about = "SciRust Hub control plane daemon",
    version
)]
struct Args {
    /// Address to bind, e.g. 127.0.0.1:8477 or 0.0.0.0:8477.
    #[arg(long, env = "SCIRUST_HUB_LISTEN", default_value = DEFAULT_LISTEN)]
    listen: String,
    /// Data directory for the database, artifact blobs and per-run working
    /// directories.
    #[arg(long, env = "SCIRUST_HUB_DATA_DIR", default_value = "scirust-hub-data")]
    data_dir: PathBuf,
    /// Registry/run persistence backend. `sqlite` survives restarts;
    /// `memory` keeps everything in process (tests, throwaway runs).
    #[arg(
        long,
        env = "SCIRUST_HUB_STORE",
        value_enum,
        default_value_t = StoreBackend::Sqlite
    )]
    store: StoreBackend,
    /// Execution backend. Remote mode requires worker URL + bearer token.
    #[arg(
        long,
        env = "SCIRUST_HUB_EXECUTOR",
        value_enum,
        default_value_t = ExecutorBackend::Process
    )]
    executor: ExecutorBackend,
    /// One or more worker URLs. Repeat the flag or comma-separate the
    /// environment value to enable deterministic multi-worker placement.
    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_URL", value_delimiter = ',')]
    remote_worker_url: Vec<String>,
    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_TOKEN")]
    remote_worker_token: Option<String>,
    /// PEM certificate chain for native HTTPS. Must be set with `--tls-key`.
    #[arg(long, env = "SCIRUST_HUB_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for native HTTPS. Must be set with `--tls-cert`.
    #[arg(long, env = "SCIRUST_HUB_TLS_KEY")]
    tls_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum ExecutorBackend {
    Process,
    Remote,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum StoreBackend {
    Sqlite,
    Memory,
}

#[derive(Debug, thiserror::Error)]
enum DaemonError {
    #[error("invalid listen address {address:?}: {source}")]
    Listen {
        address: String,
        source: std::net::AddrParseError,
    },
    #[error("initializing data directory {path:?}: {source}")]
    DataDir {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("serving: {0}")]
    Serve(std::io::Error),
    #[error("opening persistent store: {0}")]
    Store(String),
    #[error("invalid execution configuration: {0}")]
    ExecutorConfig(String),
    #[error("invalid control-plane security configuration: {0}")]
    SecurityConfig(String),
    #[error("invalid TLS configuration: {0}")]
    TlsConfig(String),
}

impl From<hub_core::CoreError> for DaemonError {
    fn from(error: hub_core::CoreError) -> Self {
        DaemonError::Store(error.to_string())
    }
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(args) {
        // One structured line on stderr; no panic, no backtrace noise.
        eprintln!("scirust-hubd: {error}");
        std::process::exit(1);
    }
}

#[allow(clippy::too_many_arguments)]
fn build_orchestrator(
    components: Arc<dyn ComponentRepository>,
    runs: Arc<dyn RunRepository>,
    artifacts_meta: Arc<dyn ArtifactMetadataRepository>,
    workflows: Arc<dyn WorkflowRepository>,
    blob_store: FileSystemArtifactStore,
    executor: Arc<dyn Executor>,
    workdir_root: PathBuf,
) -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(
        Arc::new(SystemClock),
        components,
        runs,
        artifacts_meta,
        workflows,
        blob_store,
        executor,
        Limits::default(),
        workdir_root,
    ))
}

fn build_executor(
    backend: ExecutorBackend,
    remote_worker_urls: Vec<String>,
    remote_worker_token: Option<String>,
    remote_worker_tokens_json: Option<&str>,
) -> Result<Arc<dyn Executor>, DaemonError> {
    match backend {
        ExecutorBackend::Process => Ok(Arc::new(ProcessExecutor::new())),
        ExecutorBackend::Remote => {
            if remote_worker_urls.is_empty() {
                return Err(DaemonError::ExecutorConfig(
                    "at least one --remote-worker-url is required with --executor remote".into(),
                ));
            }
            let credentials = remote_worker_credentials(
                &remote_worker_urls,
                remote_worker_token,
                remote_worker_tokens_json,
            )?;
            if credentials.len() == 1 {
                let (url, token) = credentials.into_iter().next().expect("checked length");
                Ok(Arc::new(
                    RemoteExecutor::new(url, token).map_err(DaemonError::ExecutorConfig)?,
                ))
            } else {
                Ok(Arc::new(
                    RemotePoolExecutor::from_credentials(credentials)
                        .map_err(DaemonError::ExecutorConfig)?,
                ))
            }
        }
    }
}

fn normalize_worker_endpoint(endpoint: &str) -> String {
    endpoint.trim_end_matches('/').to_owned()
}

fn remote_worker_credentials(
    remote_worker_urls: &[String],
    shared_token: Option<String>,
    per_worker_json: Option<&str>,
) -> Result<Vec<(String, String)>, DaemonError> {
    if shared_token.is_some() && per_worker_json.is_some() {
        return Err(DaemonError::ExecutorConfig(format!(
            "SCIRUST_HUB_REMOTE_WORKER_TOKEN and {REMOTE_WORKER_TOKENS_JSON_ENV} are mutually exclusive"
        )));
    }

    let mut normalized_urls = Vec::with_capacity(remote_worker_urls.len());
    let mut seen_urls = BTreeMap::<String, ()>::new();
    for endpoint in remote_worker_urls {
        let normalized = normalize_worker_endpoint(endpoint);
        if seen_urls.insert(normalized.clone(), ()).is_some() {
            return Err(DaemonError::ExecutorConfig(format!(
                "duplicate remote worker endpoint {normalized:?}"
            )));
        }
        normalized_urls.push(normalized);
    }

    match (shared_token, per_worker_json) {
        (Some(token), None) => {
            if token.is_empty() {
                return Err(DaemonError::ExecutorConfig(
                    "remote worker bearer token must not be empty".into(),
                ));
            }
            Ok(normalized_urls
                .into_iter()
                .map(|endpoint| (endpoint, token.clone()))
                .collect())
        }
        (None, Some(raw)) => {
            let configured: BTreeMap<String, String> = serde_json::from_str(raw).map_err(|error| {
                DaemonError::ExecutorConfig(format!(
                    "invalid {REMOTE_WORKER_TOKENS_JSON_ENV} JSON object: {error}"
                ))
            })?;
            if configured.is_empty() {
                return Err(DaemonError::ExecutorConfig(format!(
                    "{REMOTE_WORKER_TOKENS_JSON_ENV} must not be empty"
                )));
            }

            let mut tokens = BTreeMap::<String, String>::new();
            for (endpoint, token) in configured {
                let normalized = normalize_worker_endpoint(&endpoint);
                if token.is_empty() {
                    return Err(DaemonError::ExecutorConfig(format!(
                        "remote worker bearer token must not be empty for endpoint {normalized:?}"
                    )));
                }
                if tokens.insert(normalized.clone(), token).is_some() {
                    return Err(DaemonError::ExecutorConfig(format!(
                        "duplicate normalized credential endpoint {normalized:?} in {REMOTE_WORKER_TOKENS_JSON_ENV}"
                    )));
                }
            }

            let mut credentials = Vec::with_capacity(normalized_urls.len());
            for endpoint in normalized_urls {
                let token = tokens.remove(&endpoint).ok_or_else(|| {
                    DaemonError::ExecutorConfig(format!(
                        "missing worker credential for configured endpoint {endpoint:?} in {REMOTE_WORKER_TOKENS_JSON_ENV}"
                    ))
                })?;
                credentials.push((endpoint, token));
            }
            if !tokens.is_empty() {
                let extras = tokens.keys().cloned().collect::<Vec<_>>().join(", ");
                return Err(DaemonError::ExecutorConfig(format!(
                    "unused worker credential endpoint(s) in {REMOTE_WORKER_TOKENS_JSON_ENV}: {extras}"
                )));
            }
            Ok(credentials)
        }
        (None, None) => Err(DaemonError::ExecutorConfig(format!(
            "remote execution requires SCIRUST_HUB_REMOTE_WORKER_TOKEN or {REMOTE_WORKER_TOKENS_JSON_ENV}"
        ))),
        (Some(_), Some(_)) => unreachable!("mutual exclusion checked above"),
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TlsFiles {
    cert: PathBuf,
    key: PathBuf,
}

fn tls_files(cert: Option<PathBuf>, key: Option<PathBuf>) -> Result<Option<TlsFiles>, DaemonError> {
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => Ok(Some(TlsFiles { cert, key })),
        (Some(_), None) => Err(DaemonError::TlsConfig(
            "--tls-cert requires --tls-key (or set both SCIRUST_HUB_TLS_CERT and SCIRUST_HUB_TLS_KEY)".into(),
        )),
        (None, Some(_)) => Err(DaemonError::TlsConfig(
            "--tls-key requires --tls-cert (or set both SCIRUST_HUB_TLS_CERT and SCIRUST_HUB_TLS_KEY)".into(),
        )),
    }
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn run(args: Args) -> Result<(), DaemonError> {
    // Cargo feature unification can make both built-in Rustls providers
    // available through the server and HTTP-client dependency graph. Select
    // one process-wide provider before either server or remote-client TLS can
    // construct a Rustls config.
    install_rustls_crypto_provider();
    init_tracing();

    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
        address: args.listen.clone(),
        source,
    })?;
    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let principals_json = std::env::var(PRINCIPALS_JSON_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    if api_token.is_some() && principals_json.is_some() {
        return Err(DaemonError::SecurityConfig(format!(
            "SCIRUST_HUB_TOKEN and {PRINCIPALS_JSON_ENV} are mutually exclusive"
        )));
    }
    let static_principals = parse_static_principals(principals_json.as_deref())?;
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    validate_control_plane_security(listen, api_token.is_some() || static_principals.is_some())?;
    if !listen.ip().is_loopback() && tls.is_none() {
        tracing::warn!(
            %listen,
            "control plane is non-loopback: bearer auth is enabled, but HTTP is plaintext; enable native TLS or terminate TLS at a trusted boundary"
        );
    }

    std::fs::create_dir_all(&args.data_dir).map_err(|source| DaemonError::DataDir {
        path: args.data_dir.clone(),
        source,
    })?;
    let blob_store = FileSystemArtifactStore::open(args.data_dir.join("blobs")).map_err(|e| {
        DaemonError::DataDir {
            path: args.data_dir.join("blobs"),
            source: std::io::Error::other(e.to_string()),
        }
    })?;
    let workdir_root = args.data_dir.join("runs");
    std::fs::create_dir_all(&workdir_root).map_err(|source| DaemonError::DataDir {
        path: workdir_root.clone(),
        source,
    })?;

    let remote_worker_tokens_json = std::env::var(REMOTE_WORKER_TOKENS_JSON_ENV)
        .ok()
        .filter(|value| !value.trim().is_empty());
    let executor = build_executor(
        args.executor,
        args.remote_worker_url,
        args.remote_worker_token,
        remote_worker_tokens_json.as_deref(),
    )?;

    // One store instance serves all repository ports; `Arc` is coerced
    // separately per port.
    let (orchestrator, event_store): (Arc<Orchestrator>, Arc<dyn LifecycleEventRepository>) =
        match args.store {
            StoreBackend::Sqlite => {
                let db = args.data_dir.join("hub.db");
                let store = Arc::new(SqliteStore::open(&db)?);
                tracing::info!(db = %db.display(), "using durable sqlite stores");
                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );
                (orchestrator, store)
            }
            StoreBackend::Memory => {
                tracing::info!("using in-memory stores (state resets on restart)");
                let store = Arc::new(InMemoryHubStore::default());
                let orchestrator = build_orchestrator(
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    store.clone(),
                    blob_store,
                    executor.clone(),
                    workdir_root,
                );
                (orchestrator, store)
            }
        };

    let recovered_cancellations = orchestrator.recover_workflow_cancellations()?;
    if recovered_cancellations > 0 {
        tracing::info!(
            recovered_cancellations,
            "reconciled workflow cancellations after restart"
        );
    }
    let recovered_interruptions = orchestrator.recover_interrupted_workflows()?;
    if recovered_interruptions > 0 {
        tracing::warn!(
            recovered_interruptions,
            "failed closed workflows interrupted by the previous daemon lifetime"
        );
    }

    tracing::info!(
        %listen,
        data_dir = %args.data_dir.display(),
        executor = orchestrator.executor_backend_id(),
        transport = if tls.is_some() { "https" } else { "http" },
        "scirust-hubd starting"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| std::io::Error::other(e.to_string()))
        .map_err(DaemonError::Serve)?;

    runtime.block_on(async move {
        let mut state = HubState::new(orchestrator).with_event_repository(event_store);
        if let Some(principals) = static_principals {
            state = state
                .with_static_principals(principals)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        } else if let Some(token) = api_token {
            state = state
                .with_bearer_token(token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        }
        let app = hub_api::router(state);
        if let Some(tls) = tls {
            let config = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
                .await
                .map_err(|error| {
                    DaemonError::TlsConfig(format!("loading PEM certificate/private key: {error}"))
                })?;
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            tokio::spawn(async move {
                shutdown_signal().await;
                shutdown_handle.graceful_shutdown(Some(Duration::from_secs(30)));
            });
            axum_server::bind_rustls(listen, config)
                .handle(handle)
                .serve(app.into_make_service())
                .await
                .map_err(DaemonError::Serve)
        } else {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(DaemonError::Serve)?;
            axum::serve(listener, app)
                .with_graceful_shutdown(shutdown_signal())
                .await
                .map_err(DaemonError::Serve)
        }
    })
}

fn validate_control_plane_security(
    listen: SocketAddr,
    authentication_configured: bool,
) -> Result<(), DaemonError> {
    if !listen.ip().is_loopback() && !authentication_configured {
        return Err(DaemonError::SecurityConfig(format!(
            "refusing unauthenticated non-loopback bind {listen}; set SCIRUST_HUB_TOKEN or {PRINCIPALS_JSON_ENV}"
        )));
    }
    Ok(())
}

fn parse_static_principals(raw: Option<&str>) -> Result<Option<Vec<StaticPrincipal>>, DaemonError> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    let value: serde_json::Value = serde_json::from_str(raw).map_err(|error| {
        DaemonError::SecurityConfig(format!("invalid {PRINCIPALS_JSON_ENV} JSON: {error}"))
    })?;
    let root = value.as_object().ok_or_else(|| {
        DaemonError::SecurityConfig(format!("{PRINCIPALS_JSON_ENV} must be a JSON object"))
    })?;
    for key in root.keys() {
        if !matches!(key.as_str(), "schema_version" | "principals") {
            return Err(DaemonError::SecurityConfig(format!(
                "unknown {PRINCIPALS_JSON_ENV} field {key:?}"
            )));
        }
    }
    if root
        .get("schema_version")
        .and_then(serde_json::Value::as_u64)
        != Some(1)
    {
        return Err(DaemonError::SecurityConfig(format!(
            "{PRINCIPALS_JSON_ENV} requires schema_version 1"
        )));
    }
    let records = root
        .get("principals")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            DaemonError::SecurityConfig(format!(
                "{PRINCIPALS_JSON_ENV}.principals must be an array"
            ))
        })?;
    let mut principals = Vec::with_capacity(records.len());
    for record in records {
        let object = record.as_object().ok_or_else(|| {
            DaemonError::SecurityConfig("each static principal must be a JSON object".into())
        })?;
        for key in object.keys() {
            if !matches!(key.as_str(), "id" | "token" | "permissions") {
                return Err(DaemonError::SecurityConfig(format!(
                    "unknown static principal field {key:?}"
                )));
            }
        }
        let id = object
            .get("id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                DaemonError::SecurityConfig("static principal id must be a string".into())
            })?;
        let token = object
            .get("token")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "static principal {id:?} token must be a string"
                ))
            })?;
        let permission_values = object
            .get("permissions")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "static principal {id:?} permissions must be an array"
                ))
            })?;
        let mut permissions = Vec::with_capacity(permission_values.len());
        for permission in permission_values {
            let raw_permission = permission.as_str().ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "static principal {id:?} permission must be a string"
                ))
            })?;
            permissions.push(AuthPermission::parse(raw_permission).ok_or_else(|| {
                DaemonError::SecurityConfig(format!(
                    "unknown static principal permission {raw_permission:?} for {id:?}"
                ))
            })?);
        }
        principals.push(
            StaticPrincipal::new(id, token, permissions)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?,
        );
    }
    hub_api::validate_static_principals(&principals)
        .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
    Ok(Some(principals))
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::warn!(%e, "ctrl_c listener failed");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(e) => tracing::warn!(%e, "SIGTERM listener unavailable"),
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_static_principal_config_is_strict_and_secret_safe() {
        let raw = r#"{"schema_version":1,"principals":[{"id":"observer","token":"secret-a","permissions":["inspect","metrics"]},{"id":"controller","token":"secret-b","permissions":["inspect","control","metrics"]}]}"#;
        let principals = parse_static_principals(Some(raw))
            .expect("valid")
            .expect("configured");
        assert_eq!(principals.len(), 2);
        assert_eq!(principals[0].id(), "observer");

        let unknown = r#"{"schema_version":1,"principals":[{"id":"observer","token":"secret-a","permissions":["admin"]}]}"#;
        assert!(parse_static_principals(Some(unknown)).is_err());
        let duplicate_id = r#"{"schema_version":1,"principals":[{"id":"same","token":"secret-a","permissions":["inspect"]},{"id":"same","token":"secret-b","permissions":["control"]}]}"#;
        assert!(parse_static_principals(Some(duplicate_id)).is_err());
        let duplicate_token = r#"{"schema_version":1,"principals":[{"id":"a","token":"same-secret","permissions":["inspect"]},{"id":"b","token":"same-secret","permissions":["control"]}]}"#;
        assert!(parse_static_principals(Some(duplicate_token)).is_err());
    }

    #[test]
    fn rustls_crypto_provider_is_installed_explicitly() {
        install_rustls_crypto_provider();
        assert!(rustls::crypto::CryptoProvider::get_default().is_some());
    }

    #[test]
    fn tls_configuration_requires_cert_and_key_together() {
        assert!(tls_files(None, None).expect("disabled").is_none());
        assert!(tls_files(Some("cert.pem".into()), None).is_err());
        assert!(tls_files(None, Some("key.pem".into())).is_err());
        assert_eq!(
            tls_files(Some("cert.pem".into()), Some("key.pem".into()))
                .expect("pair")
                .expect("enabled"),
            TlsFiles {
                cert: "cert.pem".into(),
                key: "key.pem".into(),
            }
        );
    }

    #[test]
    fn repeated_remote_worker_urls_select_pool_configuration() {
        let single = build_executor(
            ExecutorBackend::Remote,
            vec!["http://worker-a:8488".into()],
            Some("secret".into()),
            None,
        )
        .expect("single");
        assert_eq!(single.backend_id(), "remote:http://worker-a:8488");

        let pool = build_executor(
            ExecutorBackend::Remote,
            vec!["http://worker-a:8488".into(), "http://worker-b:8488".into()],
            Some("secret".into()),
            None,
        )
        .expect("pool");
        assert_eq!(pool.backend_id(), "remote-pool");
    }

    #[test]
    fn per_worker_credentials_match_normalized_endpoints_exactly() {
        let urls = vec![
            "http://worker-a:8488/".to_owned(),
            "http://worker-b:8488".to_owned(),
        ];
        let credentials = remote_worker_credentials(
            &urls,
            None,
            Some(r#"{"http://worker-a:8488":"secret-a","http://worker-b:8488/":"secret-b"}"#),
        )
        .expect("credentials");
        assert_eq!(
            credentials,
            vec![
                ("http://worker-a:8488".to_owned(), "secret-a".to_owned()),
                ("http://worker-b:8488".to_owned(), "secret-b".to_owned()),
            ]
        );
    }

    #[test]
    fn per_worker_credentials_fail_closed_on_ambiguous_or_drifted_configuration() {
        let urls = vec![
            "http://worker-a:8488".to_owned(),
            "http://worker-b:8488".to_owned(),
        ];

        let both = remote_worker_credentials(
            &urls,
            Some("shared-secret".into()),
            Some(r#"{"http://worker-a:8488":"secret-a","http://worker-b:8488":"secret-b"}"#),
        )
        .expect_err("mutually exclusive configuration");
        assert!(both.to_string().contains("mutually exclusive"));
        assert!(!both.to_string().contains("shared-secret"));
        assert!(!both.to_string().contains("secret-a"));

        let missing =
            remote_worker_credentials(&urls, None, Some(r#"{"http://worker-a:8488":"secret-a"}"#))
                .expect_err("missing credential");
        assert!(missing.to_string().contains("worker-b"));
        assert!(!missing.to_string().contains("secret-a"));

        let extra = remote_worker_credentials(
            &urls,
            None,
            Some(r#"{"http://worker-a:8488":"secret-a","http://worker-b:8488":"secret-b","http://worker-c:8488":"secret-c"}"#),
        )
        .expect_err("unused credential");
        assert!(extra.to_string().contains("worker-c"));
        assert!(!extra.to_string().contains("secret-c"));
    }

    #[test]
    fn per_worker_credentials_reject_empty_and_normalized_duplicates() {
        let urls = vec![
            "http://worker-a:8488".to_owned(),
            "http://worker-b:8488".to_owned(),
        ];
        let empty = remote_worker_credentials(
            &urls,
            None,
            Some(r#"{"http://worker-a:8488":"secret-a","http://worker-b:8488":""}"#),
        )
        .expect_err("empty token");
        assert!(empty.to_string().contains("worker-b"));
        assert!(!empty.to_string().contains("secret-a"));

        let duplicate_urls = vec![
            "http://worker-a:8488".to_owned(),
            "http://worker-a:8488/".to_owned(),
        ];
        let duplicate =
            remote_worker_credentials(&duplicate_urls, Some("shared-secret".into()), None)
                .expect_err("duplicate normalized configured endpoint");
        assert!(duplicate
            .to_string()
            .contains("duplicate remote worker endpoint"));
        assert!(!duplicate.to_string().contains("shared-secret"));

        let normalized_map_duplicate = remote_worker_credentials(
            &urls,
            None,
            Some(r#"{"http://worker-a:8488":"secret-a","http://worker-a:8488/":"secret-b","http://worker-b:8488":"secret-c"}"#),
        )
        .expect_err("duplicate normalized credential endpoint");
        assert!(normalized_map_duplicate
            .to_string()
            .contains("duplicate normalized credential endpoint"));
        assert!(!normalized_map_duplicate.to_string().contains("secret-a"));
        assert!(!normalized_map_duplicate.to_string().contains("secret-b"));
    }

    #[test]
    fn loopback_may_remain_unauthenticated_for_local_compatibility() {
        let listen: SocketAddr = "127.0.0.1:8477".parse().unwrap();
        assert!(validate_control_plane_security(listen, false).is_ok());
    }

    #[test]
    fn non_loopback_bind_requires_control_plane_token() {
        let listen: SocketAddr = "0.0.0.0:8477".parse().unwrap();
        assert!(validate_control_plane_security(listen, false).is_err());
        assert!(validate_control_plane_security(listen, true).is_ok());
    }
}
