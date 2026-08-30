//! # scirust-hubd — the SciRust Hub daemon
//!
//! Boot order (each step fails fast with a typed error):
//! 1. parse and validate configuration;
//! 2. initialize tracing;
//! 3. initialize stores (in-memory registries + file-backed blobs);
//! 4. wire the orchestrator with the process executor;
//! 5. serve `/api/v1` until SIGINT/SIGTERM;
//! 6. shut down gracefully.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use clap::Parser;
use hub_api::HubState;
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
) -> Result<Arc<dyn Executor>, DaemonError> {
    match backend {
        ExecutorBackend::Process => Ok(Arc::new(ProcessExecutor::new())),
        ExecutorBackend::Remote => {
            if remote_worker_urls.is_empty() {
                return Err(DaemonError::ExecutorConfig(
                    "at least one --remote-worker-url is required with --executor remote".into(),
                ));
            }
            let token = remote_worker_token.ok_or_else(|| {
                DaemonError::ExecutorConfig(
                    "--remote-worker-token is required with --executor remote".into(),
                )
            })?;
            if remote_worker_urls.len() == 1 {
                let url = remote_worker_urls
                    .into_iter()
                    .next()
                    .expect("checked length");
                Ok(Arc::new(
                    RemoteExecutor::new(url, token).map_err(DaemonError::ExecutorConfig)?,
                ))
            } else {
                Ok(Arc::new(
                    RemotePoolExecutor::new(remote_worker_urls, token)
                        .map_err(DaemonError::ExecutorConfig)?,
                ))
            }
        }
    }
}

fn run(args: Args) -> Result<(), DaemonError> {
    init_tracing();

    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
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

    let executor = build_executor(
        args.executor,
        args.remote_worker_url,
        args.remote_worker_token,
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
        "scirust-hubd starting"
    );

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| std::io::Error::other(e.to_string()))
        .map_err(DaemonError::Serve)?;

    runtime.block_on(async move {
        let mut state = HubState::new(orchestrator).with_event_repository(event_store);
        if let Some(token) = api_token {
            state = state
                .with_bearer_token(token)
                .map_err(|error| DaemonError::SecurityConfig(error.to_string()))?;
        }
        let app = hub_api::router(state);
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(DaemonError::Serve)?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(DaemonError::Serve)
    })
}

fn validate_control_plane_security(
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
    fn repeated_remote_worker_urls_select_pool_configuration() {
        let single = build_executor(
            ExecutorBackend::Remote,
            vec!["http://worker-a:8488".into()],
            Some("secret".into()),
        )
        .expect("single");
        assert_eq!(single.backend_id(), "remote:http://worker-a:8488");

        let pool = build_executor(
            ExecutorBackend::Remote,
            vec!["http://worker-a:8488".into(), "http://worker-b:8488".into()],
            Some("secret".into()),
        )
        .expect("pool");
        assert_eq!(pool.backend_id(), "remote-pool");
    }

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
