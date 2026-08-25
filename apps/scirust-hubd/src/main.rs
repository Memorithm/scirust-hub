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
use hub_core::limits::Limits;
use hub_core::memory::{
    FileSystemArtifactStore, InMemoryArtifactMeta, InMemoryComponents, InMemoryRuns,
};
use hub_core::store::{ArtifactMetadataRepository, ComponentRepository, RunRepository};
use hub_core::Orchestrator;
use hub_executor::ProcessExecutor;
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
    blob_store: FileSystemArtifactStore,
    workdir_root: PathBuf,
) -> Arc<Orchestrator> {
    Arc::new(Orchestrator::new(
        Arc::new(SystemClock),
        components,
        runs,
        artifacts_meta,
        blob_store,
        Arc::new(ProcessExecutor::new()),
        Limits::default(),
        workdir_root,
    ))
}

fn run(args: Args) -> Result<(), DaemonError> {
    init_tracing();

    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
        address: args.listen.clone(),
        source,
    })?;

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

    // One store instance serves all three repository ports; `Arc` is
    // coerced separately per port.
    let orchestrator = match args.store {
        StoreBackend::Sqlite => {
            let db = args.data_dir.join("hub.db");
            let store = Arc::new(SqliteStore::open(&db)?);
            tracing::info!(db = %db.display(), "using durable sqlite stores");
            build_orchestrator(
                store.clone(),
                store.clone(),
                store,
                blob_store,
                workdir_root,
            )
        }
        StoreBackend::Memory => {
            tracing::info!("using in-memory stores (state resets on restart)");
            build_orchestrator(
                Arc::new(InMemoryComponents::default()),
                Arc::new(InMemoryRuns::default()),
                Arc::new(InMemoryArtifactMeta::default()),
                blob_store,
                workdir_root,
            )
        }
    };

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
        let app = hub_api::router(HubState::new(orchestrator));
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(DaemonError::Serve)?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(DaemonError::Serve)
    })
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
