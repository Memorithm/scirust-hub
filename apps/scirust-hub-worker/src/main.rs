//! `scirust-hub-worker` — authenticated remote process execution worker.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use hub_executor::worker::WorkerService;

const DEFAULT_LISTEN: &str = "127.0.0.1:8488";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(
    name = "scirust-hub-worker",
    about = "SciRust Hub remote execution worker",
    version
)]
struct Args {
    #[arg(long, env = "SCIRUST_HUB_WORKER_LISTEN", default_value = DEFAULT_LISTEN)]
    listen: String,
    #[arg(long, env = "SCIRUST_HUB_WORKER_ID", default_value = "worker-1")]
    worker_id: String,
    /// Shared bearer token. Required; never logged.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TOKEN")]
    token: String,
    #[arg(
        long,
        env = "SCIRUST_HUB_WORKER_DATA_DIR",
        default_value = "scirust-hub-worker-data"
    )]
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
