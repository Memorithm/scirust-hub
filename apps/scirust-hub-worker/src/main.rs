//! `scirust-hub-worker` — authenticated remote process execution worker.

use std::net::SocketAddr;
use std::path::PathBuf;

use axum_server::tls_rustls::RustlsConfig;
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
    /// PEM certificate chain for native HTTPS. Must be set with `--tls-key`.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for native HTTPS. Must be set with `--tls-cert`.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TLS_KEY")]
    tls_key: Option<PathBuf>,
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
    #[error("invalid TLS configuration: {0}")]
    TlsConfig(String),
}

fn main() {
    let args = Args::parse();
    if let Err(error) = run(args) {
        eprintln!("scirust-hub-worker: {error}");
        std::process::exit(1);
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TlsFiles {
    cert: PathBuf,
    key: PathBuf,
}

fn tls_files(cert: Option<PathBuf>, key: Option<PathBuf>) -> Result<Option<TlsFiles>, WorkerError> {
    match (cert, key) {
        (None, None) => Ok(None),
        (Some(cert), Some(key)) => Ok(Some(TlsFiles { cert, key })),
        (Some(_), None) => Err(WorkerError::TlsConfig(
            "--tls-cert requires --tls-key (or set both SCIRUST_HUB_WORKER_TLS_CERT and SCIRUST_HUB_WORKER_TLS_KEY)".into(),
        )),
        (None, Some(_)) => Err(WorkerError::TlsConfig(
            "--tls-key requires --tls-cert (or set both SCIRUST_HUB_WORKER_TLS_CERT and SCIRUST_HUB_WORKER_TLS_KEY)".into(),
        )),
    }
}

fn install_rustls_crypto_provider() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
}

fn run(args: Args) -> Result<(), WorkerError> {
    // Select the process-level provider explicitly before Rustls configuration
    // is built; this is robust to additive Cargo features enabling both
    // built-in providers elsewhere in the dependency graph.
    install_rustls_crypto_provider();
    let listen: SocketAddr = args
        .listen
        .parse()
        .map_err(|_| WorkerError::Listen(args.listen.clone()))?;
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    if !listen.ip().is_loopback() && tls.is_none() {
        eprintln!(
            "scirust-hub-worker: warning: non-loopback worker bind uses plaintext HTTP; enable native TLS or a trusted TLS/tunnel boundary"
        );
    }
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
        if let Some(tls) = tls {
            let config = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
                .await
                .map_err(|error| {
                    WorkerError::TlsConfig(format!("loading PEM certificate/private key: {error}"))
                })?;
            axum_server::bind_rustls(listen, config)
                .serve(hub_executor::worker::router(service).into_make_service())
                .await
                .map_err(WorkerError::Serve)
        } else {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(WorkerError::Serve)?;
            hub_executor::worker::serve(listener, service)
                .await
                .map_err(WorkerError::Serve)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
