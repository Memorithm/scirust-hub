from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:160]!r}")
    p.write_text(text.replace(old, new, 1))


# Workspace/server dependencies.
replace_once(
    "Cargo.toml",
    'axum = { version = "0.8", default-features = false, features = ["http1", "json", "tokio", "query"] }\n',
    'axum = { version = "0.8", default-features = false, features = ["http1", "json", "tokio", "query"] }\naxum-server = { version = "0.8", features = ["tls-rustls"] }\n',
)
for manifest in ["apps/scirust-hubd/Cargo.toml", "apps/scirust-hub-worker/Cargo.toml"]:
    replace_once(
        manifest,
        'tokio = { workspace = true }\n',
        'tokio = { workspace = true }\naxum-server = { workspace = true }\n',
    )


# ---------------------------------------------------------------------------
# Control-plane daemon: opt-in native HTTPS using PEM cert/key files.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hubd/src/main.rs",
    'use std::sync::Arc;\n',
    'use std::sync::Arc;\nuse std::time::Duration;\n\nuse axum_server::tls_rustls::RustlsConfig;\n',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_TOKEN")]
    remote_worker_token: Option<String>,
}''',
    '''    #[arg(long, env = "SCIRUST_HUB_REMOTE_WORKER_TOKEN")]
    remote_worker_token: Option<String>,
    /// PEM certificate chain for native HTTPS. Must be set with `--tls-key`.
    #[arg(long, env = "SCIRUST_HUB_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for native HTTPS. Must be set with `--tls-cert`.
    #[arg(long, env = "SCIRUST_HUB_TLS_KEY")]
    tls_key: Option<PathBuf>,
}''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[error("invalid control-plane security configuration: {0}")]
    SecurityConfig(String),
}''',
    '''    #[error("invalid control-plane security configuration: {0}")]
    SecurityConfig(String),
    #[error("invalid TLS configuration: {0}")]
    TlsConfig(String),
}''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''fn run(args: Args) -> Result<(), DaemonError> {
    init_tracing();

    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
''',
    '''#[derive(Clone, Debug, PartialEq, Eq)]
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

fn run(args: Args) -> Result<(), DaemonError> {
    init_tracing();

    let listen: SocketAddr = args.listen.parse().map_err(|source| DaemonError::Listen {
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
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
    '''    let api_token = std::env::var("SCIRUST_HUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty());
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    validate_control_plane_security(listen, api_token.as_deref())?;
    if !listen.ip().is_loopback() && tls.is_none() {
        tracing::warn!(
            %listen,
            "control plane is non-loopback: bearer auth is enabled, but HTTP is plaintext; enable native TLS or terminate TLS at a trusted boundary"
        );
    }
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    tracing::info!(
        %listen,
        data_dir = %args.data_dir.display(),
        executor = orchestrator.executor_backend_id(),
        "scirust-hubd starting"
    );
''',
    '''    tracing::info!(
        %listen,
        data_dir = %args.data_dir.display(),
        executor = orchestrator.executor_backend_id(),
        transport = if tls.is_some() { "https" } else { "http" },
        "scirust-hubd starting"
    );
''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''        let app = hub_api::router(state);
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(DaemonError::Serve)?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(DaemonError::Serve)
    })
}''',
    '''        let app = hub_api::router(state);
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
}''',
)
replace_once(
    "apps/scirust-hubd/src/main.rs",
    '''    #[test]
    fn repeated_remote_worker_urls_select_pool_configuration() {
''',
    '''    #[test]
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
''',
)


# ---------------------------------------------------------------------------
# Worker: same pairwise TLS configuration, preserving bearer authentication.
# ---------------------------------------------------------------------------
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    'use clap::Parser;\n',
    'use axum_server::tls_rustls::RustlsConfig;\nuse clap::Parser;\n',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''    #[arg(long, env = "SCIRUST_HUB_WORKER_MAX_PAYLOAD_BYTES", default_value_t = DEFAULT_MAX_PAYLOAD_BYTES)]
    max_payload_bytes: usize,
}''',
    '''    #[arg(long, env = "SCIRUST_HUB_WORKER_MAX_PAYLOAD_BYTES", default_value_t = DEFAULT_MAX_PAYLOAD_BYTES)]
    max_payload_bytes: usize,
    /// PEM certificate chain for native HTTPS. Must be set with `--tls-key`.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TLS_CERT")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for native HTTPS. Must be set with `--tls-cert`.
    #[arg(long, env = "SCIRUST_HUB_WORKER_TLS_KEY")]
    tls_key: Option<PathBuf>,
}''',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''    #[error("worker server failed: {0}")]
    Serve(std::io::Error),
}''',
    '''    #[error("worker server failed: {0}")]
    Serve(std::io::Error),
    #[error("invalid TLS configuration: {0}")]
    TlsConfig(String),
}''',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''fn run(args: Args) -> Result<(), WorkerError> {
    let listen: SocketAddr = args
''',
    '''#[derive(Clone, Debug, PartialEq, Eq)]
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

fn run(args: Args) -> Result<(), WorkerError> {
    let listen: SocketAddr = args
''',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''        .parse()
        .map_err(|_| WorkerError::Listen(args.listen.clone()))?;
    let service = WorkerService::new(
''',
    '''        .parse()
        .map_err(|_| WorkerError::Listen(args.listen.clone()))?;
    let tls = tls_files(args.tls_cert, args.tls_key)?;
    if !listen.ip().is_loopback() && tls.is_none() {
        eprintln!(
            "scirust-hub-worker: warning: non-loopback worker bind uses plaintext HTTP; enable native TLS or a trusted TLS/tunnel boundary"
        );
    }
    let service = WorkerService::new(
''',
)
replace_once(
    "apps/scirust-hub-worker/src/main.rs",
    '''    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(listen)
            .await
            .map_err(WorkerError::Serve)?;
        hub_executor::worker::serve(listener, service)
            .await
            .map_err(WorkerError::Serve)
    })
}
''',
    '''    runtime.block_on(async move {
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
''',
)


# ---------------------------------------------------------------------------
# Documentation: native server TLS is optional; mTLS remains unclaimed.
# ---------------------------------------------------------------------------
replace_once(
    "README.md",
    '''A non-loopback Hub bind is refused unless `SCIRUST_HUB_TOKEN` is configured.
The daemon still speaks HTTP rather than native TLS; production exposure must
therefore terminate TLS at a trusted reverse proxy, service mesh or tunnel.
`/health` and `/ready` intentionally remain unauthenticated supervisor probes.
''',
    '''A non-loopback Hub bind is refused unless `SCIRUST_HUB_TOKEN` is configured.
Native HTTPS is opt-in: provide both `SCIRUST_HUB_TLS_CERT` and
`SCIRUST_HUB_TLS_KEY` (PEM certificate chain + private key), or the matching
`--tls-cert`/`--tls-key` flags. Supplying only one fails closed. HTTP remains the
loopback-compatible default. TLS protects transport but does not replace bearer
authentication. `/health` and `/ready` intentionally remain unauthenticated
supervisor probes.
''',
)
replace_once(
    "README.md",
    '''# worker host
export SCIRUST_HUB_WORKER_TOKEN='replace-with-a-worker-secret'
cargo run -p scirust-hub-worker -- \\
  --listen 127.0.0.1:8488 \\
  --data-dir ./worker-data

# Hub host
export SCIRUST_HUB_REMOTE_WORKER_URL='http://127.0.0.1:8488'
''',
    '''# worker host
export SCIRUST_HUB_WORKER_TOKEN='replace-with-a-worker-secret'
# Optional native HTTPS on the worker:
# export SCIRUST_HUB_WORKER_TLS_CERT='/etc/scirust/worker-cert.pem'
# export SCIRUST_HUB_WORKER_TLS_KEY='/etc/scirust/worker-key.pem'
cargo run -p scirust-hub-worker -- \\
  --listen 127.0.0.1:8488 \\
  --data-dir ./worker-data

# Hub host (use https:// when worker TLS is enabled)
export SCIRUST_HUB_REMOTE_WORKER_URL='http://127.0.0.1:8488'
''',
)
replace_once(
    "README.md",
    '''The worker bearer token is a separate trust boundary from
`SCIRUST_HUB_TOKEN`. Plain HTTP does not protect either credential on an
untrusted network; use a trusted private/tunneled/TLS boundary for remote
traffic. A single configured URL retains the original direct `RemoteExecutor`.
''',
    '''The worker bearer token is a separate trust boundary from
`SCIRUST_HUB_TOKEN`. The worker can serve native HTTPS when both
`SCIRUST_HUB_WORKER_TLS_CERT` and `SCIRUST_HUB_WORKER_TLS_KEY` are configured.
`RemoteExecutor` already accepts `https://` endpoints and validates them through
its TLS client stack/system trust roots; self-signed/private CAs must therefore
be trusted by the Hub host rather than bypassed. Plain HTTP remains suitable
only for loopback or a trusted private/tunneled boundary. A single configured
URL retains the original direct `RemoteExecutor`.
''',
)
replace_once(
    "README.md",
    '''- Hub and worker do not provide native TLS/mTLS; trusted TLS/tunnel boundaries
  are still required for untrusted networks.
''',
    '''- Hub and worker provide opt-in native server TLS with PEM certificate/key
  files, but not mTLS/client-certificate authentication or certificate hot reload.
''',
)
replace_once(
    "README.md",
    '''control-plane authentication, lifecycle-event cursor durability and
configured multi-worker placement/identity safety.
''',
    '''control-plane authentication, lifecycle-event cursor durability,
configured multi-worker placement/identity safety and TLS configuration gates.
''',
)

replace_once(
    "CHANGELOG.md",
    '''### Added

- Configured multi-worker remote placement:''',
    '''### Added

- Opt-in native Rustls HTTPS for both `scirust-hubd` and
  `scirust-hub-worker`. PEM certificate and private-key paths must be configured
  together and malformed/missing material fails startup closed. HTTP remains
  the local-compatible default; bearer authentication remains independent and
  mTLS/client-certificate authentication is not claimed.

- Configured multi-worker remote placement:''',
)

Path("docs/adr/0015-native-server-tls.md").write_text('''# ADR-0015: Opt-in native server TLS\n\n- Status: Accepted\n- Date: 2026-08-30\n\n## Context\n\nSciRust Hub already authenticates the control plane and remote worker protocol\nwith distinct bearer credentials, and `RemoteExecutor` accepts HTTPS worker\nURLs. Before this change the two Rust servers themselves only accepted plaintext\nHTTP, so deployments on untrusted networks required an external TLS terminator\nor tunnel.\n\n## Decision\n\n`scirust-hubd` and `scirust-hub-worker` gain optional native HTTPS using\n`axum-server` 0.8 with its Rustls feature. TLS is enabled only when both a PEM\ncertificate-chain path and PEM private-key path are configured. A half-configured\npair fails startup before binding. Malformed or unreadable PEM material also\nfails startup.\n\nThe Hub variables are `SCIRUST_HUB_TLS_CERT` and `SCIRUST_HUB_TLS_KEY`; the\nworker variables are `SCIRUST_HUB_WORKER_TLS_CERT` and\n`SCIRUST_HUB_WORKER_TLS_KEY`. Matching CLI flags are available.\n\nHTTP remains the default for local compatibility. A non-loopback control-plane\nbind still requires `SCIRUST_HUB_TOKEN` regardless of TLS: encryption does not\nreplace application authentication. The worker bearer token also remains\nmandatory. No secret value or private-key contents are logged.\n\nThe TLS daemon path uses `axum-server`'s handle-based graceful shutdown with a\n30-second drain bound. The plaintext path retains Axum's existing graceful\nshutdown behavior.\n\n## Client trust\n\n`RemoteExecutor` already accepts `https://` endpoints through its TLS-enabled\nHTTP client. Standard certificate validation remains enabled. This ADR does not\nadd an insecure verification bypass; private/self-signed CAs must be installed\nin the Hub host's trust configuration.\n\n## Non-goals\n\nThis change does not implement:\n\n- mTLS or client-certificate authorization;\n- certificate issuance/rotation;\n- certificate hot reload;\n- ACME;\n- replacement of bearer credentials.\n\n## Consequences\n\nDeployments can terminate TLS in the Hub/worker binaries without introducing a\nreverse proxy, while existing loopback HTTP workflows continue unchanged. The\nsecurity boundary remains explicit: Rustls provides transport confidentiality\nand server authentication; bearer tokens remain the application authentication\nmechanism.\n''')

print("native TLS implementation staged")
