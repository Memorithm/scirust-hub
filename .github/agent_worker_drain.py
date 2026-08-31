from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:160]!r}")
    p.write_text(text.replace(old, new, 1))


worker = "crates/hub-executor/src/worker.rs"
replace_once(
    worker,
    '''#[derive(Default)]
struct LeaseBook {
    leases: BTreeMap<String, LeaseEntry>,
    attempts: BTreeMap<String, String>,
}
''',
    '''#[derive(Default)]
struct LeaseBook {
    draining: bool,
    leases: BTreeMap<String, LeaseEntry>,
    attempts: BTreeMap<String, String>,
}
''',
)

replace_once(
    worker,
    '''    #[must_use]
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
''',
    '''    #[must_use]
    pub fn descriptor(&self) -> WorkerDescriptor {
        WorkerDescriptor {
            protocol_version: WORKER_PROTOCOL_VERSION,
            worker_id: self.inner.worker_id.clone(),
            capabilities: vec![PROCESS_EXECUTION_CAPABILITY.to_owned()],
            max_payload_bytes: self.inner.max_payload_bytes as u64,
            heartbeat_interval_ms: HEARTBEAT_INTERVAL_MS,
        }
    }

    /// Enters fail-closed drain mode and requests cancellation of every active
    /// lease. Queued leases become terminal before they can spawn a process;
    /// running leases observe their existing cooperative cancellation token.
    /// Idempotent replays of already-reserved attempts remain readable.
    pub fn begin_draining(&self) -> Result<usize, String> {
        let mut book = self
            .inner
            .leases
            .lock()
            .map_err(|_| "lease book lock poisoned".to_owned())?;
        book.draining = true;
        let now = now_ms();
        let mut active = 0usize;
        for entry in book.leases.values_mut() {
            if entry.state.is_terminal() {
                continue;
            }
            active = active.saturating_add(1);
            entry.cancel.cancel();
            if entry.state == LeaseState::Queued {
                entry.state = LeaseState::Cancelled;
                entry.last_heartbeat_ms = now;
            }
        }
        Ok(active)
    }

    /// Whether the worker has stopped accepting new leases.
    pub fn is_draining(&self) -> Result<bool, String> {
        self.inner
            .leases
            .lock()
            .map(|book| book.draining)
            .map_err(|_| "lease book lock poisoned".to_owned())
    }

    /// Number of leases that have not yet reached a terminal state.
    pub fn active_lease_count(&self) -> Result<usize, String> {
        self.inner
            .leases
            .lock()
            .map(|book| {
                book.leases
                    .values()
                    .filter(|entry| !entry.state.is_terminal())
                    .count()
            })
            .map_err(|_| "lease book lock poisoned".to_owned())
    }

    fn authorize(&self, headers: &HeaderMap) -> bool {
''',
)

replace_once(
    worker,
    '''            return Ok(self.create_response(&existing_id, existing));
        }

        let lease_id = uuid::Uuid::new_v4().to_string();
''',
    '''            return Ok(self.create_response(&existing_id, existing));
        }
        if book.draining {
            return Err(ReserveError::Draining);
        }

        let lease_id = uuid::Uuid::new_v4().to_string();
''',
)

replace_once(
    worker,
    '''        let entry = book
            .leases
            .get_mut(lease_id)
            .ok_or_else(|| "lease disappeared before execution".to_owned())?;
        entry.state = LeaseState::Running;
        entry.last_heartbeat_ms = now_ms();
        Ok((entry.execution.clone(), entry.cancel.clone()))
''',
    '''        let entry = book
            .leases
            .get_mut(lease_id)
            .ok_or_else(|| "lease disappeared before execution".to_owned())?;
        if entry.state != LeaseState::Queued {
            return Err(format!(
                "lease cannot start from terminal/non-queued state {:?}",
                entry.state
            ));
        }
        if entry.cancel.is_cancelled() {
            entry.state = LeaseState::Cancelled;
            entry.last_heartbeat_ms = now_ms();
            return Err("lease was cancelled before execution".to_owned());
        }
        entry.state = LeaseState::Running;
        entry.last_heartbeat_ms = now_ms();
        Ok((entry.execution.clone(), entry.cancel.clone()))
''',
)

replace_once(
    worker,
    '''pub async fn serve(
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
''',
    '''pub async fn serve(
    listener: tokio::net::TcpListener,
    service: WorkerService,
) -> std::io::Result<()> {
    axum::serve(listener, router(service)).await
}

pub async fn serve_with_shutdown<F>(
    listener: tokio::net::TcpListener,
    service: WorkerService,
    shutdown: F,
) -> std::io::Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    axum::serve(listener, router(service))
        .with_graceful_shutdown(shutdown)
        .await
}

async fn describe_worker(State(service): State<WorkerService>, headers: HeaderMap) -> Response {
    if !service.authorize(&headers) {
        return unauthorized();
    }
    match service.is_draining() {
        Ok(false) => Json(service.descriptor()).into_response(),
        Ok(true) => worker_error(StatusCode::SERVICE_UNAVAILABLE, "worker is draining"),
        Err(error) => worker_error(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}
''',
)

replace_once(
    worker,
    '''        Err(ReserveError::Conflict(error)) => return worker_error(StatusCode::CONFLICT, error),
        Err(ReserveError::PayloadTooLarge) => {
''',
    '''        Err(ReserveError::Conflict(error)) => return worker_error(StatusCode::CONFLICT, error),
        Err(ReserveError::Draining) => {
            return worker_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "worker is draining and not accepting new leases",
            )
        }
        Err(ReserveError::PayloadTooLarge) => {
''',
)

replace_once(
    worker,
    '''enum ReserveError {
    BadRequest(String),
    Conflict(String),
    PayloadTooLarge,
    Internal(String),
}
''',
    '''enum ReserveError {
    BadRequest(String),
    Conflict(String),
    Draining,
    PayloadTooLarge,
    Internal(String),
}
''',
)

replace_once(
    worker,
    '''    #[test]
    fn traversal_paths_are_rejected() {
''',
    '''    #[test]
    fn draining_rejects_new_leases_but_preserves_idempotent_replay() {
        let service = service();
        let first = service
            .reserve_lease(&request("attempt-drain"))
            .expect("reserve");

        assert_eq!(service.begin_draining().expect("drain"), 1);
        assert!(service.is_draining().expect("state"));
        assert_eq!(service.active_lease_count().expect("active"), 0);
        assert_eq!(
            service
                .status(&first.lease_id)
                .expect("status")
                .expect("lease")
                .state,
            LeaseState::Cancelled
        );

        let replay = service
            .reserve_lease(&request("attempt-drain"))
            .expect("idempotent replay remains safe");
        assert_eq!(replay.lease_id, first.lease_id);
        assert!(matches!(
            service.reserve_lease(&request("attempt-new")),
            Err(ReserveError::Draining)
        ));
    }

    #[test]
    fn queued_cancellation_prevents_process_start() {
        let service = service();
        let lease = service
            .reserve_lease(&request("attempt-cancel-before-start"))
            .expect("reserve");
        assert!(service.cancel(&lease.lease_id).expect("cancel"));
        assert!(service.mark_running(&lease.lease_id).is_err());
        assert_eq!(
            service
                .status(&lease.lease_id)
                .expect("status")
                .expect("lease")
                .state,
            LeaseState::Cancelled
        );
    }

    #[test]
    fn draining_requests_cancellation_of_running_lease() {
        let service = service();
        let lease = service
            .reserve_lease(&request("attempt-running-drain"))
            .expect("reserve");
        let (_, cancel) = service.mark_running(&lease.lease_id).expect("running");
        assert!(!cancel.is_cancelled());
        assert_eq!(service.begin_draining().expect("drain"), 1);
        assert!(cancel.is_cancelled());
        assert_eq!(service.active_lease_count().expect("active"), 1);
    }

    #[test]
    fn traversal_paths_are_rejected() {
''',
)

main = "apps/scirust-hub-worker/src/main.rs"
replace_once(
    main,
    '''use std::path::PathBuf;
''',
    '''use std::path::PathBuf;
use std::time::Duration;
''',
)
replace_once(
    main,
    '''const DEFAULT_LISTEN: &str = "127.0.0.1:8488";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
''',
    '''const DEFAULT_LISTEN: &str = "127.0.0.1:8488";
const DEFAULT_MAX_PAYLOAD_BYTES: usize = 64 * 1024 * 1024;
const SHUTDOWN_GRACE: Duration = Duration::from_secs(30);
const SERVER_DRAIN_GRACE: Duration = Duration::from_secs(5);
''',
)
replace_once(
    main,
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
''',
    '''    let result = runtime.block_on(async move {
        if let Some(tls) = tls {
            let config = RustlsConfig::from_pem_file(&tls.cert, &tls.key)
                .await
                .map_err(|error| {
                    WorkerError::TlsConfig(format!("loading PEM certificate/private key: {error}"))
                })?;
            let handle = axum_server::Handle::new();
            let shutdown_handle = handle.clone();
            let shutdown_service = service.clone();
            tokio::spawn(async move {
                drain_on_shutdown(shutdown_service).await;
                shutdown_handle.graceful_shutdown(Some(SERVER_DRAIN_GRACE));
            });
            axum_server::bind_rustls(listen, config)
                .handle(handle)
                .serve(hub_executor::worker::router(service).into_make_service())
                .await
                .map_err(WorkerError::Serve)
        } else {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(WorkerError::Serve)?;
            let shutdown_service = service.clone();
            hub_executor::worker::serve_with_shutdown(
                listener,
                service,
                drain_on_shutdown(shutdown_service),
            )
            .await
            .map_err(WorkerError::Serve)
        }
    });
    runtime.shutdown_timeout(SERVER_DRAIN_GRACE);
    result
}

async fn drain_on_shutdown(service: WorkerService) {
    shutdown_signal().await;
    let active = match service.begin_draining() {
        Ok(active) => active,
        Err(error) => {
            eprintln!("scirust-hub-worker: failed to enter drain mode: {error}");
            return;
        }
    };
    eprintln!("scirust-hub-worker: draining {active} active lease(s)");

    let deadline = tokio::time::Instant::now() + SHUTDOWN_GRACE;
    loop {
        match service.active_lease_count() {
            Ok(0) => return,
            Ok(active) if tokio::time::Instant::now() >= deadline => {
                eprintln!(
                    "scirust-hub-worker: drain deadline reached with {active} active lease(s)"
                );
                return;
            }
            Ok(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            Err(error) => {
                eprintln!("scirust-hub-worker: failed to inspect drain state: {error}");
                return;
            }
        }
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("scirust-hub-worker: ctrl-c listener failed: {error}");
        }
    };
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(error) => {
                eprintln!("scirust-hub-worker: SIGTERM listener unavailable: {error}");
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

#[cfg(test)]
''',
)

replace_once(
    "README.md",
    '''is never replayed elsewhere. All configured workers currently share the same
environment-only worker bearer token.

## Workflows
''',
    '''is never replayed elsewhere. All configured workers currently share the same
environment-only worker bearer token.

On SIGINT/SIGTERM a worker enters drain mode before closing its listener.
Descriptor discovery and genuinely new lease creation return `503`, so a
configured pool can skip the draining worker, while idempotent replays of an
already-reserved attempt remain safe. Queued leases are cancelled before
process start and running leases receive their existing cancellation token; the
worker waits up to 30 seconds for non-terminal leases to settle before server
shutdown. Worker lease state remains intentionally ephemeral, so a hard crash
or exhausted drain still fails the affected Hub run closed rather than replaying
it elsewhere.

## Workflows
''',
)

replace_once(
    "CHANGELOG.md",
    '''### Added

- Opt-in native Rustls HTTPS for both `scirust-hubd` and
''',
    '''### Added

- Fail-closed remote-worker graceful drain (ADR-0016): SIGINT/SIGTERM stops
  descriptor eligibility and new lease admission, preserves idempotent replay
  of already-reserved attempts, cancels queued leases before process start,
  propagates cancellation to running leases, and waits a bounded interval for
  active leases to settle before server/runtime shutdown.

- Opt-in native Rustls HTTPS for both `scirust-hubd` and
''',
)

Path("docs/adr/0016-worker-graceful-drain.md").write_text('''# ADR 0016 — Remote worker graceful drain

Status: accepted

## Context

`scirust-hub-worker` already gives every lease a cooperative `CancelToken`, and
the process executor kills its direct child when cancellation is observed.
Before this change, however, SIGINT/SIGTERM only ended the worker process by
tearing down its runtime. There was no admission barrier for new leases and no
explicit propagation of shutdown to active lease cancellation.

A worker disappearing is already fail-closed at the Hub: remote leases are
ephemeral and an affected run is never silently replayed on another worker.
That crash behavior remains the final safety net, but planned service shutdown
should use the cancellation machinery that already exists.

## Decision

The worker lease book owns a `draining` state under the same mutex that guards
attempt reservation. This makes the transition to drain mode atomic with
respect to genuinely new lease admission.

When draining starts:

1. descriptor discovery returns `503 Service Unavailable`, allowing a
   configured pool to exclude the worker before dispatch;
2. new attempt ids are rejected with `503`;
3. an idempotent replay of an already-reserved attempt is still returned, so
   drain mode does not turn a safe retry into a conflicting second execution;
4. queued leases are marked `cancelled` before process start and their tokens
   are cancelled;
5. running leases receive the same `CancelToken` cancellation path used by the
   normal lease-cancel endpoint.

The binary handles SIGINT/SIGTERM by entering drain mode, keeping status/cancel
HTTP handling available while active leases settle, and waiting up to 30
seconds before initiating server shutdown. TLS uses the existing
`axum-server` handle; plaintext HTTP uses Axum graceful shutdown. Runtime
blocking-task shutdown receives an additional short bound after the server
returns.

## Invariants and non-goals

- Worker protocol v1 is unchanged; no new wire fields are fabricated.
- Draining never admits a new attempt after the drain bit is committed.
- A queued lease cancelled before execution must not spawn a process.
- A running lease is not reported as terminal until its executor path commits a
  terminal result/failure.
- This does not make worker lease state durable and does not add post-dispatch
  failover, process sandboxing, descendant-process tree supervision, dynamic
  worker registration or global scheduling.
- A hard crash remains distinguishable from a graceful drain and continues to
  fail the corresponding Hub run closed.
''')

print("worker graceful drain patch staged")
