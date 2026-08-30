use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::time::Duration;

use hub_core::exec::{CancelToken, ExecutionRequest, Executor};
use hub_executor::worker::{serve, WorkerService};
use hub_executor::{RemoteExecutor, RemotePoolExecutor};

fn temp_dir(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hub-remote-{tag}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).expect("mkdir");
    path
}

async fn start_worker(token: &str) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    start_named_worker("worker-e2e", token).await
}

async fn start_named_worker(
    worker_id: &str,
    token: &str,
) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let root = temp_dir(worker_id);
    let service = WorkerService::new(worker_id, token, root.clone(), 8 * 1024 * 1024)
        .expect("worker service");
    let task = tokio::spawn(async move {
        serve(listener, service).await.expect("serve worker");
    });
    (format!("http://{address}"), root, task)
}

fn request(workdir: PathBuf) -> ExecutionRequest {
    fs::create_dir_all(workdir.join("inputs")).expect("inputs");
    fs::create_dir_all(workdir.join("outputs")).expect("outputs");
    fs::write(workdir.join("inputs/source"), b"remote-payload\n").expect("input");
    ExecutionRequest {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "cat inputs/source > outputs/result".into()],
        working_dir: workdir,
        env: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
        timeout_ms: 5_000,
        max_capture_bytes_per_stream: 1024 * 1024,
    }
}

fn slow_request(workdir: PathBuf) -> ExecutionRequest {
    let mut request = request(workdir);
    request.args = vec![
        "-c".into(),
        "sleep 0.5; cat inputs/source > outputs/result".into(),
    ];
    request
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pool_places_concurrent_runs_on_distinct_workers() {
    let (endpoint_a, root_a, task_a) = start_named_worker("worker-a", "secret").await;
    let (endpoint_b, root_b, task_b) = start_named_worker("worker-b", "secret").await;
    // Reverse endpoint order deliberately: lexical worker identity is the
    // deterministic zero-load tie-break, not configuration order.
    let pool = std::sync::Arc::new(
        RemotePoolExecutor::new(vec![endpoint_b, endpoint_a], "secret")
            .expect("pool")
            .with_max_payload_bytes(8 * 1024 * 1024),
    );
    let local_a = temp_dir("pool-a");
    let local_b = temp_dir("pool-b");

    let first_pool = pool.clone();
    let first_dir = local_a.clone();
    let first = tokio::task::spawn_blocking(move || {
        first_pool.execute_report(&slow_request(first_dir), &CancelToken::new())
    });
    tokio::time::sleep(Duration::from_millis(75)).await;
    let second_pool = pool.clone();
    let second_dir = local_b.clone();
    let second = tokio::task::spawn_blocking(move || {
        second_pool.execute_report(&slow_request(second_dir), &CancelToken::new())
    });

    let first = first.await.expect("join first").expect("first report");
    let second = second.await.expect("join second").expect("second report");
    assert!(first.outcome.exited_cleanly(), "{:?}", first.outcome);
    assert!(second.outcome.exited_cleanly(), "{:?}", second.outcome);
    let targets = std::collections::BTreeSet::from([first.backend_id, second.backend_id]);
    assert_eq!(
        targets.len(),
        2,
        "concurrent placements must spread at zero/one load"
    );
    assert!(targets.iter().any(|target| target.contains("worker-a@")));
    assert!(targets.iter().any(|target| target.contains("worker-b@")));

    task_a.abort();
    task_b.abort();
    for path in [local_a, local_b, root_a, root_b] {
        let _ = fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_pool_rejects_duplicate_worker_identity_before_dispatch() {
    let (endpoint_a, root_a, task_a) = start_named_worker("worker-dup", "secret").await;
    let (endpoint_b, root_b, task_b) = start_named_worker("worker-dup", "secret").await;
    let pool = RemotePoolExecutor::new(vec![endpoint_a, endpoint_b], "secret")
        .expect("pool")
        .with_max_payload_bytes(8 * 1024 * 1024);
    let local = temp_dir("dup-id");
    let local_for_run = local.clone();
    let report = tokio::task::spawn_blocking(move || {
        pool.execute_report(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("report");
    assert!(report
        .outcome
        .start_error
        .as_deref()
        .is_some_and(|error| error.contains("duplicate worker identity")));
    assert!(!local.join("outputs/result").exists());

    task_a.abort();
    task_b.abort();
    for path in [local, root_a, root_b] {
        let _ = fs::remove_dir_all(path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_executor_transports_inputs_and_materializes_outputs() {
    let (endpoint, worker_root, task) = start_worker("secret").await;
    let local = temp_dir("local");
    let exec = RemoteExecutor::new(endpoint, "secret")
        .expect("remote")
        .with_max_payload_bytes(8 * 1024 * 1024);
    let local_for_run = local.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        exec.execute(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(outcome.exited_cleanly(), "{outcome:?}");
    assert_eq!(
        fs::read(local.join("outputs/result")).expect("result"),
        b"remote-payload\n"
    );
    task.abort();
    let _ = fs::remove_dir_all(local);
    let _ = fs::remove_dir_all(worker_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_refuses_bad_auth_without_executing() {
    let (endpoint, worker_root, task) = start_worker("secret").await;
    let local = temp_dir("auth");
    let exec = RemoteExecutor::new(endpoint, "wrong-secret")
        .expect("remote")
        .with_max_payload_bytes(8 * 1024 * 1024);
    let local_for_run = local.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        exec.execute(&request(local_for_run), &CancelToken::new())
    })
    .await
    .expect("join")
    .expect("executor");
    assert!(outcome
        .start_error
        .as_deref()
        .is_some_and(|error| error.contains("authorization refused")));
    assert!(!local.join("outputs/result").exists());
    task.abort();
    let _ = fs::remove_dir_all(local);
    let _ = fs::remove_dir_all(worker_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_worker_fails_closed_as_observed_run_outcome() {
    let probe = StdTcpListener::bind("127.0.0.1:0").expect("probe");
    let address = probe.local_addr().expect("addr");
    drop(probe);
    let local = temp_dir("lost");
    let exec = RemoteExecutor::new(format!("http://{address}"), "secret").expect("remote");
    let local_for_run = local.clone();
    let outcome = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::task::spawn_blocking(move || {
            exec.execute(&request(local_for_run), &CancelToken::new())
        }),
    )
    .await
    .expect("must fail promptly")
    .expect("join")
    .expect("executor");
    assert!(outcome
        .start_error
        .as_deref()
        .is_some_and(|error| error.contains("unavailable")));
    assert!(!outcome.exited_cleanly());
    let _ = fs::remove_dir_all(local);
}
