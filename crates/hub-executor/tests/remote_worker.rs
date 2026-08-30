use std::collections::BTreeMap;
use std::fs;
use std::net::TcpListener as StdTcpListener;
use std::path::PathBuf;
use std::time::Duration;

use hub_core::exec::{CancelToken, ExecutionRequest, Executor};
use hub_executor::worker::{serve, WorkerService};
use hub_executor::RemoteExecutor;

fn temp_dir(tag: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("hub-remote-{tag}-{}", uuid::Uuid::new_v4()));
    fs::create_dir_all(&path).expect("mkdir");
    path
}

async fn start_worker(token: &str) -> (String, PathBuf, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let address = listener.local_addr().expect("addr");
    let root = temp_dir("worker");
    let service = WorkerService::new("worker-e2e", token, root.clone(), 8 * 1024 * 1024)
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
