//! End-to-end proof: the real `scirust-hubd` binary serves the real HTTP API,
//! and a raw TCP client walks the whole vertical slice without any Hub-side
//! test doubles.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct DaemonGuard {
    child: Child,
    #[allow(dead_code)]
    data_dir: PathBuf,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("probe bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn start_daemon() -> (DaemonGuard, u16) {
    let port = free_port();
    let data_dir = std::env::temp_dir().join(format!("hub-e2e-{}-{}", std::process::id(), port));
    let child = Command::new(env!("CARGO_BIN_EXE_scirust-hubd"))
        .args([
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--data-dir",
            data_dir.to_str().expect("utf8 path"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn scirust-hubd");
    let guard = DaemonGuard { child, data_dir };

    // Poll /health until the listener answers (or fail loudly).
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() > deadline {
            panic!("daemon did not become healthy within 15s");
        }
        if let Ok((status, _)) = http(port, "GET", "/health", None) {
            assert_eq!(status, 200);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

/// Minimal HTTP/1.0 client: no chunked transfer, server closes the stream.
fn http(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> Result<(u16, String), std::io::Error> {
    let payload = body.unwrap_or("");
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    let request = format!(
        "{method} {path} HTTP/1.0\r\n\
         Host: 127.0.0.1\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {payload}",
        payload.len()
    );
    stream.write_all(request.as_bytes())?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw)?;
    let status_line = raw.lines().next().unwrap_or_default().to_owned();
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, status_line))?;
    let body_start = raw.find("\r\n\r\n").map(|i| i + 4).unwrap_or(raw.len());
    Ok((status, raw[body_start..].to_owned()))
}

fn echo_manifest(component_id: &str) -> String {
    format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{component_id}",
                "name": "demo-echo",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{"name": "demo.echo", "contract_version": "1.0.0"}}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["{{params}}"]
                }}
            }}
        }}"#
    )
}

#[test]
fn walking_skeleton_through_the_real_daemon() {
    let (_guard, port) = start_daemon();

    // 1. Register a component (idempotent replay included).
    let id = uuid_v4();
    let (status, body) = http(
        port,
        "POST",
        "/api/v1/components",
        Some(&echo_manifest(&id)),
    )
    .expect("register");
    assert_eq!(status, 201, "body: {body}");
    assert!(body.contains("\"created\""), "body: {body}");
    let digest_start = body.find("\"manifest_digest\":\"").expect("digest field");
    let digest: String = body[digest_start + 19..]
        .chars()
        .take_while(|c| *c != '"')
        .collect();
    assert_eq!(digest.len(), 64, "sha-256 hex digest");

    let (status, body) = http(
        port,
        "POST",
        "/api/v1/components",
        Some(&echo_manifest(&id)),
    )
    .expect("replay");
    assert_eq!(status, 200, "replay must be 200, got {status}: {body}");
    assert!(body.contains("\"already_registered\""), "body: {body}");

    // 2. Capability discovery.
    let (status, body) = http(port, "GET", "/api/v1/capabilities", None).expect("caps");
    assert_eq!(status, 200);
    assert!(body.contains("demo.echo"), "body: {body}");

    // 3. Submit a run referencing the capability.
    let submit = serde_json::json!({
        "schema_version": 1,
        "run_spec": {
            "component": id,
            "capability": "demo.echo",
            "parameters": {"msg": "e2e"},
            "inputs": [],
            "timeout_ms": 5000
        }
    })
    .to_string();
    let (status, body) = http(port, "POST", "/api/v1/runs", Some(&submit)).expect("submit");
    assert_eq!(status, 201, "body: {body}");
    assert!(body.contains("\"queued\""), "body: {body}");
    let run_id: String = json_field(&body, "\"id\":\"").expect("run id");

    // 4. Execute synchronously through the real process executor.
    let exec_body = serde_json::json!(run_id).to_string();
    let (status, body) =
        http(port, "POST", "/api/v1/executions", Some(&exec_body)).expect("execute");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("\"succeeded\""), "body: {body}");
    assert!(body.contains("\"exit_code\":0"), "body: {body}");
    assert!(
        body.contains("\"executor_backend\":\"process\""),
        "body: {body}"
    );

    // 5. Provenance: lifecycle transitions recorded.
    let (status, body) =
        http(port, "GET", &format!("/api/v1/runs/{run_id}"), None).expect("run record");
    assert_eq!(status, 200);
    for state in ["created", "validated", "queued", "running", "succeeded"] {
        assert!(body.contains(state), "transition {state} missing: {body}");
    }

    // 6. Output artifact retrievable with exact captured bytes.
    let artifact_id: String = json_field(&body, "\"artifact\":\"").expect("artifact ref");
    let (status, body) = http(
        port,
        "GET",
        &format!("/api/v1/artifacts/{artifact_id}?include=content"),
        None,
    )
    .expect("artifact");
    assert_eq!(status, 200);
    // Raw body: JSON-escaped quotes plus escaped trailing newline from echo.
    assert!(
        body.contains(r#"content_text":"{\"msg\":\"e2e\"}\n""#),
        "captured stdout mismatch: {body}"
    );
    // Artifact digest present and distinct from the manifest digest
    // (different hash domains).
    let artifact_digest: String = json_field(&body, "\"digest\":\"").expect("artifact digest");
    assert_eq!(artifact_digest.len(), 64);
    assert_ne!(artifact_digest, digest);

    // 7. Unknown resources produce structured 404s.
    let (status, body) =
        http(port, "GET", &format!("/api/v1/runs/{}", uuid_v4()), None).expect("missing run");
    assert_eq!(status, 404);
    assert!(body.contains("\"not_found\""), "body: {body}");
}

/// Extracts the first string value following `key` in a JSON-ish body.
fn json_field(body: &str, key: &str) -> Option<String> {
    let start = body.find(key)? + key.len();
    Some(body[start..].chars().take_while(|c| *c != '"').collect())
}

fn uuid_v4() -> String {
    // Random enough for tests without extra deps: time + pid + counter.
    use std::sync::atomic::{AtomicU32, Ordering};
    static COUNTER: AtomicU32 = AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let counter = COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&(u64::from(nanos) << 32 | u64::from(counter)).to_le_bytes());
    b[8..12].copy_from_slice(&std::process::id().to_le_bytes());
    let mixed = counter.wrapping_mul(0x9E37_79B9).rotate_left(17);
    b[12..16].copy_from_slice(&mixed.to_le_bytes());
    b[6] = (b[6] & 0x0f) | 0x40; // version 4 marker bits
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        b[12], b[13], b[14], b[15]
    )
}

#[test]
fn malformed_requests_get_structured_errors_not_panics() {
    let (_guard, port) = start_daemon();

    // Invalid JSON body.
    let (status, body) =
        http(port, "POST", "/api/v1/components", Some("{not json")).expect("bad json");
    assert_eq!(status, 400);
    assert!(body.contains("bad_request"), "body: {body}");

    // Wrong schema version.
    let bad_version =
        echo_manifest(&uuid_v4()).replace("\"schema_version\": 1", "\"schema_version\": 7");
    let (status, body) =
        http(port, "POST", "/api/v1/components", Some(&bad_version)).expect("wrong version");
    assert_eq!(status, 400);
    assert!(body.contains("unsupported_schema_version"), "body: {body}");

    // Manifest failing domain validation (duplicate capabilities).
    let invalid = echo_manifest(&uuid_v4()).replace(
        r#""capabilities": [
                    {"name": "demo.echo", "contract_version": "1.0.0"}
                ]"#,
        r#""capabilities": [
                    {"name": "demo.echo", "contract_version": "1.0.0"},
                    {"name": "demo.echo", "contract_version": "1.0.0"}
                ]"#,
    );
    let (status, body) =
        http(port, "POST", "/api/v1/components", Some(&invalid)).expect("invalid manifest");
    assert_eq!(status, 422);
    assert!(body.contains("validation_failed"), "body: {body}");
}
