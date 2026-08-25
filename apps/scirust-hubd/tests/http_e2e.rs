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

#[test]
fn declared_output_files_round_trip_through_http() {
    let (_guard, port) = start_daemon();

    // Component copies its materialized input into a declared output file.
    let manifest = format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{id}",
                "name": "demo-copy",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{
                        "name": "demo.copy",
                        "contract_version": "1.0.0",
                        "inputs": [{{"name": "source"}}]
                    }}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/cp",
                    "args": ["{{input:source}}", "{{output:copy}}"],
                    "outputs": [
                        {{
                            "name": "copy",
                            "path": "out/copy.txt",
                            "media_type": "text/plain",
                            "required": true
                        }}
                    ]
                }}
            }}
        }}"#,
        id = uuid_v4()
    );
    let (status, body) =
        http(port, "POST", "/api/v1/components", Some(&manifest)).expect("register copy");
    assert_eq!(status, 201, "body: {body}");

    // Produce an input artifact using echo (stream capture), then feed it
    // into the copy component.
    let echo = format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{id}",
                "name": "demo-echo-seed",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{"name": "demo.echo", "contract_version": "1.0.0"}}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["seed-for-copy"]
                }}
            }}
        }}"#,
        id = uuid_v4()
    );
    let (status, _body) =
        http(port, "POST", "/api/v1/components", Some(&echo)).expect("register echo");
    assert_eq!(status, 201);
    let submit_echo = serde_json::json!({
        "schema_version": 1,
        "run_spec": {
            "component": serde_json::Value::Null,
            "capability": "demo.echo",
            "parameters": {},
            "inputs": [],
            "timeout_ms": 5000
        }
    });
    // Resolve the echo component id by listing and matching the name.
    let (status, list) = http(port, "GET", "/api/v1/components", None).expect("list");
    assert_eq!(status, 200);
    let echo_id = find_component_id_by_name(&list, "demo-echo-seed");
    let copy_id = find_component_id_by_name(&list, "demo-copy");
    assert!(echo_id.is_some() && copy_id.is_some(), "list: {list}");

    let mut spec = submit_echo;
    spec["run_spec"]["component"] = serde_json::json!(echo_id.clone().unwrap());
    let (status, body) =
        http(port, "POST", "/api/v1/runs", Some(&spec.to_string())).expect("submit echo");
    assert_eq!(status, 201, "body: {body}");
    let run_id: String = json_field(&body, "\"id\":\"").expect("echo run id");
    let (status, executed) = http(
        port,
        "POST",
        "/api/v1/executions",
        Some(&serde_json::json!(run_id).to_string()),
    )
    .expect("execute echo");
    assert_eq!(status, 200);
    assert!(executed.contains("\"succeeded\""), "body: {executed}");
    let seed_artifact: String = json_field(&executed, "\"artifact\":\"").expect("seed artifact");

    // Copy run consumes the seed artifact and declares the file output.
    let submit_copy = serde_json::json!({
        "schema_version": 1,
        "run_spec": {
            "component": copy_id.unwrap(),
            "capability": "demo.copy",
            "parameters": {},
            "inputs": [{"name": "source", "artifact": seed_artifact}],
            "timeout_ms": 5000
        }
    })
    .to_string();
    let (status, body) =
        http(port, "POST", "/api/v1/runs", Some(&submit_copy)).expect("submit copy");
    assert_eq!(status, 201, "body: {body}");
    let copy_run_id: String = json_field(&body, "\"id\":\"").expect("copy run id");
    let (status, executed) = http(
        port,
        "POST",
        "/api/v1/executions",
        Some(&serde_json::json!(copy_run_id).to_string()),
    )
    .expect("execute copy");
    assert_eq!(status, 200, "body: {executed}");
    assert!(executed.contains("\"succeeded\""), "body: {executed}");

    // The ingested file artifact carries byte-exact content of the seed.
    // Seed stdout was "seed-for-copy\n"; cp preserved it verbatim.
    let file_ref_start = executed.find("\"name\":\"file:copy\"").expect("file ref");
    let artifact_id: String =
        json_field(&executed[file_ref_start..], "\"artifact\":\"").expect("ingested artifact id");
    let (status, artifact) = http(
        port,
        "GET",
        &format!("/api/v1/artifacts/{artifact_id}?include=content"),
        None,
    )
    .expect("fetch ingested artifact");
    assert_eq!(status, 200, "body: {artifact}");
    assert!(
        artifact.contains("seed-for-copy"),
        "ingested content mismatch: {artifact}"
    );
}

/// Finds a component id by display name in a serialized components list.
/// ComponentDto serializes `id` immediately before `name`, so the closest
/// preceding id belongs to that object.
fn find_component_id_by_name(list_json: &str, name: &str) -> Option<String> {
    let needle = format!("\"name\":\"{name}\"");
    let mut search_from = 0;
    while let Some(rel) = list_json[search_from..].find(&needle) {
        let name_pos = search_from + rel;
        let id_key = list_json[..name_pos].rfind("\"id\":\"")?;
        let raw = &list_json[id_key + "\"id\":\"".len()..];
        // Ensure this id is not claimed by an earlier name match.
        let prev_name = list_json[..id_key].rfind("\"name\":\"");
        let valid = prev_name.is_none_or(|p| p < search_from || p < name_pos && false);
        if valid
            || prev_name
                .map(|p| p < name_pos.saturating_sub(needle.len()))
                .unwrap_or(true)
        {
            return Some(raw.chars().take_while(|c| *c != '"').collect());
        }
        search_from = name_pos + needle.len();
    }
    None
}

#[test]
fn two_step_workflow_chains_artifacts_over_http() {
    let (_guard, port) = start_daemon();

    // emit component: params -> stdout
    let echo = format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{id}",
                "name": "wf-emit",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{"name": "demo.emit", "contract_version": "1.0.0"}}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["{{params}}"]
                }}
            }}
        }}"#,
        id = uuid_v4()
    );
    // copy component: input file -> declared output file
    let copy = format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{id}",
                "name": "wf-copy",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{
                        "name": "demo.copy",
                        "contract_version": "1.0.0",
                        "inputs": [{{"name": "source"}}]
                    }}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/cp",
                    "args": ["{{input:source}}", "{{output:copy}}"],
                    "outputs": [
                        {{"name": "copy", "path": "out/copy.txt", "required": true}}
                    ]
                }}
            }}
        }}"#,
        id = uuid_v4()
    );
    for manifest in [&echo, &copy] {
        let (status, body) =
            http(port, "POST", "/api/v1/components", Some(manifest)).expect("register");
        assert_eq!(status, 201, "body: {body}");
    }
    let (status, list) = http(port, "GET", "/api/v1/components", None).expect("list");
    assert_eq!(status, 200);
    let emit_id = find_component_id_by_name(&list, "wf-emit").expect("wf-emit id");
    let copy_id = find_component_id_by_name(&list, "wf-copy").expect("wf-copy id");

    // Workflow spec referencing the previous step's stdout.
    let workflow = serde_json::json!({
        "schema_version": 1,
        "workflow": {
            "schema_version": 1,
            "name": "echo-then-copy",
            "steps": [
                {
                    "key": "emit",
                    "component": emit_id,
                    "capability": "demo.emit",
                    "parameters": {"msg": "via-workflow"},
                    "inputs": {},
                    "timeout_ms": 5000,
                    "after": []
                },
                {
                    "key": "store",
                    "component": copy_id,
                    "capability": "demo.copy",
                    "parameters": {},
                    "inputs": {
                        "source": {"from_step": {"key": "emit", "output": "stdout"}}
                    },
                    "timeout_ms": 5000,
                    "after": ["emit"]
                }
            ]
        }
    })
    .to_string();
    let (status, body) =
        http(port, "POST", "/api/v1/workflows", Some(&workflow)).expect("submit wf");
    assert_eq!(status, 201, "body: {body}");
    assert!(body.contains("\"created\""), "body: {body}");
    let workflow_id: String = json_field(&body, "\"id\":\"").expect("workflow id");

    // Execute and verify success with both steps recorded.
    let (status, executed) = http(
        port,
        "POST",
        &format!("/api/v1/workflows/{workflow_id}/executions"),
        None,
    )
    .expect("execute wf");
    assert_eq!(status, 200, "body: {executed}");
    assert!(executed.contains("\"succeeded\""), "body: {executed}");
    assert!(executed.contains("\"emit\""), "body: {executed}");
    assert!(executed.contains("\"store\""), "body: {executed}");

    // The copied file artifact must exist alongside the emit stdout capture:
    let (status, artifacts) = http(port, "GET", "/api/v1/artifacts", None).expect("artifacts");
    assert_eq!(status, 200);
    // Three stream/file artifacts expected: emit stdout + store stdout? cp is
    // silent, so exactly two: emit stdout and store's out/copy.txt file.
    let count = artifacts.matches("\"media_type\"").count();
    assert!(
        count >= 2,
        "expected at least emit-stdout and copied-file artifacts: {artifacts}"
    );

    // Re-execution of a finished workflow is rejected cleanly.
    let (status, body) = http(
        port,
        "POST",
        &format!("/api/v1/workflows/{workflow_id}/executions"),
        None,
    )
    .expect("re-execute");
    assert_eq!(status, 422, "body: {body}");
    assert!(body.contains("not executable"), "body: {body}");
}

#[test]
fn reproduction_round_trip_through_the_daemon() {
    let (_guard, port) = start_daemon();

    let id = uuid_v4();
    let (status, _) = http(
        port,
        "POST",
        "/api/v1/components",
        Some(&echo_manifest(&id)),
    )
    .expect("register");
    assert_eq!(status, 201);

    // Original run, executed.
    let submit = serde_json::json!({
        "schema_version": 1,
        "run_spec": {
            "component": id,
            "capability": "demo.echo",
            "parameters": {"msg": "to-reproduce"},
            "inputs": [],
            "timeout_ms": 5000
        }
    })
    .to_string();
    let (status, body) = http(port, "POST", "/api/v1/runs", Some(&submit)).expect("submit");
    assert_eq!(status, 201);
    let original_id: String = json_field(&body, "\"id\":\"").expect("original id");
    let (_, executed) = http(
        port,
        "POST",
        "/api/v1/executions",
        Some(&serde_json::json!(original_id).to_string()),
    )
    .expect("execute");
    assert!(executed.contains("\"succeeded\""), "{executed}");

    // Reproduce: new queued run carrying reproduced_from.
    let (status, body) = http(
        port,
        "POST",
        &format!("/api/v1/runs/{original_id}/reproduce"),
        None,
    )
    .expect("reproduce");
    assert_eq!(status, 201, "body: {body}");
    assert!(
        body.contains(&format!("\"reproduced_from\":\"{original_id}\"")),
        "link missing: {body}"
    );
    let repro_id: String = json_field(&body, "\"id\":\"").expect("repro id");

    // Execute the reproduction; identical params digest proves spec parity.
    let (_, repro_executed) = http(
        port,
        "POST",
        "/api/v1/executions",
        Some(&serde_json::json!(repro_id).to_string()),
    )
    .expect("execute repro");
    assert!(repro_executed.contains("\"succeeded\""), "{repro_executed}");
    let original_digest: String =
        json_field(&executed, "\"params_digest\":\"").expect("original params digest");
    let repro_digest: String =
        json_field(&repro_executed, "\"params_digest\":\"").expect("repro params digest");
    assert_eq!(
        original_digest, repro_digest,
        "same spec must hash identically"
    );

    // Version drift blocks reproduction of runs recorded under old versions.
    let drifted = echo_manifest(&id).replace("\"version\": \"1.0.0\"", "\"version\": \"2.0.0\"");
    let (status, _) =
        http(port, "POST", "/api/v1/components", Some(&drifted)).expect("register v2");
    assert_eq!(status, 201);
    let (status, body) = http(
        port,
        "POST",
        &format!("/api/v1/runs/{original_id}/reproduce"),
        None,
    )
    .expect("reproduce after drift");
    assert_eq!(status, 422, "body: {body}");
    assert!(body.contains("evolved"), "body: {body}");
}
