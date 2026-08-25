//! End-to-end proof at the MCP boundary: a real agent-style client drives
//! the real `scirust-hub-mcp` binary over stdin/stdout (NDJSON), which in
//! turn queries a real `scirust-hubd` daemon over HTTP.

use std::io::{BufRead, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn sibling_bin(name: &str) -> PathBuf {
    let own = std::env::current_exe().expect("current exe");
    let profile_dir = own
        .ancestors()
        .find(|p| p.file_name().is_some_and(|n| n == "deps"))
        .and_then(|p| p.parent())
        .expect("target profile dir")
        .to_path_buf();
    let candidate = profile_dir.join(name);
    assert!(
        candidate.exists(),
        "{name} not found at {candidate:?}; run `cargo test --workspace` so all binaries build"
    );
    candidate
}

struct DaemonGuard {
    child: Child,
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
    let data_dir = std::env::temp_dir().join(format!("hub-mcp-e2e-{}-{port}", std::process::id()));
    let child = Command::new(sibling_bin("scirust-hubd"))
        .args([
            "--listen",
            &format!("127.0.0.1:{port}"),
            "--data-dir",
            data_dir.to_str().expect("utf8"),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn scirust-hubd");
    let guard = DaemonGuard { child, data_dir };
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() > deadline {
            panic!("daemon did not come up within 15s");
        }
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    (guard, port)
}

/// Minimal HTTP/1.0 POST used to seed state through the real API.
fn http_post(port: u16, path: &str, body: &str) -> (u16, String) {
    use std::io::Read;
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream.set_read_timeout(Some(Duration::from_secs(10))).ok();
    let request = format!(
        "POST {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(request.as_bytes()).expect("write");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("read");
    let status: u16 = raw
        .lines()
        .next()
        .unwrap_or_default()
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body_start = raw.find("\r\n\r\n").map(|i| i + 4).unwrap_or(raw.len());
    (status, raw[body_start..].to_owned())
}

struct McpSession {
    child: Child,
    stdin: std::process::ChildStdin,
    stdout: std::io::BufReader<std::process::ChildStdout>,
}

impl McpSession {
    fn start(hub_url: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_scirust-hub-mcp"))
            .args(["--url", hub_url])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn scirust-hub-mcp");
        let stdin = child.stdin.take().expect("stdin");
        let stdout = std::io::BufReader::new(child.stdout.take().expect("stdout"));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    fn request(&mut self, payload: &serde_json::Value) -> serde_json::Value {
        writeln!(self.stdin, "{payload}").expect("write request");
        self.stdin.flush().expect("flush");
        let mut line = String::new();
        self.stdout.read_line(&mut line).expect("read response");
        serde_json::from_str(line.trim()).expect("valid JSON-RPC response")
    }
}

impl Drop for McpSession {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn manifest(component_id: &str) -> String {
    format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{component_id}",
                "name": "mcp-demo",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {{"name": "demo.echo", "contract_version": "1.0.0"}}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["visible-to-agents"]
                }}
            }}
        }}"#
    )
}

#[test]
fn agent_style_mcp_session_introspects_the_hub() {
    let (_guard, port) = start_daemon();

    // Seed one component through the real API.
    let id = uuid_v4();
    let (status, _) = http_post(port, "/api/v1/components", &manifest(&id));
    assert_eq!(status, 201);

    // Handshake exactly like an MCP client would.
    let mut mcp = McpSession::start(&format!("http://127.0.0.1:{port}"));
    let initialized = mcp.request(&serde_json::json!({
        "jsonrpc": "2.0", "id": 0,
        "method": "initialize",
        "params": { "protocolVersion": "2025-06-18" },
    }));
    assert_eq!(initialized["result"]["protocolVersion"], "2025-06-18");
    // Notification: no response expected; just send it down the pipe.
    writeln!(
        mcp.stdin,
        "{}",
        serde_json::json!({"jsonrpc":"2.0","method":"notifications/initialized"})
    )
    .expect("notify");

    // tools/list advertises the read-only catalog.
    let listed = mcp.request(&serde_json::json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/list"
    }));
    let names: Vec<String> = listed["result"]["tools"]
        .as_array()
        .expect("tools")
        .iter()
        .map(|t| t["name"].as_str().expect("name").to_owned())
        .collect();
    assert!(names.contains(&"hub.get_run".to_owned()), "{names:?}");

    // tools/call: hub.status reflects seeded state.
    let status_call = mcp.request(&serde_json::json!({
        "jsonrpc": "2.0", "id": 2,
        "method": "tools/call",
        "params": { "name": "hub.status", "arguments": {} },
    }));
    let status_text = status_call["result"]["content"][0]["text"]
        .as_str()
        .expect("status text");
    let status_json: serde_json::Value = serde_json::from_str(status_text).expect("json");
    assert_eq!(status_json["components_registered"], 1);
    assert_eq!(status_json["executor_backend"], "process");

    // tools/call with arguments: component introspection by UUID.
    let component_call = mcp.request(&serde_json::json!({
        "jsonrpc": "2.0", "id": 3,
        "method": "tools/call",
        "params": {
            "name": "hub.get_component",
            "arguments": { "id": id },
        },
    }));
    let component_text = component_call["result"]["content"][0]["text"]
        .as_str()
        .expect("component text");
    assert!(component_text.contains("mcp-demo"), "{component_text}");
    assert!(component_text.contains(&id), "{component_text}");

    // Unknown tool surfaces a tool-level error, not an RPC failure.
    let unknown = mcp.request(&serde_json::json!({
        "jsonrpc": "2.0", "id": 4,
        "method": "tools/call",
        "params": { "name": "hub.submit_run", "arguments": {} },
    }));
    assert_eq!(unknown["result"]["isError"], true);
    assert!(unknown["result"]["content"][0]["text"]
        .as_str()
        .expect("text")
        .contains("unknown tool"));

    drop(mcp);
}

fn uuid_v4() -> String {
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
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
        b[12], b[13], b[14], b[15]
    )
}
