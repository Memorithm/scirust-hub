//! Durability proof: state written through the real daemon with the default
//! SQLite store survives a full process kill + restart on the same data
//! directory.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// Kills the child on drop. Deliberately does NOT touch the data directory:
/// both lifetimes share it, and the test removes it explicitly at the end.
struct DaemonGuard {
    child: Child,
}

impl Drop for DaemonGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("probe bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn spawn(listen_port: u16, data_dir: &std::path::Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_scirust-hubd"))
        .args([
            "--listen",
            &format!("127.0.0.1:{listen_port}"),
            "--data-dir",
            data_dir.to_str().expect("utf8 path"),
            // Default store is sqlite; spelled out to make the contract of
            // this test explicit.
            "--store",
            "sqlite",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn scirust-hubd")
}

fn wait_healthy(port: u16, child: &Child) {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if Instant::now() > deadline {
            panic!("daemon pid {} did not become healthy", child.id());
        }
        if let Ok((status, _)) = http(port, "GET", "/health", None) {
            assert_eq!(status, 200);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Minimal HTTP/1.0 client (no chunked transfer encoding).
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

fn manifest(component_id: &str) -> String {
    format!(
        r#"{{
            "schema_version": 1,
            "manifest": {{
                "id": "{component_id}",
                "name": "durable-demo",
                "version": "2.3.4",
                "kind": "tool",
                "capabilities": [
                    {{"name": "demo.durable", "contract_version": "1.0.0"}}
                ],
                "execution": {{
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["ok"]
                }}
            }}
        }}"#
    )
}

#[test]
fn sqlite_state_survives_daemon_restart() {
    let data_dir = std::env::temp_dir().join(format!(
        "hub-restart-{}-{}",
        std::process::id(),
        free_port()
    ));
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let component_id = uuid_v4();

    // --- first lifetime: register a component and complete a run ---
    let port_a = free_port();
    let mut first = DaemonGuard {
        child: spawn(port_a, &data_dir),
    };
    wait_healthy(port_a, &first.child);

    let (status, body) = http(
        port_a,
        "POST",
        "/api/v1/components",
        Some(&manifest(&component_id)),
    )
    .expect("register before restart");
    assert_eq!(status, 201, "body: {body}");

    let submit = serde_json::json!({
        "schema_version": 1,
        "run_spec": {
            "component": component_id,
            "capability": "demo.durable",
            "parameters": {"phase": "before"},
            "inputs": [],
            "timeout_ms": 5000
        }
    })
    .to_string();
    let (status, body) = http(port_a, "POST", "/api/v1/runs", Some(&submit)).expect("submit");
    assert_eq!(status, 201, "body: {body}");
    let run_id: String = body
        .split("\"id\":\"")
        .nth(1)
        .map(|s| s.chars().take_while(|c| *c != '"').collect())
        .expect("run id");

    // Kill hard: no graceful shutdown, WAL must still be consistent.
    let _ = first.child.kill();
    let _ = first.child.wait();
    drop(first);
    assert!(
        data_dir.join("hub.db").exists(),
        "database file must persist"
    );

    // --- second lifetime: fresh process, same data directory ---
    let port_b = free_port();
    let second = DaemonGuard {
        child: spawn(port_b, &data_dir),
    };
    wait_healthy(port_b, &second.child);

    // Component survived.
    let (status, body) = http(
        port_b,
        "GET",
        &format!("/api/v1/components/{component_id}"),
        None,
    )
    .expect("get after restart");
    assert_eq!(status, 200, "component lost across restart: {body}");
    assert!(body.contains("\"durable-demo\""), "body: {body}");
    assert!(body.contains("\"2.3.4\""), "body: {body}");

    // Run record survived with its terminal state.
    let (status, body) =
        http(port_b, "GET", &format!("/api/v1/runs/{run_id}"), None).expect("run after restart");
    assert_eq!(status, 200, "run lost across restart: {body}");
    assert!(
        body.contains("\"created\""),
        "state should be created (never executed): {body}"
    );

    // The registry is live, not just readable: replay registration is still
    // an idempotent no-op against restored content.
    let (status, body) = http(
        port_b,
        "POST",
        "/api/v1/components",
        Some(&manifest(&component_id)),
    )
    .expect("replay after restart");
    assert_eq!(status, 200, "body: {body}");
    assert!(body.contains("already_registered"), "body: {body}");

    drop(second);
    cleanup(&data_dir);
}

fn cleanup(dir: &std::path::Path) {
    let _ = std::fs::remove_dir_all(dir);
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
