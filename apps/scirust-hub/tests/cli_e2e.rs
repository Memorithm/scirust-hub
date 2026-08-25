//! End-to-end proof at the outermost boundary: the real `scirust-hub` CLI
//! drives a real `scirust-hubd` daemon over TCP, exercising the exact flows a
//! user would run.
//!
//! Requires both workspace binaries to be built (`cargo test --workspace`
//! does this); the sibling binary is located in the shared target directory.

use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn sibling_bin(name: &str) -> PathBuf {
    // This test's own binary lives in target/<profile>/deps/; workspace bins
    // land in target/<profile>/.
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
    let data_dir = std::env::temp_dir().join(format!("hub-cli-e2e-{}-{port}", std::process::id()));
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

fn run_cli(port: u16, args: &[&str]) -> Output {
    Command::new(sibling_bin("scirust-hub"))
        .arg("--url")
        .arg(format!("http://127.0.0.1:{port}").as_str())
        .args(args)
        .output()
        .expect("run scirust-hub")
}

fn expect_success(output: &Output) -> String {
    assert!(
        output.status.success(),
        "cli failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).into_owned()
}

#[test]
fn cli_drives_the_full_component_to_provenance_flow() {
    let (_guard, port) = start_daemon();

    // status
    let out = run_cli(port, &["status"]);
    let stdout = expect_success(&out);
    assert!(stdout.contains("ready: true"), "stdout: {stdout}");

    // component register (manifest via temp file)
    let manifest_path = std::env::temp_dir().join(format!("hub-cli-manifest-{port}.json"));
    std::fs::write(
        &manifest_path,
        r#"{
                "schema_version": 1,
                "manifest": {
                    "id": "11111111-2222-3333-4444-555555555555",
                    "name": "demo-cat",
                    "version": "1.0.0",
                    "kind": "tool",
                    "capabilities": [
                        {
                            "name": "demo.cat",
                            "contract_version": "1.0.0",
                            "inputs": [{"name": "source"}],
                            "outputs": [{"name": "stdout"}]
                        }
                    ],
                    "execution": {
                        "type": "process",
                        "program": "/bin/cat",
                        "args": ["{input:source}"]
                    }
                }
            }"#,
    )
    .expect("write manifest");
    let out = run_cli(
        port,
        &["component", "register", manifest_path.to_str().unwrap()],
    );
    let stdout = expect_success(&out);
    assert!(stdout.contains("created"), "stdout: {stdout}");
    let _ = std::fs::remove_file(manifest_path);

    // capability list shows the declared capability
    let stdout = expect_success(&run_cli(port, &["capabilities"]));
    assert!(stdout.contains("demo.cat"), "stdout: {stdout}");

    // seed an input artifact through the API by first producing one:
    // run the echo component? demo-cat has an input port; we need an artifact.
    // Use the HTTP API directly to store nothing extra — instead reuse a
    // produced artifact from a prior run of another component registered now.
    let manifest2 = std::env::temp_dir().join(format!("hub-cli-manifest2-{port}.json"));
    std::fs::write(
        &manifest2,
        r#"{
            "schema_version": 1,
            "manifest": {
                "id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                "name": "demo-echo",
                "version": "1.0.0",
                "kind": "tool",
                "capabilities": [
                    {"name": "demo.echo", "contract_version": "1.0.0"}
                ],
                "execution": {
                    "type": "process",
                    "program": "/bin/echo",
                    "args": ["seed-payload"]
                }
            }
        }"#,
    )
    .expect("write manifest2");
    expect_success(&run_cli(
        port,
        &["component", "register", manifest2.to_str().unwrap()],
    ));
    let _ = std::fs::remove_file(manifest2);

    // submit + wait produces the seed artifact; capture its id from JSON mode.
    let json_out = expect_success(&run_cli(
        port,
        &[
            "--output",
            "json",
            "run",
            "submit",
            "--component",
            "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "--capability",
            "demo.echo",
            "--params",
            "{}",
            "--wait",
        ],
    ));
    let submitted: serde_json::Value =
        serde_json::from_str(&json_out).expect("--output json must emit valid JSON");
    assert_eq!(submitted["state"], "succeeded");
    let seed_artifact = submitted["outcome"]["outputs"][0]["artifact"]
        .as_str()
        .expect("artifact id")
        .to_owned();

    // Now feed that artifact into demo.cat and verify the pipeline end-to-end.
    let json_cat = expect_success(&run_cli(
        port,
        &[
            "--output",
            "json",
            "run",
            "submit",
            "--component",
            "11111111-2222-3333-4444-555555555555",
            "--capability",
            "demo.cat",
            "--input",
            &format!("source={seed_artifact}"),
            "--wait",
        ],
    ));
    let cat_run: serde_json::Value =
        serde_json::from_str(&json_cat).expect("valid json for cat run");
    assert_eq!(cat_run["state"], "succeeded");
    let cat_artifact = cat_run["outcome"]["outputs"][0]["artifact"]
        .as_str()
        .expect("cat artifact id")
        .to_owned();

    // artifact inspect with content proves byte-exact propagation
    // (echo printed "seed-payload\n"; cat copied it).
    let human = expect_success(&run_cli(
        port,
        &["artifact", "inspect", &cat_artifact, "--content"],
    ));
    assert!(human.contains("seed-payload"), "human output: {human}");

    // run list / inspect show provenance
    let listing = expect_success(&run_cli(port, &["run", "list"]));
    assert!(
        listing.matches("succeeded").count() >= 2,
        "listing: {listing}"
    );
    let run_id = cat_run["id"].as_str().expect("run id").to_owned();
    let inspect = expect_success(&run_cli(
        port,
        &["--output", "json", "run", "inspect", &run_id],
    ));
    let inspected: serde_json::Value = serde_json::from_str(&inspect).expect("json");
    let transitions = inspected["transitions"].as_array().expect("transitions");
    let states: Vec<&str> = transitions
        .iter()
        .filter_map(|t| t["to"].as_str())
        .collect();
    assert_eq!(
        states,
        vec!["validated", "queued", "running", "succeeded"],
        "provenance transitions"
    );

    // error paths surface structured failures with non-zero exit
    let failing = run_cli(
        port,
        &[
            "run",
            "submit",
            "--component",
            "11111111-2222-3333-4444-555555555555",
            "--capability",
            "demo.missing",
            "--wait",
        ],
    );
    assert!(!failing.status.success());
    let stderr = String::from_utf8_lossy(&failing.stderr).into_owned();
    assert!(stderr.contains("does not declare"), "stderr: {stderr}");
}
