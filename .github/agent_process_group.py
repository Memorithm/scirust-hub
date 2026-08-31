from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text()
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected one replacement, found {count}: {old[:160]!r}")
    p.write_text(text.replace(old, new, 1))


replace_once(
    "Cargo.toml",
    'ureq = { version = "2", default-features = false, features = ["json", "tls", "native-certs"] }\n',
    'ureq = { version = "2", default-features = false, features = ["json", "tls", "native-certs"] }\nnix = { version = "0.31", default-features = false, features = ["signal", "process"] }\n',
)

replace_once(
    "crates/hub-executor/Cargo.toml",
    '''ureq = { workspace = true }\n\n[lints]\n''',
    '''ureq = { workspace = true }\n\n[target.'cfg(unix)'.dependencies]\nnix = { workspace = true }\n\n[lints]\n''',
)

lib = "crates/hub-executor/src/lib.rs"
replace_once(
    lib,
    '''//! - [`ProcessExecutor`]: supervised local subprocess execution with hard\n//!   output caps, wall-clock timeouts, cooperative cancellation and an\n//!   explicitly constructed environment. **This is resource control, not a\n//!   security sandbox**: children run with the Hub's OS privileges.\n''',
    '''//! - [`ProcessExecutor`]: supervised local subprocess execution with hard\n//!   output caps, wall-clock timeouts, cooperative cancellation and an\n//!   explicitly constructed environment. On Unix each execution is isolated\n//!   into its own process group so ordinary descendants are terminated with\n//!   the group on timeout/cancellation. **This is resource control, not a\n//!   security sandbox**: children run with the Hub's OS privileges.\n''',
)
replace_once(
    lib,
    '''use std::process::{Command, Stdio};\n''',
    '''use std::process::{Child, Command, Stdio};\n#[cfg(unix)]\nuse std::os::unix::process::CommandExt as _;\n''',
)
replace_once(
    lib,
    '''            .stdout(Stdio::piped())\n            .stderr(Stdio::piped());\n\n        // Spawn failure (missing program, permission denied, bad cwd) is an\n''',
    '''            .stdout(Stdio::piped())\n            .stderr(Stdio::piped());\n\n        // On Unix the direct child becomes leader of a fresh process group.\n        // Descendants inherit that group unless they deliberately detach, so\n        // timeout/cancellation can terminate the ordinary execution subtree\n        // without signalling the Hub's own process group.\n        #[cfg(unix)]\n        command.process_group(0);\n\n        // Spawn failure (missing program, permission denied, bad cwd) is an\n''',
)
replace_once(
    lib,
    '''            if cancel.is_cancelled() {\n                cancelled = true;\n                let _ = child.kill();\n                break child.wait().ok();\n            }\n            if Instant::now() >= deadline {\n                timed_out = true;\n                let _ = child.kill();\n                break child.wait().ok();\n            }\n''',
    '''            if cancel.is_cancelled() {\n                cancelled = true;\n                terminate_supervised_process(&mut child);\n                break child.wait().ok();\n            }\n            if Instant::now() >= deadline {\n                timed_out = true;\n                terminate_supervised_process(&mut child);\n                break child.wait().ok();\n            }\n''',
)
replace_once(
    lib,
    '''fn ms_since(started: Instant) -> u64 {\n''',
    '''fn terminate_supervised_process(child: &mut Child) {\n    #[cfg(unix)]\n    if let Ok(raw_pid) = i32::try_from(child.id()) {\n        use nix::sys::signal::{killpg, Signal};\n        use nix::unistd::Pid;\n\n        // `process_group(0)` makes the child's PID its PGID. Signal the whole\n        // group first, then retain `Child::kill` as a best-effort fallback in\n        // case group signalling fails for an OS-specific reason.\n        let _ = killpg(Pid::from_raw(raw_pid), Signal::SIGKILL);\n    }\n    let _ = child.kill();\n}\n\nfn ms_since(started: Instant) -> u64 {\n''',
)

replace_once(
    lib,
    '''    #[test]\n    fn missing_program_is_an_observed_start_error() {\n''',
    '''    #[cfg(unix)]\n    #[test]\n    fn cancellation_kills_ordinary_descendants_in_process_group() {\n        use nix::sys::signal::{getpgid, killpg, Signal};\n        use nix::unistd::Pid;\n\n        let exec = ProcessExecutor::new();\n        let workdir = temp_workdir("cancel-group");\n        let pid_file = workdir.join("pids");\n        let mut request = base_request("sh", &[]);\n        request.working_dir = workdir.clone();\n        request.args = vec![\n            "-c".to_owned(),\n            "sleep 30 & printf '%s %s\\n' \"$$\" \"$!\" > \"$1\"; wait".to_owned(),\n            "hub-process-group-test".to_owned(),\n            pid_file.display().to_string(),\n        ];\n        request.timeout_ms = 60_000;\n\n        let cancel = CancelToken::new();\n        let watcher_cancel = cancel.clone();\n        let watcher_pid_file = pid_file.clone();\n        let watcher = thread::spawn(move || {\n            let deadline = Instant::now() + Duration::from_secs(5);\n            loop {\n                if let Ok(text) = std::fs::read_to_string(&watcher_pid_file) {\n                    let mut fields = text.split_whitespace();\n                    if let (Some(parent), Some(descendant)) = (fields.next(), fields.next()) {\n                        let parent: i32 = parent.parse().expect("parent pid");\n                        let descendant: i32 = descendant.parse().expect("descendant pid");\n                        assert!(parent > 1 && descendant > 1);\n                        assert_eq!(\n                            getpgid(Some(Pid::from_raw(descendant))).expect("descendant pgid"),\n                            Pid::from_raw(parent),\n                            "descendant must inherit the executor-created process group"\n                        );\n                        watcher_cancel.cancel();\n                        return parent;\n                    }\n                }\n                assert!(\n                    Instant::now() < deadline,\n                    "child did not publish process-group membership"\n                );\n                thread::sleep(Duration::from_millis(10));\n            }\n        });\n\n        let started = Instant::now();\n        let outcome = exec.execute(&request, &cancel).expect("run");\n        let process_group = watcher.join().expect("watcher");\n        assert!(outcome.cancelled);\n        assert!(started.elapsed() < Duration::from_secs(5));\n\n        let deadline = Instant::now() + Duration::from_secs(5);\n        loop {\n            if killpg(\n                Pid::from_raw(process_group),\n                Option::<Signal>::None,\n            )\n            .is_err()\n            {\n                break;\n            }\n            assert!(\n                Instant::now() < deadline,\n                "execution process group survived cancellation"\n            );\n            thread::sleep(Duration::from_millis(10));\n        }\n        let _ = std::fs::remove_dir_all(workdir);\n    }\n\n    #[test]\n    fn missing_program_is_an_observed_start_error() {\n''',
)

replace_once(
    "README.md",
    '''The local process executor is deliberately **not a security sandbox**: registered\nprocesses run with the Hub's OS privileges. Use OS/container isolation for untrusted\ncomponents. The worker has the same boundary.\n''',
    '''The local process executor is deliberately **not a security sandbox**: registered\nprocesses run with the Hub's OS privileges. On Unix, each execution is placed in\na fresh process group and timeout/cancellation sends `SIGKILL` to that group, so\nordinary descendants that remain in the group are stopped with the direct child.\nA descendant can deliberately detach into another session/process group; this is\ntherefore resource supervision, not containment. On non-Unix targets the current\ndirect-child kill behavior remains. Use OS/container isolation for untrusted\ncomponents. The worker has the same boundary because it uses the same process\nexecutor.\n''',
)

replace_once(
    "CHANGELOG.md",
    '''### Added\n\n- Fail-closed remote-worker graceful drain (ADR-0016): SIGINT/SIGTERM stops\n''',
    '''### Added\n\n- Unix process-group supervision for `ProcessExecutor` (ADR-0017): every local\n  execution becomes leader of a fresh process group, and timeout/cancellation\n  signals that group before the existing direct-child kill fallback. Ordinary\n  descendants can no longer keep running merely because the direct child was\n  killed; deliberately detached descendants remain outside this guarantee.\n\n- Fail-closed remote-worker graceful drain (ADR-0016): SIGINT/SIGTERM stops\n''',
)

Path("docs/adr/0017-unix-process-group-supervision.md").write_text('''# ADR 0017 — Unix process-group supervision\n\nStatus: accepted\n\n## Context\n\n`ProcessExecutor` enforces wall-clock timeout and cooperative cancellation, but\nits termination path historically called `Child::kill()` on only the direct\nchild. A process may create ordinary descendants that inherit its stdout/stderr\nand continue running after the direct child is killed. That can retain pipes,\ndelay executor completion and consume resources beyond the cancelled/expired\nrun.\n\nThe execution model is explicitly not a security sandbox. The goal here is\ntherefore narrower: make the existing timeout/cancellation resource boundary\napply to ordinary Unix descendants without claiming containment against a\nprocess that deliberately escapes supervision.\n\n## Decision\n\nOn Unix, `ProcessExecutor` configures every spawned command with\n`CommandExt::process_group(0)`. The child becomes leader of a fresh process\ngroup whose PGID equals its PID. Ordinary descendants inherit that group.\n\nOn timeout or cancellation, the executor sends `SIGKILL` to the child's process\ngroup using the safe `nix::sys::signal::killpg` wrapper, then still calls\n`Child::kill()` as a best-effort fallback before waiting/reaping the direct\nchild. No `unsafe` block is introduced into the workspace.\n\nOn non-Unix targets process-group APIs are not available through this design,\nso the previous direct-child kill behavior is retained.\n\n## Invariants\n\n- Structured argv/environment semantics are unchanged; no implicit shell is\n  introduced.\n- Timeout and cancellation remain observable through the existing\n  `ExecutionOutcome` fields.\n- The Hub process is not placed in the execution group and must never be\n  signalled by execution cancellation.\n- The remote worker inherits the same behavior because it delegates to\n  `ProcessExecutor`.\n- Spawn failures remain observed outcomes rather than backend panics.\n\n## Limits / non-goals\n\nThis remains **resource supervision, not a sandbox**. A descendant can call\n`setsid`/`setpgid` or otherwise deliberately leave the inherited process group.\nThe change does not add namespaces, cgroups, seccomp, containers, Windows Job\nObjects, privilege dropping, syscall filtering, or durable process ownership.\nThose require separate platform/security designs.\n''')

print("Unix process-group supervision patch staged")
