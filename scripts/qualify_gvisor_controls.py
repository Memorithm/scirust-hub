"""Bounded real runsc controls probe; no scientific or hardware verdict."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys


def invoke(binary, request, root, destination):
    request_path = destination.with_suffix(".request.json")
    request_path.write_text(json.dumps(request), encoding="utf-8")
    proc = subprocess.Popen(
        [str(binary), str(request_path), str(root)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True,
    )
    try:
        stdout, stderr = proc.communicate(timeout=request["timeout_ms"] / 1000 + 12)
    except subprocess.TimeoutExpired:
        os.killpg(proc.pid, signal.SIGKILL)
        proc.communicate(timeout=2)
        raise AssertionError("adapter exceeded its bounded qualification deadline")
    assert len(stdout) <= 2 * 1024 * 1024 and len(stderr) <= 2 * 1024 * 1024
    receipt = json.loads(stdout)
    assert receipt["task_id"] == request["task_id"]
    assert receipt["schema"] == "scirust-hub.gvisor-execution-receipt/v1"
    assert receipt["backend_id"] == "docker-runsc"
    assert receipt["isolation"] == "userspace_kernel"
    assert (proc.returncode == 0) == (receipt["exit_code"] == 0 and not receipt["timed_out"])
    destination.write_bytes(stdout)
    return receipt


def main():
    binary, original, root, output = map(Path, sys.argv[1:])
    baseline = json.loads(original.read_text(encoding="utf-8"))
    output.mkdir()
    workspace = Path(baseline["workspace"])

    def case(name, command, timeout=5000, capture=1024, memory=536870912, pids=64, guest_pids=None):
        request = dict(baseline, task_id=baseline["task_id"] + "-" + name,
                       command=command, timeout_ms=timeout,
                       max_capture_bytes=capture, memory_bytes=memory, pids_limit=pids)
        if guest_pids is not None:
            request["guest_pids_limit"] = guest_pids
        return invoke(binary, request, root, output / (name + ".receipt.json"))

    timed = case("timeout", ["bash", "-c", "printf RUNTIME_ENTERED; exec sleep 30"], timeout=2000)
    assert timed["timed_out"] and "RUNTIME_ENTERED" in timed["stdout"]
    print("GVISOR_REAL_TIMEOUT_AND_EXACT_ID_CLEANUP_OK")

    limited = case("output", ["bash", "-c", "for ((i=0;i<4096;i++)); do printf bounded-output; done"], capture=128)
    assert limited["exit_code"] == 0 and not limited["timed_out"]
    assert limited["stdout_truncated"] and len(limited["stdout"].encode()) <= 128
    print("GVISOR_REAL_BOUNDED_OUTPUT_OK")

    # A nonroot user's denied write alone could just be DAC permissions; first
    # prove the actual root mount is read-only in the guest's mount table.
    readonly = case("readonly", ["bash", "-c",
                    "awk '$2 == \"/\" && $4 ~ /(^|,)ro(,|$)/ {found=1} END {exit !found}' /proc/mounts || exit 8; "
                    "if touch /hostile-root-write; then exit 9; fi; printf ROOT_WRITE_REFUSED"])
    assert readonly["exit_code"] == 0 and "ROOT_WRITE_REFUSED" in readonly["stdout"]
    print("GVISOR_REAL_READONLY_ROOT_OK")

    file_bound = case("file-size", ["bash", "-c",
                                   "dd if=/dev/zero of=/workspace/file-limit-probe bs=1048576 count=17 status=none"], timeout=10000)
    assert file_bound["exit_code"] not in (None, 0) and not file_bound["timed_out"]
    produced = workspace / "file-limit-probe"
    assert produced.is_file() and produced.stat().st_size == 16 * 1024 * 1024
    produced.unlink()
    print("GVISOR_REAL_PER_FILE_OS_BOUND_OK")

    # Fixed Rust source, at most 384 MiB requested within a 128 MiB cgroup;
    # no host execution, no exponential fork/allocation loop and no production.
    (workspace / "memory_probe.rs").write_text(
        'fn main() { use std::io::Write; println!("MEMORY_ENTERED"); '
        'std::io::stdout().flush().unwrap(); '
        'let mut bytes = vec![0u8; 384 * 1024 * 1024]; '
        'for index in (0..bytes.len()).step_by(4096) { bytes[index] = 1; } '
        'std::hint::black_box(bytes); println!("MEMORY_ESCAPED"); }', encoding="utf-8")
    compiled = case("memory-compile", ["rustc", "memory_probe.rs", "-o", "memory_probe"], timeout=60000)
    assert compiled["exit_code"] == 0 and not compiled["timed_out"]
    memory = case("memory", ["./memory_probe"], timeout=10000, memory=128 * 1024 * 1024)
    assert "MEMORY_ENTERED" in memory["stdout"]
    assert "MEMORY_ESCAPED" not in memory["stdout"]
    assert memory["exit_code"] == 137 and not memory["timed_out"]
    print("GVISOR_REAL_MEMORY_AND_SWAP_BOUND_OK")

    # Fixed finite probe: eight sleeping child processes and at most96 threads.
    # All creation and cleanup happen in runsc, never on the qualification host.
    # Positive128 control distinguishes denial from a broken startup/runtime.
    (workspace / "pid_probe.rs").write_text(r'''
use std::process::{Command, Stdio};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};
use std::time::Duration;
fn main() {
    let limits = std::fs::read_to_string("/proc/self/limits").unwrap();
    let line = limits.lines().find(|line| line.starts_with("Max processes")).unwrap();
    println!("PID_LIMIT={line}");
    let status = std::fs::read_to_string("/proc/self/status").unwrap();
    println!("PID_UID={}", status.lines().find(|line| line.starts_with("Uid:")).unwrap());
    println!("PID_BOUND_ENTERED");
    let mut children = Vec::new();
    let mut error = 0;
    for _ in 0..8 {
        match Command::new("sleep").arg("30").stdout(Stdio::null()).stderr(Stdio::null()).spawn() {
            Ok(child) => children.push(child),
            Err(e) => { error = e.raw_os_error().unwrap_or(-1); break; }
        }
    }
    let stop = Arc::new(AtomicBool::new(false));
    let mut threads = Vec::new();
    if children.len() == 8 {
        for _ in 0..96 {
            let flag = stop.clone();
            match std::thread::Builder::new().spawn(move || {
                while !flag.load(Ordering::Acquire) { std::thread::sleep(Duration::from_millis(10)); }
            }) {
                Ok(thread) => threads.push(thread),
                Err(e) => { error = e.raw_os_error().unwrap_or(-1); break; }
            }
        }
    }
    let processes = children.len();
    let thread_count = threads.len();
    stop.store(true, Ordering::Release);
    for thread in threads { thread.join().unwrap(); }
    for mut child in children { child.kill().unwrap(); child.wait().unwrap(); }
    println!("PID_COUNTS={processes},{thread_count},{error}");
}
''', encoding="utf-8")
    compiled = case("pid-compile", ["rustc", "pid_probe.rs", "-o", "pid_probe"], timeout=60000)
    assert compiled["exit_code"] == 0 and not compiled["timed_out"]

    def pid_observation(name, budget):
        # Hold the host control fixed well above the entire finite probe range;
        # only the guest RLIMIT_NPROC changes64->128. No confounded host denial.
        receipt = case(name, ["./pid_probe"], timeout=15000, capture=4096, pids=512, guest_pids=budget)
        assert receipt["exit_code"] == 0 and not receipt["timed_out"]
        assert not receipt["stdout_truncated"] and not receipt["stderr_truncated"]
        lines = receipt["stdout"].splitlines()
        assert "PID_BOUND_ENTERED" in lines
        limit = next(line for line in lines if line.startswith("PID_LIMIT=")).split()
        assert limit[2:4] == [str(budget), str(budget)], limit
        uid = next(line for line in lines if line.startswith("PID_UID=")).split()[1]
        assert uid != "0"
        return tuple(map(int, next(line for line in lines if line.startswith("PID_COUNTS=")).split("=", 1)[1].split(",")))

    low = pid_observation("pid-limited", 64)
    high = pid_observation("pid-positive-control", 128)
    assert low[0] == 8 and 0 < low[1] and low[0] + low[1] < 64 and low[2] == 11, low
    assert high == (8, 96, 0), high
    print("GVISOR_REAL_GUEST_PID_BOUND_AND_POSITIVE_CONTROL_OK")


if __name__ == "__main__":
    main()
