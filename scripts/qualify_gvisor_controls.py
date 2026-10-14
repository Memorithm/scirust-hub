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

    def case(name, command, timeout=5000, capture=1024, memory=536870912):
        request = dict(baseline, task_id=baseline["task_id"] + "-" + name,
                       command=command, timeout_ms=timeout,
                       max_capture_bytes=capture, memory_bytes=memory)
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


if __name__ == "__main__":
    main()
