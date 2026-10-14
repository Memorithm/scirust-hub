"""Finite real persistent aggregate byte/inode controls, never science."""
import json
import os
from pathlib import Path
import subprocess
import sys

from qualify_gvisor_controls import invoke


SOURCE = r'''
use std::fs::{File, remove_file};
use std::io::{Read, Write};
fn main() {
    let mode = std::env::args().nth(1).unwrap();
    if mode == "persist" {
        let mut value = String::new();
        File::open("persistent-marker").unwrap().read_to_string(&mut value).unwrap();
        assert_eq!(value, "PERSISTENT_QUOTA_OK");
        println!("PERSISTENT_QUOTA_OK");
        return;
    }
    let mut denied = None;
    let mut created = 0;
    let bound = if mode == "bytes" { 32 } else { 512 };
    for index in 0..bound {
        let path = format!("escape/quota-{mode}-{index}");
        let result = File::create(&path).and_then(|mut file| {
            if mode == "bytes" { file.write_all(&vec![1u8; 4 * 1024 * 1024])?; }
            file.sync_all()
        });
        match result {
            Ok(()) => created += 1,
            Err(error) => { denied = error.raw_os_error(); break; }
        }
    }
    assert_eq!(denied, Some(28), "must be ENOSPC, not per-file/PID/timeout denial");
    assert!(created > 0 && created < bound);
    if mode == "bytes" { assert!(created < 16, "must fit the 64 MiB volume, not the 128 MiB nested mount"); }
    println!("AGGREGATE_{mode}_ENOSPC={created}");
    // Finite known probe paths only; preserve marker and compiled artifact.
    for index in 0..bound {
        let _ = remove_file(format!("escape/quota-{mode}-{index}"));
    }
}
'''


def control(args):
    return subprocess.run(args, check=True, capture_output=True, timeout=30)


def main():
    binary, original, root, output = map(Path, sys.argv[1:])
    baseline = json.loads(original.read_text(encoding="utf-8"))
    output.mkdir()
    workspace = root / "quota-task"
    workspace.mkdir()
    image = output / "dedicated-ext4.img"
    # New CI-owned 64 MiB image, not a host block device or production mount.
    with image.open("xb") as file:
        file.truncate(64 * 1024 * 1024)
    control(["mkfs.ext4", "-F", "-q", "-m", "0", "-N", "256", str(image)])
    mounted = False
    nested = False
    try:
        control(["sudo", "mount", "-o", "loop,nodev,nosuid", str(image), str(workspace)])
        mounted = True
        control(["sudo", "chown", f"{os.getuid()}:{os.getgid()}", str(workspace)])
        (workspace / "escape").mkdir()
        (workspace / "quota_probe.rs").write_text(SOURCE, encoding="utf-8")
        with (workspace / "persistent-marker").open("x") as file:
            file.write("PERSISTENT_QUOTA_OK")
            file.flush()
            os.fsync(file.fileno())

        def case(name, command):
            request = dict(baseline, task_id=baseline["task_id"] + "-quota-" + name,
                           workspace=str(workspace), command=command, timeout_ms=60000,
                           workspace_quota={"max_bytes": 64 * 1024 * 1024, "max_inodes": 256})
            receipt = invoke(binary, request, root, output / (name + ".receipt.json"))
            assert receipt["exit_code"] == 0 and not receipt["timed_out"], receipt
            assert not receipt["stdout_truncated"] and not receipt["stderr_truncated"]
            # The private journal survives even when the guest filesystem fills.
            observed = subprocess.run([str(binary), "--inspect", receipt["task_id"], str(root)],
                                      check=True, capture_output=True, timeout=10)
            metadata = json.loads(observed.stdout)
            assert metadata["state"] == "cleaned", metadata
            (output / (name + ".attempt.json")).write_bytes(observed.stdout)
            return receipt

        case("compile", ["rustc", "quota_probe.rs", "-o", "quota_probe"])
        # Prove persistence beyond container lifetime AND actual host unmount.
        control(["sudo", "umount", str(workspace)])
        mounted = False
        control(["sudo", "mount", "-o", "loop,nodev,nosuid", str(image), str(workspace)])
        mounted = True
        assert "PERSISTENT_QUOTA_OK" in case("persist", ["./quota_probe", "persist"])["stdout"]
        # A larger nested host mount must not be imported into the guest.
        control(["sudo", "mount", "-t", "tmpfs", "-o", "size=128m", "tmpfs", str(workspace / "escape")])
        nested = True
        control(["sudo", "chmod", "0777", str(workspace / "escape")])
        negative_request = dict(baseline, task_id=baseline["task_id"] + "-quota-nested-refusal",
                                workspace=str(workspace), command=["true"],
                                workspace_quota={"max_bytes": 64 * 1024 * 1024, "max_inodes": 256})
        negative_path = output / "nested-refusal.request.json"
        negative_path.write_text(json.dumps(negative_request), encoding="utf-8")
        negative = subprocess.run([str(binary), str(negative_path), str(root)],
                                  capture_output=True, timeout=10)
        assert negative.returncode != 0 and not negative.stdout
        assert b"nested host mounts" in negative.stderr, negative.stderr
        (output / "nested-refusal.json").write_text(json.dumps({
            "request_task_id": negative_request["task_id"],
            "exit_code": negative.returncode, "stderr": negative.stderr.decode(),
            "no_receipt": True}), encoding="utf-8")
        control(["sudo", "umount", str(workspace / "escape")])
        nested = False
        byte_receipt = case("bytes", ["./quota_probe", "bytes"])
        assert "AGGREGATE_bytes_ENOSPC=" in byte_receipt["stdout"]
        inode_receipt = case("inodes", ["./quota_probe", "inodes"])
        assert "AGGREGATE_inodes_ENOSPC=" in inode_receipt["stdout"]
        summary = {"schema": "scirust-hub.workspace-quota-probe/v1",
                   "source_commit": os.environ["SCIRUST_HUB_SOURCE_COMMIT"],
                   "workflow_run_id": int(os.environ["GITHUB_RUN_ID"]),
                   "filesystem_image_bytes": image.stat().st_size,
                   "persistent_remount_positive": True,
                   "bounded_multi_file_enospc": True, "bounded_inode_enospc": True,
                   "nested_host_mount_refused_before_create": True, "journal_outside_guest_volume": True,
                   "scope": "hosted Linux fixture; no production provisioning or scientific verdict"}
        (output / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print("GVISOR_REAL_PERSISTENT_AGGREGATE_BYTES_INODES_NONRECURSIVE_OK")
    finally:
        if nested:
            control(["sudo", "umount", str(workspace / "escape")])
        if mounted:
            control(["sudo", "umount", str(workspace)])


if __name__ == "__main__":
    main()
