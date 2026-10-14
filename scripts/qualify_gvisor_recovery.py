"""Finite crash/lost-response probes on CI-owned real Docker/runsc resources.

No scientific verdict, implicit replay or production-resource reclamation.
"""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time


def bounded(argv, *, env=None, timeout=10):
    result = subprocess.run(argv, env=env, capture_output=True, timeout=timeout)
    assert len(result.stdout) + len(result.stderr) <= 2 * 1024 * 1024
    return result


def main():
    binary, original, root, output = (p.resolve() for p in map(Path, sys.argv[1:]))
    baseline = json.loads(original.read_text(encoding="utf-8"))
    output.mkdir()
    docker = shutil.which("docker")
    assert docker and Path(docker).is_absolute()

    def record(task):
        result = bounded([str(binary), "--inspect", task, str(root)])
        assert result.returncode == 0, result.stderr
        return json.loads(result.stdout)

    def reconcile(task, succeeds):
        result = bounded([str(binary), "--reconcile", task, str(root)])
        assert (result.returncode == 0) == succeeds, result.stderr
        return record(task)

    def request(name, command):
        workspace = root / ("recovery-" + name)
        workspace.mkdir()
        req = dict(baseline, task_id=baseline["task_id"] + "-recovery-" + name,
                   workspace=str(workspace), command=command, timeout_ms=30000)
        path = output / (name + ".request.json")
        path.write_text(json.dumps(req), encoding="utf-8")
        return req, path, workspace

    def replay_refused(path, req):
        assert bounded([str(binary), str(path), str(root)]).returncode != 0
        other = dict(req, task_id=req["task_id"] + "-other")
        other_path = path.with_name(path.stem + ".other.json")
        other_path.write_text(json.dumps(other), encoding="utf-8")
        assert bounded([str(binary), str(other_path), str(root)]).returncode != 0

    def observation(owned, running):
        cid = owned["owned_handle"]
        assert isinstance(cid, str) and len(cid) == 64 and all(c in "0123456789abcdef" for c in cid)
        result = bounded([docker, "inspect", cid])
        assert result.returncode == 0, result.stderr
        info = json.loads(result.stdout)[0]
        assert info["Id"] == cid and info["State"]["Running"] == running
        assert info["Config"]["Labels"]["org.memorithm.hub.attempt"] == owned["attempt_id"]
        assert info["Config"]["Labels"]["org.memorithm.hub.request"] == owned["request_sha256"]
        return {"container_id": cid, "running": running, "labels": info["Config"]["Labels"]}

    def remove_owned(cid):
        # Only an exact immutable ID already matched to our durable nonce/digest.
        result = bounded([docker, "rm", "--force", cid])
        assert result.returncode == 0, result.stderr

    # SIGKILL the controller after a positive guest-entry witness. Capture its
    # own Docker client and PID start time before killing, avoiding PID reuse.
    req, path, workspace = request("crash", ["bash", "-c",
                                           "printf entered > /workspace/recovery-entered; exec sleep 30"])
    proc = subprocess.Popen([str(binary), str(path), str(root)], stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, start_new_session=True)
    cid = None
    children = []
    try:
        deadline = time.monotonic() + 20
        while not (workspace / "recovery-entered").exists():
            assert proc.poll() is None and time.monotonic() < deadline, "guest did not enter before bounded crash probe"
            time.sleep(0.05)
        before = record(req["task_id"])
        assert before["state"] == "owned"
        cid = before["owned_handle"]
        for pid in (Path(f"/proc/{proc.pid}/task/{proc.pid}/children").read_text().split()):
            stat = Path(f"/proc/{pid}/stat").read_text().split(")", 1)[1].split()
            children.append((int(pid), stat[19]))
        assert children, "expected the controller's owned attach client"
        proc.kill()
        proc.wait(timeout=2)
        for pid, start_time in children:
            stat_path = Path(f"/proc/{pid}/stat")
            if stat_path.exists() and stat_path.read_text().split(")", 1)[1].split()[19] == start_time:
                assert os.getpgid(pid) == pid
                os.killpg(pid, signal.SIGKILL)
        proc.communicate(timeout=2)
        real = observation(before, True)
        after = record(req["task_id"])
        assert after == before
        replay_refused(path, req)
        assert reconcile(req["task_id"], False) == before
        observation(before, True)  # Reconciliation did not stop/start/delete it.
        remove_owned(cid)
        cid = None
        cleaned = reconcile(req["task_id"], True)
        assert cleaned["state"] == "cleaned" and cleaned["owned_handle"] == before["owned_handle"]
        assert bounded([str(binary), str(path), str(root)]).returncode != 0
        (output / "crash.json").write_text(json.dumps({"before": before, "after": after,
            "daemon": real, "cleaned": cleaned, "guest_entry_observed": True}, indent=2), encoding="utf-8")
        print("GVISOR_REAL_CONTROLLER_CRASH_QUARANTINE_RECONCILIATION_OK")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=2)
        if cid:
            remove_owned(cid)

    # Real creation succeeds, but a CI-only transport wrapper discards the
    # returned ID and reports failure. This is not a fake isolation backend.
    req, path, _ = request("lost-response", ["true"])
    wrapper_dir = output / "fault-transport"
    wrapper_dir.mkdir()
    wrapper = wrapper_dir / "docker"
    wrapper.write_text('#!/bin/sh\nif [ "$1" = create ]; then\n'
                       f'  "{docker}" "$@" >/dev/null\n  exit 7\nfi\n'
                       f'exec "{docker}" "$@"\n', encoding="utf-8")
    wrapper.chmod(0o700)
    env = dict(os.environ, PATH=str(wrapper_dir) + os.pathsep + os.environ["PATH"])
    assert bounded([str(binary), str(path), str(root)], env=env, timeout=40).returncode != 0
    reserved = record(req["task_id"])
    assert reserved["state"] == "reserved" and reserved["owned_handle"] is None
    cid = None
    try:
        owned = reconcile(req["task_id"], False)
        assert owned["state"] == "owned" and owned["attempt_id"] == reserved["attempt_id"]
        cid = owned["owned_handle"]
        real = observation(owned, False)
        replay_refused(path, req)
        remove_owned(cid)
        cid = None
        cleaned = reconcile(req["task_id"], True)
        assert cleaned["state"] == "cleaned"
        (output / "lost-response.json").write_text(json.dumps({"reserved": reserved, "owned": owned,
            "daemon": real, "cleaned": cleaned, "never_started": True}, indent=2), encoding="utf-8")
        print("GVISOR_REAL_LOST_CREATE_RESPONSE_READONLY_RECONCILIATION_OK")
    finally:
        if cid:
            remove_owned(cid)


if __name__ == "__main__":
    main()
