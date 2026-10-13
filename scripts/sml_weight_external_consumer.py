#!/usr/bin/env python3
"""SciRust Hub consumer for independently admitted SML weight publications."""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import selectors
import subprocess
import sys
import time

MAX_RECEIPT_BYTES = 262_144
OWNER_TIMEOUT_SECONDS = 45
RECEIPT_SCHEMA_V1 = "scirust-hub.sml-weight-admission-receipt/v1"


def _run_bounded(command: list[str]) -> tuple[bytes, bytes]:
    process = subprocess.Popen(
        command,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        bufsize=0,
        env={"PATH": "/usr/bin:/bin"},
    )
    selector = selectors.DefaultSelector()
    assert process.stdout is not None
    assert process.stderr is not None
    selector.register(process.stdout, selectors.EVENT_READ, "stdout")
    selector.register(process.stderr, selectors.EVENT_READ, "stderr")
    captured = {"stdout": bytearray(), "stderr": bytearray()}
    failure: str | None = None
    deadline = time.monotonic() + OWNER_TIMEOUT_SECONDS

    try:
        while selector.get_map():
            if failure is None and time.monotonic() >= deadline:
                failure = "timed out"
                process.kill()
            events = selector.select(0.1)
            for key, _ in events:
                chunk = os.read(key.fileobj.fileno(), 65_536)
                if not chunk:
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
                    continue
                output = captured[key.data]
                remaining = MAX_RECEIPT_BYTES - len(output)
                output.extend(chunk[: max(0, remaining)])
                if len(chunk) > remaining and failure is None:
                    failure = "exceeded output bounds"
                    process.kill()
        returncode = process.wait()
    finally:
        selector.close()
        if process.poll() is None:
            process.kill()
            process.wait()

    if failure is not None or returncode != 0:
        raise ValueError(f"SML owner admission failed: {failure or 'nonzero exit'}")
    return bytes(captured["stdout"]), bytes(captured["stderr"])


def consume(
    executable: pathlib.Path,
    dossier: pathlib.Path,
    run_id: str,
    owner: pathlib.Path,
    policy: pathlib.Path,
    allowed_signers: pathlib.Path,
    expected_code_sha: str,
    expected_publication_id: str,
) -> dict[str, object]:
    if len(expected_code_sha) != 40 or any(
        character not in "0123456789abcdef" for character in expected_code_sha
    ):
        raise ValueError("expected code SHA must be a full lowercase commit identifier")
    command = [
        str(executable),
        str(dossier),
        run_id,
        str(owner),
        str(policy),
        str(allowed_signers),
    ]
    stdout, _stderr = _run_bounded(command)
    receipt = json.loads(stdout)
    required = {
        "repository",
        "code_sha",
        "publication_id",
        "publisher",
        "run_id",
        "bundle_sha256",
        "manifest_sha256",
        "artifacts",
        "artifact_bytes",
        "scientific_qualification",
        "merge_authorization",
    }
    if set(receipt) != required:
        raise ValueError("unexpected SML admission receipt fields")
    if (
        receipt["repository"] != "Memorithm/SML-GENIUS"
        or receipt["code_sha"] != expected_code_sha
        or receipt["publication_id"] != expected_publication_id
        or receipt["publisher"] != "memorithm-sml-production"
        or receipt["run_id"] != run_id
        or receipt["scientific_qualification"] != "not_evaluated"
        or receipt["merge_authorization"] is not False
        or not isinstance(receipt["artifacts"], int)
        or receipt["artifacts"] < 1
        or not isinstance(receipt["artifact_bytes"], int)
        or receipt["artifact_bytes"] < 1
    ):
        raise ValueError("SML admission receipt violates the external consumer policy")
    for field in ("bundle_sha256", "manifest_sha256"):
        value = receipt[field]
        if (
            not isinstance(value, str)
            or len(value) != 64
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError(f"invalid {field}")
    return {"schema": RECEIPT_SCHEMA_V1, **receipt}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--executable", required=True, type=pathlib.Path)
    parser.add_argument("--dossier", required=True, type=pathlib.Path)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--owner", required=True, type=pathlib.Path)
    parser.add_argument("--policy", required=True, type=pathlib.Path)
    parser.add_argument("--allowed-signers", required=True, type=pathlib.Path)
    parser.add_argument("--expected-code-sha", required=True)
    parser.add_argument("--expected-publication-id", required=True)
    args = parser.parse_args()
    receipt = consume(
        args.executable,
        args.dossier,
        args.run_id,
        args.owner,
        args.policy,
        args.allowed_signers,
        args.expected_code_sha,
        args.expected_publication_id,
    )
    json.dump(receipt, sys.stdout, sort_keys=True, separators=(",", ":"))
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
