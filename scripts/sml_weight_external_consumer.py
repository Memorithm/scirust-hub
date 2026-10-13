#!/usr/bin/env python3
"""SciRust Hub consumer for independently admitted SML weight publications."""

from __future__ import annotations

import argparse
import json
import pathlib
import subprocess
import sys

MAX_RECEIPT_BYTES = 262_144


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
    command = [
        str(executable),
        str(dossier),
        run_id,
        str(owner),
        str(policy),
        str(allowed_signers),
    ]
    completed = subprocess.run(
        command,
        check=False,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=45,
        env={"PATH": "/usr/bin:/bin"},
    )
    if (
        completed.returncode != 0
        or len(completed.stdout) > MAX_RECEIPT_BYTES
        or len(completed.stderr) > MAX_RECEIPT_BYTES
    ):
        raise ValueError("SML owner admission failed or exceeded output bounds")
    receipt = json.loads(completed.stdout)
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
    return receipt


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
