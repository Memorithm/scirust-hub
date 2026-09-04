#!/usr/bin/env python3
"""Executed evaluator for Forge's SOUP post-training search domain.

Forge owns candidate search. SOUP owns training/evaluation semantics. This
adapter materializes one contract-supplied candidate, uses ``soup train
--dry-run`` as the verify gate, and only reports metrics produced by real SOUP
training/evaluation or a monotonic wall-clock measurement around those runs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Any, Sequence

SCHEMA_VERSION = 1
SOUP_REPOSITORY = "MakazhanAlpamys/Soup"
SOUP_QUALIFIED_COMMIT = "05b646523727925990530667e7012ede50bd30b2"
DATASET_TOKEN = "${SOUP_HUB_DATASET}"
OUTPUT_TOKEN = "${SOUP_HUB_OUTPUT}"
CANDIDATE_TOKEN_PREFIX = "${FORGE_SOUP:"
MAX_CONFIG_BYTES = 1024 * 1024
MAX_REQUEST_BYTES = 1024 * 1024
MAX_LOG_EXCERPT = 4096
MAX_DETAILS_BYTES = 1024 * 1024
NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")
VALUE_RE = re.compile(r"^[A-Za-z0-9_./:+-]{1,256}$")
BENCHMARK_RE = re.compile(r"^[A-Za-z0-9_.:+-]{1,128}$")
DEVICE_RE = re.compile(r"^(?:cpu|mps|cuda(?::[0-9]+)?)$")
TIMING_OBJECTIVES = {"train_wall_ms", "eval_wall_ms", "total_wall_ms"}


def _regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file")


def _regular_directory(path: Path, label: str, *, create: bool = False) -> None:
    if create:
        path.mkdir(parents=True, exist_ok=True)
    if path.is_symlink() or not path.is_dir():
        raise ValueError(f"{label} must be a regular non-symlink directory")


def _parse_params(raw: str) -> dict[str, Any]:
    try:
        params = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid parameter JSON: {exc}") from exc
    if not isinstance(params, dict):
        raise ValueError("parameters must be a JSON object")
    allowed = {"gpus", "trust_remote_code", "fewshot", "batch_size", "device"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise ValueError(f"unsupported parameters: {', '.join(unknown)}")

    gpus = params.get("gpus")
    if gpus is not None and gpus != "auto":
        if not isinstance(gpus, int) or isinstance(gpus, bool) or not 1 <= gpus <= 64:
            raise ValueError("gpus must be 'auto' or an integer in [1, 64]")
    trust_remote_code = params.get("trust_remote_code", False)
    if not isinstance(trust_remote_code, bool):
        raise ValueError("trust_remote_code must be boolean")
    fewshot = params.get("fewshot")
    if fewshot is not None:
        if not isinstance(fewshot, int) or isinstance(fewshot, bool) or not 0 <= fewshot <= 100:
            raise ValueError("fewshot must be an integer in [0, 100]")
    batch_size = params.get("batch_size", 8)
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or not 1 <= batch_size <= 4096:
        raise ValueError("batch_size must be an integer in [1, 4096]")
    device = params.get("device")
    if device is not None and (not isinstance(device, str) or not DEVICE_RE.fullmatch(device)):
        raise ValueError("device must be cpu, mps, cuda, or cuda:<index>")
    return params


def _load_json(path: Path, label: str, max_bytes: int = MAX_REQUEST_BYTES) -> dict[str, Any]:
    _regular_file(path, label)
    if path.stat().st_size > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} bytes")
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid {label} JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a JSON object")
    return value


def _load_request() -> dict[str, Any]:
    payload = sys.stdin.buffer.read(MAX_REQUEST_BYTES + 1)
    if len(payload) > MAX_REQUEST_BYTES:
        raise ValueError(f"Forge evaluator request exceeds {MAX_REQUEST_BYTES} bytes")
    try:
        request = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Forge evaluator request JSON: {exc}") from exc
    if not isinstance(request, dict):
        raise ValueError("Forge evaluator request must be a JSON object")
    required = {
        "schema_version",
        "phase",
        "domain_id",
        "candidate_id",
        "candidate",
        "generation",
        "trial_seed",
    }
    if set(request) != required:
        raise ValueError("Forge evaluator request fields do not match schema v1")
    if request["schema_version"] != SCHEMA_VERSION:
        raise ValueError("unsupported Forge evaluator schema_version")
    if request["phase"] not in {"verify", "measure"}:
        raise ValueError("Forge evaluator phase must be verify or measure")
    for key in ("candidate_id", "generation", "trial_seed"):
        value = request[key]
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError(f"Forge evaluator {key} must be a non-negative integer")
    candidate = request["candidate"]
    if not isinstance(candidate, dict) or set(candidate) != {"values"}:
        raise ValueError("Forge evaluator candidate must contain only values")
    values = candidate["values"]
    if not isinstance(values, dict) or not values:
        raise ValueError("Forge evaluator candidate.values must be a non-empty object")
    for name, value in values.items():
        if not isinstance(name, str) or not NAME_RE.fullmatch(name):
            raise ValueError(f"unsupported candidate dimension name {name!r}")
        if not isinstance(value, str) or not VALUE_RE.fullmatch(value):
            raise ValueError(f"unsupported candidate value for {name!r}")
    return request


def _campaign_contract(campaign: dict[str, Any], request: dict[str, Any]) -> tuple[list[str], list[str]]:
    if campaign.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("unsupported Forge SOUP campaign schema_version")
    external = campaign.get("external_domain")
    if not isinstance(external, dict):
        raise ValueError("campaign.external_domain must be an object")
    upstream = external.get("upstream")
    if not isinstance(upstream, dict):
        raise ValueError("campaign.external_domain.upstream must be an object")
    if upstream.get("repository") != SOUP_REPOSITORY:
        raise ValueError("Hub Forge/SOUP edge only accepts MakazhanAlpamys/Soup")
    if upstream.get("commit_id") != SOUP_QUALIFIED_COMMIT:
        raise ValueError("Hub Forge/SOUP edge requires the qualified SOUP commit")
    domain_id = external.get("domain_id")
    if domain_id != request["domain_id"]:
        raise ValueError("request domain_id does not match campaign")

    dimensions = campaign.get("dimensions")
    if not isinstance(dimensions, dict) or not dimensions:
        raise ValueError("campaign.dimensions must be a non-empty object")
    if set(dimensions) != set(request["candidate"]["values"]):
        raise ValueError("candidate dimensions do not match campaign dimensions")

    objective_rows = external.get("objectives")
    if not isinstance(objective_rows, list) or not objective_rows:
        raise ValueError("campaign must declare objectives")
    objectives: list[str] = []
    benchmarks: list[str] = []
    for row in objective_rows:
        if not isinstance(row, dict) or set(row) != {"name", "direction"}:
            raise ValueError("campaign objective shape does not match v1")
        name = row["name"]
        if not isinstance(name, str):
            raise ValueError("campaign objective name must be a string")
        if name in TIMING_OBJECTIVES:
            objectives.append(name)
            continue
        if name.startswith("benchmark:"):
            benchmark = name.removeprefix("benchmark:")
            if not BENCHMARK_RE.fullmatch(benchmark):
                raise ValueError(f"unsupported benchmark objective {name!r}")
            objectives.append(name)
            benchmarks.append(benchmark)
            continue
        raise ValueError(
            f"unsupported Forge/SOUP objective {name!r}; use benchmark:<task> or a measured wall-time objective"
        )
    if len(objectives) != len(set(objectives)):
        raise ValueError("campaign objective names must be unique")
    return objectives, benchmarks


def _materialize_config(
    template_path: Path,
    dataset: Path,
    output_dir: Path,
    candidate_values: dict[str, str],
    destination: Path,
) -> str:
    _regular_file(template_path, "SOUP config template")
    _regular_file(dataset, "SOUP dataset")
    if template_path.stat().st_size > MAX_CONFIG_BYTES:
        raise ValueError(f"SOUP config template exceeds {MAX_CONFIG_BYTES} bytes")
    text = template_path.read_text(encoding="utf-8")
    if DATASET_TOKEN not in text or OUTPUT_TOKEN not in text:
        raise ValueError(f"SOUP config must contain {DATASET_TOKEN} and {OUTPUT_TOKEN}")
    text = text.replace(DATASET_TOKEN, str(dataset)).replace(OUTPUT_TOKEN, str(output_dir))
    for name, value in candidate_values.items():
        token = f"{CANDIDATE_TOKEN_PREFIX}{name}}}"
        if token not in text:
            raise ValueError(f"SOUP config is missing candidate token {token}")
        text = text.replace(token, value)
    if CANDIDATE_TOKEN_PREFIX in text:
        raise ValueError("SOUP config contains unresolved Forge candidate tokens")
    destination.write_text(text, encoding="utf-8")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _train_command(soup_bin: str, config: Path, params: dict[str, Any], *, dry_run: bool) -> list[str]:
    command = [soup_bin, "train", "--config", str(config), "--yes"]
    if dry_run:
        command.append("--dry-run")
    gpus = params.get("gpus")
    if gpus is not None:
        command += ["--gpus", str(gpus)]
    if params.get("trust_remote_code", False):
        command.append("--trust-remote-code")
    return command


def _benchmark_command(
    soup_bin: str,
    model_dir: Path,
    benchmarks: Sequence[str],
    params: dict[str, Any],
) -> list[str]:
    command = [
        soup_bin,
        "eval",
        "benchmark",
        "--model",
        str(model_dir),
        "--benchmarks",
        ",".join(benchmarks),
        "--batch-size",
        str(params.get("batch_size", 8)),
    ]
    if params.get("fewshot") is not None:
        command += ["--fewshot", str(params["fewshot"])]
    if params.get("device") is not None:
        command += ["--device", params["device"]]
    return command


def _run_logged(command: Sequence[str], cwd: Path, env: dict[str, str], log_dir: Path, role: str) -> tuple[int, int, dict[str, Any]]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / f"{role}.stdout.log"
    stderr_path = log_dir / f"{role}.stderr.log"
    started = time.monotonic_ns()
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        completed = subprocess.run(
            list(command),
            cwd=cwd,
            env=env,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            shell=False,
            check=False,
        )
    elapsed_ns = time.monotonic_ns() - started
    return completed.returncode, elapsed_ns, {
        "stdout": _log_record(stdout_path),
        "stderr": _log_record(stderr_path),
    }


def _log_record(path: Path) -> dict[str, Any]:
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    with path.open("rb") as handle:
        excerpt = handle.read(MAX_LOG_EXCERPT).decode("utf-8", errors="replace")
    return {"bytes": size, "sha256": digest.hexdigest(), "excerpt": excerpt}


def _soup_version(soup_bin: str) -> str:
    try:
        completed = subprocess.run(
            [soup_bin, "--version"],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=False,
            check=False,
            timeout=10,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        return f"unavailable:{type(exc).__name__}"
    text = completed.stdout[:4096].decode("utf-8", errors="replace").strip()
    return f"exit={completed.returncode};{text}"


def _optional_command(argv: Sequence[str]) -> str | None:
    try:
        completed = subprocess.run(
            list(argv),
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=False,
            check=False,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout[:8192].decode("utf-8", errors="replace").strip()


def _environment(soup_bin: str, params: dict[str, Any]) -> tuple[dict[str, Any], str]:
    device_model = None
    model_path = Path("/proc/device-tree/model")
    if model_path.is_file():
        try:
            device_model = model_path.read_bytes()[:4096].replace(b"\x00", b"").decode("utf-8", errors="replace")
        except OSError:
            device_model = None
    payload = {
        "system": platform.system(),
        "release": platform.release(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "soup_version": _soup_version(soup_bin),
        "device_parameter": params.get("device"),
        "cuda_visible_devices": os.environ.get("CUDA_VISIBLE_DEVICES"),
        "device_tree_model": device_model,
        "nvidia_smi": _optional_command(
            [
                "nvidia-smi",
                "--query-gpu=name,driver_version,memory.total",
                "--format=csv,noheader,nounits",
            ]
        ),
    }
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return payload, f"sha256:{hashlib.sha256(canonical).hexdigest()}"


def _benchmark_scores(db_path: Path, benchmarks: Sequence[str]) -> tuple[dict[str, float], dict[str, Any]]:
    if not db_path.is_file():
        raise RuntimeError("SOUP benchmark produced no isolated experiment database")
    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute(
            "SELECT benchmark, score, details_json FROM eval_results ORDER BY id"
        ).fetchall()
    finally:
        conn.close()
    expected = list(benchmarks)
    found: dict[str, tuple[float, Any]] = {}
    for benchmark, score, details_json in rows:
        if benchmark in found:
            raise RuntimeError(f"SOUP benchmark produced duplicate score for {benchmark}")
        if benchmark not in expected:
            raise RuntimeError(f"SOUP benchmark produced unexpected score for {benchmark}")
        try:
            numeric = float(score)
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"SOUP benchmark score for {benchmark} is not numeric") from exc
        if not (numeric == numeric and abs(numeric) != float("inf")):
            raise RuntimeError(f"SOUP benchmark score for {benchmark} is not finite")
        details: Any = None
        if details_json:
            if len(details_json.encode("utf-8")) > MAX_DETAILS_BYTES:
                raise RuntimeError(f"SOUP benchmark details for {benchmark} exceed the evidence bound")
            try:
                details = json.loads(details_json)
            except json.JSONDecodeError as exc:
                raise RuntimeError(f"SOUP benchmark details for {benchmark} are invalid JSON") from exc
        found[str(benchmark)] = (numeric, details)
    if set(found) != set(expected):
        missing = sorted(set(expected) - set(found))
        raise RuntimeError(f"SOUP benchmark omitted scores for: {', '.join(missing)}")
    return (
        {f"benchmark:{name}": found[name][0] for name in expected},
        {name: found[name][1] for name in expected},
    )


def _write_evidence(evidence_dir: Path, payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    evidence_id = hashlib.sha256(canonical).hexdigest()
    final = dict(payload)
    final["evidence_id"] = evidence_id
    filename = (
        f"candidate-{payload['candidate_id']}-trial-{payload['trial_seed']}-"
        f"{payload['phase']}-{evidence_id}.json"
    )
    path = evidence_dir / filename
    with path.open("x", encoding="utf-8") as handle:
        json.dump(final, handle, sort_keys=True, indent=2)
        handle.write("\n")
    return evidence_id


def evaluate(args: argparse.Namespace) -> dict[str, Any]:
    campaign_path = Path(args.campaign)
    config_path = Path(args.config)
    dataset_path = Path(args.dataset)
    evidence_dir = Path(args.evidence_dir)
    _regular_file(campaign_path, "campaign")
    _regular_file(config_path, "SOUP config template")
    _regular_file(dataset_path, "SOUP dataset")
    _regular_directory(evidence_dir, "evidence directory", create=True)
    params = _parse_params(args.params)
    request = _load_request()
    campaign = _load_json(campaign_path, "campaign")
    objectives, benchmarks = _campaign_contract(campaign, request)
    environment, fingerprint = _environment(args.soup_bin, params)

    with tempfile.TemporaryDirectory(prefix="forge-soup-eval-", dir=str(evidence_dir.parent)) as raw_tmp:
        workdir = Path(raw_tmp)
        output_dir = workdir / "model-output"
        staged = workdir / "soup.yaml"
        config_sha256 = _materialize_config(
            config_path,
            dataset_path,
            output_dir,
            request["candidate"]["values"],
            staged,
        )
        env = dict(os.environ)
        logs: dict[str, Any] = {}
        metrics: dict[str, float] = {}
        details: dict[str, Any] = {}
        total_started = time.monotonic_ns()

        if request["phase"] == "verify":
            returncode, elapsed_ns, train_logs = _run_logged(
                _train_command(args.soup_bin, staged, params, dry_run=True),
                workdir,
                env,
                workdir / "logs",
                "verify",
            )
            logs["verify"] = train_logs
            passed = returncode == 0
            evidence = {
                "schema_version": SCHEMA_VERSION,
                "phase": "verify",
                "candidate_id": request["candidate_id"],
                "trial_seed": request["trial_seed"],
                "generation": request["generation"],
                "domain_id": request["domain_id"],
                "candidate": request["candidate"],
                "config_sha256": config_sha256,
                "dataset_bytes": dataset_path.stat().st_size,
                "passed": passed,
                "returncode": returncode,
                "elapsed_ns": elapsed_ns,
                "environment": environment,
                "environment_fingerprint": fingerprint,
                "logs": logs,
            }
            evidence_id = _write_evidence(evidence_dir, evidence)
            return {
                "schema_version": SCHEMA_VERSION,
                "candidate_id": request["candidate_id"],
                "trial_seed": request["trial_seed"],
                "passed": passed,
                "evidence_id": evidence_id,
                "environment_fingerprint": fingerprint,
            }

        train_rc, train_ns, train_logs = _run_logged(
            _train_command(args.soup_bin, staged, params, dry_run=False),
            workdir,
            env,
            workdir / "logs",
            "train",
        )
        logs["train"] = train_logs
        if train_rc != 0:
            raise RuntimeError(f"SOUP train failed with exit {train_rc}")
        if output_dir.is_symlink() or not output_dir.is_dir():
            raise RuntimeError("SOUP train produced no regular model output directory")
        if "train_wall_ms" in objectives:
            metrics["train_wall_ms"] = train_ns / 1_000_000.0

        eval_ns = 0
        if benchmarks:
            db_path = workdir / "experiments.db"
            eval_env = dict(env)
            eval_env["SOUP_DB_PATH"] = str(db_path)
            eval_rc, eval_ns, eval_logs = _run_logged(
                _benchmark_command(args.soup_bin, output_dir, benchmarks, params),
                workdir,
                eval_env,
                workdir / "logs",
                "benchmark",
            )
            logs["benchmark"] = eval_logs
            if eval_rc != 0:
                raise RuntimeError(f"SOUP eval benchmark failed with exit {eval_rc}")
            benchmark_metrics, benchmark_details = _benchmark_scores(db_path, benchmarks)
            metrics.update(benchmark_metrics)
            details["benchmarks"] = benchmark_details
        if "eval_wall_ms" in objectives:
            if not benchmarks:
                raise ValueError("eval_wall_ms requires at least one benchmark:<task> objective")
            metrics["eval_wall_ms"] = eval_ns / 1_000_000.0
        if "total_wall_ms" in objectives:
            metrics["total_wall_ms"] = (time.monotonic_ns() - total_started) / 1_000_000.0
        if set(metrics) != set(objectives):
            raise RuntimeError("executed metric set does not exactly match campaign objectives")

        evidence = {
            "schema_version": SCHEMA_VERSION,
            "phase": "measure",
            "candidate_id": request["candidate_id"],
            "trial_seed": request["trial_seed"],
            "generation": request["generation"],
            "domain_id": request["domain_id"],
            "candidate": request["candidate"],
            "config_sha256": config_sha256,
            "dataset_bytes": dataset_path.stat().st_size,
            "metrics": metrics,
            "details": details,
            "environment": environment,
            "environment_fingerprint": fingerprint,
            "logs": logs,
        }
        evidence_id = _write_evidence(evidence_dir, evidence)
        return {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": request["candidate_id"],
            "trial_seed": request["trial_seed"],
            "evidence_id": evidence_id,
            "environment_fingerprint": fingerprint,
            "metrics": metrics,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--evidence-dir", required=True)
    parser.add_argument("--params", default="{}")
    parser.add_argument("--soup-bin", default="soup")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        response = evaluate(build_parser().parse_args(argv))
    except Exception as exc:  # process boundary: fail closed with concise stderr
        print(f"forge-soup evaluator: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    sys.stdout.write(json.dumps(response, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
