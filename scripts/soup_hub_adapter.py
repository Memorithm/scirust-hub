#!/usr/bin/env python3
"""Process adapter for versioned SOUP capabilities exposed through SciRust Hub.

The adapter owns process-contract translation, deterministic bundle handling,
and boundary validation. It deliberately does not implement SOUP training,
evaluation, scoring, model loading, or export semantics.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from typing import Any, Iterable, Sequence

SOUP_EXIT_SHIP = 0
SOUP_EXIT_RUNTIME_ERROR = 1
SOUP_EXIT_DONT_SHIP = 2
SOUP_EXIT_USAGE_ERROR = 3

BUNDLE_PREFIX = "artifact"
BUNDLE_SCHEMA = "application/vnd.scirust-hub.soup-bundle.v1+tar"
DATASET_TOKEN = "${SOUP_HUB_DATASET}"
OUTPUT_TOKEN = "${SOUP_HUB_OUTPUT}"
MAX_CONFIG_BYTES = 1024 * 1024
MAX_BUNDLE_MEMBERS = 100_000
MAX_EXTRACTED_BYTES = 64 * 1024 * 1024 * 1024
MAX_CAPTURE_TEXT = 1024 * 1024

MODEL_SENTINELS = (
    "adapter_config.json",
    "config.json",
    "mole_manifest.json",
    "tokenizer_config.json",
)
EXPORT_FORMATS = {
    "gguf",
    "onnx",
    "tensorrt",
    "awq",
    "gptq",
    "bitnet",
    "tq1_0",
    "torchao",
    "gguf-ud",
}
DEVICE_RE = re.compile(r"^(?:cpu|mps|cuda(?::[0-9]+)?)$")
BENCHMARK_RE = re.compile(r"^[A-Za-z0-9_.:+-]+(?:,[A-Za-z0-9_.:+-]+)*$")


def classify_ship_exit(returncode: int, verdict_exists: bool) -> tuple[bool, str]:
    """Translate SOUP's verdict-oriented exit taxonomy into Hub process status."""
    if returncode in (SOUP_EXIT_SHIP, SOUP_EXIT_DONT_SHIP):
        if verdict_exists:
            return True, ""
        return False, "SOUP returned a semantic verdict exit but produced no verdict artifact"
    if returncode == SOUP_EXIT_RUNTIME_ERROR:
        return False, "SOUP reported a runtime error (exit 1)"
    if returncode == SOUP_EXIT_USAGE_ERROR:
        return False, "SOUP rejected the request (exit 3)"
    if returncode < 0:
        return False, f"SOUP terminated by signal {-returncode}"
    return False, f"SOUP exited with unsupported status {returncode}"


def _require_regular_input(path: Path, label: str) -> None:
    if path.is_symlink():
        raise ValueError(f"{label} {path!s} must not be a symbolic link")
    if not path.is_file():
        raise ValueError(f"{label} {path!s} must be a regular file")


def _require_directory(path: Path, label: str) -> None:
    if path.is_symlink():
        raise ValueError(f"{label} {path!s} must not be a symbolic link")
    if not path.is_dir():
        raise ValueError(f"{label} {path!s} must be a directory")


def _verdict_is_regular_file(path: Path) -> bool:
    return not path.is_symlink() and path.is_file()


def _is_under(root: Path, path: Path) -> bool:
    root = root.resolve()
    try:
        path.resolve().relative_to(root)
    except ValueError:
        return False
    return True


def _require_under(root: Path, path: Path, label: str) -> None:
    if not _is_under(root, path):
        raise ValueError(f"{label} must stay inside the Hub run directory")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.is_symlink():
        raise ValueError(f"refusing symbolic-link output {path!s}")
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _parse_params(raw: str, allowed: set[str]) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Hub parameter JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ValueError("Hub parameters must be a JSON object")
    unknown = sorted(set(parsed) - allowed)
    if unknown:
        raise ValueError(f"unsupported parameters: {', '.join(unknown)}")
    return parsed


def _safe_relative_subpath(raw: str, label: str) -> PurePosixPath:
    if not raw or "\x00" in raw or "\\" in raw:
        raise ValueError(f"{label} must be a non-empty forward-slash relative path")
    path = PurePosixPath(raw)
    if path.is_absolute() or any(part in ("", ".", "..") for part in path.parts):
        raise ValueError(f"{label} must be a normalized relative path")
    return path


def _iter_bundle_entries(root: Path) -> Iterable[tuple[Path, PurePosixPath, bool]]:
    """Yield regular files/directories in deterministic order and reject links."""

    def visit(current: Path, relative: PurePosixPath) -> Iterable[tuple[Path, PurePosixPath, bool]]:
        with os.scandir(current) as scan:
            entries = sorted(scan, key=lambda entry: entry.name)
        for entry in entries:
            path = Path(entry.path)
            rel = relative / entry.name
            if entry.is_symlink():
                raise ValueError(f"bundle source contains symbolic link: {rel.as_posix()}")
            if entry.is_dir(follow_symlinks=False):
                yield path, rel, True
                yield from visit(path, rel)
            elif entry.is_file(follow_symlinks=False):
                yield path, rel, False
            else:
                raise ValueError(f"bundle source contains non-regular entry: {rel.as_posix()}")

    yield from visit(root, PurePosixPath())


def create_deterministic_bundle(source: Path, bundle: Path) -> tuple[int, int]:
    """Create a byte-reproducible uncompressed tar from one regular directory."""
    _require_directory(source, "bundle source")
    bundle.parent.mkdir(parents=True, exist_ok=True)
    if bundle.is_symlink():
        raise ValueError(f"bundle output {bundle!s} must not be a symbolic link")

    entries = list(_iter_bundle_entries(source))
    if len(entries) > MAX_BUNDLE_MEMBERS:
        raise ValueError(f"bundle contains more than {MAX_BUNDLE_MEMBERS} entries")

    total_bytes = 0
    with tarfile.open(bundle, mode="w", format=tarfile.PAX_FORMAT) as archive:
        root_info = tarfile.TarInfo(BUNDLE_PREFIX)
        root_info.type = tarfile.DIRTYPE
        root_info.mode = 0o755
        root_info.uid = root_info.gid = 0
        root_info.uname = root_info.gname = ""
        root_info.mtime = 0
        archive.addfile(root_info)

        for path, relative, is_dir in entries:
            arcname = f"{BUNDLE_PREFIX}/{relative.as_posix()}"
            info = tarfile.TarInfo(arcname)
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            if is_dir:
                info.type = tarfile.DIRTYPE
                info.mode = 0o755
                archive.addfile(info)
                continue
            stat = path.stat(follow_symlinks=False)
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"bundle source changed type during capture: {relative.as_posix()}")
            info.type = tarfile.REGTYPE
            info.mode = 0o644
            info.size = stat.st_size
            total_bytes += stat.st_size
            if total_bytes > MAX_EXTRACTED_BYTES:
                raise ValueError("bundle payload exceeds the Hub ML artifact budget")
            with path.open("rb") as handle:
                archive.addfile(info, handle)
    return len(entries), total_bytes


def extract_deterministic_bundle(bundle: Path, destination: Path) -> Path:
    """Extract a SOUP bundle without path traversal, links, devices, or overwrite."""
    _require_regular_input(bundle, "bundle")
    destination.mkdir(parents=True, exist_ok=False)
    seen: set[str] = set()
    total = 0
    count = 0

    with tarfile.open(bundle, mode="r:") as archive:
        for member in archive:
            count += 1
            if count > MAX_BUNDLE_MEMBERS:
                raise ValueError(f"bundle contains more than {MAX_BUNDLE_MEMBERS} entries")
            path = PurePosixPath(member.name)
            if path.is_absolute() or not path.parts or path.parts[0] != BUNDLE_PREFIX:
                raise ValueError(f"bundle member has invalid root: {member.name!r}")
            if any(part in ("", ".", "..") for part in path.parts):
                raise ValueError(f"bundle member has unsafe path: {member.name!r}")
            if member.issym() or member.islnk() or member.isdev() or member.isfifo():
                raise ValueError(f"bundle member type is forbidden: {member.name!r}")
            key = path.as_posix()
            if key in seen:
                raise ValueError(f"bundle contains duplicate member: {member.name!r}")
            seen.add(key)
            relative_parts = path.parts[1:]
            target = destination.joinpath(*relative_parts)
            _require_under(destination, target, "bundle extraction")
            if member.isdir():
                target.mkdir(parents=True, exist_ok=True)
                continue
            if not member.isfile():
                raise ValueError(f"bundle member is neither file nor directory: {member.name!r}")
            total += member.size
            if total > MAX_EXTRACTED_BYTES:
                raise ValueError("extracted bundle exceeds the Hub ML artifact budget")
            target.parent.mkdir(parents=True, exist_ok=True)
            if target.exists() or target.is_symlink():
                raise ValueError(f"bundle extraction would overwrite {target!s}")
            source = archive.extractfile(member)
            if source is None:
                raise ValueError(f"cannot read bundle member: {member.name!r}")
            with source, target.open("xb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)
    return destination


def _resolve_model_root(extracted: Path, model_subpath: str | None) -> Path:
    if model_subpath:
        relative = _safe_relative_subpath(model_subpath, "model_subpath")
        candidate = extracted.joinpath(*relative.parts)
        _require_directory(candidate, "model_subpath")
        return candidate

    candidates: list[Path] = []
    for directory in [extracted, *sorted((p for p in extracted.rglob("*") if p.is_dir()))]:
        if directory.is_symlink():
            raise ValueError(f"extracted model tree contains symbolic link: {directory!s}")
        if any((directory / sentinel).is_file() for sentinel in MODEL_SENTINELS):
            candidates.append(directory)
    non_checkpoints = [
        candidate
        for candidate in candidates
        if not any(part.startswith("checkpoint-") for part in candidate.relative_to(extracted).parts)
    ]
    selected = non_checkpoints or candidates
    if len(selected) != 1:
        rendered = ", ".join(str(path.relative_to(extracted)) for path in selected[:8])
        raise ValueError(
            "could not resolve one model root from bundle; set model_subpath explicitly"
            + (f" (candidates: {rendered})" if rendered else "")
        )
    return selected[0]


def _read_capture(path: Path) -> tuple[str, bool]:
    data = path.read_bytes()
    truncated = len(data) > MAX_CAPTURE_TEXT
    return data[:MAX_CAPTURE_TEXT].decode("utf-8", errors="replace"), truncated


def _run_logged(argv: list[str], *, cwd: Path, log_dir: Path) -> tuple[int, str, str, bool]:
    log_dir.mkdir(parents=True, exist_ok=True)
    stdout_path = log_dir / "stdout.log"
    stderr_path = log_dir / "stderr.log"
    with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
        completed = subprocess.run(
            argv,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=stdout,
            stderr=stderr,
            check=False,
            shell=False,
        )
    stdout_text, stdout_truncated = _read_capture(stdout_path)
    stderr_text, stderr_truncated = _read_capture(stderr_path)
    return (
        completed.returncode,
        stdout_text,
        stderr_text,
        stdout_truncated or stderr_truncated,
    )


def _require_success(returncode: int, operation: str) -> None:
    if returncode == 0:
        return
    if returncode < 0:
        raise RuntimeError(f"SOUP {operation} terminated by signal {-returncode}")
    raise RuntimeError(f"SOUP {operation} failed with exit {returncode}")


def run_ship_offline(*, soup_bin: str, evidence: Path, verdict: Path) -> None:
    """Replay immutable evidence and preserve both SHIP and DON'T-SHIP verdicts."""
    _require_regular_input(evidence, "evidence")
    verdict.parent.mkdir(parents=True, exist_ok=True)

    completed = subprocess.run(
        [
            soup_bin,
            "ship",
            "--evidence",
            str(evidence),
            "--output",
            str(verdict),
        ],
        stdin=subprocess.DEVNULL,
        check=False,
        shell=False,
    )
    ok, message = classify_ship_exit(completed.returncode, _verdict_is_regular_file(verdict))
    if not ok:
        raise RuntimeError(message)


def run_train(
    *,
    soup_bin: str,
    config: Path,
    dataset: Path,
    bundle: Path,
    report: Path,
    params_raw: str,
) -> None:
    """Run SOUP training against immutable Hub inputs and bundle the exact output tree."""
    _require_regular_input(config, "config")
    _require_regular_input(dataset, "dataset")
    workdir = Path.cwd().resolve()
    _require_under(workdir, bundle, "bundle output")
    _require_under(workdir, report, "report output")

    if config.stat().st_size > MAX_CONFIG_BYTES:
        raise ValueError(f"SOUP config exceeds {MAX_CONFIG_BYTES} bytes")
    template = config.read_text(encoding="utf-8")
    if DATASET_TOKEN not in template:
        raise ValueError(f"SOUP config must contain {DATASET_TOKEN}")
    if OUTPUT_TOKEN not in template:
        raise ValueError(f"SOUP config must contain {OUTPUT_TOKEN}")

    hub_dir = workdir / ".hub-soup"
    hub_dir.mkdir(parents=True, exist_ok=True)
    model_output = workdir / "soup-output"
    if model_output.exists() or model_output.is_symlink():
        raise ValueError("soup-output must not exist before training")
    staged = hub_dir / "soup.yaml"
    staged.write_text(
        template.replace(DATASET_TOKEN, str(dataset)).replace(OUTPUT_TOKEN, str(model_output)),
        encoding="utf-8",
    )

    params = _parse_params(params_raw, {"gpus", "trust_remote_code"})
    command = [soup_bin, "train", "--config", str(staged), "--yes"]
    gpus = params.get("gpus")
    if gpus is not None:
        if gpus != "auto" and (not isinstance(gpus, int) or isinstance(gpus, bool) or not 1 <= gpus <= 64):
            raise ValueError("gpus must be 'auto' or an integer in [1, 64]")
        command += ["--gpus", str(gpus)]
    trust_remote_code = params.get("trust_remote_code", False)
    if not isinstance(trust_remote_code, bool):
        raise ValueError("trust_remote_code must be boolean")
    if trust_remote_code:
        command.append("--trust-remote-code")

    returncode, stdout, stderr, truncated = _run_logged(command, cwd=workdir, log_dir=hub_dir / "train-logs")
    _require_success(returncode, "train")
    _require_directory(model_output, "SOUP training output")
    entry_count, payload_bytes = create_deterministic_bundle(model_output, bundle)
    _write_json(
        report,
        {
            "schema_version": 1,
            "operation": "train",
            "bundle_media_type": BUNDLE_SCHEMA,
            "bundle_entries": entry_count,
            "payload_bytes": payload_bytes,
            "parameters": params,
            "stdout": stdout,
            "stderr": stderr,
            "logs_truncated": truncated,
        },
    )


def run_eval(
    *,
    soup_bin: str,
    bundle: Path,
    result: Path,
    params_raw: str,
) -> None:
    """Safely unpack a Hub model bundle and run SOUP's benchmark evaluator."""
    params = _parse_params(
        params_raw,
        {"benchmarks", "fewshot", "batch_size", "device", "model_subpath"},
    )
    benchmarks = params.get("benchmarks", "mmlu")
    if not isinstance(benchmarks, str) or not BENCHMARK_RE.fullmatch(benchmarks):
        raise ValueError("benchmarks must be a safe comma-separated lm-eval task list")
    fewshot = params.get("fewshot")
    if fewshot is not None and (not isinstance(fewshot, int) or isinstance(fewshot, bool) or not 0 <= fewshot <= 100):
        raise ValueError("fewshot must be an integer in [0, 100]")
    batch_size = params.get("batch_size", 8)
    if not isinstance(batch_size, int) or isinstance(batch_size, bool) or not 1 <= batch_size <= 4096:
        raise ValueError("batch_size must be an integer in [1, 4096]")
    device = params.get("device")
    if device is not None and (not isinstance(device, str) or not DEVICE_RE.fullmatch(device)):
        raise ValueError("device must be cpu, mps, cuda, or cuda:<index>")
    model_subpath = params.get("model_subpath")
    if model_subpath is not None and not isinstance(model_subpath, str):
        raise ValueError("model_subpath must be a string")

    workdir = Path.cwd().resolve()
    hub_dir = workdir / ".hub-soup"
    hub_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="eval-", dir=hub_dir) as raw_tmp:
        extracted = extract_deterministic_bundle(bundle, Path(raw_tmp) / "bundle")
        model_root = _resolve_model_root(extracted, model_subpath)
        command = [
            soup_bin,
            "eval",
            "benchmark",
            "--model",
            str(model_root),
            "--benchmarks",
            benchmarks,
            "--batch-size",
            str(batch_size),
        ]
        if fewshot is not None:
            command += ["--fewshot", str(fewshot)]
        if device is not None:
            command += ["--device", device]
        returncode, stdout, stderr, truncated = _run_logged(
            command, cwd=workdir, log_dir=hub_dir / "eval-logs"
        )
        _require_success(returncode, "eval benchmark")
        _write_json(
            result,
            {
                "schema_version": 1,
                "operation": "eval",
                "parameters": params,
                "resolved_model_subpath": str(model_root.relative_to(extracted)),
                "stdout": stdout,
                "stderr": stderr,
                "logs_truncated": truncated,
            },
        )


def run_export(
    *,
    soup_bin: str,
    bundle: Path,
    exported_bundle: Path,
    report: Path,
    params_raw: str,
) -> None:
    """Safely unpack a model bundle, run SOUP export, and re-bundle its output."""
    params = _parse_params(params_raw, {"format", "quant", "base", "model_subpath"})
    export_format = params.get("format")
    if not isinstance(export_format, str) or export_format not in EXPORT_FORMATS:
        raise ValueError(f"format must be one of: {', '.join(sorted(EXPORT_FORMATS))}")
    quant = params.get("quant")
    if quant is not None:
        if not isinstance(quant, str) or not re.fullmatch(r"[A-Za-z0-9_.+-]{1,64}", quant):
            raise ValueError("quant must be a short alphanumeric quantization identifier")
    base = params.get("base")
    if base is not None:
        if not isinstance(base, str) or not base or len(base) > 512 or "\x00" in base:
            raise ValueError("base must be a non-empty model path/id up to 512 characters")
    model_subpath = params.get("model_subpath")
    if model_subpath is not None and not isinstance(model_subpath, str):
        raise ValueError("model_subpath must be a string")

    workdir = Path.cwd().resolve()
    hub_dir = workdir / ".hub-soup"
    hub_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="export-", dir=hub_dir) as raw_tmp:
        tmp = Path(raw_tmp)
        extracted = extract_deterministic_bundle(bundle, tmp / "model")
        model_root = _resolve_model_root(extracted, model_subpath)
        export_root = tmp / "exported"
        export_root.mkdir()
        target = export_root / "artifact"
        command = [
            soup_bin,
            "export",
            "--model",
            str(model_root),
            "--format",
            export_format,
            "--output",
            str(target),
        ]
        if quant is not None:
            command += ["--quant", quant]
        if base is not None:
            command += ["--base", base]
        returncode, stdout, stderr, truncated = _run_logged(
            command, cwd=workdir, log_dir=hub_dir / "export-logs"
        )
        _require_success(returncode, "export")
        if target.is_file() and not target.is_symlink():
            normalized = tmp / "export-normalized"
            normalized.mkdir()
            shutil.copyfile(target, normalized / target.name)
            bundle_source = normalized
        else:
            _require_directory(target, "SOUP export output")
            bundle_source = target
        entry_count, payload_bytes = create_deterministic_bundle(bundle_source, exported_bundle)
        _write_json(
            report,
            {
                "schema_version": 1,
                "operation": "export",
                "bundle_media_type": BUNDLE_SCHEMA,
                "bundle_entries": entry_count,
                "payload_bytes": payload_bytes,
                "parameters": params,
                "resolved_model_subpath": str(model_root.relative_to(extracted)),
                "stdout": stdout,
                "stderr": stderr,
                "logs_truncated": truncated,
            },
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="soup_hub_adapter",
        description="Versioned process adapter between SciRust Hub and SOUP",
    )
    parser.add_argument(
        "--soup-bin",
        default="soup",
        help="SOUP executable to invoke (default: soup from PATH)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    ship = subparsers.add_parser(
        "ship-offline",
        help="Replay SOUP ship evidence and emit a verdict artifact",
    )
    ship.add_argument("--evidence", type=Path, required=True)
    ship.add_argument("--verdict", type=Path, required=True)

    train = subparsers.add_parser("train", help="Train with SOUP and emit a deterministic model bundle")
    train.add_argument("--config", type=Path, required=True)
    train.add_argument("--dataset", type=Path, required=True)
    train.add_argument("--bundle", type=Path, required=True)
    train.add_argument("--report", type=Path, required=True)
    train.add_argument("--params", default="{}")

    evaluate = subparsers.add_parser("eval", help="Evaluate a deterministic SOUP model bundle")
    evaluate.add_argument("--bundle", type=Path, required=True)
    evaluate.add_argument("--result", type=Path, required=True)
    evaluate.add_argument("--params", default="{}")

    export = subparsers.add_parser("export", help="Export a deterministic SOUP model bundle")
    export.add_argument("--bundle", type=Path, required=True)
    export.add_argument("--exported-bundle", type=Path, required=True)
    export.add_argument("--report", type=Path, required=True)
    export.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "ship-offline":
            run_ship_offline(
                soup_bin=args.soup_bin,
                evidence=args.evidence,
                verdict=args.verdict,
            )
        elif args.command == "train":
            run_train(
                soup_bin=args.soup_bin,
                config=args.config,
                dataset=args.dataset,
                bundle=args.bundle,
                report=args.report,
                params_raw=args.params,
            )
        elif args.command == "eval":
            run_eval(
                soup_bin=args.soup_bin,
                bundle=args.bundle,
                result=args.result,
                params_raw=args.params,
            )
        elif args.command == "export":
            run_export(
                soup_bin=args.soup_bin,
                bundle=args.bundle,
                exported_bundle=args.exported_bundle,
                report=args.report,
                params_raw=args.params,
            )
        else:
            raise ValueError(f"unsupported adapter command: {args.command}")
        return 0
    except (OSError, RuntimeError, ValueError, tarfile.TarError, UnicodeError) as exc:
        print(f"soup_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
