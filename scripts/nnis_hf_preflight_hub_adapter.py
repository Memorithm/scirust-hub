#!/usr/bin/env python3
"""Artifact adapter for the NNIS CPU-only Hugging Face preflight contract.

SciRust Hub owns deterministic bundle materialization and process orchestration.
NNIS remains authoritative for config, tokenizer, Safetensors, dtype, tensor,
decoder-graph, and direct-execution readiness semantics.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tarfile
import tempfile
from typing import Sequence

from soup_hub_adapter import (
    _parse_params,
    _require_regular_input,
    _require_under,
    _resolve_model_root,
    _run_logged,
    extract_deterministic_bundle,
)

NNIS_PREFLIGHT_SCHEMA = "nnis.hf-preflight@1"
MAX_REPORT_BYTES = 4 * 1024 * 1024
MAX_ERROR_TEXT = 4096


def validate_report_contract(report: Path) -> None:
    """Validate only the versioned wire boundary; do not reinterpret NNIS policy."""
    _require_regular_input(report, "NNIS preflight report")
    if report.stat().st_size > MAX_REPORT_BYTES:
        raise ValueError(f"NNIS preflight report exceeds {MAX_REPORT_BYTES} bytes")
    try:
        payload = json.loads(report.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"NNIS preflight report is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("NNIS preflight report must be a JSON object")
    if payload.get("schema") != NNIS_PREFLIGHT_SCHEMA:
        raise ValueError(
            "unsupported NNIS preflight schema: "
            f"{payload.get('schema')!r}; expected {NNIS_PREFLIGHT_SCHEMA!r}"
        )


def run_preflight(
    *,
    nnis_hf_bin: str,
    bundle: Path,
    report: Path,
    params_raw: str,
) -> None:
    """Materialize one Hub bundle and delegate all model admission to NNIS."""
    _require_regular_input(bundle, "model bundle")
    params = _parse_params(params_raw, {"model_subpath"})
    model_subpath = params.get("model_subpath")
    if model_subpath is not None and not isinstance(model_subpath, str):
        raise ValueError("model_subpath must be a string")

    workdir = Path.cwd().resolve()
    _require_under(workdir, report, "preflight report output")
    if report.exists() or report.is_symlink():
        raise ValueError("preflight report output must not exist before execution")

    hub_dir = workdir / ".hub-nnis"
    hub_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hf-preflight-", dir=hub_dir) as raw_tmp:
        tmp = Path(raw_tmp)
        extracted = extract_deterministic_bundle(bundle, tmp / "model")
        model_root = _resolve_model_root(extracted, model_subpath)
        command = [
            nnis_hf_bin,
            "validate",
            "--model",
            str(model_root),
            "--json",
            "--output",
            str(report),
        ]
        returncode, _stdout, stderr, truncated = _run_logged(
            command,
            cwd=workdir,
            log_dir=hub_dir / "preflight-logs",
        )
        if returncode != 0:
            if returncode < 0:
                raise RuntimeError(f"NNIS HF preflight terminated by signal {-returncode}")
            suffix = " [captured logs truncated]" if truncated else ""
            detail = stderr.strip()[:MAX_ERROR_TEXT]
            if detail:
                raise RuntimeError(
                    f"NNIS HF preflight rejected the model with exit {returncode}: "
                    f"{detail}{suffix}"
                )
            raise RuntimeError(
                f"NNIS HF preflight rejected the model with exit {returncode}{suffix}"
            )
        validate_report_contract(report)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nnis_hf_preflight_hub_adapter",
        description="Materialize a Hub model bundle and run the NNIS-owned HF preflight",
    )
    parser.add_argument(
        "--nnis-hf-bin",
        default=os.environ.get("NNIS_HF_BIN", "nnis-hf"),
        help="nnis-hf executable to invoke (default: NNIS_HF_BIN or nnis-hf from PATH)",
    )
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_preflight(
            nnis_hf_bin=args.nnis_hf_bin,
            bundle=args.bundle,
            report=args.report,
            params_raw=args.params,
        )
        return 0
    except (OSError, RuntimeError, ValueError, tarfile.TarError, UnicodeError) as exc:
        print(f"nnis_hf_preflight_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
