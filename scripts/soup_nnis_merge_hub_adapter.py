#!/usr/bin/env python3
"""Narrow SOUP adapter-to-dense-F32 producer for the NNIS artifact boundary.

Hub materializes and bundles artifacts. SOUP remains authoritative for model
loading, LoRA merge, tokenizer handling, and checkpoint serialization. NNIS
remains authoritative for downstream model admission.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys
import tarfile
import tempfile
from typing import Sequence

from soup_hub_adapter import (
    BUNDLE_SCHEMA,
    _parse_params,
    _require_directory,
    _require_regular_input,
    _require_success,
    _require_under,
    _resolve_model_root,
    _run_logged,
    _write_json,
    create_deterministic_bundle,
    extract_deterministic_bundle,
)

SOUP_SOURCE_COMMIT = "9f1e48b33a5fcda7e621e612ec9305cb52a38e07"
MERGE_DTYPE = "float32"
MERGE_SAVE_FORMAT = "fp16"
MERGE_HUB = "hf"


def run_merge(
    *,
    soup_bin: str,
    adapter_bundle: Path,
    merged_bundle: Path,
    report: Path,
    params_raw: str,
) -> None:
    """Ask SOUP to merge one immutable LoRA adapter bundle into dense F32."""
    _require_regular_input(adapter_bundle, "adapter bundle")
    params = _parse_params(params_raw, {"model_subpath"})
    model_subpath = params.get("model_subpath")
    if model_subpath is not None and not isinstance(model_subpath, str):
        raise ValueError("model_subpath must be a string")

    workdir = Path.cwd().resolve()
    _require_under(workdir, merged_bundle, "merged model bundle output")
    _require_under(workdir, report, "merge report output")
    if merged_bundle.exists() or merged_bundle.is_symlink():
        raise ValueError("merged model bundle output must not exist before execution")
    if report.exists() or report.is_symlink():
        raise ValueError("merge report output must not exist before execution")

    hub_dir = workdir / ".hub-soup-nnis"
    hub_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="merge-", dir=hub_dir) as raw_tmp:
        tmp = Path(raw_tmp)
        extracted = extract_deterministic_bundle(adapter_bundle, tmp / "adapter-bundle")
        adapter_root = _resolve_model_root(extracted, model_subpath)
        adapter_config = adapter_root / "adapter_config.json"
        _require_regular_input(adapter_config, "SOUP LoRA adapter config")

        merged_root = tmp / "merged-f32"
        command = [
            soup_bin,
            "merge",
            "--adapter",
            str(adapter_root),
            "--output",
            str(merged_root),
            "--dtype",
            MERGE_DTYPE,
            "--save-format",
            MERGE_SAVE_FORMAT,
            "--hub",
            MERGE_HUB,
        ]
        returncode, stdout, stderr, truncated = _run_logged(
            command,
            cwd=workdir,
            log_dir=hub_dir / "merge-logs",
        )
        _require_success(returncode, "merge")
        _require_directory(merged_root, "SOUP merged model output")

        entry_count, payload_bytes = create_deterministic_bundle(merged_root, merged_bundle)
        _write_json(
            report,
            {
                "schema_version": 1,
                "operation": "merge_nnis_dense_f32",
                "bundle_media_type": BUNDLE_SCHEMA,
                "bundle_entries": entry_count,
                "payload_bytes": payload_bytes,
                "parameters": params,
                "resolved_adapter_subpath": str(adapter_root.relative_to(extracted)),
                "requested_soup_merge": {
                    "dtype": MERGE_DTYPE,
                    "save_format": MERGE_SAVE_FORMAT,
                    "hub": MERGE_HUB,
                    "trust_remote_code": False,
                    "base_override": None,
                },
                "qualified_soup_source_commit": SOUP_SOURCE_COMMIT,
                "stdout": stdout,
                "stderr": stderr,
                "logs_truncated": truncated,
            },
        )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="soup_nnis_merge_hub_adapter",
        description="Use SOUP to merge a LoRA bundle into the dense F32 artifact required by NNIS",
    )
    parser.add_argument(
        "--soup-bin",
        default=os.environ.get("SOUP_BIN", "soup"),
        help="SOUP executable to invoke (default: SOUP_BIN or soup from PATH)",
    )
    parser.add_argument("--adapter-bundle", type=Path, required=True)
    parser.add_argument("--merged-bundle", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_merge(
            soup_bin=args.soup_bin,
            adapter_bundle=args.adapter_bundle,
            merged_bundle=args.merged_bundle,
            report=args.report,
            params_raw=args.params,
        )
        return 0
    except (OSError, RuntimeError, ValueError, tarfile.TarError, UnicodeError) as exc:
        print(f"soup_nnis_merge_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
