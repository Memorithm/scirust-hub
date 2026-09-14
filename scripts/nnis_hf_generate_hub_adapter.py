#!/usr/bin/env python3
"""Artifact adapter for the NNIS local-HF generation process contract.

SciRust Hub owns deterministic bundle materialization and orchestration. NNIS
remains authoritative for model loading, CUDA execution, tokenization, greedy
sampling, decoded output, and the versioned generation-result contract.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import stat
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

NNIS_GENERATION_CONTRACT = "nnis.hf-generation@1.0.0"
NNIS_GENERATION_MEDIA_TYPE = "application/vnd.nnis.hf-generation.v1+json"
NNIS_GENERATION_SCHEMA_VERSION = 1
MAX_RESULT_BYTES = 40 * 1024 * 1024
MAX_ERROR_TEXT = 4096
# Both limits apply: JSON escaping can expand a prompt's serialized envelope.
MAX_PARAMS_BYTES = 16_384
MAX_PROMPT_BYTES = 16_000
MAX_NEW_TOKENS = 65_536
MAX_DEVICE_ORDINAL = 2_147_483_647


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Reject duplicate keys rather than selecting an ambiguous wire value."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key!r}")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise ValueError(f"non-finite JSON constant: {value}")


def validate_result_contract(result: Path) -> bytes:
    """Read bounded bytes once, validate only the wire envelope, and retain them.

    Returning those exact bytes prevents a second read from publishing different
    content after validation. This does not recompute NNIS model semantics.
    """
    _require_regular_input(result, "NNIS generation result")
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0)
    flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    with os.fdopen(os.open(result, flags), "rb") as handle:
        metadata = os.fstat(handle.fileno())
        if not stat.S_ISREG(metadata.st_mode):
            raise ValueError("NNIS generation result must be a regular file")
        if not 0 < metadata.st_size <= MAX_RESULT_BYTES:
            raise ValueError(f"NNIS generation result size must be 1..={MAX_RESULT_BYTES} bytes")
        raw = handle.read(MAX_RESULT_BYTES + 1)
    if len(raw) != metadata.st_size or len(raw) > MAX_RESULT_BYTES:
        raise ValueError("NNIS generation result size changed or exceeded its byte limit")
    try:
        payload = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_unique_object,
            parse_constant=_reject_constant,
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ValueError(f"NNIS generation result is not valid JSON: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("NNIS generation result must be a JSON object")
    if (
        type(payload.get("schema_version")) is not int
        or payload["schema_version"] != NNIS_GENERATION_SCHEMA_VERSION
    ):
        raise ValueError(
            "unsupported NNIS generation schema_version: "
            f"{payload.get('schema_version')!r}; expected {NNIS_GENERATION_SCHEMA_VERSION}"
        )
    if payload.get("contract") != NNIS_GENERATION_CONTRACT:
        raise ValueError(
            "unsupported NNIS generation contract: "
            f"{payload.get('contract')!r}; expected {NNIS_GENERATION_CONTRACT!r}"
        )
    if payload.get("media_type") != NNIS_GENERATION_MEDIA_TYPE:
        raise ValueError(
            "unsupported NNIS generation media_type: "
            f"{payload.get('media_type')!r}; expected {NNIS_GENERATION_MEDIA_TYPE!r}"
        )
    if payload.get("status") != "generated":
        raise ValueError(
            "unsupported NNIS generation status: "
            f"{payload.get('status')!r}; expected 'generated'"
        )
    return raw


def _copy_new_exact(source: Path, destination: Path) -> None:
    """Publish the validated snapshot atomically without replacing any destination."""
    if destination.exists() or destination.is_symlink():
        raise ValueError(f"generation output already exists: {destination}")
    raw = validate_result_contract(source)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(mode="wb", dir=destination.parent, prefix=".hub-nnis-result-", delete=False) as writer:
            temporary = Path(writer.name)
            writer.write(raw)
            writer.flush()
            os.fsync(writer.fileno())
        os.link(temporary, destination)
    except FileExistsError as exc:
        raise ValueError(f"generation output already exists: {destination}") from exc
    finally:
        if temporary is not None:
            try:
                temporary.unlink()
            except OSError:
                # Never mask the primary error or remove another publisher's result.
                pass


def run_generation(
    *,
    python_bin: str,
    nnis_process: str,
    nnis_hf_bin: str,
    bundle: Path,
    generation: Path,
    params_raw: str,
) -> None:
    """Materialize one Hub bundle and delegate all generation semantics to NNIS."""
    _require_regular_input(bundle, "model bundle")
    if len(params_raw.encode("utf-8")) > MAX_PARAMS_BYTES:
        raise ValueError(f"serialized parameters exceed {MAX_PARAMS_BYTES} UTF-8 bytes")
    params = _parse_params(params_raw, {"model_subpath", "prompt", "device", "max_new_tokens"})

    model_subpath = params.get("model_subpath")
    if model_subpath is not None and not isinstance(model_subpath, str):
        raise ValueError("model_subpath must be a string")

    prompt = params.get("prompt")
    if not isinstance(prompt, str) or "\0" in prompt:
        raise ValueError("prompt is required and must be a string without NUL bytes")
    prompt_bytes = prompt.encode("utf-8")
    if not prompt_bytes or len(prompt_bytes) > MAX_PROMPT_BYTES:
        raise ValueError(f"prompt must encode to 1..={MAX_PROMPT_BYTES} UTF-8 bytes")

    device = params.get("device", 0)
    if (
        not isinstance(device, int)
        or isinstance(device, bool)
        or not 0 <= device <= MAX_DEVICE_ORDINAL
    ):
        raise ValueError(f"device must be an integer in [0, {MAX_DEVICE_ORDINAL}]")

    max_new_tokens = params.get("max_new_tokens", 16)
    if (
        not isinstance(max_new_tokens, int)
        or isinstance(max_new_tokens, bool)
        or not 1 <= max_new_tokens <= MAX_NEW_TOKENS
    ):
        raise ValueError(f"max_new_tokens must be an integer in [1, {MAX_NEW_TOKENS}]")

    workdir = Path.cwd().resolve()
    _require_under(workdir, generation, "generation output")
    if generation.exists() or generation.is_symlink():
        raise ValueError("generation output must not exist before execution")

    hub_dir = workdir / ".hub-nnis"
    hub_dir.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="hf-generation-", dir=hub_dir) as raw_tmp:
        tmp = Path(raw_tmp)
        extracted = extract_deterministic_bundle(bundle, tmp / "model")
        model_root = _resolve_model_root(extracted, model_subpath)
        producer_result = tmp / "nnis-generation.json"
        command = [
            python_bin,
            nnis_process,
            "--model",
            str(model_root),
            "--prompt",
            prompt,
            "--device",
            str(device),
            "--max-new-tokens",
            str(max_new_tokens),
            "--result",
            str(producer_result),
            "--nnis-hf-bin",
            nnis_hf_bin,
        ]
        returncode, _stdout, stderr, truncated = _run_logged(
            command,
            cwd=workdir,
            log_dir=hub_dir / "generation-logs",
        )
        if returncode != 0:
            if returncode < 0:
                raise RuntimeError(f"NNIS HF generation process terminated by signal {-returncode}")
            suffix = " [captured logs truncated]" if truncated else ""
            detail = stderr.strip()[:MAX_ERROR_TEXT]
            if detail:
                raise RuntimeError(
                    f"NNIS HF generation process failed with exit {returncode}: {detail}{suffix}"
                )
            raise RuntimeError(f"NNIS HF generation process failed with exit {returncode}{suffix}")

        _copy_new_exact(producer_result, generation)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="nnis_hf_generate_hub_adapter",
        description="Materialize a Hub model bundle and run NNIS-owned local-HF generation",
    )
    parser.add_argument(
        "--python-bin",
        default=os.environ.get("NNIS_PYTHON_BIN", "/usr/bin/python3"),
        help="Python executable for the NNIS process contract",
    )
    parser.add_argument(
        "--nnis-process",
        default=os.environ.get(
            "NNIS_HF_GENERATE_PROCESS", "/opt/nnis/libexec/nnis_hub_hf_generate.py"
        ),
        help="NNIS-owned nnis.hf-generation@1.0.0 process path",
    )
    parser.add_argument(
        "--nnis-hf-bin",
        default=os.environ.get("NNIS_HF_BIN", "nnis-hf"),
        help="native nnis-hf executable delegated to by the NNIS process",
    )
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--generation", type=Path, required=True)
    parser.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_generation(
            python_bin=args.python_bin,
            nnis_process=args.nnis_process,
            nnis_hf_bin=args.nnis_hf_bin,
            bundle=args.bundle,
            generation=args.generation,
            params_raw=args.params,
        )
        return 0
    except (OSError, RuntimeError, ValueError, tarfile.TarError, UnicodeError) as exc:
        print(f"nnis_hf_generate_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
