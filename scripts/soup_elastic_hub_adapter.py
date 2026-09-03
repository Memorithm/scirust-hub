#!/usr/bin/env python3
"""Hub handoff for ElasticXxx SOUP pre-execution resource plans.

This adapter consumes the published
``elastic.soup.run-resource-plan@1.0.0`` JSON envelope, revalidates the known
wire contract, materializes only its reviewed SOUP configuration knobs into an
explicit template seam, then delegates execution and artifact handling to the
existing SOUP Hub adapter.

It does not choose resource policy, schedule workers, or implement SOUP training
semantics. ElasticXxx owns the plan; SOUP remains the final config/runtime
validator; Hub owns orchestration and immutable input provenance.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Any, Sequence

import soup_hub_adapter as base

ELASTIC_SOUP_PLAN_CONTRACT = "elastic.soup.run-resource-plan@1.0.0"
ELASTIC_SOUP_PLAN_MEDIA_TYPE = "application/vnd.elastic.soup.run-resource-plan.v1+json"
ELASTIC_SOUP_SOURCE_MERGE = "6e0952e59842eb9c14f808266d6f3eb0b1f33014"
QUALIFIED_SOUP_COMMIT = "05b646523727925990530667e7012ede50bd30b2"
RESOURCE_TASK_TOKEN = "${SOUP_HUB_RESOURCE_TASK}"
RESOURCE_PLAN_TOKEN = "${SOUP_HUB_RESOURCE_PLAN}"
RESOURCE_TASK_LINE = f"task: {RESOURCE_TASK_TOKEN}"
RESOURCE_PLAN_LINE = f"  {RESOURCE_PLAN_TOKEN}"
MAX_RESOURCE_PLAN_BYTES = 64 * 1024
MAX_FIXED_BATCH_SIZE = 2**32 - 1
STREAM_TASKS = frozenset({"sft", "dpo", "orpo", "simpo", "kto"})
AUTO_BATCH_STRATEGIES = frozenset({"auto", "static", "probe"})
STREAM_SOURCES = frozenset({"auto", "ram", "disk"})


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Reject duplicate JSON keys instead of silently keeping the last value."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field {key!r} in Elastic SOUP resource plan")
        result[key] = value
    return result


def _require_exact_keys(value: dict[str, Any], expected: set[str], label: str) -> None:
    actual = set(value)
    missing = sorted(expected - actual)
    unknown = sorted(actual - expected)
    if missing or unknown:
        details: list[str] = []
        if missing:
            details.append(f"missing: {', '.join(missing)}")
        if unknown:
            details.append(f"unknown: {', '.join(unknown)}")
        raise ValueError(f"{label} fields do not match v1 ({'; '.join(details)})")


def load_elastic_soup_resource_plan(path: Path) -> dict[str, Any]:
    """Load and semantically revalidate the published ElasticXxx v1 wire plan."""
    base._require_regular_input(path, "Elastic SOUP resource plan")
    if path.stat().st_size > MAX_RESOURCE_PLAN_BYTES:
        raise ValueError(
            f"Elastic SOUP resource plan exceeds {MAX_RESOURCE_PLAN_BYTES} bytes"
        )
    try:
        raw = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_strict_object)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Elastic SOUP resource-plan JSON: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError("Elastic SOUP resource plan must be a JSON object")

    _require_exact_keys(
        raw,
        {
            "contract",
            "upstream_commit",
            "task",
            "batch_size",
            "auto_batch_strategy",
            "streaming",
        },
        "Elastic SOUP resource plan",
    )
    if raw["contract"] != ELASTIC_SOUP_PLAN_CONTRACT:
        raise ValueError(
            f"unsupported Elastic SOUP resource-plan contract {raw['contract']!r}; "
            f"expected {ELASTIC_SOUP_PLAN_CONTRACT}"
        )
    if raw["upstream_commit"] != QUALIFIED_SOUP_COMMIT:
        raise ValueError(
            f"unqualified SOUP revision {raw['upstream_commit']!r}; "
            f"expected {QUALIFIED_SOUP_COMMIT}"
        )

    task_raw = raw["task"]
    if not isinstance(task_raw, str):
        raise ValueError("Elastic SOUP resource-plan task must be a string")
    task = task_raw.strip()
    if not task:
        raise ValueError("Elastic SOUP resource-plan task must not be blank")

    batch = raw["batch_size"]
    if not isinstance(batch, dict):
        raise ValueError("Elastic SOUP batch_size must be an object")
    mode = batch.get("mode")
    if mode == "auto":
        _require_exact_keys(batch, {"mode"}, "Elastic SOUP auto batch_size")
        canonical_batch: dict[str, Any] = {"mode": "auto"}
    elif mode == "fixed":
        _require_exact_keys(batch, {"mode", "value"}, "Elastic SOUP fixed batch_size")
        value = batch["value"]
        if (
            not isinstance(value, int)
            or isinstance(value, bool)
            or not 1 <= value <= MAX_FIXED_BATCH_SIZE
        ):
            raise ValueError(
                f"Elastic SOUP fixed batch_size value must be an integer in "
                f"[1, {MAX_FIXED_BATCH_SIZE}]"
            )
        canonical_batch = {"mode": "fixed", "value": value}
    else:
        raise ValueError("Elastic SOUP batch_size mode must be 'auto' or 'fixed'")

    strategy = raw["auto_batch_strategy"]
    if not isinstance(strategy, str) or strategy not in AUTO_BATCH_STRATEGIES:
        raise ValueError(
            "Elastic SOUP auto_batch_strategy must be one of: auto, static, probe"
        )

    streaming = raw["streaming"]
    canonical_streaming: dict[str, Any] | None
    if streaming is None:
        canonical_streaming = None
    else:
        if not isinstance(streaming, dict):
            raise ValueError("Elastic SOUP streaming must be null or an object")
        _require_exact_keys(streaming, {"source", "buffers"}, "Elastic SOUP streaming")
        source = streaming["source"]
        buffers = streaming["buffers"]
        if not isinstance(source, str) or source not in STREAM_SOURCES:
            raise ValueError("Elastic SOUP stream source must be one of: auto, ram, disk")
        if not isinstance(buffers, int) or isinstance(buffers, bool) or not 2 <= buffers <= 8:
            raise ValueError("Elastic SOUP stream buffers must be an integer in [2, 8]")
        if task not in STREAM_TASKS:
            raise ValueError(
                f"Elastic SOUP layer streaming is not qualified for task {task!r}"
            )
        canonical_streaming = {"source": source, "buffers": buffers}

    return {
        "contract": ELASTIC_SOUP_PLAN_CONTRACT,
        "upstream_commit": QUALIFIED_SOUP_COMMIT,
        "task": task,
        "batch_size": canonical_batch,
        "auto_batch_strategy": strategy,
        "streaming": canonical_streaming,
    }


def render_resource_block(plan: dict[str, Any]) -> list[str]:
    """Render only the SOUP knobs published by the ElasticXxx v1 contract."""
    batch = plan["batch_size"]
    batch_value = "auto" if batch["mode"] == "auto" else str(batch["value"])
    lines = [
        f"batch_size: {batch_value}",
        f"auto_batch_size_strategy: {plan['auto_batch_strategy']}",
    ]
    streaming = plan["streaming"]
    if streaming is None:
        lines.append("stream_layers: false")
    else:
        lines.extend(
            [
                "stream_layers: true",
                f"stream_source: {streaming['source']}",
                f"stream_buffers: {streaming['buffers']}",
            ]
        )
    return lines


def prepare_elastic_soup_template(template: str, plan: dict[str, Any]) -> str:
    """Fill the two explicit Elastic-owned template seams, nothing else."""
    if template.count(RESOURCE_TASK_TOKEN) != 1:
        raise ValueError(f"SOUP Elastic config must contain {RESOURCE_TASK_TOKEN} exactly once")
    if template.count(RESOURCE_PLAN_TOKEN) != 1:
        raise ValueError(f"SOUP Elastic config must contain {RESOURCE_PLAN_TOKEN} exactly once")

    had_trailing_newline = template.endswith(("\n", "\r"))
    rendered: list[str] = []
    saw_task = False
    saw_plan = False
    for line in template.splitlines():
        if RESOURCE_TASK_TOKEN in line:
            if line != RESOURCE_TASK_LINE:
                raise ValueError(
                    f"{RESOURCE_TASK_TOKEN} must occupy exactly the root line "
                    f"{RESOURCE_TASK_LINE!r}"
                )
            rendered.append(f"task: {json.dumps(plan['task'])}")
            saw_task = True
            continue
        if RESOURCE_PLAN_TOKEN in line:
            if line != RESOURCE_PLAN_LINE:
                raise ValueError(
                    f"{RESOURCE_PLAN_TOKEN} must occupy exactly the two-space-indented "
                    f"line {RESOURCE_PLAN_LINE!r} under training:"
                )
            rendered.extend(f"  {entry}" for entry in render_resource_block(plan))
            saw_plan = True
            continue
        rendered.append(line)

    if not saw_task or not saw_plan:
        raise ValueError("SOUP Elastic template placeholders were not materialized")
    result = "\n".join(rendered)
    if had_trailing_newline:
        result += "\n"
    return result


def run_train_elastic(
    *,
    soup_bin: str,
    config: Path,
    dataset: Path,
    resource_plan: Path,
    bundle: Path,
    report: Path,
    params_raw: str,
) -> None:
    """Apply one validated Elastic preflight plan, then delegate to SOUP training."""
    base._require_regular_input(config, "config")
    base._require_regular_input(dataset, "dataset")
    if config.stat().st_size > base.MAX_CONFIG_BYTES:
        raise ValueError(f"SOUP config exceeds {base.MAX_CONFIG_BYTES} bytes")

    plan = load_elastic_soup_resource_plan(resource_plan)
    template = config.read_text(encoding="utf-8")
    prepared = prepare_elastic_soup_template(template, plan)

    workdir = Path.cwd().resolve()
    with tempfile.TemporaryDirectory(prefix=".hub-soup-elastic-", dir=workdir) as raw_tmp:
        prepared_path = Path(raw_tmp) / "soup-template.yaml"
        prepared_path.write_text(prepared, encoding="utf-8")
        base.run_train(
            soup_bin=soup_bin,
            config=prepared_path,
            dataset=dataset,
            bundle=bundle,
            report=report,
            params_raw=params_raw,
        )

    base._require_regular_input(report, "SOUP training report")
    payload = json.loads(report.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("SOUP training report must be a JSON object")
    payload["elastic_resource_plan"] = {
        "contract": ELASTIC_SOUP_PLAN_CONTRACT,
        "media_type": ELASTIC_SOUP_PLAN_MEDIA_TYPE,
        "elastic_source_merge": ELASTIC_SOUP_SOURCE_MERGE,
        "upstream_commit": QUALIFIED_SOUP_COMMIT,
        "task": plan["task"],
    }
    base._write_json(report, payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="soup_elastic_hub_adapter",
        description="SciRust Hub handoff for ElasticXxx SOUP resource plans",
    )
    parser.add_argument(
        "--soup-bin",
        default="soup",
        help="SOUP executable to invoke (default: soup from PATH)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    train = subparsers.add_parser("train", help="Train SOUP from an ElasticXxx v1 resource plan")
    train.add_argument("--config", type=Path, required=True)
    train.add_argument("--dataset", type=Path, required=True)
    train.add_argument("--resource-plan", type=Path, required=True)
    train.add_argument("--bundle", type=Path, required=True)
    train.add_argument("--report", type=Path, required=True)
    train.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command != "train":
            raise ValueError(f"unsupported adapter command: {args.command}")
        run_train_elastic(
            soup_bin=args.soup_bin,
            config=args.config,
            dataset=args.dataset,
            resource_plan=args.resource_plan,
            bundle=args.bundle,
            report=args.report,
            params_raw=args.params,
        )
        return 0
    except (OSError, RuntimeError, ValueError, UnicodeError) as exc:
        print(f"soup_elastic_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
