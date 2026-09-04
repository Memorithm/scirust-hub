#!/usr/bin/env python3
"""Hub handoff for SOUP training with SciRust symbolic-equivalence reward.

This wrapper stages only an explicit GRPO task/reward seam, pins the trusted
SciRust reward bridge and `scirust-reward` process to fixed deployment paths,
then delegates training/artifact handling to the existing SOUP Hub adapter.

Hub does not implement symbolic mathematics, reward scoring, or SOUP training.
SciRust owns the prover/process contract; SOUP owns the post-training runtime;
Hub owns orchestration and provenance.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import tempfile
from typing import Sequence

import soup_hub_adapter as base

SCIRUST_SOURCE_HEAD = "58d850899cee1d62449cc02816b787b7f8a8a3de"
SCIRUST_SOURCE_MERGE = "f6bdadb6234129e14e9ea4d69f46901c6dcecbd0"
SOUP_QUALIFIED_COMMIT = "05b646523727925990530667e7012ede50bd30b2"
SCIRUST_REWARD_SCHEMA_VERSION = 1
SCIRUST_REWARD_KIND = "symbolic_equivalence"
SCIRUST_REWARD_BRIDGE = Path("/opt/scirust-hub/libexec/scirust_symbolic_reward.py")
SCIRUST_REWARD_BIN = Path("/opt/scirust-hub/libexec/scirust-reward")
SCIRUST_TASK_TOKEN = "${SOUP_HUB_SCIRUST_SYMBOLIC_TASK}"
SCIRUST_REWARD_TOKEN = "${SOUP_HUB_SCIRUST_SYMBOLIC_REWARD}"
SCIRUST_TASK_LINE = f"task: {SCIRUST_TASK_TOKEN}"
SCIRUST_REWARD_LINE = f"  {SCIRUST_REWARD_TOKEN}"


def _require_executable_regular_file(path: Path, label: str) -> None:
    base._require_regular_input(path, label)
    if not os.access(path, os.X_OK):
        raise ValueError(f"{label} {path!s} must be executable")


def validate_scirust_reward_installation(
    *,
    reward_bridge: Path = SCIRUST_REWARD_BRIDGE,
    reward_bin: Path = SCIRUST_REWARD_BIN,
) -> None:
    """Fail closed unless both pinned deployment dependencies are usable."""
    base._require_regular_input(reward_bridge, "SciRust SOUP reward bridge")
    _require_executable_regular_file(reward_bin, "SciRust reward binary")


def prepare_scirust_symbolic_template(
    template: str,
    *,
    reward_bridge: Path = SCIRUST_REWARD_BRIDGE,
) -> str:
    """Materialize the exact GRPO task and trusted reward-file seams only."""
    if template.count(SCIRUST_TASK_TOKEN) != 1:
        raise ValueError(
            f"SOUP SciRust config must contain {SCIRUST_TASK_TOKEN} exactly once"
        )
    if template.count(SCIRUST_REWARD_TOKEN) != 1:
        raise ValueError(
            f"SOUP SciRust config must contain {SCIRUST_REWARD_TOKEN} exactly once"
        )

    had_trailing_newline = template.endswith(("\n", "\r"))
    rendered: list[str] = []
    saw_task = False
    saw_reward = False
    for line in template.splitlines():
        stripped = line.strip()
        if SCIRUST_TASK_TOKEN in line:
            if line != SCIRUST_TASK_LINE:
                raise ValueError(
                    f"{SCIRUST_TASK_TOKEN} must occupy exactly the root line "
                    f"{SCIRUST_TASK_LINE!r}"
                )
            rendered.append('task: "grpo"')
            saw_task = True
            continue
        if SCIRUST_REWARD_TOKEN in line:
            if line != SCIRUST_REWARD_LINE:
                raise ValueError(
                    f"{SCIRUST_REWARD_TOKEN} must occupy exactly the two-space-indented "
                    f"line {SCIRUST_REWARD_LINE!r} under training:"
                )
            rendered.append(f"  reward_fn: {json.dumps(str(reward_bridge))}")
            saw_reward = True
            continue

        # Reject duplicate semantic keys rather than relying on YAML parser
        # duplicate-key behavior. The v1 seam owns these two fields entirely.
        if line.startswith("task:"):
            raise ValueError("SOUP SciRust template must not declare another root task")
        if stripped.startswith("reward_fn:"):
            raise ValueError("SOUP SciRust template must not declare another reward_fn")
        rendered.append(line)

    if not saw_task or not saw_reward:
        raise ValueError("SOUP SciRust template placeholders were not materialized")
    result = "\n".join(rendered)
    if had_trailing_newline:
        result += "\n"
    return result


def run_train_scirust_symbolic(
    *,
    soup_bin: str,
    config: Path,
    dataset: Path,
    bundle: Path,
    report: Path,
    params_raw: str,
    reward_bridge: Path = SCIRUST_REWARD_BRIDGE,
    reward_bin: Path = SCIRUST_REWARD_BIN,
) -> None:
    """Launch one SOUP GRPO run wired to the qualified SciRust reward bridge."""
    base._require_regular_input(config, "config")
    base._require_regular_input(dataset, "dataset")
    validate_scirust_reward_installation(
        reward_bridge=reward_bridge,
        reward_bin=reward_bin,
    )
    if config.stat().st_size > base.MAX_CONFIG_BYTES:
        raise ValueError(f"SOUP config exceeds {base.MAX_CONFIG_BYTES} bytes")

    template = config.read_text(encoding="utf-8")
    prepared = prepare_scirust_symbolic_template(
        template,
        reward_bridge=reward_bridge,
    )

    workdir = Path.cwd().resolve()
    with tempfile.TemporaryDirectory(prefix=".hub-soup-scirust-", dir=workdir) as raw_tmp:
        prepared_path = Path(raw_tmp) / "soup-template.yaml"
        prepared_path.write_text(prepared, encoding="utf-8")

        previous_reward_bin = os.environ.get("SCIRUST_REWARD_BIN")
        os.environ["SCIRUST_REWARD_BIN"] = str(reward_bin)
        try:
            base.run_train(
                soup_bin=soup_bin,
                config=prepared_path,
                dataset=dataset,
                bundle=bundle,
                report=report,
                params_raw=params_raw,
            )
        finally:
            if previous_reward_bin is None:
                os.environ.pop("SCIRUST_REWARD_BIN", None)
            else:
                os.environ["SCIRUST_REWARD_BIN"] = previous_reward_bin

    base._require_regular_input(report, "SOUP training report")
    payload = json.loads(report.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("SOUP training report must be a JSON object")
    payload["scirust_symbolic_reward"] = {
        "source_head": SCIRUST_SOURCE_HEAD,
        "source_merge": SCIRUST_SOURCE_MERGE,
        "schema_version": SCIRUST_REWARD_SCHEMA_VERSION,
        "kind": SCIRUST_REWARD_KIND,
        "soup_upstream_commit": SOUP_QUALIFIED_COMMIT,
        "task": "grpo",
    }
    base._write_json(report, payload)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="soup_scirust_symbolic_hub_adapter",
        description="SciRust Hub handoff for SOUP GRPO with SciRust symbolic reward",
    )
    parser.add_argument(
        "--soup-bin",
        default="soup",
        help="SOUP executable to invoke (default: soup from PATH)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    train = subparsers.add_parser("train", help="Run SOUP GRPO with SciRust symbolic reward")
    train.add_argument("--config", type=Path, required=True)
    train.add_argument("--dataset", type=Path, required=True)
    train.add_argument("--bundle", type=Path, required=True)
    train.add_argument("--report", type=Path, required=True)
    train.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command != "train":
            raise ValueError(f"unsupported adapter command: {args.command}")
        run_train_scirust_symbolic(
            soup_bin=args.soup_bin,
            config=args.config,
            dataset=args.dataset,
            bundle=args.bundle,
            report=args.report,
            params_raw=args.params,
        )
        return 0
    except (OSError, RuntimeError, ValueError, UnicodeError) as exc:
        print(f"soup_scirust_symbolic_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
