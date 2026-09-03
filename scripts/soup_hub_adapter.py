#!/usr/bin/env python3
"""Process adapter for versioned SOUP capabilities exposed through SciRust Hub.

The adapter deliberately owns only process-contract translation. It does not
implement SOUP training, evaluation, scoring, model loading, or verdict logic.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import subprocess
import sys
from typing import Sequence

SOUP_EXIT_SHIP = 0
SOUP_EXIT_RUNTIME_ERROR = 1
SOUP_EXIT_DONT_SHIP = 2
SOUP_EXIT_USAGE_ERROR = 3


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


def _verdict_is_regular_file(path: Path) -> bool:
    return not path.is_symlink() and path.is_file()


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
            return 0
        raise ValueError(f"unsupported adapter command: {args.command}")
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"soup_hub_adapter: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
