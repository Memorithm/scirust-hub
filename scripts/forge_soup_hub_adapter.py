#!/usr/bin/env python3
"""SciRust Hub boundary adapter for Forge-driven SOUP post-training search."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
import sys
import tarfile
from typing import Any, Sequence

SCHEMA_VERSION = 1
FORGE_RUNNER_MERGE = "9e1f3fc568c176f401735c121780d9fbe6834f5d"
FORGE_DOMAIN_MERGE = "1385c71a541419f15a558a5e94bc8a4a60567a4a"
SOUP_QUALIFIED_COMMIT = "05b646523727925990530667e7012ede50bd30b2"
SOUP_REPOSITORY = "MakazhanAlpamys/Soup"
MAX_CAMPAIGN_BYTES = 1024 * 1024
MAX_EVIDENCE_FILES = 250_000
MAX_EVIDENCE_BYTES = 8 * 1024 * 1024 * 1024
MAX_EVALUATION_BUDGET = 100_000
REPORT_MEDIA_TYPE = "application/vnd.scirust-hub.forge-soup-campaign-report.v1+json"
EVIDENCE_MEDIA_TYPE = "application/vnd.scirust-hub.forge-soup-evidence.v1+tar"


def _regular_file(path: Path, label: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{label} must be a regular non-symlink file")


def _under(root: Path, path: Path, label: str) -> None:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError as exc:
        raise ValueError(f"{label} must remain inside the Hub run directory") from exc


def _parse_params(raw: str) -> dict[str, Any]:
    try:
        params = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Hub parameter JSON: {exc}") from exc
    if not isinstance(params, dict):
        raise ValueError("Hub parameters must be a JSON object")
    allowed = {"gpus", "trust_remote_code", "fewshot", "batch_size", "device"}
    unknown = sorted(set(params) - allowed)
    if unknown:
        raise ValueError(f"unsupported parameters: {', '.join(unknown)}")
    return params


def _load_campaign(path: Path) -> dict[str, Any]:
    _regular_file(path, "Forge campaign")
    if path.stat().st_size > MAX_CAMPAIGN_BYTES:
        raise ValueError(f"Forge campaign exceeds {MAX_CAMPAIGN_BYTES} bytes")
    try:
        campaign = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Forge campaign JSON: {exc}") from exc
    if not isinstance(campaign, dict) or campaign.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Forge campaign must be schema_version 1")
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
    environment = external.get("environment")
    if not isinstance(environment, dict):
        raise ValueError("campaign.external_domain.environment must be an object")
    if environment.get("isolation_required") is True:
        raise ValueError(
            "Forge/SOUP campaign requires external isolation, but this Hub v1 local-process edge does not provide it"
        )
    engine = campaign.get("engine")
    if not isinstance(engine, dict):
        raise ValueError("campaign.engine must be an object")
    generations = engine.get("generations")
    population = engine.get("population")
    if (
        not isinstance(generations, int)
        or isinstance(generations, bool)
        or generations < 1
        or not isinstance(population, int)
        or isinstance(population, bool)
        or population < 1
    ):
        raise ValueError("campaign engine generations/population must be positive integers")
    if generations * population > MAX_EVALUATION_BUDGET:
        raise ValueError(
            f"campaign generations*population exceeds Hub v1 evaluation budget {MAX_EVALUATION_BUDGET}"
        )
    return campaign


def _evaluator_argv(
    evaluator_script: Path,
    campaign: Path,
    config: Path,
    dataset: Path,
    evidence_dir: Path,
    params_raw: str,
    soup_bin: str,
) -> list[str]:
    return [
        str(evaluator_script),
        "--campaign",
        str(campaign),
        "--config",
        str(config),
        "--dataset",
        str(dataset),
        "--evidence-dir",
        str(evidence_dir),
        "--params",
        params_raw,
        "--soup-bin",
        soup_bin,
    ]


def _runner_command(
    forge_runner: Path,
    evaluator_script: Path,
    campaign: Path,
    config: Path,
    dataset: Path,
    evidence_dir: Path,
    report: Path,
    params_raw: str,
    soup_bin: str,
) -> list[str]:
    command = [
        str(forge_runner),
        "--campaign",
        str(campaign),
        "--evaluator",
        "/usr/bin/python3",
    ]
    for arg in _evaluator_argv(
        evaluator_script,
        campaign,
        config,
        dataset,
        evidence_dir,
        params_raw,
        soup_bin,
    ):
        command += ["--evaluator-arg", arg]
    command += ["--output", str(report)]
    return command


def _bundle_evidence(source: Path, destination: Path) -> tuple[int, int]:
    if source.is_symlink() or not source.is_dir():
        raise ValueError("evidence source must be a regular non-symlink directory")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.is_symlink():
        raise ValueError("evidence bundle output must not be a symlink")

    entries: list[Path] = []
    total_bytes = 0
    for entry in sorted(source.iterdir(), key=lambda path: path.name):
        if entry.is_symlink() or not entry.is_file():
            raise ValueError(f"evidence directory contains non-regular entry {entry.name!r}")
        if entry.suffix != ".json":
            raise ValueError(f"evidence directory contains unsupported entry {entry.name!r}")
        entries.append(entry)
        total_bytes += entry.stat().st_size
        if len(entries) > MAX_EVIDENCE_FILES:
            raise ValueError(f"evidence file count exceeds {MAX_EVIDENCE_FILES}")
        if total_bytes > MAX_EVIDENCE_BYTES:
            raise ValueError(f"evidence payload exceeds {MAX_EVIDENCE_BYTES} bytes")

    with tarfile.open(destination, mode="w", format=tarfile.PAX_FORMAT) as archive:
        root = tarfile.TarInfo("evidence")
        root.type = tarfile.DIRTYPE
        root.mode = 0o755
        root.uid = root.gid = 0
        root.uname = root.gname = ""
        root.mtime = 0
        archive.addfile(root)
        for entry in entries:
            relative = PurePosixPath("evidence") / entry.name
            info = tarfile.TarInfo(relative.as_posix())
            info.type = tarfile.REGTYPE
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.mtime = 0
            info.size = entry.stat().st_size
            with entry.open("rb") as handle:
                archive.addfile(info, handle)
    return len(entries), total_bytes


def _validate_report(path: Path, campaign: dict[str, Any]) -> None:
    _regular_file(path, "Forge campaign report")
    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid Forge campaign report JSON: {exc}") from exc
    if not isinstance(report, dict) or report.get("schema_version") != SCHEMA_VERSION:
        raise ValueError("Forge campaign report must be schema_version 1")
    if report.get("forge_domain_source_merge") != FORGE_DOMAIN_MERGE:
        raise ValueError("Forge campaign report source merge does not match the qualified SOUP domain")
    external = campaign["external_domain"]
    if report.get("domain_id") != external.get("domain_id"):
        raise ValueError("Forge campaign report domain_id does not match input campaign")
    upstream = external["upstream"]
    if report.get("upstream_repository") != upstream.get("repository"):
        raise ValueError("Forge campaign report upstream repository mismatch")
    if report.get("upstream_commit_id") != upstream.get("commit_id"):
        raise ValueError("Forge campaign report upstream commit mismatch")


def run_campaign(
    *,
    forge_runner: Path,
    evaluator_script: Path,
    soup_bin: str,
    campaign: Path,
    config: Path,
    dataset: Path,
    report: Path,
    evidence_bundle: Path,
    params_raw: str,
) -> None:
    _regular_file(forge_runner, "Forge SOUP runner")
    _regular_file(evaluator_script, "Forge SOUP evaluator")
    _regular_file(config, "SOUP config template")
    _regular_file(dataset, "SOUP dataset")
    params = _parse_params(params_raw)
    normalized_params = json.dumps(params, sort_keys=True, separators=(",", ":"))
    campaign_value = _load_campaign(campaign)

    workdir = Path.cwd().resolve()
    _under(workdir, report, "report output")
    _under(workdir, evidence_bundle, "evidence output")
    report.parent.mkdir(parents=True, exist_ok=True)
    evidence_bundle.parent.mkdir(parents=True, exist_ok=True)
    evidence_dir = workdir / ".hub-forge-soup-evidence"
    if evidence_dir.exists() or evidence_dir.is_symlink():
        raise ValueError(".hub-forge-soup-evidence must not exist before campaign execution")
    evidence_dir.mkdir()

    command = _runner_command(
        forge_runner,
        evaluator_script,
        campaign,
        config,
        dataset,
        evidence_dir,
        report,
        normalized_params,
        soup_bin,
    )
    completed = subprocess.run(
        command,
        cwd=workdir,
        stdin=subprocess.DEVNULL,
        stdout=sys.stdout,
        stderr=sys.stderr,
        shell=False,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"Forge SOUP runner failed with exit {completed.returncode}")
    _validate_report(report, campaign_value)
    file_count, payload_bytes = _bundle_evidence(evidence_dir, evidence_bundle)
    if file_count == 0:
        raise RuntimeError("Forge SOUP campaign produced no executed evidence")
    # Keep the evidence bundle self-describing through the Hub manifest and
    # provenance. The report itself remains Forge-owned and is not rewritten.
    if payload_bytes <= 0:
        raise RuntimeError("Forge SOUP evidence bundle is empty")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--forge-runner", default="/opt/scirust-hub/libexec/forge-soup-posttrain")
    parser.add_argument("--evaluator", default="/opt/scirust-hub/libexec/forge_soup_hub_evaluator.py")
    parser.add_argument("--soup-bin", default="soup")
    parser.add_argument("--campaign", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--report", required=True)
    parser.add_argument("--evidence-bundle", required=True)
    parser.add_argument("--params", default="{}")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_campaign(
            forge_runner=Path(args.forge_runner),
            evaluator_script=Path(args.evaluator),
            soup_bin=args.soup_bin,
            campaign=Path(args.campaign),
            config=Path(args.config),
            dataset=Path(args.dataset),
            report=Path(args.report),
            evidence_bundle=Path(args.evidence_bundle),
            params_raw=args.params,
        )
    except Exception as exc:
        print(f"forge-soup hub adapter: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
