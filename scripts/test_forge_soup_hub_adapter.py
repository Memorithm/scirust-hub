from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sqlite3
import tarfile
import tempfile
import unittest


def _load(name: str, filename: str):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hub = _load("forge_soup_hub_adapter", "forge_soup_hub_adapter.py")
evaluator = _load("forge_soup_hub_evaluator", "forge_soup_hub_evaluator.py")


class ForgeSoupHubContractTests(unittest.TestCase):
    def campaign(self, *, isolation_required: bool = False) -> dict:
        return {
            "schema_version": 1,
            "external_domain": {
                "schema_version": 1,
                "domain_id": "soup/posttrain-v1",
                "upstream": {
                    "repository": evaluator.SOUP_REPOSITORY,
                    "commit_id": evaluator.SOUP_QUALIFIED_COMMIT,
                    "contract_sha256": "a" * 64,
                },
                "allowed_candidate_dimensions": ["recipe.learning_rate"],
                "data_boundary": {
                    "generation_sources": ["train"],
                    "verification_sources": ["validation"],
                    "final_holdout_sources": ["holdout"],
                },
                "verification": {"adapter_id": "hub/forge-soup-v1", "adapter_sha256": "b" * 64},
                "objectives": [
                    {"name": "benchmark:mmlu", "direction": "maximize"},
                    {"name": "train_wall_ms", "direction": "minimize"},
                ],
                "environment": {
                    "fingerprint_required": True,
                    "isolation_required": isolation_required,
                },
            },
            "dimensions": {"recipe.learning_rate": ["1e-5", "2e-5"]},
            "baseline": {"recipe.learning_rate": "1e-5"},
            "engine": {"generations": 2, "population": 4, "survivors": 2, "base_seed": 7},
        }

    def request(self) -> dict:
        return {
            "schema_version": 1,
            "phase": "measure",
            "domain_id": "soup/posttrain-v1",
            "candidate_id": 42,
            "candidate": {"values": {"recipe.learning_rate": "2e-5"}},
            "generation": 1,
            "trial_seed": 9,
        }

    def test_campaign_contract_derives_only_executed_objectives(self):
        objectives, benchmarks = evaluator._campaign_contract(self.campaign(), self.request())
        self.assertEqual(objectives, ["benchmark:mmlu", "train_wall_ms"])
        self.assertEqual(benchmarks, ["mmlu"])

        bad = self.campaign()
        bad["external_domain"]["objectives"] = [{"name": "peak_vram_bytes", "direction": "minimize"}]
        with self.assertRaisesRegex(ValueError, "unsupported Forge/SOUP objective"):
            evaluator._campaign_contract(bad, self.request())

    def test_verify_command_is_real_soup_dry_run(self):
        command = evaluator._train_command(
            "soup",
            Path("candidate.yaml"),
            {"gpus": 1, "trust_remote_code": False},
            dry_run=True,
        )
        self.assertEqual(command[:4], ["soup", "train", "--config", "candidate.yaml"])
        self.assertIn("--dry-run", command)
        self.assertIn("--yes", command)
        self.assertNotIn("--trust-remote-code", command)

    def test_materialization_requires_all_candidate_tokens(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            config = root / "template.yaml"
            dataset = root / "data.jsonl"
            output = root / "model"
            staged = root / "candidate.yaml"
            dataset.write_text("{}\n", encoding="utf-8")
            config.write_text(
                "data:\n  path: ${SOUP_HUB_DATASET}\noutput: ${SOUP_HUB_OUTPUT}\ntraining:\n  learning_rate: ${FORGE_SOUP:recipe.learning_rate}\n",
                encoding="utf-8",
            )
            digest = evaluator._materialize_config(
                config,
                dataset,
                output,
                {"recipe.learning_rate": "2e-5"},
                staged,
            )
            text = staged.read_text(encoding="utf-8")
            self.assertIn("learning_rate: 2e-5", text)
            self.assertNotIn("FORGE_SOUP", text)
            self.assertEqual(len(digest), 64)

    def test_benchmark_scores_are_read_from_isolated_soup_tracker(self):
        with tempfile.TemporaryDirectory() as raw:
            db = Path(raw) / "experiments.db"
            conn = sqlite3.connect(db)
            conn.execute(
                "CREATE TABLE eval_results (id INTEGER PRIMARY KEY AUTOINCREMENT, benchmark TEXT, score REAL, details_json TEXT)"
            )
            conn.execute(
                "INSERT INTO eval_results (benchmark, score, details_json) VALUES (?, ?, ?)",
                ("mmlu", 0.625, json.dumps({"acc,none": 0.625})),
            )
            conn.commit()
            conn.close()
            metrics, details = evaluator._benchmark_scores(db, ["mmlu"])
            self.assertEqual(metrics, {"benchmark:mmlu": 0.625})
            self.assertEqual(details["mmlu"]["acc,none"], 0.625)

    def test_hub_v1_refuses_campaigns_that_claim_external_isolation(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "campaign.json"
            path.write_text(json.dumps(self.campaign(isolation_required=True)), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "does not provide it"):
                hub._load_campaign(path)

    def test_runner_command_never_asserts_isolation_available(self):
        command = hub._runner_command(
            Path("/opt/scirust-hub/libexec/forge-soup-posttrain"),
            Path("/opt/scirust-hub/libexec/forge_soup_hub_evaluator.py"),
            Path("campaign.json"),
            Path("config.yaml"),
            Path("dataset.jsonl"),
            Path("evidence"),
            Path("report.json"),
            "{}",
            "soup",
        )
        self.assertNotIn("--isolation-available", command)
        self.assertEqual(command[0], "/opt/scirust-hub/libexec/forge-soup-posttrain")
        self.assertIn("/usr/bin/python3", command)

    def test_evidence_bundle_is_deterministic_and_regular_only(self):
        with tempfile.TemporaryDirectory() as raw:
            root = Path(raw)
            evidence = root / "evidence"
            evidence.mkdir()
            (evidence / "b.json").write_text('{"b":2}\n', encoding="utf-8")
            (evidence / "a.json").write_text('{"a":1}\n', encoding="utf-8")
            first = root / "first.tar"
            second = root / "second.tar"
            count, size = hub._bundle_evidence(evidence, first)
            hub._bundle_evidence(evidence, second)
            self.assertEqual(count, 2)
            self.assertGreater(size, 0)
            self.assertEqual(first.read_bytes(), second.read_bytes())
            with tarfile.open(first, "r:") as archive:
                self.assertEqual(archive.getnames(), ["evidence", "evidence/a.json", "evidence/b.json"])


if __name__ == "__main__":
    unittest.main()
