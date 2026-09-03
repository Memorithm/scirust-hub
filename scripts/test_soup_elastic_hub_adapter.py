from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import soup_elastic_hub_adapter as adapter  # noqa: E402


class SoupElasticHubAdapterTests(unittest.TestCase):
    def _plan(self, **overrides):
        plan = {
            "contract": adapter.ELASTIC_SOUP_PLAN_CONTRACT,
            "upstream_commit": adapter.QUALIFIED_SOUP_COMMIT,
            "task": "sft",
            "batch_size": {"mode": "fixed", "value": 2},
            "auto_batch_strategy": "auto",
            "streaming": None,
        }
        plan.update(overrides)
        return plan

    def _write_plan(self, path: Path, plan: dict) -> None:
        path.write_text(json.dumps(plan) + "\n", encoding="utf-8")

    def test_valid_wire_plan_is_canonicalized(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            path = Path(raw_tmp) / "plan.json"
            self._write_plan(path, self._plan(task="  sft  "))
            plan = adapter.load_elastic_soup_resource_plan(path)
            self.assertEqual(plan["task"], "sft")
            self.assertEqual(plan["batch_size"], {"mode": "fixed", "value": 2})

    def test_duplicate_and_unknown_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            duplicate = tmp / "duplicate.json"
            duplicate.write_text(
                '{"contract":"elastic.soup.run-resource-plan@1.0.0",'
                '"contract":"elastic.soup.run-resource-plan@1.0.0",'
                '"upstream_commit":"05b646523727925990530667e7012ede50bd30b2",'
                '"task":"sft","batch_size":{"mode":"auto"},'
                '"auto_batch_strategy":"auto","streaming":null}\n',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "duplicate JSON field"):
                adapter.load_elastic_soup_resource_plan(duplicate)

            unknown = tmp / "unknown.json"
            payload = self._plan()
            payload["scheduler_policy"] = "hub-decides"
            self._write_plan(unknown, payload)
            with self.assertRaisesRegex(ValueError, "unknown"):
                adapter.load_elastic_soup_resource_plan(unknown)

    def test_nested_wire_shapes_and_numeric_bounds_fail_closed(self) -> None:
        cases = [
            self._plan(batch_size={"mode": "auto", "value": 1}),
            self._plan(batch_size={"mode": "fixed", "value": 0}),
            self._plan(batch_size={"mode": "fixed", "value": True}),
            self._plan(batch_size={"mode": "fixed", "value": 2**32}),
            self._plan(streaming={"source": "ram", "buffers": 1}),
            self._plan(streaming={"source": "ram", "buffers": True}),
            self._plan(streaming={"source": "tape", "buffers": 2}),
            self._plan(auto_batch_strategy="future"),
        ]
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            for index, payload in enumerate(cases):
                with self.subTest(index=index):
                    path = tmp / f"case-{index}.json"
                    self._write_plan(path, payload)
                    with self.assertRaises(ValueError):
                        adapter.load_elastic_soup_resource_plan(path)

    def test_contract_revision_and_streaming_task_are_revalidated(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            cases = [
                self._plan(contract="future"),
                self._plan(upstream_commit="future"),
                self._plan(
                    task="grpo",
                    streaming={"source": "ram", "buffers": 2},
                ),
            ]
            for index, payload in enumerate(cases):
                with self.subTest(index=index):
                    path = tmp / f"case-{index}.json"
                    self._write_plan(path, payload)
                    with self.assertRaises(ValueError):
                        adapter.load_elastic_soup_resource_plan(path)

    def test_resource_block_is_deterministic_for_resident_and_streamed_plans(self) -> None:
        resident = self._plan(batch_size={"mode": "auto"}, auto_batch_strategy="probe")
        self.assertEqual(
            adapter.render_resource_block(resident),
            [
                "batch_size: auto",
                "auto_batch_size_strategy: probe",
                "stream_layers: false",
            ],
        )
        streamed = self._plan(
            streaming={"source": "disk", "buffers": 8},
            auto_batch_strategy="static",
        )
        self.assertEqual(
            adapter.render_resource_block(streamed),
            [
                "batch_size: 2",
                "auto_batch_size_strategy: static",
                "stream_layers: true",
                "stream_source: disk",
                "stream_buffers: 8",
            ],
        )

    def test_template_seams_are_exact_and_no_general_yaml_rewrite_occurs(self) -> None:
        template = (
            "base: fixture/base\n"
            f"task: {adapter.RESOURCE_TASK_TOKEN}\n"
            "data:\n"
            f"  train: {adapter.base.DATASET_TOKEN}\n"
            "training:\n"
            "  epochs: 7\n"
            f"  {adapter.RESOURCE_PLAN_TOKEN}\n"
            "  lora:\n"
            "    r: 8\n"
            f"output: {adapter.base.OUTPUT_TOKEN}\n"
        )
        rendered = adapter.prepare_elastic_soup_template(template, self._plan())
        self.assertIn('task: "sft"', rendered)
        self.assertIn("  batch_size: 2", rendered)
        self.assertIn("  auto_batch_size_strategy: auto", rendered)
        self.assertIn("  stream_layers: false", rendered)
        self.assertIn("  epochs: 7", rendered)
        self.assertIn("    r: 8", rendered)
        self.assertIn(adapter.base.DATASET_TOKEN, rendered)
        self.assertIn(adapter.base.OUTPUT_TOKEN, rendered)
        self.assertNotIn(adapter.RESOURCE_TASK_TOKEN, rendered)
        self.assertNotIn(adapter.RESOURCE_PLAN_TOKEN, rendered)

        with self.assertRaisesRegex(ValueError, "root line"):
            adapter.prepare_elastic_soup_template(
                template.replace(
                    f"task: {adapter.RESOURCE_TASK_TOKEN}",
                    f"  task: {adapter.RESOURCE_TASK_TOKEN}",
                ),
                self._plan(),
            )
        with self.assertRaisesRegex(ValueError, "two-space-indented"):
            adapter.prepare_elastic_soup_template(
                template.replace(
                    f"  {adapter.RESOURCE_PLAN_TOKEN}",
                    f"    {adapter.RESOURCE_PLAN_TOKEN}",
                ),
                self._plan(),
            )

    def _fake_soup(self, directory: Path) -> Path:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        fake = directory / "fake-soup"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys\n"
            "argv = sys.argv[1:]\n"
            "if argv[0] != 'train': raise SystemExit(3)\n"
            "cfg = Path(argv[argv.index('--config') + 1]).read_text(encoding='utf-8')\n"
            "assert 'task: \"sft\"' in cfg\n"
            "assert '  batch_size: 2' in cfg\n"
            "assert '  auto_batch_size_strategy: auto' in cfg\n"
            "assert '  stream_layers: false' in cfg\n"
            "assert '${SOUP_HUB_RESOURCE_PLAN}' not in cfg\n"
            "assert '${SOUP_HUB_RESOURCE_TASK}' not in cfg\n"
            "output_line = next(line for line in cfg.splitlines() if line.startswith('output:'))\n"
            "output = Path(output_line.split(':', 1)[1].strip().strip('\\\"\\\''))\n"
            "output.mkdir(parents=True, exist_ok=False)\n"
            "(output / 'adapter_config.json').write_text('{}\\n', encoding='utf-8')\n"
            "(output / 'adapter_model.safetensors').write_bytes(b'weights')\n"
            "print('elastic-train-ok')\n"
            "raise SystemExit(0)\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_train_elastic_delegates_to_existing_soup_adapter_and_annotates_report(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            fake_soup = self._fake_soup(tmp)
            inputs = tmp / "inputs"
            inputs.mkdir()
            config = inputs / "soup.yaml"
            dataset = inputs / "train.jsonl"
            resource_plan = inputs / "elastic-plan.json"
            dataset.write_text('{"prompt":"p","response":"r"}\n', encoding="utf-8")
            config.write_text(
                "base: fixture/base\n"
                f"task: {adapter.RESOURCE_TASK_TOKEN}\n"
                "data:\n"
                f"  train: {adapter.base.DATASET_TOKEN}\n"
                "training:\n"
                f"  {adapter.RESOURCE_PLAN_TOKEN}\n"
                "  lora:\n"
                "    r: 8\n"
                f"output: {adapter.base.OUTPUT_TOKEN}\n",
                encoding="utf-8",
            )
            self._write_plan(resource_plan, self._plan())
            bundle = tmp / "outputs" / "model.tar"
            report = tmp / "outputs" / "report.json"

            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_train_elastic(
                    soup_bin=str(fake_soup),
                    config=config,
                    dataset=dataset,
                    resource_plan=resource_plan,
                    bundle=bundle,
                    report=report,
                    params_raw='{"trust_remote_code":false}',
                )
            finally:
                os.chdir(old_cwd)

            self.assertTrue(bundle.is_file())
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "train")
            self.assertIn("elastic-train-ok", payload["stdout"])
            self.assertEqual(
                payload["elastic_resource_plan"]["contract"],
                adapter.ELASTIC_SOUP_PLAN_CONTRACT,
            )
            self.assertEqual(
                payload["elastic_resource_plan"]["elastic_source_merge"],
                adapter.ELASTIC_SOUP_SOURCE_MERGE,
            )

    def test_resource_plan_symlink_is_rejected(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks unavailable")
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            target = tmp / "plan-target.json"
            self._write_plan(target, self._plan())
            link = tmp / "plan.json"
            try:
                link.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlink creation unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                adapter.load_elastic_soup_resource_plan(link)


if __name__ == "__main__":
    unittest.main()
