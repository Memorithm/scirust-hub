from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import soup_scirust_symbolic_hub_adapter as adapter  # noqa: E402


class SoupSciRustSymbolicHubAdapterTests(unittest.TestCase):
    def _template(self) -> str:
        return (
            "base: fixture/base\n"
            f"task: {adapter.SCIRUST_TASK_TOKEN}\n"
            "data:\n"
            f"  train: {adapter.base.DATASET_TOKEN}\n"
            "training:\n"
            "  epochs: 1\n"
            f"  {adapter.SCIRUST_REWARD_TOKEN}\n"
            "  num_generations: 2\n"
            "  lora:\n"
            "    r: 8\n"
            f"output: {adapter.base.OUTPUT_TOKEN}\n"
        )

    def _dependencies(self, tmp: Path) -> tuple[Path, Path]:
        bridge = tmp / "scirust_symbolic_reward.py"
        bridge.write_text("def reward_fn(completions, **kwargs): return [0.0] * len(completions)\n")
        binary = tmp / "scirust-reward"
        binary.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        binary.chmod(0o755)
        return bridge, binary

    def test_template_materializes_only_fixed_grpo_and_reward_seams(self) -> None:
        rendered = adapter.prepare_scirust_symbolic_template(
            self._template(),
            reward_bridge=Path("/trusted/scirust_symbolic_reward.py"),
        )
        self.assertIn('task: "grpo"', rendered)
        self.assertIn(
            '  reward_fn: "/trusted/scirust_symbolic_reward.py"', rendered
        )
        self.assertIn("  epochs: 1", rendered)
        self.assertIn("  num_generations: 2", rendered)
        self.assertIn("    r: 8", rendered)
        self.assertIn(adapter.base.DATASET_TOKEN, rendered)
        self.assertIn(adapter.base.OUTPUT_TOKEN, rendered)
        self.assertNotIn(adapter.SCIRUST_TASK_TOKEN, rendered)
        self.assertNotIn(adapter.SCIRUST_REWARD_TOKEN, rendered)

    def test_missing_misindented_or_duplicate_semantic_seams_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "exactly once"):
            adapter.prepare_scirust_symbolic_template(
                self._template().replace(adapter.SCIRUST_REWARD_TOKEN, "removed"),
                reward_bridge=Path("/trusted/reward.py"),
            )
        with self.assertRaisesRegex(ValueError, "root line"):
            adapter.prepare_scirust_symbolic_template(
                self._template().replace(
                    f"task: {adapter.SCIRUST_TASK_TOKEN}",
                    f"  task: {adapter.SCIRUST_TASK_TOKEN}",
                ),
                reward_bridge=Path("/trusted/reward.py"),
            )
        with self.assertRaisesRegex(ValueError, "two-space-indented"):
            adapter.prepare_scirust_symbolic_template(
                self._template().replace(
                    f"  {adapter.SCIRUST_REWARD_TOKEN}",
                    f"    {adapter.SCIRUST_REWARD_TOKEN}",
                ),
                reward_bridge=Path("/trusted/reward.py"),
            )
        with self.assertRaisesRegex(ValueError, "another reward_fn"):
            adapter.prepare_scirust_symbolic_template(
                self._template().replace(
                    "  epochs: 1",
                    "  epochs: 1\n  reward_fn: ./attacker.py",
                ),
                reward_bridge=Path("/trusted/reward.py"),
            )
        with self.assertRaisesRegex(ValueError, "another root task"):
            adapter.prepare_scirust_symbolic_template(
                self._template().replace(
                    "base: fixture/base",
                    "base: fixture/base\ntask: sft",
                ),
                reward_bridge=Path("/trusted/reward.py"),
            )

    def test_installation_rejects_missing_symlink_and_non_executable_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bridge, binary = self._dependencies(tmp)
            adapter.validate_scirust_reward_installation(
                reward_bridge=bridge,
                reward_bin=binary,
            )

            binary.chmod(0o644)
            with self.assertRaisesRegex(ValueError, "executable"):
                adapter.validate_scirust_reward_installation(
                    reward_bridge=bridge,
                    reward_bin=binary,
                )

            if hasattr(os, "symlink"):
                binary.chmod(0o755)
                link = tmp / "reward-link.py"
                try:
                    link.symlink_to(bridge)
                except OSError:
                    pass
                else:
                    with self.assertRaisesRegex(ValueError, "symbolic link"):
                        adapter.validate_scirust_reward_installation(
                            reward_bridge=link,
                            reward_bin=binary,
                        )

    def _fake_soup(self, directory: Path, expected_bridge: Path, expected_binary: Path) -> Path:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        fake = directory / "fake-soup"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import os, sys\n"
            "argv = sys.argv[1:]\n"
            "if argv[0] != 'train': raise SystemExit(3)\n"
            "cfg = Path(argv[argv.index('--config') + 1]).read_text(encoding='utf-8')\n"
            "assert 'task: \"grpo\"' in cfg\n"
            f"assert '  reward_fn: \"{expected_bridge}\"' in cfg\n"
            f"assert os.environ.get('SCIRUST_REWARD_BIN') == '{expected_binary}'\n"
            "assert '${SOUP_HUB_SCIRUST_SYMBOLIC_TASK}' not in cfg\n"
            "assert '${SOUP_HUB_SCIRUST_SYMBOLIC_REWARD}' not in cfg\n"
            "output_line = next(line for line in cfg.splitlines() if line.startswith('output:'))\n"
            "output = Path(output_line.split(':', 1)[1].strip().strip('\\\"\\\''))\n"
            "output.mkdir(parents=True, exist_ok=False)\n"
            "(output / 'adapter_config.json').write_text('{}\\n', encoding='utf-8')\n"
            "(output / 'adapter_model.safetensors').write_bytes(b'weights')\n"
            "print('scirust-symbolic-train-ok')\n"
            "raise SystemExit(0)\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_training_delegates_to_base_adapter_with_pinned_reward_environment(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bridge, binary = self._dependencies(tmp)
            fake_soup = self._fake_soup(tmp, bridge, binary)
            inputs = tmp / "inputs"
            inputs.mkdir()
            config = inputs / "soup.yaml"
            dataset = inputs / "train.jsonl"
            config.write_text(self._template(), encoding="utf-8")
            dataset.write_text(
                '{"prompt":"simplify x+x","answer":"2*x"}\n',
                encoding="utf-8",
            )
            bundle = tmp / "outputs" / "model.tar"
            report = tmp / "outputs" / "report.json"

            sentinel = "restore-me"
            previous = os.environ.get("SCIRUST_REWARD_BIN")
            os.environ["SCIRUST_REWARD_BIN"] = sentinel
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_train_scirust_symbolic(
                    soup_bin=str(fake_soup),
                    config=config,
                    dataset=dataset,
                    bundle=bundle,
                    report=report,
                    params_raw='{"trust_remote_code":false}',
                    reward_bridge=bridge,
                    reward_bin=binary,
                )
                self.assertEqual(os.environ.get("SCIRUST_REWARD_BIN"), sentinel)
            finally:
                os.chdir(old_cwd)
                if previous is None:
                    os.environ.pop("SCIRUST_REWARD_BIN", None)
                else:
                    os.environ["SCIRUST_REWARD_BIN"] = previous

            self.assertTrue(bundle.is_file())
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "train")
            self.assertIn("scirust-symbolic-train-ok", payload["stdout"])
            reward = payload["scirust_symbolic_reward"]
            self.assertEqual(reward["source_merge"], adapter.SCIRUST_SOURCE_MERGE)
            self.assertEqual(reward["schema_version"], 1)
            self.assertEqual(reward["kind"], "symbolic_equivalence")
            self.assertEqual(reward["task"], "grpo")


if __name__ == "__main__":
    unittest.main()
