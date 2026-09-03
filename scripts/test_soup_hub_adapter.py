from __future__ import annotations

import io
import json
from pathlib import Path
import os
import sys
import tarfile
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import soup_hub_adapter as adapter  # noqa: E402


class SoupHubAdapterTests(unittest.TestCase):
    def test_ship_and_dont_ship_are_semantic_successes_when_verdict_exists(self) -> None:
        self.assertEqual(adapter.classify_ship_exit(0, True), (True, ""))
        self.assertEqual(adapter.classify_ship_exit(2, True), (True, ""))

    def test_semantic_exit_without_verdict_fails_closed(self) -> None:
        ok, message = adapter.classify_ship_exit(2, False)
        self.assertFalse(ok)
        self.assertIn("no verdict artifact", message)

    def test_runtime_usage_signal_and_unknown_exits_are_failures(self) -> None:
        for code in (1, 3, -9, 7):
            with self.subTest(code=code):
                ok, message = adapter.classify_ship_exit(code, True)
                self.assertFalse(ok)
                self.assertTrue(message)

    def _fake_soup(self, directory: Path) -> Path:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        fake = directory / "fake-soup"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys\n"
            "argv = sys.argv[1:]\n"
            "if argv[0] == 'ship':\n"
            "    out = Path(argv[argv.index('--output') + 1])\n"
            "    out.parent.mkdir(parents=True, exist_ok=True)\n"
            "    out.write_text('{\\\"decision\\\":\\\"DONT_SHIP\\\"}\\n', encoding='utf-8')\n"
            "    raise SystemExit(2)\n"
            "if argv[0] == 'train':\n"
            "    cfg = Path(argv[argv.index('--config') + 1]).read_text(encoding='utf-8')\n"
            "    output_line = next(line for line in cfg.splitlines() if line.startswith('output:'))\n"
            "    output = Path(output_line.split(':', 1)[1].strip().strip('\\\"\\\''))\n"
            "    output.mkdir(parents=True, exist_ok=False)\n"
            "    (output / 'adapter_config.json').write_text('{\\\"base_model_name_or_path\\\":\\\"fixture/base\\\"}\\n', encoding='utf-8')\n"
            "    (output / 'adapter_model.safetensors').write_bytes(b'weights')\n"
            "    print('train-ok')\n"
            "    raise SystemExit(0)\n"
            "if argv[:2] == ['eval', 'benchmark']:\n"
            "    model = Path(argv[argv.index('--model') + 1])\n"
            "    assert (model / 'adapter_config.json').is_file()\n"
            "    print('fixture_accuracy=0.75')\n"
            "    raise SystemExit(0)\n"
            "if argv[0] == 'export':\n"
            "    model = Path(argv[argv.index('--model') + 1])\n"
            "    assert (model / 'adapter_config.json').is_file()\n"
            "    output = Path(argv[argv.index('--output') + 1])\n"
            "    output.mkdir(parents=True, exist_ok=False)\n"
            "    (output / 'model.gguf').write_bytes(b'gguf-fixture')\n"
            "    print('export-ok')\n"
            "    raise SystemExit(0)\n"
            "raise SystemExit(3)\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_dont_ship_process_with_verdict_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            evidence = tmp / "evidence.json"
            evidence.write_text('{"schema":"fixture"}\n', encoding="utf-8")
            verdict = tmp / "out" / "verdict.json"
            fake_soup = self._fake_soup(tmp)

            adapter.run_ship_offline(
                soup_bin=str(fake_soup), evidence=evidence, verdict=verdict
            )
            self.assertTrue(verdict.is_file())
            self.assertIn("DONT_SHIP", verdict.read_text(encoding="utf-8"))

    def test_usage_error_remains_failure(self) -> None:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            evidence = tmp / "evidence.json"
            evidence.write_text("{}\n", encoding="utf-8")
            fake_soup = tmp / "fake-soup"
            fake_soup.write_text(
                "#!/usr/bin/env python3\nraise SystemExit(3)\n", encoding="utf-8"
            )
            fake_soup.chmod(0o755)

            with self.assertRaisesRegex(RuntimeError, "exit 3"):
                adapter.run_ship_offline(
                    soup_bin=str(fake_soup),
                    evidence=evidence,
                    verdict=tmp / "verdict.json",
                )

    def test_symlink_evidence_is_rejected(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks unavailable")
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            target = tmp / "target.json"
            target.write_text("{}\n", encoding="utf-8")
            link = tmp / "evidence.json"
            try:
                link.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlink creation unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                adapter.run_ship_offline(
                    soup_bin="unused",
                    evidence=link,
                    verdict=tmp / "verdict.json",
                )

    def test_deterministic_bundle_is_byte_identical_and_round_trips(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            source = tmp / "source"
            (source / "nested").mkdir(parents=True)
            (source / "adapter_config.json").write_text("{}\n", encoding="utf-8")
            (source / "nested" / "weights.bin").write_bytes(b"abc" * 100)
            first = tmp / "first.tar"
            second = tmp / "second.tar"
            adapter.create_deterministic_bundle(source, first)
            os.utime(source / "nested" / "weights.bin", (123456789, 123456789))
            adapter.create_deterministic_bundle(source, second)
            self.assertEqual(first.read_bytes(), second.read_bytes())

            extracted = adapter.extract_deterministic_bundle(first, tmp / "extracted")
            self.assertEqual((extracted / "adapter_config.json").read_text(), "{}\n")
            self.assertEqual((extracted / "nested" / "weights.bin").read_bytes(), b"abc" * 100)

    def test_bundle_rejects_symbolic_links(self) -> None:
        if not hasattr(os, "symlink"):
            self.skipTest("symlinks unavailable")
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            source = tmp / "source"
            source.mkdir()
            target = source / "target"
            target.write_text("x", encoding="utf-8")
            link = source / "link"
            try:
                link.symlink_to(target)
            except OSError as exc:
                self.skipTest(f"symlink creation unavailable: {exc}")
            with self.assertRaisesRegex(ValueError, "symbolic link"):
                adapter.create_deterministic_bundle(source, tmp / "bundle.tar")

    def test_bundle_extraction_rejects_parent_traversal(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = tmp / "bad.tar"
            with tarfile.open(bundle, "w") as archive:
                info = tarfile.TarInfo("artifact/../escape.txt")
                payload = b"escape"
                info.size = len(payload)
                archive.addfile(info, io.BytesIO(payload))
            with self.assertRaisesRegex(ValueError, "unsafe path"):
                adapter.extract_deterministic_bundle(bundle, tmp / "extract")
            self.assertFalse((tmp / "escape.txt").exists())

    def test_train_materializes_hub_paths_and_emits_model_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            fake_soup = self._fake_soup(tmp)
            inputs = tmp / "inputs"
            inputs.mkdir()
            config = inputs / "soup.yaml"
            dataset = inputs / "train.jsonl"
            dataset.write_text('{"prompt":"p","response":"r"}\n', encoding="utf-8")
            config.write_text(
                "base: fixture/base\n"
                "task: sft\n"
                "data:\n"
                f"  train: {adapter.DATASET_TOKEN}\n"
                f"output: {adapter.OUTPUT_TOKEN}\n",
                encoding="utf-8",
            )
            bundle = tmp / "outputs" / "model.tar"
            report = tmp / "outputs" / "train.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_train(
                    soup_bin=str(fake_soup),
                    config=config,
                    dataset=dataset,
                    bundle=bundle,
                    report=report,
                    params_raw='{"gpus":1,"trust_remote_code":false}',
                )
            finally:
                os.chdir(old_cwd)
            self.assertTrue(bundle.is_file())
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "train")
            self.assertIn("train-ok", payload["stdout"])
            extracted = adapter.extract_deterministic_bundle(bundle, tmp / "verify-model")
            self.assertTrue((extracted / "adapter_config.json").is_file())
            self.assertEqual((extracted / "adapter_model.safetensors").read_bytes(), b"weights")

    def test_train_requires_dataset_and_output_placeholders(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            config = tmp / "soup.yaml"
            dataset = tmp / "train.jsonl"
            dataset.write_text("{}\n", encoding="utf-8")
            config.write_text("base: fixture\noutput: out\n", encoding="utf-8")
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "SOUP_HUB_DATASET"):
                    adapter.run_train(
                        soup_bin="unused",
                        config=config,
                        dataset=dataset,
                        bundle=tmp / "bundle.tar",
                        report=tmp / "report.json",
                        params_raw="{}",
                    )
            finally:
                os.chdir(old_cwd)

    def _fixture_model_bundle(self, tmp: Path) -> Path:
        model = tmp / "model"
        model.mkdir()
        (model / "adapter_config.json").write_text(
            '{"base_model_name_or_path":"fixture/base"}\n', encoding="utf-8"
        )
        (model / "adapter_model.safetensors").write_bytes(b"weights")
        bundle = tmp / "model.tar"
        adapter.create_deterministic_bundle(model, bundle)
        return bundle

    def test_eval_bundle_invokes_soup_and_writes_result(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            fake_soup = self._fake_soup(tmp)
            bundle = self._fixture_model_bundle(tmp)
            result = tmp / "outputs" / "eval.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_eval(
                    soup_bin=str(fake_soup),
                    bundle=bundle,
                    result=result,
                    params_raw='{"benchmarks":"mmlu,gsm8k","batch_size":4,"device":"cpu"}',
                )
            finally:
                os.chdir(old_cwd)
            payload = json.loads(result.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "eval")
            self.assertIn("fixture_accuracy=0.75", payload["stdout"])

    def test_export_bundle_invokes_soup_and_rebundles_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            fake_soup = self._fake_soup(tmp)
            bundle = self._fixture_model_bundle(tmp)
            exported = tmp / "outputs" / "export.tar"
            report = tmp / "outputs" / "export.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_export(
                    soup_bin=str(fake_soup),
                    bundle=bundle,
                    exported_bundle=exported,
                    report=report,
                    params_raw='{"format":"gguf"}',
                )
            finally:
                os.chdir(old_cwd)
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "export")
            self.assertIn("export-ok", payload["stdout"])
            extracted = adapter.extract_deterministic_bundle(exported, tmp / "verify-export")
            self.assertEqual((extracted / "model.gguf").read_bytes(), b"gguf-fixture")

    def test_unknown_parameters_fail_closed(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported parameters"):
            adapter._parse_params('{"shell":"rm -rf /"}', {"device"})


if __name__ == "__main__":
    unittest.main()
