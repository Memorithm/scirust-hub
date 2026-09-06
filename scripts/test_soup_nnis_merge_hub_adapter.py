from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import soup_hub_adapter  # noqa: E402
import soup_nnis_merge_hub_adapter as adapter  # noqa: E402


class SoupNnisMergeHubAdapterTests(unittest.TestCase):
    def _bundle(self, tmp: Path, *, adapter: bool = True) -> Path:
        source = tmp / "source"
        source.mkdir()
        if adapter:
            (source / "adapter_config.json").write_text(
                '{"base_model_name_or_path":"fixture/base"}\n', encoding="utf-8"
            )
            (source / "adapter_model.safetensors").write_bytes(b"adapter")
        else:
            (source / "config.json").write_text('{"model_type":"fixture"}\n', encoding="utf-8")
            (source / "model.safetensors").write_bytes(b"dense")
        bundle = tmp / "input.tar"
        soup_hub_adapter.create_deterministic_bundle(source, bundle)
        return bundle

    def _fake_soup(self, tmp: Path, *, exit_code: int = 0) -> Path:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        fake = tmp / "fake-soup"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys\n"
            "argv = sys.argv[1:]\n"
            "assert argv[0] == 'merge'\n"
            "adapter = Path(argv[argv.index('--adapter') + 1])\n"
            "assert (adapter / 'adapter_config.json').is_file()\n"
            "assert argv[argv.index('--dtype') + 1] == 'float32'\n"
            "assert argv[argv.index('--save-format') + 1] == 'fp16'\n"
            "assert argv[argv.index('--hub') + 1] == 'hf'\n"
            "assert '--base' not in argv\n"
            "assert '--trust-remote-code' not in argv\n"
            "output = Path(argv[argv.index('--output') + 1])\n"
            "output.mkdir(parents=True)\n"
            "(output / 'config.json').write_text('{\\\"model_type\\\":\\\"fixture\\\",\\\"torch_dtype\\\":\\\"float32\\\"}\\n', encoding='utf-8')\n"
            "(output / 'tokenizer.json').write_text('{}\\n', encoding='utf-8')\n"
            "(output / 'model.safetensors').write_bytes(b'dense-f32-fixture')\n"
            "print('fake merge complete')\n"
            f"raise SystemExit({exit_code})\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake

    def test_merge_delegates_fixed_f32_policy_and_bundles_exact_output(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            input_bundle = self._bundle(tmp)
            fake = self._fake_soup(tmp)
            merged_bundle = tmp / "outputs" / "merged.tar"
            report = tmp / "outputs" / "merge-report.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_merge(
                    soup_bin=str(fake),
                    adapter_bundle=input_bundle,
                    merged_bundle=merged_bundle,
                    report=report,
                    params_raw="{}",
                )
                extracted = soup_hub_adapter.extract_deterministic_bundle(
                    merged_bundle, tmp / "verify"
                )
            finally:
                os.chdir(old_cwd)

            self.assertEqual((extracted / "config.json").read_text(encoding="utf-8"),
                             '{"model_type":"fixture","torch_dtype":"float32"}\n')
            self.assertEqual((extracted / "model.safetensors").read_bytes(), b"dense-f32-fixture")
            payload = json.loads(report.read_text(encoding="utf-8"))
            self.assertEqual(payload["operation"], "merge_nnis_dense_f32")
            self.assertEqual(payload["requested_soup_merge"]["dtype"], "float32")
            self.assertEqual(payload["requested_soup_merge"]["save_format"], "fp16")
            self.assertEqual(payload["requested_soup_merge"]["hub"], "hf")
            self.assertFalse(payload["requested_soup_merge"]["trust_remote_code"])
            self.assertIsNone(payload["requested_soup_merge"]["base_override"])

    def test_dense_model_bundle_is_not_misrepresented_as_lora_adapter(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            input_bundle = self._bundle(tmp, adapter=False)
            merged_bundle = tmp / "outputs" / "merged.tar"
            report = tmp / "outputs" / "merge-report.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "LoRA adapter config"):
                    adapter.run_merge(
                        soup_bin="unused",
                        adapter_bundle=input_bundle,
                        merged_bundle=merged_bundle,
                        report=report,
                        params_raw="{}",
                    )
            finally:
                os.chdir(old_cwd)
            self.assertFalse(merged_bundle.exists())
            self.assertFalse(report.exists())

    def test_unknown_parameters_fail_closed_before_soup_execution(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            input_bundle = self._bundle(tmp)
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "unsupported parameters"):
                    adapter.run_merge(
                        soup_bin="unused",
                        adapter_bundle=input_bundle,
                        merged_bundle=tmp / "outputs" / "merged.tar",
                        report=tmp / "outputs" / "report.json",
                        params_raw='{"dtype":"float16"}',
                    )
            finally:
                os.chdir(old_cwd)

    def test_soup_failure_does_not_publish_bundle_or_report(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            input_bundle = self._bundle(tmp)
            fake = self._fake_soup(tmp, exit_code=1)
            merged_bundle = tmp / "outputs" / "merged.tar"
            report = tmp / "outputs" / "report.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(RuntimeError, "SOUP merge failed with exit 1"):
                    adapter.run_merge(
                        soup_bin=str(fake),
                        adapter_bundle=input_bundle,
                        merged_bundle=merged_bundle,
                        report=report,
                        params_raw="{}",
                    )
            finally:
                os.chdir(old_cwd)
            self.assertFalse(merged_bundle.exists())
            self.assertFalse(report.exists())


if __name__ == "__main__":
    unittest.main()
