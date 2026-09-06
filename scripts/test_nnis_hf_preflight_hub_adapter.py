from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nnis_hf_preflight_hub_adapter as adapter  # noqa: E402
import soup_hub_adapter  # noqa: E402


class NnisHfPreflightHubAdapterTests(unittest.TestCase):
    def _model_bundle(self, tmp: Path) -> Path:
        model = tmp / "source-model"
        model.mkdir()
        (model / "config.json").write_text('{"model_type":"fixture"}\n', encoding="utf-8")
        (model / "tokenizer.json").write_text("{}\n", encoding="utf-8")
        (model / "model.safetensors").write_bytes(b"fixture-weights")
        bundle = tmp / "model.tar"
        soup_hub_adapter.create_deterministic_bundle(model, bundle)
        return bundle

    def _fake_nnis(
        self,
        tmp: Path,
        *,
        schema: str = adapter.NNIS_PREFLIGHT_SCHEMA,
        exit_code: int = 0,
    ) -> tuple[Path, bytes]:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        payload = (
            '{"schema":"'
            + schema
            + '","direct_execution_ready":true,"fixture":"exact-producer-bytes"}'
        ).encode("utf-8")
        fake = tmp / f"fake-nnis-{schema.replace('/', '_').replace('@', '_')}"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "from pathlib import Path\n"
            "import sys\n"
            "argv = sys.argv[1:]\n"
            "assert argv[0] == 'validate'\n"
            "model = Path(argv[argv.index('--model') + 1])\n"
            "assert (model / 'config.json').is_file()\n"
            "assert (model / 'tokenizer.json').is_file()\n"
            "assert (model / 'model.safetensors').is_file()\n"
            "assert '--json' in argv\n"
            "output = Path(argv[argv.index('--output') + 1])\n"
            "output.parent.mkdir(parents=True, exist_ok=True)\n"
            f"payload = {payload!r}\n"
            "output.write_bytes(payload)\n"
            "sys.stdout.buffer.write(payload)\n"
            f"raise SystemExit({exit_code})\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake, payload

    def test_success_preserves_exact_nnis_report_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, expected = self._fake_nnis(tmp)
            report = tmp / "outputs" / "preflight.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                adapter.run_preflight(
                    nnis_hf_bin=str(fake),
                    bundle=bundle,
                    report=report,
                    params_raw="{}",
                )
            finally:
                os.chdir(old_cwd)
            self.assertEqual(report.read_bytes(), expected)

    def test_unknown_params_fail_before_nnis_execution(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            report = tmp / "outputs" / "preflight.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "unsupported parameters"):
                    adapter.run_preflight(
                        nnis_hf_bin="unused",
                        bundle=bundle,
                        report=report,
                        params_raw='{"device":"cuda"}',
                    )
            finally:
                os.chdir(old_cwd)
            self.assertFalse(report.exists())

    def test_unknown_report_schema_fails_closed_without_rewriting_bytes(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, expected = self._fake_nnis(tmp, schema="nnis.hf-preflight@2")
            report = tmp / "outputs" / "preflight.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "unsupported NNIS preflight schema"):
                    adapter.run_preflight(
                        nnis_hf_bin=str(fake),
                        bundle=bundle,
                        report=report,
                        params_raw="{}",
                    )
            finally:
                os.chdir(old_cwd)
            self.assertEqual(report.read_bytes(), expected)

    def test_nnis_rejection_remains_process_failure(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, expected = self._fake_nnis(tmp, exit_code=1)
            report = tmp / "outputs" / "preflight.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(RuntimeError, "exit 1"):
                    adapter.run_preflight(
                        nnis_hf_bin=str(fake),
                        bundle=bundle,
                        report=report,
                        params_raw="{}",
                    )
            finally:
                os.chdir(old_cwd)
            self.assertEqual(report.read_bytes(), expected)

    def test_report_contract_rejects_non_object_json(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            report = Path(raw_tmp) / "report.json"
            report.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "JSON object"):
                adapter.validate_report_contract(report)


if __name__ == "__main__":
    unittest.main()
