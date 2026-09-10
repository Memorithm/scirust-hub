from __future__ import annotations

import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nnis_hf_generate_hub_adapter as adapter  # noqa: E402
import soup_hub_adapter  # noqa: E402


class NnisHfGenerateHubAdapterTests(unittest.TestCase):
    def _model_bundle(self, tmp: Path) -> Path:
        model = tmp / "source-model"
        model.mkdir()
        (model / "config.json").write_text('{"model_type":"fixture"}\n', encoding="utf-8")
        (model / "tokenizer.json").write_text("{}\n", encoding="utf-8")
        (model / "model.safetensors").write_bytes(b"fixture-weights")
        bundle = tmp / "model.tar"
        soup_hub_adapter.create_deterministic_bundle(model, bundle)
        return bundle

    def _fake_process(
        self,
        tmp: Path,
        *,
        contract: str = adapter.NNIS_GENERATION_CONTRACT,
        exit_code: int = 0,
    ) -> tuple[Path, bytes, Path]:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        trace = tmp / "trace.json"
        payload = {
            "schema_version": adapter.NNIS_GENERATION_SCHEMA_VERSION,
            "contract": contract,
            "media_type": adapter.NNIS_GENERATION_MEDIA_TYPE,
            "status": "generated",
            "fixture": "exact-producer-bytes",
        }
        encoded = (json.dumps(payload, sort_keys=True, separators=(",", ":")) + "\n").encode()
        fake = tmp / "fake-nnis-generation-process.py"
        fake.write_text(
            "#!/usr/bin/env python3\n"
            "import json, os, sys\n"
            "from pathlib import Path\n"
            "argv = sys.argv[1:]\n"
            "Path(os.environ['NNIS_GENERATE_TRACE']).write_text(json.dumps(argv), encoding='utf-8')\n"
            "model = Path(argv[argv.index('--model') + 1])\n"
            "assert (model / 'config.json').is_file()\n"
            "assert (model / 'tokenizer.json').is_file()\n"
            "assert (model / 'model.safetensors').is_file()\n"
            "result = Path(argv[argv.index('--result') + 1])\n"
            "result.parent.mkdir(parents=True, exist_ok=True)\n"
            f"payload = {encoded!r}\n"
            "result.write_bytes(payload)\n"
            f"raise SystemExit({exit_code})\n",
            encoding="utf-8",
        )
        fake.chmod(0o755)
        return fake, encoded, trace

    def test_success_preserves_exact_nnis_result_bytes_and_arguments(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, expected, trace = self._fake_process(tmp)
            generation = tmp / "outputs" / "generation.json"
            old_trace = os.environ.get("NNIS_GENERATE_TRACE")
            old_cwd = Path.cwd()
            os.environ["NNIS_GENERATE_TRACE"] = str(trace)
            try:
                os.chdir(tmp)
                adapter.run_generation(
                    python_bin=sys.executable,
                    nnis_process=str(fake),
                    nnis_hf_bin="/opt/nnis/bin/nnis-hf",
                    bundle=bundle,
                    generation=generation,
                    params_raw='{"prompt":"Hello from Hub","device":2,"max_new_tokens":7}',
                )
            finally:
                os.chdir(old_cwd)
                if old_trace is None:
                    os.environ.pop("NNIS_GENERATE_TRACE", None)
                else:
                    os.environ["NNIS_GENERATE_TRACE"] = old_trace

            self.assertEqual(generation.read_bytes(), expected)
            argv = json.loads(trace.read_text(encoding="utf-8"))
            self.assertEqual(argv[0], "--model")
            self.assertEqual(argv[2:4], ["--prompt", "Hello from Hub"])
            self.assertEqual(argv[4:6], ["--device", "2"])
            self.assertEqual(argv[6:8], ["--max-new-tokens", "7"])
            self.assertEqual(argv[8], "--result")
            self.assertEqual(argv[10:12], ["--nnis-hf-bin", "/opt/nnis/bin/nnis-hf"])

    def test_unknown_params_fail_before_process_execution(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            generation = tmp / "outputs" / "generation.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "unsupported parameters"):
                    adapter.run_generation(
                        python_bin=sys.executable,
                        nnis_process="unused",
                        nnis_hf_bin="unused",
                        bundle=bundle,
                        generation=generation,
                        params_raw='{"prompt":"x","temperature":0.7}',
                    )
            finally:
                os.chdir(old_cwd)
            self.assertFalse(generation.exists())

    def test_future_producer_contract_fails_closed_without_copying_result(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, _expected, trace = self._fake_process(tmp, contract="nnis.hf-generation@2.0.0")
            generation = tmp / "outputs" / "generation.json"
            old_trace = os.environ.get("NNIS_GENERATE_TRACE")
            old_cwd = Path.cwd()
            os.environ["NNIS_GENERATE_TRACE"] = str(trace)
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(ValueError, "unsupported NNIS generation contract"):
                    adapter.run_generation(
                        python_bin=sys.executable,
                        nnis_process=str(fake),
                        nnis_hf_bin="nnis-hf",
                        bundle=bundle,
                        generation=generation,
                        params_raw='{"prompt":"x"}',
                    )
            finally:
                os.chdir(old_cwd)
                if old_trace is None:
                    os.environ.pop("NNIS_GENERATE_TRACE", None)
                else:
                    os.environ["NNIS_GENERATE_TRACE"] = old_trace
            self.assertFalse(generation.exists())

    def test_producer_failure_publishes_no_hub_generation_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            fake, _expected, trace = self._fake_process(tmp, exit_code=7)
            generation = tmp / "outputs" / "generation.json"
            old_trace = os.environ.get("NNIS_GENERATE_TRACE")
            old_cwd = Path.cwd()
            os.environ["NNIS_GENERATE_TRACE"] = str(trace)
            try:
                os.chdir(tmp)
                with self.assertRaisesRegex(RuntimeError, "exit 7"):
                    adapter.run_generation(
                        python_bin=sys.executable,
                        nnis_process=str(fake),
                        nnis_hf_bin="nnis-hf",
                        bundle=bundle,
                        generation=generation,
                        params_raw='{"prompt":"x"}',
                    )
            finally:
                os.chdir(old_cwd)
                if old_trace is None:
                    os.environ.pop("NNIS_GENERATE_TRACE", None)
                else:
                    os.environ["NNIS_GENERATE_TRACE"] = old_trace
            self.assertFalse(generation.exists())

    def test_request_bounds_and_existing_output_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            bundle = self._model_bundle(tmp)
            generation = tmp / "outputs" / "generation.json"
            old_cwd = Path.cwd()
            try:
                os.chdir(tmp)
                for params in [
                    '{"prompt":""}',
                    '{"prompt":"x","device":true}',
                    '{"prompt":"x","device":-1}',
                    '{"prompt":"x","max_new_tokens":0}',
                    '{"prompt":"x","max_new_tokens":65537}',
                ]:
                    with self.assertRaises(ValueError):
                        adapter.run_generation(
                            python_bin=sys.executable,
                            nnis_process="unused",
                            nnis_hf_bin="unused",
                            bundle=bundle,
                            generation=generation,
                            params_raw=params,
                        )
                generation.parent.mkdir(parents=True, exist_ok=True)
                generation.write_text("existing\n", encoding="utf-8")
                with self.assertRaisesRegex(ValueError, "must not exist"):
                    adapter.run_generation(
                        python_bin=sys.executable,
                        nnis_process="unused",
                        nnis_hf_bin="unused",
                        bundle=bundle,
                        generation=generation,
                        params_raw='{"prompt":"x"}',
                    )
            finally:
                os.chdir(old_cwd)


if __name__ == "__main__":
    unittest.main()
