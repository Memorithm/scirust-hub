import importlib.util
import json
import pathlib
import stat
import tempfile
import unittest

SCRIPT = pathlib.Path(__file__).with_name("sml_weight_external_consumer.py")
SPEC = importlib.util.spec_from_file_location("sml_consumer", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


class SmlWeightExternalConsumerTests(unittest.TestCase):
    def test_accepts_exact_owner_receipt_and_forwards_trust_paths(self):
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            code_sha = "a" * 40
            run_id = "run-production"
            receipt = {
                "repository": "Memorithm/SML-GENIUS",
                "code_sha": code_sha,
                "publication_id": "sml-main-v1",
                "publisher": "memorithm-sml-production",
                "run_id": run_id,
                "bundle_sha256": "b" * 64,
                "manifest_sha256": "c" * 64,
                "artifacts": 1,
                "artifact_bytes": 17,
                "scientific_qualification": "not_evaluated",
                "merge_authorization": False,
            }
            executable = root / "sml-publication"
            executable.write_text(
                "#!/bin/sh\nprintf '%s\\n' '" + json.dumps(receipt) + "'\n",
                encoding="utf-8",
            )
            executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
            result = MODULE.consume(
                executable,
                root / "dossier",
                run_id,
                root / "owner",
                root / "policy",
                root / "allowed_signers",
                code_sha,
                "sml-main-v1",
            )
            self.assertEqual(result, receipt)

    def test_rejects_wrong_publisher_or_scientific_promotion(self):
        with tempfile.TemporaryDirectory() as raw:
            root = pathlib.Path(raw)
            receipt = {
                "repository": "Memorithm/SML-GENIUS",
                "code_sha": "a" * 40,
                "publication_id": "sml-main-v1",
                "publisher": "development-publisher",
                "run_id": "run-production",
                "bundle_sha256": "b" * 64,
                "manifest_sha256": "c" * 64,
                "artifacts": 1,
                "artifact_bytes": 17,
                "scientific_qualification": "passed",
                "merge_authorization": True,
            }
            executable = root / "sml-publication"
            executable.write_text(
                "#!/bin/sh\nprintf '%s\\n' '" + json.dumps(receipt) + "'\n",
                encoding="utf-8",
            )
            executable.chmod(executable.stat().st_mode | stat.S_IXUSR)
            with self.assertRaises(ValueError):
                MODULE.consume(
                    executable,
                    root / "dossier",
                    "run-production",
                    root / "owner",
                    root / "policy",
                    root / "allowed_signers",
                    "a" * 40,
                    "sml-main-v1",
                )


if __name__ == "__main__":
    unittest.main()
