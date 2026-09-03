from __future__ import annotations

from pathlib import Path
import os
import sys
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

    def test_dont_ship_process_with_verdict_is_accepted(self) -> None:
        if os.name == "nt":
            self.skipTest("fake executable fixture uses a POSIX shebang")
        with tempfile.TemporaryDirectory() as raw_tmp:
            tmp = Path(raw_tmp)
            evidence = tmp / "evidence.json"
            evidence.write_text('{"schema":"fixture"}\n', encoding="utf-8")
            verdict = tmp / "out" / "verdict.json"
            fake_soup = tmp / "fake-soup"
            fake_soup.write_text(
                "#!/usr/bin/env python3\n"
                "from pathlib import Path\n"
                "import sys\n"
                "out = Path(sys.argv[sys.argv.index('--output') + 1])\n"
                "out.parent.mkdir(parents=True, exist_ok=True)\n"
                "out.write_text('{\\\"decision\\\":\\\"DONT_SHIP\\\"}\\n', encoding='utf-8')\n"
                "raise SystemExit(2)\n",
                encoding="utf-8",
            )
            fake_soup.chmod(0o755)

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


if __name__ == "__main__":
    unittest.main()
