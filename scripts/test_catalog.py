#!/usr/bin/env python3
"""Offline catalog integrity. Does not call GitHub."""

from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "CATALOG.md"

REQUIRED_REPOS = [
    "scirust",
    "SoulSystem",
    "TurboQuant",
    "nonlocal-relativity-v2",
    "FLAT-ATTENTION",
    "PAPERS-AGENT",
    "CCOS-Enterprise",
    "CCOS-Core",
    "TDI",
    "ADA",
    "SLHAv2",
    "NNIS",
    "RSI",
    "itd-simulator",
    "ElasticXxx",
    "octasoma",
    "scirust-hub",
    "COGNO-1",
    "riemann_ndim_bench",
    "Replikans",
    "orchestrator",
    "Forge",
    "ProofLab",
    "SciCapsule",
    "ExtremEngine",
    "NoiseLab",
    "KVLab",
    "SciRust-Verify",
    "GOT",
    "scirust-automotive",
]


class CatalogTests(unittest.TestCase):
    def test_catalog_exists(self):
        self.assertTrue(CATALOG.is_file(), "CATALOG.md missing")

    def test_all_known_repos_are_named(self):
        text = CATALOG.read_text(encoding="utf-8")
        missing = [name for name in REQUIRED_REPOS if name not in text]
        self.assertEqual(missing, [], f"CATALOG.md missing repos: {missing}")

    def test_thirty_repos_claimed(self):
        text = CATALOG.read_text(encoding="utf-8")
        self.assertIn("30 repositories", text)
        self.assertEqual(len(REQUIRED_REPOS), 30)


if __name__ == "__main__":
    unittest.main()
