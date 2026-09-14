"""Wire/byte-publication tests, not model or CUDA qualification."""
from __future__ import annotations

import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))
import nnis_hf_generate_hub_adapter as adapter


class GenerationArtifactTransportTests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.source = self.root / "producer.json"
        self.destination = self.root / "generation.json"
        self.document = {
            "schema_version": 1,
            "contract": adapter.NNIS_GENERATION_CONTRACT,
            "media_type": adapter.NNIS_GENERATION_MEDIA_TYPE,
            "status": "generated",
            "producer_owned": {"text": "exact bytes\n"},
        }
        self.raw = (json.dumps(self.document, indent=2) + "\n").encode()
        self.source.write_bytes(self.raw)

    def test_validation_and_publication_preserve_exact_whitespace(self) -> None:
        self.assertEqual(adapter.validate_result_contract(self.source), self.raw)
        adapter._copy_new_exact(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), self.raw)
        self.assertEqual(list(self.root.glob(".hub-nnis-result-*")), [])

    def test_boolean_float_and_future_schema_versions_are_rejected(self) -> None:
        for version in (True, 1.0, "1", 2, None):
            with self.subTest(version=version):
                self.source.write_text(json.dumps(self.document | {"schema_version": version}))
                with self.assertRaisesRegex(ValueError, "schema_version"):
                    adapter._copy_new_exact(self.source, self.destination)
                self.assertFalse(self.destination.exists())

    def test_duplicate_keys_and_nonfinite_json_are_rejected(self) -> None:
        for raw in (
            self.raw.replace(b'"schema_version": 1', b'"schema_version": 2, "schema_version": 1'),
            self.raw.replace(b'"producer_owned": {', b'"extra": NaN, "producer_owned": {'),
            self.raw.replace(b'"producer_owned": {', b'"extra": Infinity, "producer_owned": {'),
            self.raw.replace(b'"producer_owned": {', b'"producer_owned": {"same": 1, "same": 2,'),
        ):
            with self.subTest(raw=raw):
                self.source.write_bytes(raw)
                with self.assertRaisesRegex(ValueError, "not valid JSON"):
                    adapter._copy_new_exact(self.source, self.destination)
                self.assertFalse(self.destination.exists())

    def test_empty_invalid_utf8_and_nonobject_are_rejected(self) -> None:
        for raw in (b"", b"\xff", b"[]", b"{"):
            with self.subTest(raw=raw):
                self.source.write_bytes(raw)
                with self.assertRaises(ValueError):
                    adapter._copy_new_exact(self.source, self.destination)
                self.assertFalse(self.destination.exists())

    def test_oversize_is_rejected_before_publication(self) -> None:
        with mock.patch.object(adapter, "MAX_RESULT_BYTES", len(self.raw) - 1):
            with self.assertRaisesRegex(ValueError, "size must"):
                adapter._copy_new_exact(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_size_drift_is_detected_after_the_bounded_read(self) -> None:
        metadata = mock.Mock(st_mode=stat.S_IFREG | 0o600, st_size=len(self.raw) - 1)
        with mock.patch.object(adapter.os, "fstat", return_value=metadata):
            with self.assertRaisesRegex(ValueError, "size changed"):
                adapter._copy_new_exact(self.source, self.destination)
        self.assertFalse(self.destination.exists())

    def test_source_mutation_after_validation_cannot_change_published_bytes(self) -> None:
        real_temporary = tempfile.NamedTemporaryFile
        def mutate_after_read(*args, **kwargs):
            self.source.write_bytes(b'{"unvalidated":true}')
            return real_temporary(*args, **kwargs)
        with mock.patch.object(adapter.tempfile, "NamedTemporaryFile", side_effect=mutate_after_read):
            adapter._copy_new_exact(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), self.raw)
        self.assertNotEqual(self.destination.read_bytes(), self.source.read_bytes())

    def test_sync_or_link_fault_does_not_expose_partial_destination(self) -> None:
        for operation in ("fsync", "link"):
            with self.subTest(operation=operation), mock.patch.object(
                adapter.os, operation, side_effect=OSError("injected I/O fault")
            ):
                with self.assertRaises(OSError):
                    adapter._copy_new_exact(self.source, self.destination)
            self.assertFalse(self.destination.exists())
            self.assertEqual(list(self.root.glob(".hub-nnis-result-*")), [])

    def test_result_is_absent_until_sync_completes(self) -> None:
        real_sync = os.fsync
        def check_sync(fd: int) -> None:
            self.assertFalse(self.destination.exists())
            real_sync(fd)
        with mock.patch.object(adapter.os, "fsync", side_effect=check_sync):
            adapter._copy_new_exact(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), self.raw)

    def test_concurrent_destination_is_preserved(self) -> None:
        real_link = os.link
        def competing_link(source: Path, destination: Path) -> None:
            destination.write_bytes(b"competing result")
            real_link(source, destination)
        with mock.patch.object(adapter.os, "link", side_effect=competing_link):
            with self.assertRaisesRegex(ValueError, "already exists"):
                adapter._copy_new_exact(self.source, self.destination)
        self.assertEqual(self.destination.read_bytes(), b"competing result")
        self.assertEqual(list(self.root.glob(".hub-nnis-result-*")), [])

    @unittest.skipUnless(os.name == "posix", "POSIX special-file fixture")
    def test_symlinks_and_fifo_fail_without_overwrite_or_blocking(self) -> None:
        link = self.root / "producer-link"
        link.symlink_to(self.source)
        with self.assertRaises(ValueError):
            adapter.validate_result_contract(link)
        fifo = self.root / "producer-fifo"
        os.mkfifo(fifo)
        with self.assertRaises(ValueError):
            adapter.validate_result_contract(fifo)
        self.destination.symlink_to(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "already exists"):
            adapter._copy_new_exact(self.source, self.destination)
        self.assertTrue(self.destination.is_symlink())
        self.assertFalse((self.root / "missing").exists())

    def test_escaped_parameter_envelope_and_nul_fail_before_execution(self) -> None:
        for params in (
            json.dumps({"prompt": "\x01" * 3000}),
            json.dumps({"prompt": "a\0b"}),
        ):
            with self.subTest(params=params[:30]), mock.patch.object(adapter, "_run_logged") as run:
                with self.assertRaises(ValueError):
                    adapter.run_generation(
                        python_bin=sys.executable, nnis_process="unused", nnis_hf_bin="unused",
                        bundle=self.source, generation=self.destination, params_raw=params,
                    )
                run.assert_not_called()
        self.assertFalse(self.destination.exists())


if __name__ == "__main__":
    unittest.main()
