import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from kvstore import CorruptLogError, Store


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.path = Path(self.temp.name)

    def tearDown(self):
        self.temp.cleanup()

    def test_replay_unicode_and_delete(self):
        store = Store(self.path)
        store.put("?????", "hello")
        store.put("a", "one")
        store.put("a", "two")
        self.assertEqual(Store(self.path).get("a"), "two")
        self.assertTrue(store.delete("a"))
        reopened = Store(self.path)
        self.assertIsNone(reopened.get("a"))
        self.assertEqual(reopened.keys(), ["?????"])

    def test_compaction_and_replay(self):
        store = Store(self.path)
        store.put("a", "1")
        store.compact()
        store.put("b", "2")
        self.assertEqual(Store(self.path).keys(), ["a", "b"])
        store.compact()
        self.assertEqual(Store(self.path).get("b"), "2")
        self.assertEqual((self.path / "wal.bin").stat().st_size, 0)

    def test_incomplete_tail_is_truncated(self):
        store = Store(self.path)
        store.put("a", "1")
        size = (self.path / "wal.bin").stat().st_size
        with (self.path / "wal.bin").open("ab") as log:
            log.write(b"\0\0\0\x40\xff")
        reopened = Store(self.path)
        self.assertEqual((self.path / "wal.bin").stat().st_size, size)
        reopened.put("b", "2")
        self.assertEqual(Store(self.path).keys(), ["a", "b"])

    def test_corrupted_checksum_is_rejected(self):
        store = Store(self.path)
        store.put("a", "1")
        log = self.path / "wal.bin"
        with log.open("r+b") as output:
            output.seek(4)
            output.write(b"\x00\x00\x00\x00")
        with self.assertRaisesRegex(CorruptLogError, "checksum"):
            Store(self.path)

    def test_invalid_write_does_not_append(self):
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.put("", "bad")
        self.assertFalse((self.path / "wal.bin").exists())

    def test_acknowledged_write_survives_abrupt_exit(self):
        code = (
            "import os,sys; from kvstore import Store; "
            "Store(sys.argv[1]).put('saved','yes'); os._exit(0)"
        )
        subprocess.run([sys.executable, "-c", code, str(self.path)],
                       check=True, env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])})
        self.assertEqual(Store(self.path).get("saved"), "yes")


if __name__ == "__main__":
    unittest.main()

