import json
from pathlib import Path
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

    def test_compaction_and_replay_after_snapshot(self):
        store = Store(self.path)
        store.put("a", "1")
        store.compact()
        store.put("b", "2")
        self.assertEqual(Store(self.path).keys(), ["a", "b"])
        store.compact()
        self.assertEqual(Store(self.path).get("b"), "2")

    def test_incomplete_tail_is_truncated(self):
        store = Store(self.path)
        store.put("a", "1")
        with (self.path / "wal.jsonl").open("ab") as log:
            log.write(b'{"sequence":2')
        reopened = Store(self.path)
        reopened.put("b", "2")
        self.assertEqual(Store(self.path).keys(), ["a", "b"])

    def test_corrupted_complete_record_is_rejected(self):
        store = Store(self.path)
        store.put("a", "1")
        log = self.path / "wal.jsonl"
        record = json.loads(log.read_text(encoding="utf-8"))
        record["value"] = "tampered"
        log.write_text(json.dumps(record) + "\n", encoding="utf-8")
        with self.assertRaises(CorruptLogError):
            Store(self.path)

    def test_invalid_write_does_not_append(self):
        store = Store(self.path)
        with self.assertRaises(ValueError):
            store.put("", "bad")
        self.assertFalse((self.path / "wal.jsonl").exists())


if __name__ == "__main__":
    unittest.main()

