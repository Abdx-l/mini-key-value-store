# Mini Key-Value Store

A small persistent key-value store in Python with a binary write-ahead log
(WAL), an in-memory index of key to file offset, and snapshot compaction.

Each PUT and DELETE is encoded as `[payload length][CRC32][operation][key
length][key][value]`. Both integers in the header are four-byte big-endian
values. The record is flushed and fsynced before the call returns. On restart,
the store scans the compacted segment and then the WAL, stopping at a bad
checksum. An incomplete final record is truncated before new writes.

Compaction copies live key/value pairs into a new segment, fsyncs it, and
atomically replaces the old segment before truncating the WAL. If interrupted
between replacement and truncation, replaying the duplicated WAL records
preserves the latest value. The implementation is deliberately single-process;
it does not provide cross-process locking.

## Run

```sh
python -m kvstore.cli ./data
```

Commands:

```text
put greeting hello world
get greeting
keys
delete greeting
compact
quit
```

The first word after `put` is the key; the remaining text is its value.
One-shot usage: `python -m kvstore.cli ./data put greeting "hello world"`.
Run tests with `python -m unittest discover -s tests -v`.
No third-party packages are required.

This is a learning implementation, not a distributed database or a substitute
for SQLite. The tests cover replay, compaction, incomplete writes, checksum
failure, and a process terminated with `os._exit` after an acknowledged write.

