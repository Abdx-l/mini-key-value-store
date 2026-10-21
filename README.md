# Mini Key-Value Store

A small persistent key-value store in Python. Updates append to a write-ahead
log (WAL) before changing the in-memory index. On startup the store loads a
snapshot and replays later log records. Each record has a CRC32 checksum;
recovery ignores an incomplete tail record and refuses a corrupted complete
record.

The format is intentionally simple and local. It supports one writer process
per directory, Unicode keys and values, `put`, `get`, `delete`, `keys`,
and snapshot compaction. This is a learning implementation, not a distributed
database or a substitute for SQLite.

## Run

```sh
python -m kvstore.cli ./data
```

Commands in the interactive prompt:

```text
put greeting hello world
get greeting
keys
delete greeting
compact
quit
```

The first word after `put` is the key; the remaining text is its value.
You can also run one command without a prompt:
`python -m kvstore.cli ./data put greeting "hello world"`.

Run tests with `python -m unittest discover -s tests -v`.
No third-party packages are required.

## Recovery model

Each WAL record is one JSON line with a sequence number, operation, key,
optional value, and checksum. A complete record is flushed and fsynced before
the operation is visible to the caller. During compaction a new snapshot is
written to a temporary file, fsynced, and atomically replaced. The WAL is
then truncated. Records whose sequence number is already in the snapshot are
skipped during recovery, so an interruption between these two steps is safe.
An interrupted final line is discarded and truncated before the next write.

Only a single process may open the same store for writing. The API does not
enforce an OS-level process lock; callers must avoid concurrent processes.

