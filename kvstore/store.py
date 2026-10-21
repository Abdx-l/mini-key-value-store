"""Single-writer key-value store with a checksummed write-ahead log."""

from __future__ import annotations

import json
import os
from pathlib import Path
import threading
import zlib


class CorruptLogError(ValueError):
    pass


def canonical(payload: dict) -> bytes:
    return json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def checksum(payload: dict) -> int:
    return zlib.crc32(canonical(payload))


def sync_dir(path: Path) -> None:
    # Directory fsync makes a renamed snapshot durable on POSIX systems.
    if os.name != "posix":
        return
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class Store:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.snapshot_path = self.directory / "snapshot.json"
        self.log_path = self.directory / "wal.jsonl"
        self._lock = threading.RLock()
        self._data: dict[str, str] = {}
        self._sequence = 0
        self._load()

    def _load(self) -> None:
        if self.snapshot_path.exists():
            try:
                snapshot = json.loads(self.snapshot_path.read_text(encoding="utf-8"))
                payload = {k: snapshot[k] for k in ("sequence", "data")}
                if (snapshot["checksum"] != checksum(payload)
                        or not isinstance(snapshot["sequence"], int)
                        or snapshot["sequence"] < 0
                        or not isinstance(snapshot["data"], dict)
                        or any(not isinstance(k, str) or not isinstance(v, str)
                               for k, v in snapshot["data"].items())):
                    raise ValueError("invalid snapshot")
                self._sequence = snapshot["sequence"]
                self._data = snapshot["data"]
            except (ValueError, KeyError, TypeError) as exc:
                raise CorruptLogError("invalid snapshot") from exc

        if not self.log_path.exists():
            return
        expected = self._sequence + 1
        valid_end = 0
        with self.log_path.open("rb") as log:
            while raw := log.readline():
                if not raw.endswith(b"\n"):
                    break  # incomplete last append
                try:
                    record = json.loads(raw)
                    payload = {k: record[k] for k in ("sequence", "op", "key", "value")}
                    seq = payload["sequence"]
                    if (record["checksum"] != checksum(payload)
                            or not isinstance(seq, int)
                            or not isinstance(payload["key"], str)
                            or payload["op"] not in ("put", "delete")
                            or (payload["op"] == "put"
                                and not isinstance(payload["value"], str))
                            or (payload["op"] == "delete"
                                and payload["value"] is not None)):
                        raise ValueError("invalid record")
                except (ValueError, KeyError, TypeError) as exc:
                    raise CorruptLogError(
                        f"corrupted WAL at byte {valid_end}"
                    ) from exc
                if seq > self._sequence:
                    if seq != expected:
                        raise CorruptLogError(f"missing WAL sequence {expected}")
                    self._apply(payload)
                    self._sequence = seq
                    expected += 1
                valid_end = log.tell()
        # An incomplete tail must be removed before another append.
        with self.log_path.open("r+b") as log:
            log.truncate(valid_end)

    def _apply(self, payload: dict) -> None:
        if payload["op"] == "put":
            self._data[payload["key"]] = payload["value"]
        else:
            self._data.pop(payload["key"], None)

    def _append(self, op: str, key: str, value: str | None) -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a nonempty string")
        if op == "put" and not isinstance(value, str):
            raise ValueError("value must be a string")
        payload = {"sequence": self._sequence + 1, "op": op,
                   "key": key, "value": value}
        record = {**payload, "checksum": checksum(payload)}
        with self.log_path.open("ab") as log:
            log.write(canonical(record) + b"\n")
            log.flush()
            os.fsync(log.fileno())
        self._apply(payload)
        self._sequence += 1

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._append("put", key, value)

    def get(self, key: str) -> str | None:
        with self._lock:
            return self._data.get(key)

    def delete(self, key: str) -> bool:
        with self._lock:
            existed = key in self._data
            self._append("delete", key, None)
            return existed

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._data)

    def compact(self) -> None:
        with self._lock:
            payload = {"sequence": self._sequence, "data": self._data.copy()}
            snapshot = {**payload, "checksum": checksum(payload)}
            temp = self.directory / "snapshot.tmp"
            with temp.open("wb") as output:
                output.write(canonical(snapshot) + b"\n")
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp, self.snapshot_path)
            sync_dir(self.directory)
            with self.log_path.open("wb") as log:
                log.flush()
                os.fsync(log.fileno())

