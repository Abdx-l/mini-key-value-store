"""Single-writer key-value store with binary WAL and compacted segment."""

from __future__ import annotations

import os
from pathlib import Path
import struct
import threading
import zlib

HEADER = struct.Struct(">II")  # payload length, CRC32
KEY_LENGTH = struct.Struct(">I")
MAX_RECORD = 64 * 1024 * 1024


class CorruptLogError(ValueError):
    pass


def sync_dir(path: Path) -> None:
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def encode(op: bytes, key: str, value: str = "") -> bytes:
    key_bytes, value_bytes = key.encode("utf-8"), value.encode("utf-8")
    payload = op + KEY_LENGTH.pack(len(key_bytes)) + key_bytes + value_bytes
    if len(payload) > MAX_RECORD:
        raise ValueError("record exceeds 64 MiB")
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def decode(payload: bytes) -> tuple[bytes, str, str]:
    if len(payload) < 5 or payload[:1] not in (b"P", b"D"):
        raise CorruptLogError("invalid operation")
    key_length = KEY_LENGTH.unpack(payload[1:5])[0]
    if key_length == 0 or 5 + key_length > len(payload):
        raise CorruptLogError("invalid key length")
    try:
        key = payload[5:5 + key_length].decode("utf-8")
        value = payload[5 + key_length:].decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CorruptLogError("invalid UTF-8") from exc
    if payload[:1] == b"D" and value:
        raise CorruptLogError("delete record contains a value")
    return payload[:1], key, value


def record_at(path: Path, offset: int) -> tuple[bytes, str, str]:
    with path.open("rb") as stream:
        stream.seek(offset)
        header = stream.read(HEADER.size)
        if len(header) != HEADER.size:
            raise CorruptLogError("missing indexed record")
        length, crc = HEADER.unpack(header)
        payload = stream.read(length)
        if len(payload) != length or zlib.crc32(payload) != crc:
            raise CorruptLogError("invalid indexed record")
        return decode(payload)


class Store:
    def __init__(self, directory: str | Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.segment_path = self.directory / "segment.bin"
        self.log_path = self.directory / "wal.bin"
        self._lock = threading.RLock()
        self._index: dict[str, tuple[Path, int]] = {}
        self._scan(self.segment_path, allow_tail=False)
        self._scan(self.log_path, allow_tail=True)

    def _scan(self, path: Path, allow_tail: bool) -> None:
        if not path.exists():
            return
        valid_end = 0
        with path.open("rb") as stream:
            while True:
                offset = stream.tell()
                header = stream.read(HEADER.size)
                if not header:
                    break
                if len(header) < HEADER.size:
                    if allow_tail:
                        break
                    raise CorruptLogError(f"incomplete segment header at {offset}")
                length, crc = HEADER.unpack(header)
                if length < 5 or length > MAX_RECORD:
                    raise CorruptLogError(f"invalid record length at {offset}")
                payload = stream.read(length)
                if len(payload) < length:
                    if allow_tail:
                        break
                    raise CorruptLogError(f"incomplete segment record at {offset}")
                if zlib.crc32(payload) != crc:
                    raise CorruptLogError(f"bad checksum at {offset}")
                op, key, _ = decode(payload)
                if op == b"P":
                    self._index[key] = (path, offset)
                else:
                    self._index.pop(key, None)
                valid_end = stream.tell()
        if allow_tail:
            with path.open("r+b") as stream:
                stream.truncate(valid_end)

    def _append(self, op: bytes, key: str, value: str = "") -> None:
        if not isinstance(key, str) or not key:
            raise ValueError("key must be a nonempty string")
        if not isinstance(value, str):
            raise ValueError("value must be a string")
        record = encode(op, key, value)
        with self.log_path.open("ab") as stream:
            offset = stream.tell()
            stream.write(record)
            stream.flush()
            os.fsync(stream.fileno())
        if op == b"P":
            self._index[key] = (self.log_path, offset)
        else:
            self._index.pop(key, None)

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._append(b"P", key, value)

    def get(self, key: str) -> str | None:
        with self._lock:
            entry = self._index.get(key)
            if entry is None:
                return None
            op, indexed_key, value = record_at(*entry)
            if op != b"P" or indexed_key != key:
                raise CorruptLogError("index points to wrong record")
            return value

    def delete(self, key: str) -> bool:
        with self._lock:
            existed = key in self._index
            self._append(b"D", key)
            return existed

    def keys(self) -> list[str]:
        with self._lock:
            return sorted(self._index)

    def compact(self) -> None:
        with self._lock:
            temp = self.directory / "segment.tmp"
            new_index: dict[str, tuple[Path, int]] = {}
            with temp.open("wb") as output:
                for key in sorted(self._index):
                    value = self.get(key)
                    offset = output.tell()
                    output.write(encode(b"P", key, value))
                    new_index[key] = (self.segment_path, offset)
                output.flush()
                os.fsync(output.fileno())
            os.replace(temp, self.segment_path)
            sync_dir(self.directory)
            with self.log_path.open("wb") as log:
                log.flush()
                os.fsync(log.fileno())
            self._index = new_index

