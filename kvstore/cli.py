"""Command-line interface for the local store."""

import argparse
import shlex

from .store import Store


def execute(store: Store, words: list[str]) -> bool:
    if not words:
        return True
    command, *args = words
    if command in ("quit", "exit") and not args:
        return False
    if command == "put" and len(args) >= 2:
        store.put(args[0], " ".join(args[1:]))
        print("ok")
    elif command == "get" and len(args) == 1:
        value = store.get(args[0])
        print(value if value is not None else "(missing)")
    elif command == "delete" and len(args) == 1:
        print("deleted" if store.delete(args[0]) else "(missing)")
    elif command == "keys" and not args:
        print("\n".join(store.keys()))
    elif command == "compact" and not args:
        store.compact()
        print("compacted")
    else:
        print("usage: put KEY VALUE | get KEY | delete KEY | keys | compact | quit")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", help="directory for snapshot and WAL")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    store = Store(args.directory)
    if args.command:
        execute(store, args.command)
        return
    while True:
        try:
            line = input("kv> ")
        except EOFError:
            break
        try:
            if not execute(store, shlex.split(line)):
                break
        except ValueError as exc:
            print(f"error: {exc}")


if __name__ == "__main__":
    main()

