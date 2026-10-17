"""Respond to the three Cargo probes used by the storage-needs consumer proof."""
import json
import os
from pathlib import Path
import sys

arguments = sys.argv[1:]
calls_path = Path(os.environ["FAKE_CARGO_CALLS"])
with calls_path.open("a", encoding="utf-8") as calls:
    calls.write(json.dumps(arguments) + "\n")

command = arguments[0] if arguments else ""
if command == "run":
    raise SystemExit(0)
if command == "metadata":
    sys.stdout.write(Path(os.environ["FAKE_CARGO_METADATA"]).read_text(encoding="utf-8"))
    raise SystemExit(0)
if command == "tree":
    sys.stdout.write(Path(os.environ["FAKE_CARGO_TREE"]).read_text(encoding="utf-8"))
    raise SystemExit(0)

print(f"unexpected fake cargo command: {command}", file=sys.stderr)
raise SystemExit(97)
