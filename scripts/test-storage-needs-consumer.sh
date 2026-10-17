#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
# This workspace has its own resolver and lockfile so the main workspace cannot
# unify TinyBus features into the contract's dependency graph.
python3 "$repo_root/scripts/test-storage-needs-consumer.py" --verify
