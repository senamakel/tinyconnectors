"""Verify the isolated storage-needs consumer against Cargo's resolved graph."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import subprocess
import sys
from typing import Mapping, Sequence

EXPECTED_SOURCE = (
    "git+https://github.com/tinyhumansai/tinystoragedrivers"
    "?rev=c9ea6ff1615b77b55d7269403af5fd11bedc9ce8"
    "#c9ea6ff1615b77b55d7269403af5fd11bedc9ce8"
)
EXPECTED_CONTRACT_VERSION = "0.20.2"
EXPECTED_STORAGE_BUS_VERSION = "1.5.0"
EXPECTED_STORAGE_NEEDS_CRATE = "tinystoragedrivers-bus"
EXPECTED_PROOF_PACKAGE = "tinyconnectors-storage-needs-consumer-proof"
EXPECTED_TREE_PACKAGES = {
    EXPECTED_PROOF_PACKAGE,
    "tinyconnectors-bus",
    EXPECTED_STORAGE_NEEDS_CRATE,
}

FORBIDDEN_PACKAGES = {
    "async-std",
    "async-trait",
    "bson",
    "hyper",
    "libsqlite3-sys",
    "mongodb",
    "reqwest",
    "rusqlite",
    "sqlx",
    "tokio",
    "ureq",
}
TREE_PACKAGE = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9_-]*) v([^\s]+)")


class ProofError(ValueError):
    """The proof graph does not meet the canonical data-only contract."""


def _error(message: str) -> ProofError:
    return ProofError(f"storage-needs consumer proof: {message}")


def _package_name(package: object, package_id: str) -> str:
    if not isinstance(package, dict):
        raise _error(f"package metadata is missing for reachable id {package_id}")
    name = package.get("name")
    if not isinstance(name, str) or not name:
        raise _error(f"package name is missing for reachable id {package_id}")
    return name


def _normalize_cargo_dependency_name(name: object) -> str | None:
    if not isinstance(name, str) or not name:
        return None
    return name.replace("-", "_")


def _is_forbidden_package(name: str) -> bool:
    if name in FORBIDDEN_PACKAGES:
        return True
    if name.startswith("tinybus"):
        return True
    if name.startswith("tinystoragedrivers-") and name != EXPECTED_STORAGE_NEEDS_CRATE:
        return True
    if name.startswith("tinyconnectors-") and name not in {
        "tinyconnectors-bus",
        EXPECTED_PROOF_PACKAGE,
    }:
        return True
    return name == "tinyconnectors"


def _has_kind(dep_kinds: list[object], allowed: set[str | None]) -> bool:
    for dep_kind in dep_kinds:
        if not isinstance(dep_kind, dict):
            raise _error("Cargo metadata contains a malformed dependency kind")
        if dep_kind.get("kind") in allowed:
            return True
    return False


def normal_build_packages(metadata: dict[str, object]) -> dict[str, dict[str, object]]:
    """Return the normal/build reachable package nodes, failing closed on gaps."""
    resolve = metadata.get("resolve")
    if not isinstance(resolve, dict):
        raise _error("Cargo resolve graph is missing")
    root_id = resolve.get("root")
    if not isinstance(root_id, str) or not root_id:
        raise _error("Cargo resolve root is missing")

    packages = metadata.get("packages")
    nodes = resolve.get("nodes")
    if not isinstance(packages, list) or not isinstance(nodes, list):
        raise _error("Cargo package or resolve-node list is missing")

    packages_by_id: dict[str, dict[str, object]] = {}
    for package in packages:
        if not isinstance(package, dict) or not isinstance(package.get("id"), str):
            raise _error("Cargo package metadata has no package id")
        packages_by_id[package["id"]] = package

    nodes_by_id: dict[str, dict[str, object]] = {}
    for node in nodes:
        if not isinstance(node, dict) or not isinstance(node.get("id"), str):
            raise _error("Cargo resolve node has no package id")
        nodes_by_id[node["id"]] = node

    reachable: dict[str, dict[str, object]] = {}
    pending = [root_id]
    while pending:
        package_id = pending.pop()
        if package_id in reachable:
            continue
        package = packages_by_id.get(package_id)
        if package is None:
            raise _error(f"reachable package metadata is missing for {package_id}")
        node = nodes_by_id.get(package_id)
        if node is None:
            raise _error(f"reachable resolve node is missing for {package_id}")
        reachable[package_id] = package

        deps = node.get("deps")
        if not isinstance(deps, list):
            raise _error(f"dependency edges are missing for {_package_name(package, package_id)}")
        for dep in deps:
            if not isinstance(dep, dict):
                raise _error("Cargo resolve graph contains a malformed dependency edge")
            dep_kinds = dep.get("dep_kinds")
            if not isinstance(dep_kinds, list) or not dep_kinds:
                raise _error("Cargo dependency edge has no kind evidence")
            if not _has_kind(dep_kinds, {None, "build"}):
                continue
            dep_id = dep.get("pkg")
            if not isinstance(dep_id, str):
                raise _error("normal/build dependency edge has no package id")
            dep_package = packages_by_id.get(dep_id)
            if dep_package is None:
                raise _error(f"normal/build dependency package is missing for {dep_id}")
            dep_name = _package_name(dep_package, dep_id)
            if _normalize_cargo_dependency_name(dep.get("name")) != _normalize_cargo_dependency_name(dep_name):
                raise _error(f"dependency edge name does not match package id {dep_id}")
            if dep_id not in nodes_by_id:
                raise _error(f"normal/build resolve evidence is missing for {dep_id}")
            pending.append(dep_id)
    return reachable


def validate_consumer_graph(metadata: dict[str, object], tree: str) -> None:
    """Validate source identity, feature state, and closed normal/build reachability."""
    reachable = normal_build_packages(metadata)
    resolve = metadata.get("resolve")
    packages = metadata.get("packages")
    if not isinstance(resolve, dict) or not isinstance(packages, list):
        raise _error("Cargo package or resolve metadata is missing")
    root_id = resolve.get("root")
    nodes = resolve.get("nodes")
    if not isinstance(root_id, str) or not isinstance(nodes, list):
        raise _error("Cargo resolve root or node list is missing")
    packages_by_id = {
        package["id"]: package
        for package in packages
        if isinstance(package, dict) and isinstance(package.get("id"), str)
    }
    nodes_by_id = {
        node["id"]: node
        for node in nodes
        if isinstance(node, dict) and isinstance(node.get("id"), str)
    }
    root_node = nodes_by_id.get(root_id)
    root_package = packages_by_id.get(root_id)
    if root_node is None or root_package is None:
        raise _error("Cargo resolve root evidence is missing")
    if _package_name(root_package, root_id) != EXPECTED_PROOF_PACKAGE:
        raise _error("resolve root is not the isolated consumer package")

    bus_contracts = [
        (package_id, package)
        for package_id, package in reachable.items()
        if _package_name(package, package_id) == "tinyconnectors-bus"
    ]
    if len(bus_contracts) != 1:
        raise _error("the TinyConnectors bus contract is missing or duplicated")
    contract_id, contract = bus_contracts[0]
    if contract.get("version") != EXPECTED_CONTRACT_VERSION:
        raise _error("the TinyConnectors bus contract version is not the workspace release")

    storage_packages = [
        (package_id, package)
        for package_id, package in reachable.items()
        if _package_name(package, package_id) == EXPECTED_STORAGE_NEEDS_CRATE
    ]
    if len(storage_packages) != 1:
        raise _error("the canonical storage bus package is missing or duplicated")
    storage_id, storage_package = storage_packages[0]
    if storage_package.get("version") != EXPECTED_STORAGE_BUS_VERSION:
        raise _error("the canonical storage bus package version is not 1.5.0")
    if storage_package.get("source") != EXPECTED_SOURCE:
        raise _error("the canonical storage bus source is not the pinned v1.5.0 commit")

    root_deps = root_node.get("deps")
    if not isinstance(root_deps, list):
        raise _error("consumer root has no dependency edge evidence")
    direct_storage_dependency = any(
        isinstance(dep, dict)
        and dep.get("pkg") == storage_id
        and _normalize_cargo_dependency_name(dep.get("name"))
        == _normalize_cargo_dependency_name(EXPECTED_STORAGE_NEEDS_CRATE)
        and isinstance(dep.get("dep_kinds"), list)
        and _has_kind(dep["dep_kinds"], {None})
        for dep in root_deps
    )
    if not direct_storage_dependency:
        raise _error("consumer does not directly depend on the canonical storage bus")

    storage_node = nodes_by_id[storage_id]
    features = storage_node.get("features")
    if not isinstance(features, list):
        raise _error("canonical storage bus feature evidence is missing")
    if "client" in features or "default" in features:
        raise _error("canonical storage bus enabled its default/client feature")

    forbidden = sorted(
        {
            _package_name(package, package_id)
            for package_id, package in reachable.items()
            if _is_forbidden_package(_package_name(package, package_id))
        }
    )
    if forbidden:
        raise _error(f"normal/build graph contains forbidden packages: {', '.join(forbidden)}")

    if not isinstance(tree, str) or not tree.strip():
        raise _error("normal/build cargo tree output is missing")
    tree_packages = {
        match.group(1)
        for line in tree.splitlines()
        if (match := TREE_PACKAGE.match(line)) is not None
    }
    missing_tree_packages = sorted(EXPECTED_TREE_PACKAGES - tree_packages)
    if missing_tree_packages:
        raise _error(
            "normal/build cargo tree omitted required packages: "
            + ", ".join(missing_tree_packages)
        )
    tree_forbidden = sorted(name for name in tree_packages if _is_forbidden_package(name))
    if tree_forbidden:
        raise _error(f"normal/build cargo tree contains forbidden packages: {', '.join(tree_forbidden)}")


def _run_cargo(
    cargo_command: Sequence[str],
    arguments: Sequence[str],
    *,
    repo_root: Path,
    env: Mapping[str, str],
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    if not cargo_command:
        raise _error("Cargo command is empty")
    result = subprocess.run(
        [*cargo_command, *arguments],
        cwd=repo_root,
        env=dict(env),
        check=False,
        text=True,
        capture_output=capture_output,
    )
    if result.returncode != 0:
        if result.stderr:
            sys.stderr.write(result.stderr)
        raise _error(f"cargo {arguments[0]} exited with {result.returncode}")
    return result


def verify_consumer(
    repo_root: Path,
    *,
    cargo_command: Sequence[str] = ("cargo",),
    env_overrides: Mapping[str, str] | None = None,
) -> None:
    """Build the proof consumer and validate Cargo's locked normal/build graph."""
    repo_root = repo_root.resolve()
    manifest = repo_root / "proof-workspaces/storage-needs/Cargo.toml"
    target_dir = repo_root / "target/storage-needs-consumer"
    environment = os.environ.copy()
    if env_overrides:
        environment.update(env_overrides)

    _run_cargo(
        cargo_command,
        ["run", "--locked", "--target-dir", str(target_dir), "--manifest-path", str(manifest)],
        repo_root=repo_root,
        env=environment,
    )
    metadata_result = _run_cargo(
        cargo_command,
        ["metadata", "--locked", "--manifest-path", str(manifest), "--format-version", "1"],
        repo_root=repo_root,
        env=environment,
        capture_output=True,
    )
    tree_result = _run_cargo(
        cargo_command,
        [
            "tree",
            "--locked",
            "--manifest-path",
            str(manifest),
            "--prefix",
            "none",
            "--edges",
            "normal,build",
            "--target",
            "all",
        ],
        repo_root=repo_root,
        env=environment,
        capture_output=True,
    )
    try:
        metadata = json.loads(metadata_result.stdout)
    except json.JSONDecodeError as error:
        raise _error("cargo metadata returned invalid JSON") from error
    if not isinstance(metadata, dict):
        raise _error("cargo metadata root is not an object")
    validate_consumer_graph(metadata, tree_result.stdout)
    print(tree_result.stdout, end="")
    print("storage-needs consumer dependency graph is canonical and data-only")
