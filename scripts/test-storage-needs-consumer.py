"""Test the independent storage-needs consumer's source and feature guards."""
from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import tomllib
import unittest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

from storage_needs_consumer import (  # noqa: E402
    EXPECTED_SOURCE,
    EXPECTED_CONTRACT_VERSION,
    ProofError,
    validate_consumer_graph,
    verify_consumer,
)

FAKE_CARGO = ROOT / "scripts/test-support/fake-storage-needs-cargo.py"
ROOT_ID = "path+file:///fixture/storage-needs#tinyconnectors-storage-needs-consumer-proof@0.0.0"
BUS_ID = f"path+file:///fixture/crates/tinyconnectors-bus#tinyconnectors-bus@{EXPECTED_CONTRACT_VERSION}"
TSD_ID = (
    "git+https://github.com/tinyhumansai/tinystoragedrivers"
    "?rev=a04e3a174fa8f28d5229a816ca6b39c72a9efd7b"
    "#tinystoragedrivers-bus@1.9.0"
)
SERDE_ID = "registry+https://github.com/rust-lang/crates.io-index#serde@1.0.229"
SERDE_JSON_ID = "registry+https://github.com/rust-lang/crates.io-index#serde_json@1.0.154"


def edge(name, package_id, kind=None):
    return {"name": name.replace("-", "_"), "pkg": package_id, "dep_kinds": [{"kind": kind, "target": None}]}


def good_metadata():
    return {
        "packages": [
            {"id": ROOT_ID, "name": "tinyconnectors-storage-needs-consumer-proof", "version": "0.0.0", "source": None},
            {"id": BUS_ID, "name": "tinyconnectors-bus", "version": EXPECTED_CONTRACT_VERSION, "source": None},
            {"id": TSD_ID, "name": "tinystoragedrivers-bus", "version": "1.9.0", "source": EXPECTED_SOURCE},
            {"id": SERDE_ID, "name": "serde", "version": "1.0.229", "source": "registry+https://github.com/rust-lang/crates.io-index"},
            {"id": SERDE_JSON_ID, "name": "serde_json", "version": "1.0.154", "source": "registry+https://github.com/rust-lang/crates.io-index"},
        ],
        "resolve": {
            "root": ROOT_ID,
            "nodes": [
                {
                    "id": ROOT_ID,
                    "features": [],
                    "deps": [edge("tinyconnectors-bus", BUS_ID), edge("tinystoragedrivers-bus", TSD_ID)],
                },
                {"id": BUS_ID, "features": [], "deps": [edge("tinystoragedrivers-bus", TSD_ID)]},
                {
                    "id": TSD_ID,
                    "features": [],
                    "deps": [edge("serde", SERDE_ID), edge("serde_json", SERDE_JSON_ID)],
                },
                {"id": SERDE_ID, "features": ["derive"], "deps": []},
                {"id": SERDE_JSON_ID, "features": [], "deps": [edge("serde", SERDE_ID)]},
            ],
        },
    }


def good_tree():
    return "\n".join(
        [
            "tinyconnectors-storage-needs-consumer-proof v0.0.0 (/fixture/storage-needs)",
            f"tinyconnectors-bus v{EXPECTED_CONTRACT_VERSION} (/fixture/crates/tinyconnectors-bus)",
            "tinystoragedrivers-bus v1.9.0 (https://github.com/tinyhumansai/tinystoragedrivers?rev=a04e3a174fa8f28d5229a816ca6b39c72a9efd7b#a04e3a17)",
            "serde v1.0.229",
            "serde_json v1.0.154",
        ]
    ) + "\n"


def add_dependency(metadata, package_id, package_name, *, kind=None, features=None):
    package = {
        "id": package_id,
        "name": package_name,
        "version": "0.1.0",
        "source": "registry+https://github.com/rust-lang/crates.io-index",
    }
    metadata["packages"].append(package)
    metadata["resolve"]["nodes"].append(
        {"id": package_id, "features": features or [], "deps": []}
    )
    root_node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == ROOT_ID)
    root_node["deps"].append(edge(package_name, package_id, kind))


class StorageNeedsConsumerTests(unittest.TestCase):
    def test_storage_implementation_lives_outside_the_bus_contract(self):
        bus_manifest = tomllib.loads(
            (ROOT / "crates/tinyconnectors-bus/Cargo.toml").read_text()
        )
        self.assertNotIn("tinystoragedrivers-core", bus_manifest["dependencies"])
        self.assertNotIn(
            "tinystoragedrivers-core", bus_manifest.get("dev-dependencies", {})
        )
        self.assertEqual(bus_manifest["features"]["storage"], [])

        bus_storage = ROOT / "crates/tinyconnectors-bus/src/storage"
        self.assertEqual(
            {path.name for path in bus_storage.glob("*.rs")},
            {"mod.rs", "names.rs"},
        )
        bus_lib = (ROOT / "crates/tinyconnectors-bus/src/lib.rs").read_text()
        self.assertIn("STORAGE_NEEDS", bus_lib)
        self.assertIn("StorageNeeds", bus_lib)

        module_manifest = tomllib.loads(
            (ROOT / "crates/tinyconnectors/Cargo.toml").read_text()
        )
        self.assertNotIn(
            "storage",
            module_manifest["dependencies"]["tinyconnectors-bus"].get("features", []),
        )
        schema = ROOT / "crates/tinyconnectors/src/state/storage_schema.rs"
        self.assertIn("Step::new(", schema.read_text())
        self.assertIn("Step::stream(", schema.read_text())
        self.assertTrue(
            (ROOT / "crates/tinyconnectors/src/state/storage_schema_tests.rs").is_file()
        )

    def test_accepts_direct_exact_source_without_client_features(self):
        validate_consumer_graph(good_metadata(), good_tree())

    def test_rejects_wrong_contract_version(self):
        metadata = good_metadata()
        contract = next(package for package in metadata["packages"] if package["id"] == BUS_ID)
        contract["version"] = "0.20.2"
        with self.assertRaisesRegex(ProofError, "contract version is not the workspace release"):
            validate_consumer_graph(metadata, good_tree())

    def test_rejects_forbidden_normal_dependency(self):
        metadata = good_metadata()
        add_dependency(metadata, "registry#tinybus@0.1.7", "tinybus")
        with self.assertRaisesRegex(ProofError, "forbidden packages: tinybus"):
            validate_consumer_graph(metadata, good_tree() + "\ntinybus v0.1.7")

    def test_rejects_forbidden_build_dependency(self):
        metadata = good_metadata()
        add_dependency(metadata, "registry#tinybus@0.1.7", "tinybus", kind="build")
        with self.assertRaisesRegex(ProofError, "forbidden packages: tinybus"):
            validate_consumer_graph(metadata, good_tree() + "\ntinybus v0.1.7")

    def test_rejects_storage_driver_implementation_dependency(self):
        metadata = good_metadata()
        add_dependency(metadata, "git#tinystoragedrivers-client@1.0.0", "tinystoragedrivers-client")
        with self.assertRaisesRegex(ProofError, "tinystoragedrivers-client"):
            validate_consumer_graph(metadata, good_tree() + "tinystoragedrivers-client v1.0.0\n")

    def test_rejects_tinyconnectors_implementation_dependency(self):
        metadata = good_metadata()
        add_dependency(metadata, "path#tinyconnectors@0.21.1", "tinyconnectors")
        with self.assertRaisesRegex(ProofError, "forbidden packages: tinyconnectors"):
            validate_consumer_graph(metadata, good_tree() + "tinyconnectors v0.21.1\n")

    def test_rejects_async_runtime_dependency(self):
        metadata = good_metadata()
        add_dependency(metadata, "registry#tokio@1.0.0", "tokio")
        with self.assertRaisesRegex(ProofError, "forbidden packages: tokio"):
            validate_consumer_graph(metadata, good_tree() + "tokio v1.0.0\n")

    def test_dev_only_forbidden_dependency_is_outside_the_normal_build_graph(self):
        metadata = good_metadata()
        add_dependency(metadata, "registry#tinybus@0.1.7", "tinybus", kind="dev")
        validate_consumer_graph(metadata, good_tree())

    def test_rejects_wrong_canonical_revision(self):
        metadata = good_metadata()
        package = next(package for package in metadata["packages"] if package["id"] == TSD_ID)
        package["source"] = EXPECTED_SOURCE.replace("a04e3a1", "08caa16")
        with self.assertRaisesRegex(ProofError, "source is not the pinned"):
            validate_consumer_graph(metadata, good_tree())

    def test_rejects_tag_source_even_when_name_and_version_match(self):
        metadata = good_metadata()
        package = next(package for package in metadata["packages"] if package["id"] == TSD_ID)
        package["source"] = "git+https://github.com/tinyhumansai/tinystoragedrivers?tag=v1.9.0#a04e3a17"
        with self.assertRaisesRegex(ProofError, "source is not the pinned"):
            validate_consumer_graph(metadata, good_tree())

    def test_rejects_client_feature_even_when_source_is_correct(self):
        metadata = good_metadata()
        node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == TSD_ID)
        node["features"] = ["client"]
        with self.assertRaisesRegex(ProofError, "default/client feature"):
            validate_consumer_graph(metadata, good_tree())

    def test_rejects_missing_direct_source_dependency(self):
        metadata = good_metadata()
        root_node = next(node for node in metadata["resolve"]["nodes"] if node["id"] == ROOT_ID)
        root_node["deps"] = [dep for dep in root_node["deps"] if dep["pkg"] != TSD_ID]
        with self.assertRaisesRegex(ProofError, "does not directly depend"):
            validate_consumer_graph(metadata, good_tree())

    def test_fails_closed_when_a_reachable_resolve_node_is_missing(self):
        metadata = good_metadata()
        metadata["resolve"]["nodes"] = [
            node for node in metadata["resolve"]["nodes"] if node["id"] != TSD_ID
        ]
        with self.assertRaisesRegex(ProofError, "resolve evidence is missing"):
            validate_consumer_graph(metadata, good_tree())

    def test_fails_closed_when_the_resolve_root_is_missing(self):
        metadata = good_metadata()
        metadata["resolve"]["root"] = None
        with self.assertRaisesRegex(ProofError, "resolve root is missing"):
            validate_consumer_graph(metadata, good_tree())

    def test_fails_closed_when_normal_build_tree_omits_the_storage_package(self):
        tree = good_tree().replace(
            "tinystoragedrivers-bus v1.9.0 (https://github.com/tinyhumansai/tinystoragedrivers?rev=a04e3a174fa8f28d5229a816ca6b39c72a9efd7b#a04e3a17)\n",
            "",
        )
        with self.assertRaisesRegex(ProofError, "cargo tree omitted required packages"):
            validate_consumer_graph(good_metadata(), tree)

    def test_fake_cargo_probes_run_metadata_and_normal_build_tree(self):
        metadata = good_metadata()
        tree = good_tree()
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary = Path(temporary_directory)
            metadata_path = temporary / "metadata.json"
            tree_path = temporary / "tree.txt"
            calls_path = temporary / "calls.jsonl"
            metadata_path.write_text(json.dumps(metadata))
            tree_path.write_text(tree)
            env = {
                "FAKE_CARGO_METADATA": str(metadata_path),
                "FAKE_CARGO_TREE": str(tree_path),
                "FAKE_CARGO_CALLS": str(calls_path),
            }
            verify_consumer(
                ROOT,
                cargo_command=(sys.executable, str(FAKE_CARGO)),
                env_overrides=env,
            )
            calls = [json.loads(line) for line in calls_path.read_text().splitlines()]

        self.assertEqual([call[0] for call in calls], ["run", "metadata", "tree"])
        proof_manifest = str(ROOT / "proof-workspaces/storage-needs/Cargo.toml")
        self.assertIn("--locked", calls[0])
        self.assertIn("--target-dir", calls[0])
        self.assertIn(proof_manifest, calls[0])
        self.assertIn("--locked", calls[1])
        self.assertIn(proof_manifest, calls[1])
        self.assertIn("--locked", calls[2])
        self.assertIn("normal,build", calls[2])
        self.assertEqual(calls[2][calls[2].index("--target") + 1], "all")


if __name__ == "__main__":
    if sys.argv[1:] == ["--verify"]:
        try:
            verify_consumer(ROOT)
        except ProofError as error:
            print(error, file=sys.stderr)
            raise SystemExit(1) from error
    else:
        unittest.main()
