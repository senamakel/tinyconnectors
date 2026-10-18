"""Exercise routing and input guards in the GitHub release workflow."""
import os
from pathlib import Path
import re
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parent.parent
WORKFLOW = ROOT / ".github/workflows/release.yml"
CI_WORKFLOW = ROOT / ".github/workflows/ci.yml"
REVIEWED_RELEASE_PIN = "6ed83b04aab2d8977ec62e966cb914531e534fa4"
LEGACY_RELEASE_PIN = "4adc1080fb495098e7af1aade82ed7e6e3c69bb7"


def resolve_script(workflow):
    match = re.search(r"(?ms)^      - id: tag\n.*?^        run: \|\n((?:^          .*\n|^\n)+)", workflow)
    if not match:
        raise AssertionError("release resolve script was not found")
    return "\n".join(line[10:] if line.startswith("          ") else "" for line in match[1].splitlines())


def job_block(workflow, name):
    match = re.search(rf"(?ms)^  {re.escape(name)}:\n(.*?)(?=^  [a-zA-Z0-9_-]+:|\Z)", workflow)
    if not match:
        raise AssertionError(f"release job {name} was not found")
    return match[1]


def job_permissions(block):
    match = re.search(r"(?ms)^    permissions:\n((?:^      [a-z-]+: [a-z]+\n)+)", block)
    if not match:
        raise AssertionError("job permissions were not found")
    return dict(re.findall(r"(?m)^      ([a-z-]+): ([a-z]+)$", match[1]))


class ReleaseWorkflowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.workflow = WORKFLOW.read_text()
        cls.script = resolve_script(cls.workflow)

    def run_resolve(self, mode="main", reviewed_pr="", event="workflow_dispatch", tag="v1.2.3"):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            effective_mode = "main" if event == "push" else mode
            env = dict(os.environ, TAG=tag, RELEASE_MODE=effective_mode,
                       REVIEWED_PR=reviewed_pr, GITHUB_OUTPUT=str(output))
            if event == "push":
                env["TAG"] = "v2.3.4"
            result = subprocess.run(["bash", "-c", self.script], env=env,
                                    cwd=directory, text=True, capture_output=True)
            return result, output.read_text() if output.exists() else ""

    def output_values(self, output):
        return dict(line.split("=", maxsplit=1) for line in output.splitlines())

    def test_manual_default_and_explicit_main_route_to_legacy_release(self):
        self.assertIn("default: main", self.workflow)
        result, output = self.run_resolve("main")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output_values(output), {
            "tag": "v1.2.3",
            "version": "1.2.3",
            "release_mode": "main",
            "reviewed_pr": "0",
        })
        self.assertIn("github.event_name == 'push' && 'main' || inputs.release_mode", self.workflow)
        self.assertIn("github.event_name == 'push' && github.ref_name || inputs.tag", self.workflow)

    def test_tag_push_forces_legacy_release_mode(self):
        result, output = self.run_resolve("reviewed-epic", "", event="push")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output_values(output), {
            "tag": "v2.3.4",
            "version": "2.3.4",
            "release_mode": "main",
            "reviewed_pr": "0",
        })

    def test_reviewed_epic_requires_and_forwards_positive_integer_pr(self):
        result, output = self.run_resolve("reviewed-epic", "91")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.output_values(output), {
            "tag": "v1.2.3",
            "version": "1.2.3",
            "release_mode": "reviewed-epic",
            "reviewed_pr": "91",
        })

    def test_reviewed_epic_rejects_missing_zero_and_non_integer_prs(self):
        for value in ("", "0", "-1", "1.5", "abc", "01"):
            with self.subTest(value=value):
                result, _ = self.run_resolve("reviewed-epic", value)
                self.assertEqual(result.returncode, 2)
                self.assertIn("reviewed_pr must be a positive integer", result.stderr)

    def test_unknown_mode_and_invalid_tags_are_rejected(self):
        result, _ = self.run_resolve("other")
        self.assertEqual(result.returncode, 2)
        self.assertIn("release_mode must be main or reviewed-epic", result.stderr)
        result, _ = self.run_resolve(tag="1.2.3")
        self.assertEqual(result.returncode, 2)
        self.assertIn("tag must be v-prefixed semver", result.stderr)

    def test_ordinary_path_rejects_reviewed_pr_and_keeps_fixed_package_profile(self):
        result, _ = self.run_resolve("main", "91")
        self.assertEqual(result.returncode, 2)
        self.assertIn("reviewed_pr is only valid", result.stderr)
        ordinary = job_block(self.workflow, "module-release")
        self.assertRegex(
            ordinary,
            rf"(?m)^    uses: tinyhumansai/\.github/\.github/workflows/module-release\.yml@{LEGACY_RELEASE_PIN}$",
        )
        self.assertIn("module-crate: tinyconnectors", ordinary)
        self.assertNotIn("extra-packaged-files:", ordinary)
        self.assertNotIn("inputs.package", self.workflow)
        self.assertNotIn("inputs.repo", self.workflow)

    def test_shared_workflows_have_exact_full_commit_pins(self):
        reviewed = job_block(self.workflow, "module-reviewed-epic-release")
        self.assertRegex(
            reviewed,
            rf"(?m)^    uses: tinyhumansai/\.github/\.github/workflows/module-reviewed-epic-release\.yml@{REVIEWED_RELEASE_PIN}$",
        )
        self.assertEqual(len(REVIEWED_RELEASE_PIN), 40)
        refs = re.findall(
            r"(?m)^    uses: tinyhumansai/\.github/\.github/workflows/[^@\s]+@([0-9a-f]+)$",
            self.workflow,
        )
        self.assertEqual(len(refs), 2, refs)
        self.assertTrue(all(len(ref) == 40 for ref in refs), refs)

    def test_release_routes_are_exclusive_and_permissions_are_minimal(self):
        ordinary = job_block(self.workflow, "module-release")
        reviewed = job_block(self.workflow, "module-reviewed-epic-release")
        self.assertRegex(ordinary, r"(?m)^    if: needs\.resolve\.outputs\.release_mode == 'main'$")
        self.assertRegex(reviewed, r"(?m)^    if: needs\.resolve\.outputs\.release_mode == 'reviewed-epic'$")
        self.assertEqual(job_permissions(ordinary), {"contents": "write"})
        self.assertEqual(job_permissions(reviewed), {
            "contents": "write",
            "pull-requests": "read",
            "checks": "read",
            "actions": "read",
        })
        self.assertIn("reviewed-pr: ${{ fromJSON(needs.resolve.outputs.reviewed_pr) }}", reviewed)

    def test_release_dependencies_match_the_published_bus_and_sdk_sources(self):
        manifest = (ROOT / "Cargo.toml").read_text()
        self.assertIn('version = "0.20.1"', manifest)
        self.assertIn(
            'tinystoragedrivers-bus = { git = "https://github.com/tinyhumansai/tinystoragedrivers", '
            'rev = "c9ea6ff1615b77b55d7269403af5fd11bedc9ce8", default-features = false }',
            manifest,
        )
        self.assertRegex(manifest, r'(?m)^tinybus = \{ version = "0\.3\.2",')
        self.assertRegex(manifest, r'(?m)^tinybus-module = "0\.3\.2"$')
        self.assertIn(
            '[patch.crates-io]\ntinybus = { path = "vendor/tinybus/crates/tinybus" }\n'
            'tinybus-module = { path = "vendor/tinybus/crates/tinybus-module" }',
            manifest,
        )
        self.assertIn(
            '[patch."https://github.com/tinyhumansai/tinybus"]\n'
            'tinybus = { path = "vendor/tinybus/crates/tinybus" }',
            manifest,
        )
        tree_entry = subprocess.run(
            ["git", "ls-tree", "HEAD", "vendor/tinybus"], cwd=ROOT,
            text=True, capture_output=True, check=True,
        ).stdout.split()
        self.assertEqual(tree_entry[:2], ["160000", "commit"])
        self.assertEqual(tree_entry[2], "cba2de0e573f99fe4c7e45ba14e2d6b5a2a7757f")
        proof_manifest = (ROOT / "proof-workspaces/storage-needs/Cargo.toml").read_text()
        self.assertIn(
            'tinystoragedrivers-bus = { git = "https://github.com/tinyhumansai/tinystoragedrivers", '
            'rev = "c9ea6ff1615b77b55d7269403af5fd11bedc9ce8", default-features = false }',
            proof_manifest,
        )
        self.assertNotRegex(proof_manifest, r"(?m)^tinybus(?:-module)?\s*=")

    def test_python_release_contract_job_checks_out_source_with_read_only_access(self):
        ci = CI_WORKFLOW.read_text()
        job = job_block(ci, "release-workflow-tests")
        self.assertEqual(job_permissions(job), {"contents": "read"})
        self.assertRegex(
            job,
            r"(?m)^    steps:\n      - uses: actions/checkout@[0-9a-f]{40} # v4\.4\.0$",
        )
        self.assertIn("run: python3 scripts/test-release-workflow.py", job)


if __name__ == "__main__":
    unittest.main()
