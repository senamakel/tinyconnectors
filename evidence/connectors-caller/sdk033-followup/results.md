# TinyConnectors SDK 0.3.3 Cargo follow-up

Source provenance before lock refresh: `e436e0876aa5ce0ea6299167b98278e498c2af81`, tree `91de620f744f85871a2c55d9be4c5fb5acf54beb`. Lockfile baseline: canonical merge `21ef5f87b9a86967c4fbeb2190e2c33ecad98134`.

Lock refresh used Cargo, without `--locked`:

- `cargo update --manifest-path Cargo.toml -p tinybus --precise 0.3.3` — exit 0; advanced TinyBus crates and the three TinyConnectors workspace crates to their manifest versions.
- `cargo update --manifest-path proof-workspaces/storage-needs/Cargo.toml` — exit 0; advanced only `tinyconnectors-bus` to 0.20.2.

Verification (all commands invoked through `/Users/enamakel/work/workflow-openhuman/openhuman/scripts/ci-cancel-aware.sh`; logs are beside this file):

- `cargo check --workspace --all-targets --all-features --locked` — exit 0 (`cargo-check.log`).
- `cargo fmt --all -- --check` — exit 0 (`fmt.log`).
- `cargo clippy --workspace --all-targets --all-features --locked -- -D warnings` — exit 0 (`clippy.log`).
- `cargo test --workspace --locked` — exit 0 (`cargo-test-default.log`).
- `cargo test --workspace --all-features --locked` — exit 0 (`cargo-test.log`).
- `bash scripts/test-storage-needs-consumer.sh` — exit 0 (`storage-needs-consumer.log`); compiled and ran the isolated locked consumer, inspected actual Cargo metadata and normal/build dependency tree, and proved the canonical revision and data-only dependency graph.
- `bash scripts/test-e2e.sh` — first attempt built the module archive, then verifier Cargo failed because `target/release/deps` disappeared (`module-e2e.log`). Local script audit found no command removing that directory. A single exact retry passed (`module-e2e-retry.log`): release cdylib built, archived, loaded through the real TinyBus verifier, and reported `verified ... as TinyBus module `tinyconnectors``.
- `git diff --check` — exit 0.

Host target reported by `rustc -vV`: `aarch64-apple-darwin` (macOS native module). Archive SHA-256: `bd0fd6282bb6a1d4976e405c9f05f758552f8380f13d119ca4234de5699a4b22`. Packaged cdylib SHA-256: `28c63d8b7017e605b45d81a38637075e4a5a68f51203aabb0f1875eb9004d545`.

The source tree was already frozen at the stated provenance. This follow-up changes only the workspace and proof-workspace lockfiles and adds evidence. Full hosted OS matrix, distro builds, coverage, release supply-chain checks, and hosted CI remain outside this focused Cargo lease.
