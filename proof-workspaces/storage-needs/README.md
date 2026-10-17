# Storage-needs consumer proof

This independent workspace models a data-only host using `tinyconnectors-bus`.
It also names the canonical `tinystoragedrivers-bus` package at the same pinned
release with default features disabled. That direct dependency proves the
contract's re-export is the producer's exact Rust type, not a structural copy.
The consumer imports and inspects the actual exported `STORAGE_NEEDS`, including
the complete `CollectionSpec` and every resource-prefix field. The test script
checks Cargo's resolved normal/build graph and feature set, so a wrong Git
source, enabled TinyBus client, or workspace feature unification cannot hide an
implementation dependency. The lockfile pins the consumer graph; refresh it with
`cargo update --manifest-path proof-workspaces/storage-needs/Cargo.toml --workspace`
when the root workspace package version changes.
