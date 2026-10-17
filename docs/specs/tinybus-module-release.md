# TinyBus Module Release

## Purpose

Generated projects must be usable as native TinyBus integrations and
distributable without also shipping the TinyBus host runtime.

## Contract

- The library builds as both an `rlib` and a native `cdylib`.
- The `cdylib` exports TinyBus module ABI v1, an embedded manifest, and the
  initialization entrypoint.
- The example module provides `ai.tinyhumans.template.Greeting.Greet` at
  `/ai/tinyhumans/template/Greeting`.
- Each release archive is named
  `template-<version>-<platform>.<extension>` and contains only this
  module, its SHA-256 `modules.toml`, license, and installation documentation.
- Each GitHub release publishes a separate `checksum.toml` mapping every
  archive filename to its SHA-256 digest for TinyBus's release loader.
- Release builds cover the stable native Ubuntu, macOS, and Windows runners,
  Fedora 43/44 containers, and rolling Arch Linux where official runners or
  images exist for the architecture.
- TinyBus itself remains a pinned SDK submodule and is not shipped as a release
  asset from this repository.

## Verification

CI exercises the bus interface through TinyBus's in-memory transport, enforces
90% line coverage in every source file, and builds the `cdylib`. The release
workflow builds each native module from the tagged source and records its exact
digest in the adjacent allowlist. After publishing, it downloads the Ubuntu
x86_64 archive through TinyBus's GitHub release API and calls `Greet` over an
in-memory bus.

## Reviewed-epic admission

The release workflow keeps ordinary `v*` tag pushes and manual releases on the
existing shared module-release workflow and its fixed TinyConnectors package
profile. A manual dispatch may instead select `reviewed-epic` and provide a
positive canonical pull-request number. The two release jobs are mutually
exclusive; reviewed-epic mode calls the pinned shared reviewed-release
workflow, which admits only the tag at the reviewed pull request's merged
commit after its required checks pass. The PR number is not accepted on the
ordinary path. Package and archive settings remain repository-owned rather
than dispatch inputs.
