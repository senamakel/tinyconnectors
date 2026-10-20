# 0004: Connector storage schema v2 remains module-owned

- **Status:** Accepted
- **Date:** 2026-10-20
- **Supersedes:** The schema-v2 ownership and grant-generation details in
  [ADR 0003](0003-connector-state-on-storage-ports.md).

## Context

[ADR 0003](0003-connector-state-on-storage-ports.md) records the decision to
store connector preferences and trigger history through the host's storage
ports. The v2 schema changes their durable names and migrates data from the v1
names and preference wrapper. The module is loaded as a cdylib, so the host
must not link its implementation crate to obtain schema behavior.

## Decision

- `tinyconnectors` owns the v2 schema, migration callbacks, and preference
  body encoding. The schema is owned by the module implementation and declares
  module-scoped resources.
- `tinyconnectors-bus` owns the durable resource names and the data-only
  `STORAGE_NEEDS` declaration. It requests the `connectors.user_scopes`
  collection and `connectors.triggers/` stream prefix, with no blob or secret
  prefixes.
- The host reads `tinyconnectors_bus::STORAGE_NEEDS` as input to its grant
  policy, then binds the selected scope to the authenticated caller. The host
  does not link `tinyconnectors` to rebuild the declaration.
- The schema-derived projection is a test-only consistency check between the
  implementation schema and the bus declaration. It is not the host's runtime
  grant source.
- Migration maps v1 `composio-user-scopes` documents from `{ "value": … }`
  bodies to `connectors.user_scopes` v2 bodies, and renames `triggers/<day>`
  streams to `connectors.triggers/<day>` without changing their entries. A
  released compiled-module migration exchange and host adapter must be
  available before the host can run these migration callbacks while granting
  the current resources.

## Consequences

- The bus contract can be consumed without depending on the module
  implementation or TinyBus client.
- The accepted port architecture in ADR 0003 remains in force; this ADR records
  the later schema version and ownership boundary.
- Until the host migration exchange and adapter are available, the module's
  storage schema does not by itself cause a host to migrate data.
