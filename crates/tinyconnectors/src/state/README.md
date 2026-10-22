# Connector state migration

The module prepares owner-local migrations before exposing a bound storage
handle. Completion markers are written only after each migration step returns
successfully, so a failed or interrupted step can be retried by the next open.

## Preferences

The v0.21 preference collection is paged into the current preferences
collection. `legacy_key` preserves the retired core key encoding, including its
escaping and long-key digest. A conflicting target is authoritative and is not
overwritten. Matching targets from an interrupted migration are accepted.
After copying a page, the migration rereads each source and conditionally
updates the target if it observed a concurrent legacy write. It also reconciles
the migrated targets after pagination, immediately before writing the marker.

The storage API has no transaction that spans a collection scan and marker
write. The deployment must quiesce v0.21 writers before opening this migration;
the rereads close races during the scan, but cannot prevent a legacy write after
the final reconciliation and before the completion marker.

## Trigger streams

Each legacy day stream is copied to the current prefix at matching offsets.
Existing target prefixes are verified before resuming, and a concurrent append
is accepted only when it matches the page at the expected offsets. The source
stream remains in place because the port has no compare-and-delete operation.
History reads both prefixes and deduplicates only matching values at the same
day and offset, so repeated deliveries with the same metadata remain visible.
