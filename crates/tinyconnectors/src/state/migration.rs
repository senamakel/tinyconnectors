//! Owner-local migration of the connector module's persisted records.

use serde_json::{Value, json};
use std::collections::HashMap;
use tinystoragedrivers_api::{DocumentStore, Storage, StreamStore};
use tinystoragedrivers_types::{
    CollectionSpec, Key, Precondition, Query, StorageError, Version, Versioned, WriteOptions,
};

use tinyconnectors_bus::storage::{
    LEGACY_PREFS_NAMESPACE, LEGACY_STREAM_PREFIX, MIGRATIONS_NAMESPACE, PREFS_NAMESPACE,
    PREFS_SCHEMA_VERSION, STREAM_PREFIX, encode_preference,
};

const MIGRATIONS: &str = MIGRATIONS_NAMESPACE;
const PREFS_STEP: &str = "prefs-v1-unwrap";
const STREAMS_STEP: &str = "triggers-v1-rename";
const PAGE_SIZE: usize = 128;

/// Collections the host should ensure before opening this module.
#[must_use]
pub fn collections() -> Vec<CollectionSpec> {
    vec![
        CollectionSpec::new(PREFS_NAMESPACE),
        CollectionSpec::new(MIGRATIONS),
    ]
}

/// Run `TinyConnectors`' migrations against its already-bound storage ports.
///
/// # Errors
/// Returns the first storage or migration error. A later open can safely retry
/// because each migrated record is conditionally written and each step is
/// recorded only after its transformation completes.
pub(crate) async fn prepare(storage: &Storage) -> tinystoragedrivers_types::Result<()> {
    let documents = storage.documents();
    let streams = storage.streams();
    if !done(documents.as_ref(), PREFS_STEP).await? {
        migrate_preferences(documents.as_ref()).await?;
        mark_done(documents.as_ref(), PREFS_STEP).await?;
    }
    if !done(documents.as_ref(), STREAMS_STEP).await? {
        migrate_streams(streams.as_ref()).await?;
        mark_done(documents.as_ref(), STREAMS_STEP).await?;
    }
    Ok(())
}

async fn done(documents: &dyn DocumentStore, step: &str) -> tinystoragedrivers_types::Result<bool> {
    let Some(record) = documents.get(MIGRATIONS, step).await? else {
        return Ok(false);
    };
    if record.doc["owner"] == "connectors"
        && record.doc["step"] == step
        && record.doc["done"] == true
    {
        Ok(true)
    } else {
        Err(StorageError::conflict(format!(
            "invalid connector migration marker for `{step}`"
        )))
    }
}

async fn mark_done(
    documents: &dyn DocumentStore,
    step: &str,
) -> tinystoragedrivers_types::Result<()> {
    match documents
        .put(
            MIGRATIONS,
            step,
            json!({"owner": "connectors", "step": step, "done": true}),
            Precondition::Absent,
        )
        .await
    {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
            if done(documents, step).await? {
                Ok(())
            } else {
                Err(error)
            }
        }
        Err(error) => Err(error),
    }
}

async fn migrate_preferences(
    documents: &dyn DocumentStore,
) -> tinystoragedrivers_types::Result<()> {
    let mut query = Query::all().limit(PAGE_SIZE);
    let mut migrated_targets = HashMap::new();
    loop {
        let page = documents.query(LEGACY_PREFS_NAMESPACE, &query).await?;
        if page.items.is_empty() {
            break;
        }
        let next = page.next.clone();
        for record in page.items {
            let source_id = record.id.clone();
            let id = legacy_key([record.id.as_str()])?;
            if let Some(target_version) = migrate_preference(documents, &id, record).await? {
                migrated_targets.insert(source_id, (id, target_version));
            }
        }
        let Some(cursor) = next else { break };
        query.cursor = Some(cursor);
    }
    // A legacy writer can update a row after its page was copied but before
    // pagination reaches the end. Reconcile every target written by this pass
    // once more immediately before recording the migration marker.
    for (source_id, (id, target_version)) in migrated_targets {
        reconcile_preference(documents, &source_id, &id, target_version).await?;
    }
    Ok(())
}

async fn migrate_preference(
    documents: &dyn DocumentStore,
    id: &Key,
    mut source: Versioned<Value>,
) -> tinystoragedrivers_types::Result<Option<Version>> {
    let body = preference_body(&source);
    let expires_ms = source.meta.expires_ms;
    let mut target_version = match put_preference(
        documents,
        id,
        body.clone(),
        expires_ms,
        Precondition::Absent,
    )
    .await
    {
        Ok(version) => version,
        Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
            let Some(current) = documents.get(PREFS_NAMESPACE, id.as_str()).await? else {
                return Err(error);
            };
            if current.doc != body || current.meta.expires_ms != expires_ms {
                // A target already written by another module instance is
                // authoritative. Preserve it and leave the legacy source
                // intact so a later open can retry if necessary.
                tracing::warn!(
                    preference_id = id.as_str(),
                    "preserving conflicting connector preference target"
                );
                return Ok(None);
            }
            current.version
        }
        Err(error) => return Err(error),
    };

    // A legacy writer may update this row while the paged scan is in flight.
    // Re-read after each conditional target write and follow any observed
    // source update while the target remains the version this migration wrote.
    loop {
        let Some(latest) = documents
            .get(LEGACY_PREFS_NAMESPACE, source.id.as_str())
            .await?
        else {
            match documents
                .delete(
                    PREFS_NAMESPACE,
                    id.as_str(),
                    Precondition::Version(target_version),
                )
                .await
            {
                Ok(_) => {}
                Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                    tracing::warn!(
                        preference_id = id.as_str(),
                        "preserving a preference changed after legacy deletion"
                    );
                }
                Err(error) => return Err(error),
            }
            return Ok(None);
        };
        if preference_body(&latest) == preference_body(&source)
            && latest.meta.expires_ms == source.meta.expires_ms
        {
            return Ok(Some(target_version));
        }
        let latest_body = preference_body(&latest);
        match put_preference(
            documents,
            id,
            latest_body,
            latest.meta.expires_ms,
            Precondition::Version(target_version),
        )
        .await
        {
            Ok(version) => {
                target_version = version;
                source = latest;
            }
            Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                tracing::warn!(
                    preference_id = id.as_str(),
                    "preserving a preference changed while its legacy source was being migrated"
                );
                return Ok(None);
            }
            Err(error) => return Err(error),
        }
    }
}

async fn reconcile_preference(
    documents: &dyn DocumentStore,
    source_id: &str,
    id: &Key,
    mut target_version: Version,
) -> tinystoragedrivers_types::Result<()> {
    loop {
        let Some(source) = documents.get(LEGACY_PREFS_NAMESPACE, source_id).await? else {
            match documents
                .delete(
                    PREFS_NAMESPACE,
                    id.as_str(),
                    Precondition::Version(target_version),
                )
                .await
            {
                Ok(_) => {}
                Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                    tracing::warn!(
                        preference_id = id.as_str(),
                        "preserving a preference changed after legacy deletion"
                    );
                }
                Err(error) => return Err(error),
            }
            return Ok(());
        };
        let current = documents.get(PREFS_NAMESPACE, id.as_str()).await?;
        let Some(current) = current else {
            return Err(StorageError::conflict(
                "connector preference target disappeared during reconciliation",
            ));
        };
        if current.version != target_version {
            // A target updated after the first pass is authoritative. Do not
            // overwrite a newer module write with the legacy snapshot.
            return Ok(());
        }
        let body = preference_body(&source);
        if current.doc == body && current.meta.expires_ms == source.meta.expires_ms {
            return Ok(());
        }
        match put_preference(
            documents,
            id,
            body,
            source.meta.expires_ms,
            Precondition::Version(target_version),
        )
        .await
        {
            Ok(version) => target_version = version,
            Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                return Ok(());
            }
            Err(error) => return Err(error),
        }
    }
}

async fn put_preference(
    documents: &dyn DocumentStore,
    id: &Key,
    body: Value,
    expires_ms: Option<i64>,
    precondition: Precondition,
) -> tinystoragedrivers_types::Result<Version> {
    // Expiration is application-significant and is carried over the module
    // storage bus. `created_ms` remains host-owned for this new cross-collection
    // record rather than impersonating the legacy record's timestamp.
    let mut options = WriteOptions::new().schema(PREFS_SCHEMA_VERSION);
    if let Some(expires_ms) = expires_ms {
        options = options.expires_ms(expires_ms);
    }
    documents
        .put_with(PREFS_NAMESPACE, id.as_str(), body, precondition, options)
        .await
}

fn preference_body(source: &Versioned<Value>) -> Value {
    let value = match &source.doc {
        Value::Object(fields) => fields.get("value").cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    };
    encode_preference(&value)
}

async fn migrate_streams(streams: &dyn StreamStore) -> tinystoragedrivers_types::Result<()> {
    let names = streams.streams(LEGACY_STREAM_PREFIX).await?;
    for source in names {
        // `StreamStore::streams(prefix)` returns only names with this prefix;
        // surface a contract violation without risking deletion under a bad key.
        let day = source.strip_prefix(LEGACY_STREAM_PREFIX).ok_or_else(|| {
            StorageError::invalid_input("stream listing returned a name outside its prefix")
        })?;
        let target = format!("{STREAM_PREFIX}{day}");
        let source_len = streams.len(&source).await?;
        let target_len = streams.len(&target).await?;
        if target_len > source_len {
            return Err(StorageError::conflict(format!(
                "migration target `{target}` is longer than source `{source}`"
            )));
        }

        // A prior opener may have copied some or all of this stream before it
        // failed to record completion. Verify that prefix before appending or
        // deleting the source; equal lengths alone do not prove that the
        // target contains the same events.
        let mut verified = 0;
        while verified < target_len {
            let limit = usize::try_from((target_len - verified).min(PAGE_SIZE as u64))
                .map_err(|_| StorageError::invalid_input("stream page exceeds addressable size"))?;
            let source_entries = streams.read_window(&source, verified, limit).await?;
            let target_entries = streams.read_window(&target, verified, limit).await?;
            if source_entries.len() != limit
                || target_entries.len() != limit
                || source_entries
                    .iter()
                    .zip(&target_entries)
                    .any(|(source, target)| source.value != target.value)
            {
                return Err(StorageError::conflict(format!(
                    "migration target `{target}` differs from source `{source}`"
                )));
            }
            verified += limit as u64;
        }
        let mut offset = target_len;
        while offset < source_len {
            let entries = streams.read_window(&source, offset, PAGE_SIZE).await?;
            if entries.is_empty() {
                return Err(StorageError::conflict(format!(
                    "migration source `{source}` changed while it was copied"
                )));
            }
            let values = entries
                .into_iter()
                .map(|entry| entry.value)
                .collect::<Vec<_>>();
            match streams
                .append_batch_if_offset(&target, offset, values.clone())
                .await
            {
                // The port returns the first appended offset by contract.
                Ok(_) => {}
                Err(error) if error.kind() == tinystoragedrivers_types::ErrorKind::Conflict => {
                    // Another opener may have copied this same page. Accept
                    // only an exact match at the expected offsets.
                    let raced = streams.read_window(&target, offset, values.len()).await?;
                    if raced.len() != values.len()
                        || raced
                            .iter()
                            .zip(&values)
                            .any(|(entry, value)| entry.value != *value)
                    {
                        return Err(error);
                    }
                }
                Err(error) => return Err(error),
            }
            offset = streams.len(&target).await?;
        }
        if streams.len(&source).await? != source_len {
            return Err(StorageError::conflict(format!(
                "migration source `{source}` changed while it was copied"
            )));
        }
        // Keep the legacy source after copying. StreamStore has no
        // compare-and-delete operation, so a legacy writer could append after
        // the final length check and before deletion. Trigger history reads
        // both prefixes and deduplicates exact copies by day, offset, and value.
    }
    Ok(())
}

/// Preserve the retired core `Key::of` encoding for on-disk ids.
pub(super) fn legacy_key(
    parts: impl IntoIterator<Item = impl AsRef<str>>,
) -> tinystoragedrivers_types::Result<Key> {
    const MAX_ID_LEN: usize = 512;
    let mut escaped = String::new();
    let mut count = 0;
    for part in parts {
        if count > 0 {
            escaped.push('/');
        }
        for (index, ch) in part.as_ref().chars().enumerate() {
            match ch {
                '%' => escaped.push_str("%25"),
                '/' => escaped.push_str("%2F"),
                '\0' => escaped.push_str("%00"),
                '~' if count == 0 && index == 0 => escaped.push_str("%7E"),
                other => escaped.push(other),
            }
        }
        count += 1;
    }
    if count == 0 {
        return Err(StorageError::invalid_input("a key needs at least one part"));
    }
    if escaped.len() > MAX_ID_LEN {
        use sha2::{Digest, Sha256};
        escaped = format!(
            "~{}",
            data_encoding::BASE32_NOPAD
                .encode(&Sha256::digest(escaped.as_bytes()))
                .to_ascii_lowercase()
        );
    }
    Key::new(escaped)
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
