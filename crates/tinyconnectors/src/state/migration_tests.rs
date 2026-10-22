//! Covers owner-local migration compatibility, retries, and concurrent opens.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;

#[derive(Debug)]
struct CommitThenConflictStreams {
    inner: std::sync::Arc<dyn tinystoragedrivers_api::StreamStore>,
    raced: std::sync::atomic::AtomicBool,
    append_to_source: Option<String>,
}

#[derive(Debug)]
struct CommitThenConflictMarker {
    inner: std::sync::Arc<dyn tinystoragedrivers_api::DocumentStore>,
    pending: std::sync::atomic::AtomicBool,
    drop_marker_before_conflict: std::sync::atomic::AtomicBool,
    update_legacy_after_target_write: std::sync::atomic::AtomicBool,
    delete_legacy_after_target_write: std::sync::atomic::AtomicBool,
    update_legacy_on_paged_query: std::sync::atomic::AtomicBool,
    delete_legacy_on_paged_query: std::sync::atomic::AtomicBool,
    update_target_on_paged_query: std::sync::atomic::AtomicBool,
    conflict_target_reconciliation: std::sync::atomic::AtomicBool,
}

#[tinystoragedrivers_api::async_trait]
impl tinystoragedrivers_api::DocumentStore for CommitThenConflictMarker {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    async fn ensure_collection(
        &self,
        spec: &tinystoragedrivers_types::CollectionSpec,
    ) -> tinystoragedrivers_types::Result<()> {
        self.inner.ensure_collection(spec).await
    }

    async fn get(
        &self,
        collection: &str,
        id: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::Versioned<Value>>> {
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: tinystoragedrivers_types::Precondition,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::Version> {
        let version = self.inner.put(collection, id, value, precondition).await?;
        if collection == PREFS_NAMESPACE
            && self
                .update_legacy_after_target_write
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .put(
                    LEGACY_PREFS_NAMESPACE,
                    id,
                    json!({"value": {"updated_during_migration": true}}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        if collection == PREFS_NAMESPACE
            && self
                .delete_legacy_after_target_write
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .delete(
                    LEGACY_PREFS_NAMESPACE,
                    id,
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        if collection == MIGRATIONS
            && id == PREFS_STEP
            && self
                .pending
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            if self
                .drop_marker_before_conflict
                .swap(false, std::sync::atomic::Ordering::SeqCst)
            {
                self.inner
                    .delete(collection, id, tinystoragedrivers_types::Precondition::None)
                    .await?;
            }
            return Err(tinystoragedrivers_types::StorageError::conflict(
                "marker committed by a competing opener",
            ));
        }
        Ok(version)
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        value: Value,
        precondition: tinystoragedrivers_types::Precondition,
        options: tinystoragedrivers_types::WriteOptions,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::Version> {
        if collection == PREFS_NAMESPACE
            && matches!(precondition, tinystoragedrivers_types::Precondition::Version(_))
            && self
                .conflict_target_reconciliation
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .put(
                    PREFS_NAMESPACE,
                    id,
                    json!({"concurrent_target_update": true}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        let version = self
            .inner
            .put_with(collection, id, value, precondition, options)
            .await?;
        if collection == PREFS_NAMESPACE
            && self
                .update_legacy_after_target_write
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .put(
                    LEGACY_PREFS_NAMESPACE,
                    id,
                    json!({"value": {"updated_during_migration": true}}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        if collection == PREFS_NAMESPACE
            && self
                .delete_legacy_after_target_write
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .delete(
                    LEGACY_PREFS_NAMESPACE,
                    id,
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        Ok(version)
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: tinystoragedrivers_types::Precondition,
    ) -> tinystoragedrivers_types::Result<bool> {
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &tinystoragedrivers_types::Query,
    ) -> tinystoragedrivers_types::Result<
        tinystoragedrivers_types::Page<tinystoragedrivers_types::Versioned<Value>>,
    > {
        let page = self.inner.query(collection, query).await?;
        if collection == LEGACY_PREFS_NAMESPACE
            && query.cursor.is_some()
            && self
                .update_legacy_on_paged_query
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .put(
                    LEGACY_PREFS_NAMESPACE,
                    "aaa",
                    json!({"value": {"updated_during_paging": true}}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        if collection == LEGACY_PREFS_NAMESPACE
            && query.cursor.is_some()
            && self
                .delete_legacy_on_paged_query
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .delete(
                    LEGACY_PREFS_NAMESPACE,
                    "aaa",
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        if collection == LEGACY_PREFS_NAMESPACE
            && query.cursor.is_some()
            && self
                .update_target_on_paged_query
                .swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.inner
                .put(
                    PREFS_NAMESPACE,
                    "aaa",
                    json!({"concurrent_target_update": true}),
                    tinystoragedrivers_types::Precondition::None,
                )
                .await?;
        }
        Ok(page)
    }

    async fn count(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.count(collection, filter).await
    }

    async fn delete_where(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &tinystoragedrivers_types::Filter,
        sort: &[tinystoragedrivers_types::Sort],
        patch: &Value,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::Versioned<Value>>> {
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn drop_collection(&self, collection: &str) -> tinystoragedrivers_types::Result<()> {
        self.inner.drop_collection(collection).await
    }
}

#[tinystoragedrivers_api::async_trait]
impl tinystoragedrivers_api::StreamStore for CommitThenConflictStreams {
    fn capabilities(&self) -> tinystoragedrivers_types::Capabilities {
        self.inner.capabilities()
    }

    async fn append(&self, stream: &str, value: Value) -> tinystoragedrivers_types::Result<u64> {
        self.inner.append(stream, value).await
    }

    async fn append_batch(
        &self,
        stream: &str,
        values: Vec<Value>,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.append_batch(stream, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected: u64,
        values: Vec<Value>,
    ) -> tinystoragedrivers_types::Result<u64> {
        if !self.raced.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.inner
                .append_batch_if_offset(stream, expected, values)
                .await?;
            if let Some(source) = &self.append_to_source {
                self.inner
                    .append(source, serde_json::json!({"event": "concurrent"}))
                    .await?;
            }
            return Err(tinystoragedrivers_types::StorageError::conflict(
                "competing opener committed the identical page",
            ));
        }
        self.inner
            .append_batch_if_offset(stream, expected, values)
            .await
    }

    async fn read_window(
        &self,
        stream: &str,
        from: u64,
        limit: usize,
    ) -> tinystoragedrivers_types::Result<Vec<tinystoragedrivers_types::StreamEntry>> {
        self.inner.read_window(stream, from, limit).await
    }

    async fn len(&self, stream: &str) -> tinystoragedrivers_types::Result<u64> {
        self.inner.len(stream).await
    }

    async fn truncate_before(
        &self,
        stream: &str,
        offset: u64,
    ) -> tinystoragedrivers_types::Result<u64> {
        self.inner.truncate_before(stream, offset).await
    }

    async fn delete_stream(&self, stream: &str) -> tinystoragedrivers_types::Result<bool> {
        self.inner.delete_stream(stream).await
    }

    async fn streams(&self, prefix: &str) -> tinystoragedrivers_types::Result<Vec<String>> {
        self.inner.streams(prefix).await
    }

    async fn seal(
        &self,
        stream: &str,
    ) -> tinystoragedrivers_types::Result<tinystoragedrivers_types::SealOutcome> {
        self.inner.seal(stream).await
    }

    async fn delete_sealed_stream(
        &self,
        stream: &str,
        sealed_ms: i64,
    ) -> tinystoragedrivers_types::Result<bool> {
        self.inner.delete_sealed_stream(stream, sealed_ms).await
    }

    async fn info(
        &self,
        stream: &str,
    ) -> tinystoragedrivers_types::Result<Option<tinystoragedrivers_types::StreamInfo>> {
        self.inner.info(stream).await
    }
}

fn storage() -> tinystoragedrivers_api::Storage {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    MemoryBackend::new()
        .for_scope(&Scope::new("connector-migration-tests").unwrap())
        .unwrap()
}

#[test]
fn legacy_composite_key_bytes_keep_the_pre_split_encoding() {
    assert_eq!(legacy_key(["a/b"]).unwrap().as_str(), "a%2Fb");
    assert_eq!(legacy_key(["%00"]).unwrap().as_str(), "%2500");
    assert_eq!(legacy_key(["\0"]).unwrap().as_str(), "%00");
    assert_eq!(legacy_key(["~reserved"]).unwrap().as_str(), "%7Ereserved");
    assert_eq!(legacy_key(["雪/猫"]).unwrap().as_str(), "雪%2F猫");
    assert_eq!(
        legacy_key(["owner", "part"]).unwrap().as_str(),
        "owner/part"
    );
}

#[test]
fn legacy_key_rejects_empty_input_and_hashes_values_over_the_storage_limit() {
    assert!(legacy_key(Vec::<String>::new()).is_err());

    let key = legacy_key(["x".repeat(600)]).unwrap();
    assert!(key.as_str().starts_with('~'));
    assert!(key.as_str().len() <= 512);
}

#[tokio::test]
async fn migration_refuses_an_invalid_owner_marker() {
    use tinystoragedrivers_types::Precondition;

    let storage = storage();
    storage
        .documents()
        .put(
            MIGRATIONS,
            PREFS_STEP,
            json!({"owner": "another-owner", "step": PREFS_STEP, "done": true}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let error = prepare(&storage)
        .await
        .expect_err("foreign marker is refused");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
}

#[tokio::test]
async fn completion_marker_conflict_accepts_the_valid_marker_written_by_a_racer() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-marker-race").unwrap())
        .unwrap();
    let documents = std::sync::Arc::new(CommitThenConflictMarker {
        inner: base.documents().clone(),
        pending: std::sync::atomic::AtomicBool::new(true),
        drop_marker_before_conflict: std::sync::atomic::AtomicBool::new(false),
        update_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        update_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        update_target_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        conflict_target_reconciliation: std::sync::atomic::AtomicBool::new(false),
    });
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    prepare(&storage).await.unwrap();
    assert!(
        done(storage.documents().as_ref(), PREFS_STEP)
            .await
            .unwrap()
    );
    assert!(
        done(storage.documents().as_ref(), STREAMS_STEP)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn completion_marker_conflict_without_a_committed_marker_is_returned() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-marker-uncommitted-race").unwrap())
        .unwrap();
    let documents = std::sync::Arc::new(CommitThenConflictMarker {
        inner: base.documents().clone(),
        pending: std::sync::atomic::AtomicBool::new(true),
        drop_marker_before_conflict: std::sync::atomic::AtomicBool::new(true),
        update_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        update_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        update_target_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        conflict_target_reconciliation: std::sync::atomic::AtomicBool::new(false),
    });
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("a conflict without a valid done marker is not accepted");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
}

#[tokio::test]
async fn preference_migration_follows_a_legacy_update_after_the_initial_copy() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-update-race").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"old": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let documents = std::sync::Arc::new(CommitThenConflictMarker {
        inner: base.documents().clone(),
        pending: std::sync::atomic::AtomicBool::new(false),
        drop_marker_before_conflict: std::sync::atomic::AtomicBool::new(false),
        update_legacy_after_target_write: std::sync::atomic::AtomicBool::new(true),
        delete_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        update_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
    });
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    prepare(&storage).await.unwrap();
    assert_eq!(
        base.documents()
            .get(PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"updated_during_migration": true})
    );
}

#[tokio::test]
async fn preference_migration_reconciles_a_row_changed_on_a_later_page() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-paging-race").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "aaa",
            json!({"value": {"initial": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    for index in 0..128 {
        base.documents()
            .put(
                LEGACY_PREFS_NAMESPACE,
                &format!("row-{index:03}"),
                json!({"value": {"index": index}}),
                Precondition::Absent,
            )
            .await
            .unwrap();
    }
    let documents = std::sync::Arc::new(CommitThenConflictMarker {
        inner: base.documents().clone(),
        pending: std::sync::atomic::AtomicBool::new(false),
        drop_marker_before_conflict: std::sync::atomic::AtomicBool::new(false),
        update_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        update_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(true),
        delete_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        update_target_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        conflict_target_reconciliation: std::sync::atomic::AtomicBool::new(false),
    });
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    prepare(&storage).await.unwrap();
    assert_eq!(
        base.documents()
            .get(PREFS_NAMESPACE, "aaa")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"updated_during_paging": true})
    );
}

#[tokio::test]
async fn preference_migration_drops_a_target_when_the_legacy_row_was_deleted() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-delete-race").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"read": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let documents = std::sync::Arc::new(CommitThenConflictMarker {
        inner: base.documents().clone(),
        pending: std::sync::atomic::AtomicBool::new(false),
        drop_marker_before_conflict: std::sync::atomic::AtomicBool::new(false),
        update_legacy_after_target_write: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_after_target_write: std::sync::atomic::AtomicBool::new(true),
        update_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        delete_legacy_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        update_target_on_paged_query: std::sync::atomic::AtomicBool::new(false),
        conflict_target_reconciliation: std::sync::atomic::AtomicBool::new(false),
    });
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    prepare(&storage).await.unwrap();
    assert!(
        base.documents()
            .get(PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn preference_collision_preserves_the_existing_target_and_completes() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("connector-preference-collision").unwrap())
        .unwrap();
    storage
        .documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"read": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    storage
        .documents()
        .put(
            PREFS_NAMESPACE,
            "gmail",
            json!({"read": false}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    prepare(&storage)
        .await
        .expect("an existing target remains authoritative");
    assert_eq!(
        storage
            .documents()
            .get(LEGACY_PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"value": {"read": true}})
    );
}

#[tokio::test]
async fn preference_migration_resumes_matching_targets_across_pages() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-pagination").unwrap())
        .unwrap();
    for index in 0..(PAGE_SIZE + 3) {
        let id = format!("preference-{index:03}");
        let value = json!({"enabled": index % 2 == 0, "index": index});
        storage
            .documents()
            .put(
                LEGACY_PREFS_NAMESPACE,
                &id,
                json!({"value": value}),
                Precondition::Absent,
            )
            .await
            .unwrap();
    }

    // A previous interrupted opener may have written this exact destination
    // before advancing the query cursor. It is safe to accept and continue.
    let first = encode_preference(&json!({"enabled": true, "index": 0}));
    storage
        .documents()
        .put(
            PREFS_NAMESPACE,
            "preference-000",
            first,
            Precondition::Absent,
        )
        .await
        .unwrap();

    prepare(&storage).await.unwrap();

    for index in 0..(PAGE_SIZE + 3) {
        let id = format!("preference-{index:03}");
        let expected = encode_preference(&json!({"enabled": index % 2 == 0, "index": index}));
        assert_eq!(
            storage
                .documents()
                .get(PREFS_NAMESPACE, &id)
                .await
                .unwrap()
                .unwrap()
                .doc,
            expected
        );
    }
    assert!(
        done(storage.documents().as_ref(), PREFS_STEP)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn preference_migration_preserves_expiration_and_existing_target_on_mismatch() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope, WriteOptions};

    let expires_ms = 4_000_000_000_000;
    let value = json!({"enabled": true});
    let body = encode_preference(&value);

    let collision = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-expiry-conflict").unwrap())
        .unwrap();
    collision
        .documents()
        .put_with(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": value}),
            Precondition::Absent,
            WriteOptions::new().expires_ms(expires_ms),
        )
        .await
        .unwrap();
    // The payload matches, but accepting this destination would silently
    // remove the source's absolute expiration.
    collision
        .documents()
        .put(PREFS_NAMESPACE, "gmail", body.clone(), Precondition::Absent)
        .await
        .unwrap();
    prepare(&collision)
        .await
        .expect("an existing target remains authoritative on expiry mismatch");
    assert_eq!(
        collision
            .documents()
            .get(LEGACY_PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .unwrap()
            .meta
            .expires_ms,
        Some(expires_ms)
    );

    let migrated = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-expiry-copy").unwrap())
        .unwrap();
    migrated
        .documents()
        .put_with(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": value}),
            Precondition::Absent,
            WriteOptions::new().expires_ms(expires_ms),
        )
        .await
        .unwrap();
    prepare(&migrated).await.unwrap();
    let target = migrated
        .documents()
        .get(PREFS_NAMESPACE, "gmail")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(target.doc, body);
    assert_eq!(target.meta.expires_ms, Some(expires_ms));
}

#[tokio::test]
async fn legacy_preferences_without_a_value_migrate_as_null() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-malformed").unwrap())
        .unwrap();
    storage
        .documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "missing-value",
            json!({"other": true}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    prepare(&storage).await.unwrap();

    assert_eq!(
        storage
            .documents()
            .get(PREFS_NAMESPACE, "missing-value")
            .await
            .unwrap()
            .unwrap()
            .doc,
        encode_preference(&Value::Null),
        "legacy entries without a `value` retain the old null fallback"
    );
}

#[tokio::test]
async fn preference_query_failure_is_returned_without_marking_the_step_done() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let backend = MemoryBackend::new();
    let base = backend
        .for_scope(&Scope::new("connector-preference-query-failure").unwrap())
        .unwrap();
    let faults = FaultPlan::new();
    faults.fail_operation_nth("documents.query", 1, ErrorKind::Backend);
    let documents: Arc<dyn DocumentStore> =
        Arc::new(Failing::with_plan(base.documents().clone(), faults));
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("preference query failures are surfaced to the module opener");
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        base.documents()
            .get(MIGRATIONS, PREFS_STEP)
            .await
            .unwrap()
            .is_none(),
        "failed reads must not persist a completed migration marker"
    );
}

#[tokio::test]
async fn preference_write_failure_keeps_the_source_and_step_retryable() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Precondition, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-write-failure").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"enabled": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let faults = FaultPlan::new();
    faults.fail_operation_nth("documents.put_with", 1, ErrorKind::Backend);
    let documents: Arc<dyn DocumentStore> =
        Arc::new(Failing::with_plan(base.documents().clone(), faults));
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("destination write errors stop migration before completion");
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        base.documents()
            .get(LEGACY_PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        base.documents()
            .get(MIGRATIONS, PREFS_STEP)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn a_preference_write_conflict_without_a_target_stops_migration() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Precondition, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-preference-disappearing-target").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"read": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();
    let faults = FaultPlan::new();
    faults.fail_operation_nth("documents.put_with", 1, ErrorKind::Conflict);
    let documents: Arc<dyn DocumentStore> =
        Arc::new(Failing::with_plan(base.documents().clone(), faults));
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("a racing target that vanished cannot be treated as migrated");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert!(
        base.documents()
            .get(PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn stream_discovery_failure_does_not_complete_stream_migration() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{Storage, StorageBackend, StreamStore};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-discovery-failure").unwrap())
        .unwrap();
    base.streams()
        .append("triggers/2026-10-20", json!({"event": 1}))
        .await
        .unwrap();
    let faults = FaultPlan::new();
    faults.fail_operation_nth("streams.streams", 1, ErrorKind::Backend);
    let streams: Arc<dyn StreamStore> =
        Arc::new(Failing::with_plan(base.streams().clone(), faults));
    let storage = Storage::new(
        base.documents().clone(),
        streams,
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("stream discovery errors are returned to the opener");
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(base.streams().len("triggers/2026-10-20").await.unwrap(), 1);
    assert!(
        base.documents()
            .get(MIGRATIONS, STREAMS_STEP)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn stream_copy_conflict_without_matching_raced_data_preserves_source() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{Storage, StorageBackend, StreamStore};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-race-conflict").unwrap())
        .unwrap();
    let source = "triggers/2026-10-21";
    let target = "connectors.triggers/2026-10-21";
    base.streams()
        .append(source, json!({"event": "source"}))
        .await
        .unwrap();

    // This testkit fault models an append rejected by the host after the
    // source snapshot but before any competing page became visible.
    let faults = FaultPlan::new();
    faults.fail_operation_nth("streams.append_batch_if_offset", 1, ErrorKind::Conflict);
    let streams: Arc<dyn StreamStore> =
        Arc::new(Failing::with_plan(base.streams().clone(), faults));
    let storage = Storage::new(
        base.documents().clone(),
        streams,
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("an append conflict without the expected raced page is not accepted");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(base.streams().len(source).await.unwrap(), 1);
    assert_eq!(base.streams().len(target).await.unwrap(), 0);
}

#[tokio::test]
async fn stream_copy_accepts_conflict_after_an_identical_page_was_committed() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-identical-race").unwrap())
        .unwrap();
    let source = "triggers/2026-10-22";
    let target = "connectors.triggers/2026-10-22";
    base.streams()
        .append(source, json!({"event": "same-page"}))
        .await
        .unwrap();

    let streams = std::sync::Arc::new(CommitThenConflictStreams {
        inner: base.streams().clone(),
        raced: std::sync::atomic::AtomicBool::new(false),
        append_to_source: None,
    });
    let storage = Storage::new(
        base.documents().clone(),
        streams,
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    prepare(&storage).await.unwrap();
    assert_eq!(base.streams().len(source).await.unwrap(), 1);
    assert_eq!(base.streams().len(target).await.unwrap(), 1);
    assert_eq!(
        base.streams().read_window(target, 0, 1).await.unwrap()[0].value,
        json!({"event": "same-page"})
    );
}

#[tokio::test]
async fn stream_copy_refuses_to_delete_a_source_that_grows_during_the_copy() {
    use tinystoragedrivers_api::{Storage, StorageBackend};
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-source-growth").unwrap())
        .unwrap();
    let source = "triggers/2026-10-23";
    let target = "connectors.triggers/2026-10-23";
    base.streams()
        .append(source, json!({"event": "original"}))
        .await
        .unwrap();
    let streams = std::sync::Arc::new(CommitThenConflictStreams {
        inner: base.streams().clone(),
        raced: std::sync::atomic::AtomicBool::new(false),
        append_to_source: Some(source.to_owned()),
    });
    let storage = Storage::new(
        base.documents().clone(),
        streams,
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("source growth during copy must abort deletion");
    assert_eq!(error.kind(), ErrorKind::Conflict);
    assert_eq!(base.streams().len(source).await.unwrap(), 2);
    assert_eq!(base.streams().len(target).await.unwrap(), 1);
}

#[tokio::test]
async fn failed_completion_marker_keeps_migration_retryable() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{DocumentStore, Storage, StorageBackend};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Precondition, Scope};

    let backend = MemoryBackend::new();
    let base = backend
        .for_scope(&Scope::new("connector-marker-retry").unwrap())
        .unwrap();
    base.documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"read": true}}),
            Precondition::Absent,
        )
        .await
        .unwrap();

    let faults = FaultPlan::new();
    faults.fail_operation_nth("documents.put", 1, ErrorKind::Backend);
    let documents: Arc<dyn DocumentStore> =
        Arc::new(Failing::with_plan(base.documents().clone(), faults.clone()));
    let storage = Storage::new(
        documents,
        base.streams().clone(),
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("an interrupted completion marker is reported to the opener");
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert!(
        base.documents()
            .get(PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .is_some(),
        "the transformed row is left available for an idempotent retry"
    );

    faults.clear();
    prepare(&storage).await.unwrap();
    prepare(&storage).await.unwrap();
    assert!(
        done(storage.documents().as_ref(), PREFS_STEP)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn stream_migration_refuses_a_target_longer_than_its_source() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("connector-stream-collision").unwrap())
        .unwrap();
    storage
        .streams()
        .append("triggers/2026-10-15", json!({"event": 1}))
        .await
        .unwrap();
    storage
        .streams()
        .append("connectors.triggers/2026-10-15", json!({"event": 1}))
        .await
        .unwrap();
    storage
        .streams()
        .append("connectors.triggers/2026-10-15", json!({"event": 2}))
        .await
        .unwrap();

    let error = prepare(&storage)
        .await
        .expect_err("a longer target cannot be reconciled safely");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert_eq!(
        storage.streams().len("triggers/2026-10-15").await.unwrap(),
        1,
        "source events survive a refused migration"
    );
}

#[tokio::test]
async fn stream_migration_refuses_same_length_target_with_different_events() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("connector-stream-content-collision").unwrap())
        .unwrap();
    storage
        .streams()
        .append("triggers/2026-10-16", json!({"event": "source"}))
        .await
        .unwrap();
    storage
        .streams()
        .append(
            "connectors.triggers/2026-10-16",
            json!({"event": "different"}),
        )
        .await
        .unwrap();

    let error = prepare(&storage)
        .await
        .expect_err("same-length but different target data must not be accepted");
    assert_eq!(error.kind(), tinystoragedrivers_types::ErrorKind::Conflict);
    assert_eq!(
        storage.streams().len("triggers/2026-10-16").await.unwrap(),
        1,
        "the source is preserved when target verification fails"
    );
}

#[tokio::test]
async fn stream_migration_resumes_a_matching_partial_target() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let storage = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-resume").unwrap())
        .unwrap();
    let source = "triggers/2026-10-17";
    let target = "connectors.triggers/2026-10-17";
    for index in 0..3 {
        storage
            .streams()
            .append(source, json!({"event": index}))
            .await
            .unwrap();
    }
    for index in 0..2 {
        storage
            .streams()
            .append(target, json!({"event": index}))
            .await
            .unwrap();
    }

    prepare(&storage).await.unwrap();

    assert_eq!(storage.streams().len(source).await.unwrap(), 3);
    assert_eq!(storage.streams().len(target).await.unwrap(), 3);
    assert_eq!(
        storage
            .streams()
            .read_window(target, 0, 3)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.value)
            .collect::<Vec<_>>(),
        vec![
            json!({"event": 0}),
            json!({"event": 1}),
            json!({"event": 2})
        ]
    );
}

#[tokio::test]
async fn stream_write_failure_preserves_source_for_retry() {
    use std::sync::Arc;
    use tinystoragedrivers_api::{Storage, StorageBackend, StreamStore};
    use tinystoragedrivers_testkit::{Failing, FaultPlan, MemoryBackend};
    use tinystoragedrivers_types::{ErrorKind, Scope};

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-retry").unwrap())
        .unwrap();
    let source = "triggers/2026-10-18";
    let target = "connectors.triggers/2026-10-18";
    base.streams()
        .append(source, json!({"event": "retained"}))
        .await
        .unwrap();

    let faults = FaultPlan::new();
    faults.fail_operation_nth("streams.append_batch_if_offset", 1, ErrorKind::Backend);
    let streams: Arc<dyn StreamStore> =
        Arc::new(Failing::with_plan(base.streams().clone(), faults.clone()));
    let storage = Storage::new(
        base.documents().clone(),
        streams,
        base.kv().clone(),
        base.blobs().clone(),
        base.queues().clone(),
        base.secrets().clone(),
    );

    let error = prepare(&storage)
        .await
        .expect_err("failed target append stops migration before source deletion");
    assert_eq!(error.kind(), ErrorKind::Backend);
    assert_eq!(base.streams().len(source).await.unwrap(), 1);
    assert_eq!(base.streams().len(target).await.unwrap(), 0);

    faults.clear();
    prepare(&storage).await.unwrap();
    assert_eq!(base.streams().len(source).await.unwrap(), 1);
    assert_eq!(base.streams().len(target).await.unwrap(), 1);
}

#[tokio::test]
async fn stream_migration_retains_the_legacy_source_for_late_appends() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::Scope;

    let base = MemoryBackend::new()
        .for_scope(&Scope::new("connector-stream-delete-failure").unwrap())
        .unwrap();
    let source = "triggers/2026-10-19";
    let target = "connectors.triggers/2026-10-19";
    base.streams()
        .append(source, json!({"event": "retained"}))
        .await
        .unwrap();

    prepare(&base).await.unwrap();
    assert_eq!(base.streams().len(source).await.unwrap(), 1);
    assert_eq!(base.streams().len(target).await.unwrap(), 1);

    base.streams()
        .append(source, json!({"event": "late"}))
        .await
        .unwrap();
    prepare(&base).await.unwrap();
    assert_eq!(base.streams().len(source).await.unwrap(), 2);
    assert_eq!(base.streams().len(target).await.unwrap(), 1);
}

#[tokio::test]
async fn concurrent_opens_resume_owner_migrations_without_duplicate_stream_entries() {
    use tinystoragedrivers_api::StorageBackend;
    use tinystoragedrivers_testkit::MemoryBackend;
    use tinystoragedrivers_types::{Precondition, Scope};

    let backend = MemoryBackend::new();
    let storage = backend
        .for_scope(&Scope::new("connectors").unwrap())
        .unwrap();
    storage
        .documents()
        .put(
            LEGACY_PREFS_NAMESPACE,
            "gmail",
            json!({"value": {"read": true}}),
            Precondition::None,
        )
        .await
        .unwrap();
    storage
        .streams()
        .append("triggers/2026-10-15", json!({"event": 1}))
        .await
        .unwrap();

    let (left, right) = tokio::join!(prepare(&storage), prepare(&storage));
    left.unwrap();
    right.unwrap();
    assert_eq!(
        storage
            .streams()
            .len("connectors.triggers/2026-10-15")
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        storage
            .documents()
            .get(PREFS_NAMESPACE, "gmail")
            .await
            .unwrap()
            .unwrap()
            .doc,
        json!({"read": true})
    );
}
