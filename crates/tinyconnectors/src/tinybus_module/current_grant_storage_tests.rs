use super::*;

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;
use serde_json::Value;
use tinyconnectors_bus::storage::{
    LEGACY_PREFS_NAMESPACE, LEGACY_STREAM_PREFIX, PREFS_NAMESPACE, STREAM_PREFIX,
};
use tinystoragedrivers_core::{
    Capabilities, CollectionSpec, DocumentStore, DocumentWatch, Filter, MemoryStorage, Page,
    Precondition, Query, ReadCheck, Result as StorageResult, Scope, ScopedStorage, SealOutcome,
    SearchHit, Sort, StorageBackend, StorageError, StreamEntry, StreamInfo, StreamStore, Version,
    Versioned, WatchOptions, WriteOp, WriteOptions, WriteResult,
};

#[derive(Default)]
pub(super) struct GrantAttempts {
    pub(super) current_documents: AtomicUsize,
    pub(super) legacy_documents: AtomicUsize,
    pub(super) unknown_documents: AtomicUsize,
    pub(super) current_streams: AtomicUsize,
    pub(super) legacy_streams: AtomicUsize,
    pub(super) unknown_streams: AtomicUsize,
}

struct CurrentGrantDocuments {
    inner: Arc<dyn DocumentStore>,
    attempts: Arc<GrantAttempts>,
}

impl std::fmt::Debug for CurrentGrantDocuments {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CurrentGrantDocuments")
            .finish_non_exhaustive()
    }
}

impl CurrentGrantDocuments {
    fn check(&self, collection: &str) -> StorageResult<()> {
        if collection == LEGACY_PREFS_NAMESPACE {
            self.attempts
                .legacy_documents
                .fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::invalid_input(
                "legacy user-scope collection is outside the module grant",
            ));
        }
        if collection == PREFS_NAMESPACE {
            self.attempts
                .current_documents
                .fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        self.attempts
            .unknown_documents
            .fetch_add(1, Ordering::SeqCst);
        Err(StorageError::invalid_input(
            "collection is outside the current module grant",
        ))
    }

    fn check_prefix(&self, prefix: &str) -> StorageResult<()> {
        if LEGACY_PREFS_NAMESPACE.starts_with(prefix) || prefix.starts_with(LEGACY_PREFS_NAMESPACE)
        {
            self.attempts
                .legacy_documents
                .fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::invalid_input(
                "legacy user-scope collection is outside the module grant",
            ));
        }
        if prefix == PREFS_NAMESPACE {
            self.attempts
                .current_documents
                .fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        self.attempts
            .unknown_documents
            .fetch_add(1, Ordering::SeqCst);
        Err(StorageError::invalid_input(
            "collection prefix is outside the current module grant",
        ))
    }

    fn check_ops(&self, ops: &[WriteOp]) -> StorageResult<()> {
        for op in ops {
            let collection = match op {
                WriteOp::Put { collection, .. } | WriteOp::Delete { collection, .. } => collection,
            };
            self.check(collection)?;
        }
        Ok(())
    }
}

#[async_trait]
impl DocumentStore for CurrentGrantDocuments {
    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }

    async fn watch(
        &self,
        collection: &str,
        filter: &Filter,
        options: &WatchOptions,
    ) -> StorageResult<Box<dyn DocumentWatch>> {
        self.check(collection)?;
        self.inner.watch(collection, filter, options).await
    }

    async fn ensure_collection(&self, spec: &CollectionSpec) -> StorageResult<()> {
        self.check(&spec.name)?;
        self.inner.ensure_collection(spec).await
    }

    async fn validate_collection_spec(&self, required: &CollectionSpec) -> StorageResult<()> {
        self.check(&required.name)?;
        self.inner.validate_collection_spec(required).await
    }

    async fn get(&self, collection: &str, id: &str) -> StorageResult<Option<Versioned<Value>>> {
        self.check(collection)?;
        self.inner.get(collection, id).await
    }

    async fn put(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
    ) -> StorageResult<Version> {
        self.check(collection)?;
        self.inner.put(collection, id, doc, precondition).await
    }

    async fn put_with(
        &self,
        collection: &str,
        id: &str,
        doc: Value,
        precondition: Precondition,
        options: WriteOptions,
    ) -> StorageResult<Version> {
        self.check(collection)?;
        self.inner
            .put_with(collection, id, doc, precondition, options)
            .await
    }

    async fn delete(
        &self,
        collection: &str,
        id: &str,
        precondition: Precondition,
    ) -> StorageResult<bool> {
        self.check(collection)?;
        self.inner.delete(collection, id, precondition).await
    }

    async fn query(
        &self,
        collection: &str,
        query: &Query,
    ) -> StorageResult<Page<Versioned<Value>>> {
        self.check(collection)?;
        self.inner.query(collection, query).await
    }

    async fn count(&self, collection: &str, filter: &Filter) -> StorageResult<u64> {
        self.check(collection)?;
        self.inner.count(collection, filter).await
    }

    async fn delete_where(&self, collection: &str, filter: &Filter) -> StorageResult<u64> {
        self.check(collection)?;
        self.inner.delete_where(collection, filter).await
    }

    async fn claim(
        &self,
        collection: &str,
        filter: &Filter,
        sort: &[Sort],
        patch: &Value,
    ) -> StorageResult<Option<Versioned<Value>>> {
        self.check(collection)?;
        self.inner.claim(collection, filter, sort, patch).await
    }

    async fn atomic_batch(&self, ops: Vec<WriteOp>) -> StorageResult<Vec<WriteResult>> {
        self.check_ops(&ops)?;
        self.inner.atomic_batch(ops).await
    }

    async fn atomic_batch_checked(
        &self,
        checks: Vec<ReadCheck>,
        ops: Vec<WriteOp>,
    ) -> StorageResult<Vec<WriteResult>> {
        for check in &checks {
            self.check(&check.collection)?;
        }
        self.check_ops(&ops)?;
        self.inner.atomic_batch_checked(checks, ops).await
    }

    async fn search(
        &self,
        collection: &str,
        text: &str,
        limit: usize,
    ) -> StorageResult<Vec<SearchHit>> {
        self.check(collection)?;
        self.inner.search(collection, text, limit).await
    }

    async fn drop_collection(&self, collection: &str) -> StorageResult<()> {
        self.check(collection)?;
        self.inner.drop_collection(collection).await
    }

    async fn collections(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.check_prefix(prefix)?;
        Ok(self
            .inner
            .collections(prefix)
            .await?
            .into_iter()
            .filter(|collection| collection == PREFS_NAMESPACE)
            .collect())
    }
}

struct CurrentGrantStreams {
    inner: Arc<dyn StreamStore>,
    attempts: Arc<GrantAttempts>,
}

impl std::fmt::Debug for CurrentGrantStreams {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("CurrentGrantStreams")
            .finish_non_exhaustive()
    }
}

impl CurrentGrantStreams {
    fn check(&self, stream: &str) -> StorageResult<()> {
        if stream.starts_with(LEGACY_STREAM_PREFIX) {
            self.attempts.legacy_streams.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::invalid_input(
                "legacy trigger stream is outside the module grant",
            ));
        }
        if stream.starts_with(STREAM_PREFIX) {
            self.attempts.current_streams.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        self.attempts.unknown_streams.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::invalid_input(
            "stream is outside the current module grant",
        ))
    }

    fn check_prefix(&self, prefix: &str) -> StorageResult<()> {
        if LEGACY_STREAM_PREFIX.starts_with(prefix) || prefix.starts_with(LEGACY_STREAM_PREFIX) {
            self.attempts.legacy_streams.fetch_add(1, Ordering::SeqCst);
            return Err(StorageError::invalid_input(
                "legacy trigger stream is outside the module grant",
            ));
        }
        if prefix.starts_with(STREAM_PREFIX) {
            return self.check(prefix);
        }
        self.attempts.unknown_streams.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::invalid_input(
            "stream prefix is outside the current module grant",
        ))
    }
}

#[async_trait]
impl StreamStore for CurrentGrantStreams {
    async fn append(&self, stream: &str, value: Value) -> StorageResult<u64> {
        self.check(stream)?;
        self.inner.append(stream, value).await
    }

    async fn append_batch(&self, stream: &str, values: Vec<Value>) -> StorageResult<u64> {
        self.check(stream)?;
        self.inner.append_batch(stream, values).await
    }

    async fn append_batch_if_offset(
        &self,
        stream: &str,
        expected_next_offset: u64,
        values: Vec<Value>,
    ) -> StorageResult<u64> {
        self.check(stream)?;
        self.inner
            .append_batch_if_offset(stream, expected_next_offset, values)
            .await
    }

    async fn read_window(
        &self,
        stream: &str,
        from: u64,
        limit: usize,
    ) -> StorageResult<Vec<StreamEntry>> {
        self.check(stream)?;
        self.inner.read_window(stream, from, limit).await
    }

    async fn len(&self, stream: &str) -> StorageResult<u64> {
        self.check(stream)?;
        self.inner.len(stream).await
    }

    async fn truncate_before(&self, stream: &str, offset: u64) -> StorageResult<u64> {
        self.check(stream)?;
        self.inner.truncate_before(stream, offset).await
    }

    async fn delete_stream(&self, stream: &str) -> StorageResult<bool> {
        self.check(stream)?;
        self.inner.delete_stream(stream).await
    }

    async fn seal(&self, stream: &str) -> StorageResult<SealOutcome> {
        self.check(stream)?;
        self.inner.seal(stream).await
    }

    async fn delete_sealed_stream(&self, stream: &str, sealed_ms: i64) -> StorageResult<bool> {
        self.check(stream)?;
        self.inner.delete_sealed_stream(stream, sealed_ms).await
    }

    async fn info(&self, stream: &str) -> StorageResult<Option<StreamInfo>> {
        self.check(stream)?;
        self.inner.info(stream).await
    }

    async fn streams(&self, prefix: &str) -> StorageResult<Vec<String>> {
        self.check_prefix(prefix)?;
        self.inner.streams(prefix).await
    }
}

pub(super) struct CurrentGrantHost {
    pub(super) scoped: ScopedStorage,
    pub(super) attempts: Arc<GrantAttempts>,
    _host: Connection,
    _served: tinystoragedrivers_busproxy::ServedStorage,
}

pub(super) async fn serve_current_grants(bus: &MemoryBus) -> tinybus::Result<CurrentGrantHost> {
    let base = MemoryStorage::new()
        .for_scope(&Scope::new("user").expect("scope"))
        .expect("scoped storage");
    let attempts = Arc::new(GrantAttempts::default());
    let documents = Arc::new(CurrentGrantDocuments {
        inner: Arc::clone(base.documents()),
        attempts: Arc::clone(&attempts),
    });
    let streams = Arc::new(CurrentGrantStreams {
        inner: Arc::clone(base.streams()),
        attempts: Arc::clone(&attempts),
    });
    let scoped = ScopedStorage::new(
        base.scope().clone(),
        base.driver(),
        documents,
        streams,
        Arc::clone(base.blobs()),
    );
    let bound = scoped.clone();
    let endpoint = tinystoragedrivers_busproxy::PerCallerEndpoint::new(move |_sender| {
        Some(tinystoragedrivers_client::StorageServer::new(bound.clone()))
    });
    let host = Connection::connect(bus.connect().await?).await?;
    let served = tinystoragedrivers_busproxy::serve(&host, Arc::new(endpoint)).await?;
    Ok(CurrentGrantHost {
        scoped,
        attempts,
        _host: host,
        _served: served,
    })
}

pub(super) async fn stop_setup(connection: &Connection) -> tinybus::Result<()> {
    connection.release_name(names::INTERFACE).await?;
    let object_path = names::OBJECT_PATH.try_into()?;
    assert!(connection.unserve(&object_path).await);
    connection.close().await?;
    Ok(())
}
