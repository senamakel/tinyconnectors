//! What the module needs the host to grant on its storage bus.
//!
//! The module reaches storage only through the host's storage bus, and the host
//! decides per module which collections and stream names it may touch. A grant
//! that is narrower than this list makes the module's calls fail closed, so the
//! list is the contract a host writes its grant from rather than something to
//! rediscover by reading the code. Only the host runs `ensure_collection`;
//! the module never creates a collection.

use std::sync::LazyLock;

use tinyconnectors_sync::PREFS_NAMESPACE;
use tinyconnectors_bus::CollectionSpec;

pub use tinyconnectors_bus::StorageNeeds;

static PREFERENCE_COLLECTIONS: LazyLock<[CollectionSpec; 1]> = LazyLock::new(|| {
    [CollectionSpec {
        name: PREFS_NAMESPACE.to_owned(),
        indexes: Vec::new(),
        ttl_field: None,
        search: None,
    }]
});

static BLOB_PREFIXES: [&str; 0] = [];
static STREAM_PREFIXES: [&str; 1] = [crate::triggers::STREAM_PREFIX];
static SECRET_PREFIXES: [&str; 0] = [];

/// The resources this module requests; host policy decides its storage grant.
pub static STORAGE_NEEDS: LazyLock<StorageNeeds<'static>> = LazyLock::new(|| StorageNeeds {
    collections: &PREFERENCE_COLLECTIONS[..],
    blob_prefixes: &BLOB_PREFIXES,
    stream_prefixes: &STREAM_PREFIXES,
    secret_prefixes: &SECRET_PREFIXES,
});
