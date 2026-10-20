//! Durable storage names only.
//!
//! Schema execution, migrations, and preference encoding belong to the
//! `tinyconnectors` implementation crate. The bus root exposes the canonical
//! [`crate::StorageNeeds`] value and static [`crate::STORAGE_NEEDS`] request.

mod names;

pub use names::{
    LEGACY_PREFS_NAMESPACE, LEGACY_STREAM_PREFIX, NON_OBJECT_FIELD, PREFS_NAMESPACE,
    PREFS_SCHEMA_VERSION, STREAM_NAME, STREAM_PREFIX,
};
