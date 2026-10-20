//! Where the module keeps the user's scope preferences.
//!
//! [`tinyconnectors_sync`] defines the preference store as a two-method
//! key-value seam ([`tinyconnectors_sync::PrefsStore`]) and deliberately does
//! not implement one. This module supplies the module's own.
//!
//! # Why a document per key
//!
//! A preference is read on every `Execute` and `ListTools` that applies user
//! scopes and written when the user changes it. A document per key means a
//! corrupt write damages one toolkit's preference rather than all of them. The
//! documents live in the [`tinyconnectors_sync::PREFS_NAMESPACE`] collection of
//! a `tinystoragedrivers` document port the host provides; this crate depends on
//! the ports only and never chooses a backend.

mod document_store;
mod preference;
#[cfg(test)]
mod storage_needs;
mod storage_schema;

pub use document_store::DocumentPrefsStore;
pub use tinyconnectors_bus::{STORAGE_NEEDS, StorageNeeds};
// Schema execution and schema-derived grant calculation belong to this crate;
// the bus carries only the names and the plain grant type.
pub(crate) use preference::{decode_preference, encode_preference};
#[cfg(test)]
pub(crate) use storage_needs::storage_needs;
pub use storage_schema::storage_schema;
pub(crate) use storage_schema::{
    rename_trigger_stream, unwrap_user_scopes, TRIGGERS_OPERATION,
    TRIGGERS_OPERATION_VERSION, USER_SCOPES_OPERATION, USER_SCOPES_OPERATION_VERSION,
};

#[cfg(test)]
#[path = "mod_tests.rs"]
mod test;

#[cfg(test)]
#[path = "migrated_reads_tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod migrated_reads;
