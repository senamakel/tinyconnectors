//! The implementation's projection from its schema to requested storage names.
//!
//! The bus contract carries the plain [`StorageNeeds`] type and the module's
//! static current-name request. This owner-side projection is useful when
//! validating that the executable schema and current request remain aligned.

use tinystoragedrivers_core::schema::{Item, Schema, ScopeClass};

use super::storage_schema::storage_schema;

/// Runtime view of the module's requested collection and stream names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StorageNeeds {
    pub(crate) collections: Vec<String>,
    pub(crate) stream_prefixes: Vec<String>,
}

/// The grant the host gives this module, generated from [`storage_schema`](super::storage_schema).
#[must_use]
pub(crate) fn storage_needs() -> StorageNeeds {
    needs_of(storage_schema())
}

/// The module-scoped collections and stream prefixes `schema` declares.
fn needs_of(schema: Schema) -> StorageNeeds {
    let mut needs = StorageNeeds {
        collections: Vec::new(),
        stream_prefixes: Vec::new(),
    };
    for item in schema.items {
        if item.scope() != ScopeClass::Module {
            continue;
        }
        match item {
            Item::Collection { name, .. } => needs.collections.push(name),
            Item::Stream { name, .. } => needs.stream_prefixes.push(format!("{name}/")),
            Item::Blobs { .. } | Item::Secrets { .. } => {}
        }
    }
    needs
}

#[cfg(test)]
#[path = "storage_needs_tests.rs"]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests;
