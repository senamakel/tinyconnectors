//! Tests for the host-visible storage declaration.

use super::*;

#[test]
fn declares_the_exact_preference_and_trigger_resources() {
    let needs = &*STORAGE_NEEDS;
    assert_eq!(needs.collections.len(), 1);
    let collection = &needs.collections[0];
    assert_eq!(collection.name, "composio-user-scopes");
    assert_eq!(collection.indexes, Vec::new());
    assert!(collection.ttl_field.is_none());
    assert!(collection.search.is_none());
    assert_eq!(needs.blob_prefixes, &[] as &[&str]);
    assert_eq!(needs.stream_prefixes, &["triggers/"]);
    assert_eq!(needs.secret_prefixes, &[] as &[&str]);
}
