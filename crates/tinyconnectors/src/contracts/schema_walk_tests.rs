//! Tests for the schema walks.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use super::*;
use serde_json::json;

#[test]
fn schema_array_path_prefers_the_shallowest_array() {
    let schema = json!({"properties": {
        "nested": {"properties": {"items": {"type": "array"}}},
        "top": {"type": "array"}
    }});
    assert_eq!(
        compute_primary_array_path(Some(&schema)).as_deref(),
        Some("top")
    );
}

#[test]
fn schema_array_path_recognizes_nullable_array_properties() {
    let schema = json!({"properties": {"items": {"type": ["array", "null"]}}});
    assert_eq!(
        compute_primary_array_path(Some(&schema)).as_deref(),
        Some("items")
    );
}

#[test]
fn value_array_path_skips_only_named_root_keys() {
    let value = json!({"metadata": [], "data": {"metadata": []}});
    assert_eq!(
        compute_primary_array_path_from_value(&value, &["metadata"]).as_deref(),
        Some("data.metadata")
    );
}

#[test]
fn response_fields_are_sorted_and_ignore_schema_keywords() {
    let schema = json!({"type": "object", "z": {}, "a": {}});
    assert_eq!(response_fields_from_schema(Some(&schema)), ["a", "z"]);
}

#[test]
fn response_fields_do_not_expose_json_schema_metadata() {
    let schema = json!({"type": "object", "$id": "urn:example", "$defs": {"item": {}}});
    assert_eq!(response_fields_from_schema(Some(&schema)), Vec::new());
}
