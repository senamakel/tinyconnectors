//! Tests for contract shaping, the caches and the probe scope gate.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use serde_json::json;
use tinyconnectors_bus::composio::{
    ComposioProbedOutputSample, ComposioToolContract, ComposioToolsResponse,
};
use tinyconnectors_bus::vocabulary::ToolScope;

use super::*;

fn listing(value: serde_json::Value) -> ComposioToolsResponse {
    serde_json::from_value(value).unwrap()
}

fn contract(slug: &str) -> ComposioToolContract {
    ComposioToolContract {
        slug: slug.into(),
        toolkit: "github".into(),
        description: None,
        required_args: vec![],
        input_schema: None,
        output_fields: vec!["schema_field".into()],
        output_schema: None,
        primary_array_path: Some("data.schema_list".into()),
        is_curated: false,
    }
}

fn sample(path: Option<&str>, fields: &[&str]) -> ComposioProbedOutputSample {
    ComposioProbedOutputSample {
        primary_array_path: path.map(str::to_string),
        output_fields: fields.iter().map(|f| (*f).to_string()).collect(),
        sample: json!({"data": {"private": "value"}}),
    }
}

#[test]
fn a_listing_becomes_contracts_with_required_args_and_output_shape() {
    let contracts = contracts_from_listing(
        "gmail",
        &listing(json!({"tools": [{
            "type": "function",
            "function": {
                "name": "GMAIL_FETCH_EMAILS",
                "description": "Fetch emails",
                "parameters": {"type": "object", "required": ["query", 7], "properties": {}},
                "output_parameters": {"properties": {"messages": {"type": "array"}, "next": {}}}
            }
        }]})),
    );
    assert_eq!(contracts.len(), 1);
    let c = &contracts[0];
    assert_eq!(c.slug, "GMAIL_FETCH_EMAILS");
    assert_eq!(c.toolkit, "gmail");
    assert_eq!(c.required_args, ["query"], "non-string entries are ignored");
    assert_eq!(c.output_fields, ["messages", "next"]);
    assert_eq!(c.primary_array_path.as_deref(), Some("data.messages"));
    assert_eq!(c.description.as_deref(), Some("Fetch emails"));
    assert!(c.input_schema.is_some() && c.output_schema.is_some());
}

#[test]
fn a_missing_output_schema_stays_unknown_rather_than_empty() {
    let contracts = contracts_from_listing(
        "github",
        &listing(json!({"tools": [{"function": {"name": "GITHUB_LIST_REPOSITORY_ISSUES"}}]})),
    );
    let c = &contracts[0];
    assert!(c.output_schema.is_none());
    assert_eq!(c.output_fields, Vec::<String>::new());
    assert!(c.primary_array_path.is_none());
    assert_eq!(c.required_args, Vec::<String>::new());
}

#[test]
fn curation_is_a_ranking_signal_and_never_hides_an_action() {
    let contracts = contracts_from_listing(
        "gmail",
        &listing(json!({"tools": [
            {"function": {"name": "GMAIL_SEND_EMAIL"}},
            {"function": {"name": "GMAIL_SOME_UNCURATED_THING"}}
        ]})),
    );
    assert!(contracts[0].is_curated);
    assert!(!contracts[1].is_curated);
    assert_eq!(contracts.len(), 2);
}

#[test]
fn the_array_path_always_gains_the_data_wrapper() {
    // A payload that itself has a `data` property really lives at data.data.
    let schema = json!({"properties": {"data": {"properties": {"rows": {"type": "array"}}}}});
    assert_eq!(
        compute_composio_array_path(Some(&schema)).as_deref(),
        Some("data.data.rows")
    );
    assert_eq!(compute_composio_array_path(None), None);
}

#[test]
fn a_probe_wins_over_the_schema_even_when_it_found_no_array() {
    let caches = ContractCaches::default();
    caches.remember_probe("github_list_x", &sample(None, &[]));
    let merged = caches.apply_probe(contract("GITHUB_LIST_X"));
    assert_eq!(
        merged.primary_array_path, None,
        "a disproved path is dropped"
    );
    assert_eq!(
        merged.output_fields,
        ["schema_field"],
        "an empty probe field list means unknown, not confirmed empty"
    );
}

#[test]
fn a_probe_with_fields_replaces_the_schema_fields() {
    let caches = ContractCaches::default();
    caches.remember_probe("GITHUB_LIST_X", &sample(Some("data.issues"), &["issues"]));
    let merged = caches.apply_probe(contract("github_list_x"));
    assert_eq!(merged.primary_array_path.as_deref(), Some("data.issues"));
    assert_eq!(merged.output_fields, ["issues"]);
}

#[test]
fn a_contract_with_no_probe_passes_through_unchanged() {
    let caches = ContractCaches::default();
    assert_eq!(caches.apply_probe(contract("A_B")), contract("A_B"));
}

#[test]
fn the_probe_cache_never_keeps_the_raw_sample() {
    let store = ContractCaches::default();
    store.remember_probe("GITHUB_LIST_X", &sample(Some("data.issues"), &["issues"]));
    let cached = store.probe("GITHUB_LIST_X").unwrap();
    assert_eq!(cached.sample, json!(null));
    assert_eq!(cached.primary_array_path.as_deref(), Some("data.issues"));
}

#[test]
fn expired_entries_are_misses() {
    let caches = ContractCaches::with_ttl(Duration::ZERO);
    caches.remember_catalog("gmail", vec![contract("GMAIL_X")]);
    caches.remember_probe("GMAIL_X", &sample(None, &[]));
    assert!(caches.catalog("gmail").is_none());
    assert!(caches.probe("GMAIL_X").is_none());
}

#[test]
fn fresh_entries_are_hits_until_cleared() {
    let caches = ContractCaches::default();
    caches.remember_catalog("gmail", vec![contract("GMAIL_X")]);
    caches.remember_probe("GMAIL_X", &sample(None, &[]));
    assert_eq!(caches.catalog("gmail").unwrap().len(), 1);
    caches.clear();
    assert!(caches.catalog("gmail").is_none());
    assert!(caches.probe("GMAIL_X").is_none());
}

#[test]
fn the_fetch_lock_is_shared_per_toolkit() {
    let caches = ContractCaches::default();
    let first = caches.fetch_lock("gmail");
    assert!(std::sync::Arc::ptr_eq(&first, &caches.fetch_lock("gmail")));
    assert!(!std::sync::Arc::ptr_eq(
        &first,
        &caches.fetch_lock("github")
    ));
}

#[test]
fn a_curated_read_is_confirmed_and_a_curated_write_is_not_a_read() {
    assert_eq!(
        resolve_action_scope("GMAIL_FETCH_EMAILS"),
        Some(ToolScope::Read)
    );
    assert_eq!(
        resolve_action_scope("GMAIL_SEND_EMAIL"),
        Some(ToolScope::Write)
    );
}

#[test]
fn an_uncurated_slug_on_a_cataloged_toolkit_fails_closed() {
    // The verb heuristic would call this a read; the catalog says it is unknown.
    assert_eq!(resolve_action_scope("GMAIL_SOMETHING_UNLISTED"), None);
}

#[test]
fn a_toolkit_without_a_catalog_falls_back_to_the_verb_heuristic() {
    assert_eq!(
        resolve_action_scope("ZZZUNCATALOGED_LIST_THINGS"),
        Some(ToolScope::Read)
    );
    assert_eq!(
        resolve_action_scope("ZZZUNCATALOGED_DELETE_THING"),
        Some(ToolScope::Admin)
    );
}

#[test]
fn a_slug_with_no_toolkit_has_no_scope() {
    assert_eq!(resolve_action_scope("  "), None);
}
