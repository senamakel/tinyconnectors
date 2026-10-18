//! Tests for the overview assembly rules.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use serde_json::json;
use tinyconnectors_bus::composio::{
    ComposioConnection, ComposioToolSchema, ComposioToolkitCatalogEntry,
};
use tinyconnectors_bus::vocabulary::{ToolScope, UserScopePref};

use super::*;

fn connection(id: &str, toolkit: &str, status: &str, created: Option<&str>) -> ComposioConnection {
    serde_json::from_value(json!({
        "id": id, "toolkit": toolkit, "status": status, "createdAt": created
    }))
    .unwrap()
}

fn tool(name: &str) -> ComposioToolSchema {
    serde_json::from_value(
        json!({"function": {"name": name, "description": "d", "parameters": {"type": "object"}}}),
    )
    .unwrap()
}

fn entry(
    slug: &str,
    enabled: Option<bool>,
    description: Option<&str>,
) -> ComposioToolkitCatalogEntry {
    ComposioToolkitCatalogEntry {
        slug: slug.into(),
        name: String::new(),
        logo: None,
        description: description.map(str::to_string),
        categories: vec![],
        enabled,
    }
}

fn run(
    allowlist: &[&str],
    connections: &[ComposioConnection],
    tools: &[ComposioToolSchema],
    preferences: &HashMap<String, UserScopePref>,
) -> Vec<tinyconnectors_bus::composio::ComposioConnectedIntegration> {
    assemble(&Listings {
        allowlist: allowlist.iter().map(|s| (*s).to_string()).collect(),
        connections,
        tools,
        descriptions: &HashMap::new(),
        preferences,
    })
}

#[test]
fn the_catalog_defines_the_connectable_set_and_drops_disabled_entries() {
    let catalog = [
        entry(" Gmail ", Some(true), None),
        entry("slack", Some(false), None),
        entry("notion", None, None),
    ];
    assert_eq!(
        connectable_toolkit_slugs(&["ignored".into()], &catalog),
        ["gmail"]
    );
}

#[test]
fn without_a_catalog_the_flat_toolkit_array_is_used() {
    assert_eq!(
        connectable_toolkit_slugs(&[" Gmail".into(), String::new(), "Notion".into()], &[]),
        ["gmail", "notion"]
    );
}

#[test]
fn catalog_descriptions_skip_blank_entries() {
    let map = catalog_descriptions(&[
        entry("Gmail", Some(true), Some("  Mail  ")),
        entry("slack", Some(true), Some("   ")),
        entry("notion", Some(true), None),
    ]);
    assert_eq!(map.get("gmail").map(String::as_str), Some("Mail"));
    assert_eq!(map.len(), 1);
}

#[test]
fn a_backend_description_beats_the_compiled_one() {
    let mut descriptions = HashMap::new();
    descriptions.insert("gmail".to_string(), "From the backend".to_string());
    let out = assemble(&Listings {
        allowlist: vec!["gmail".into(), "notion".into()],
        connections: &[],
        tools: &[],
        descriptions: &descriptions,
        preferences: &HashMap::new(),
    });
    assert_eq!(out[0].description, "From the backend");
    assert_eq!(
        out[1].description,
        tinyconnectors_sync::toolkits::catalog::toolkit_description("notion")
    );
}

#[test]
fn the_overview_is_sorted_and_deduplicated() {
    let out = run(&["slack", "gmail", "slack"], &[], &[], &HashMap::new());
    let slugs: Vec<_> = out.iter().map(|i| i.toolkit.as_str()).collect();
    assert_eq!(slugs, ["gmail", "slack"]);
}

#[test]
fn only_connected_toolkits_carry_tools() {
    let out = run(
        &["gmail", "github"],
        &[connection("c1", "gmail", "ACTIVE", None)],
        &[tool("GMAIL_FETCH_EMAILS"), tool("GITHUB_LIST_ISSUES")],
        &HashMap::new(),
    );
    assert!(out[0].toolkit == "github" && out[0].tools.is_empty() && !out[0].connected);
    let gmail = &out[1];
    assert!(gmail.connected);
    assert_eq!(gmail.tools.len(), 1);
    assert_eq!(gmail.tools[0].name, "GMAIL_FETCH_EMAILS");
    assert!(gmail.tools[0].parameters.is_some());
}

#[test]
fn a_prefix_never_claims_a_longer_toolkits_actions() {
    let out = run(
        &["git"],
        &[connection("c1", "git", "ACTIVE", None)],
        &[tool("GITHUB_LIST_ISSUES")],
        &HashMap::new(),
    );
    assert_eq!(out[0].tools, Vec::new());
}

#[test]
fn a_hidden_curated_action_is_gated_with_its_scope_and_an_uncurated_one_is_dropped() {
    // Default preference: admin off. GMAIL_DELETE_MESSAGE is curated admin.
    let out = run(
        &["gmail"],
        &[connection("c1", "gmail", "ACTIVE", None)],
        &[
            tool("GMAIL_DELETE_MESSAGE"),
            tool("GMAIL_NOT_IN_THE_CATALOG"),
        ],
        &HashMap::new(),
    );
    assert_eq!(out[0].tools, Vec::new());
    assert_eq!(out[0].gated_tools.len(), 1);
    assert_eq!(out[0].gated_tools[0].name, "GMAIL_DELETE_MESSAGE");
    assert_eq!(out[0].gated_tools[0].required_scope, ToolScope::Admin);
}

#[test]
fn the_stored_preference_moves_an_action_from_gated_to_callable() {
    let mut preferences = HashMap::new();
    preferences.insert(
        "gmail".to_string(),
        UserScopePref {
            read: true,
            write: true,
            admin: true,
        },
    );
    let out = run(
        &["gmail"],
        &[connection("c1", "gmail", "ACTIVE", None)],
        &[tool("GMAIL_DELETE_MESSAGE")],
        &preferences,
    );
    assert_eq!(out[0].tools.len(), 1);
    assert_eq!(out[0].gated_tools, Vec::new());
}

#[test]
fn connections_are_oldest_first_and_the_oldest_is_the_default() {
    let mut newer = connection("new", "gmail", "ACTIVE", Some("2026-02-01"));
    newer.account_email = Some("  ".into());
    newer.workspace = Some("team".into());
    let mut older = connection("old", "gmail", "ACTIVE", Some("2026-01-01"));
    older.account_email = Some("me@example.com".into());
    let out = run(&["gmail"], &[newer, older], &[], &HashMap::new());
    let ids: Vec<_> = out[0]
        .connections
        .iter()
        .map(|c| c.connection_id.as_str())
        .collect();
    assert_eq!(ids, ["old", "new"]);
    assert!(out[0].connections[0].is_default && !out[0].connections[1].is_default);
    assert_eq!(
        out[0].connections[0].label.as_deref(),
        Some("me@example.com")
    );
    assert_eq!(
        out[0].connections[1].label.as_deref(),
        Some("team"),
        "a blank email is skipped"
    );
}

#[test]
fn the_most_actionable_inactive_status_is_reported() {
    let out = run(
        &["gmail"],
        &[
            connection("a", "gmail", "INITIATED", None),
            connection("b", "gmail", "EXPIRED", None),
            connection("c", "gmail", "FAILED", None),
        ],
        &[],
        &HashMap::new(),
    );
    assert_eq!(out[0].non_active_status.as_deref(), Some("EXPIRED"));
    assert!(!out[0].connected);
}

#[test]
fn an_active_connection_suppresses_the_inactive_status() {
    let out = run(
        &["gmail"],
        &[
            connection("a", "gmail", "EXPIRED", None),
            connection("b", "gmail", "ACTIVE", None),
        ],
        &[],
        &HashMap::new(),
    );
    assert!(out[0].connected);
    assert_eq!(out[0].non_active_status, None);
    assert_eq!(out[0].connections.len(), 1);
}

#[test]
fn the_first_of_two_equally_ranked_statuses_wins() {
    let out = run(
        &["gmail"],
        &[
            connection("a", "gmail", "WEIRD_ONE", None),
            connection("b", "gmail", "WEIRD_TWO", None),
        ],
        &[],
        &HashMap::new(),
    );
    assert_eq!(out[0].non_active_status.as_deref(), Some("WEIRD_ONE"));
}

#[test]
fn active_toolkits_are_sorted_unique_and_non_empty() {
    let list = active_toolkits(&[
        connection("a", "slack", "ACTIVE", None),
        connection("b", "gmail", "ACTIVE", None),
        connection("c", "gmail", "ACTIVE", None),
        connection("d", "notion", "EXPIRED", None),
        connection("e", "", "ACTIVE", None),
    ]);
    assert_eq!(list, ["gmail", "slack"]);
}
