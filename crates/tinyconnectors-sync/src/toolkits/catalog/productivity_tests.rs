//! Compatibility tests for the compiled connector vocabulary.

#![allow(clippy::expect_used, clippy::unwrap_used, clippy::panic)]

use super::*;

// Tests for the surrounding module.

#[test]
fn todoist_catalog_is_non_empty_and_unique() {
    assert_ne!(TODOIST_CURATED, &[] as &[CuratedTool]);
    let mut slugs: Vec<&'static str> = TODOIST_CURATED.iter().map(|t| t.slug).collect();
    slugs.sort_unstable();
    slugs.dedup();
    assert_eq!(slugs.len(), TODOIST_CURATED.len());
    for tool in TODOIST_CURATED {
        assert!(tool.slug.starts_with("TODOIST_"));
    }
}

#[test]
fn todoist_catalog_covers_all_three_scopes() {
    assert!(TODOIST_CURATED.iter().any(|t| t.scope == ToolScope::Read));
    assert!(TODOIST_CURATED.iter().any(|t| t.scope == ToolScope::Write));
    assert!(TODOIST_CURATED.iter().any(|t| t.scope == ToolScope::Admin));
}
