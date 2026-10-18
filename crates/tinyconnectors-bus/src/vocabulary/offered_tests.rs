//! Tests for the offered-toolkit policy.

use super::*;

fn lists(allow: &[&str], deny: &[&str]) -> OfferedToolkits {
    OfferedToolkits {
        allow: allow.iter().map(|s| (*s).to_owned()).collect(),
        deny: deny.iter().map(|s| (*s).to_owned()).collect(),
    }
}

#[test]
fn no_lists_offer_everything() {
    let policy = OfferedToolkits::default();
    assert!(policy.is_unrestricted());
    assert!(policy.permits("gmail"));
    assert!(policy.permits("anything_else"));
}

#[test]
fn a_non_empty_allow_list_offers_only_what_it_names() {
    let policy = lists(&["Gmail", "slack"], &[]);
    assert!(policy.permits("gmail"));
    assert!(policy.permits("SLACK"));
    assert!(!policy.permits("notion"));
}

#[test]
fn deny_wins_over_allow() {
    let policy = lists(&["gmail", "slack"], &["slack"]);
    assert!(policy.permits("gmail"));
    assert!(!policy.permits("slack"));
    assert!(!lists(&[], &["slack"]).permits("Slack"));
    assert!(lists(&[], &["slack"]).permits("gmail"));
}

#[test]
fn separators_and_case_do_not_change_the_toolkit() {
    let policy = lists(&["google_calendar"], &[]);
    assert!(policy.permits("googlecalendar"));
    assert!(policy.permits("Google-Calendar"));
    assert!(!policy.permits("googledrive"));
}

#[test]
fn a_missing_field_reads_as_empty() -> serde_json::Result<()> {
    let policy: OfferedToolkits = serde_json::from_str(r#"{"deny":["slack"]}"#)?;
    assert_eq!(policy.allow, Vec::<String>::new());
    assert_eq!(policy.deny, ["slack"]);
    Ok(())
}
