//! Compatibility tests for the compiled connector vocabulary.

#![allow(clippy::expect_used)]

use super::*;
use tinyconnectors_bus::vocabulary::ToolScope;

#[test]
fn task_preparation_preserves_provider_extras() {
    let filter = TaskFetchFilter {
        extra: serde_json::json!({"future":"kept"}),
        ..Default::default()
    };
    let prepared = prepare_task_filter(filter.clone());
    assert_eq!(prepared.filter, filter);
    assert_eq!(prepared.effective_max, 25);
}

#[test]
fn every_extracted_catalog_alias_preserves_action_order_and_scope() -> serde_json::Result<()> {
    let golden: std::collections::BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!(
            "../../../tinyconnectors-sync/src/toolkits/catalog/fixtures/catalog-actions.json"
        ))?;
    let request = VocabularyRequest {
        toolkits: golden.keys().cloned().collect(),
    };
    let response = get_vocabulary(request);
    for toolkit in response.toolkits {
        assert_eq!(
            serde_json::to_value(toolkit.actions)?,
            golden[&toolkit.toolkit],
            "{}",
            toolkit.toolkit
        );
    }
    assert_eq!(
        response.agent_ready_toolkits,
        golden.keys().cloned().collect::<Vec<_>>()
    );
    Ok(())
}

#[test]
fn unknown_scope_profiles_are_explicit_and_curated_actions_take_precedence() {
    use tinyconnectors_bus::vocabulary::UserScopePref;
    let query = |slug: &str, profile| ActionVocabularyRequest {
        slug: slug.into(),
        preference: UserScopePref {
            read: true,
            write: false,
            admin: false,
        },
        unknown_scope_profile: profile,
        compare_with: None,
    };
    let response = evaluate_actions(vec![
        query(
            "CUSTOM_LIST_DRAFTS",
            UnknownScopeProfile::SubstringCompatibility,
        ),
        query("CUSTOM_LIST_DRAFTS", UnknownScopeProfile::LeadingVerb),
        query(
            "GMAIL_LIST_DRAFTS",
            UnknownScopeProfile::SubstringCompatibility,
        ),
        query("GMAIL_LIST_DRAFTS", UnknownScopeProfile::LeadingVerb),
        query("CUSTOM_DELETE_POST", UnknownScopeProfile::LeadingVerb),
        query("CUSTOM_CREATE_RECORD", UnknownScopeProfile::LeadingVerb),
        query("CUSTOM_READ_RECORD", UnknownScopeProfile::LeadingVerb),
        query("GMAIL_UNCURATED_ACTION", UnknownScopeProfile::LeadingVerb),
        query("INVALID", UnknownScopeProfile::LeadingVerb),
    ]);
    assert_eq!(response[0].scope, ToolScope::Write);
    assert!(!response[0].visible);
    assert_eq!(response[1].scope, ToolScope::Read);
    assert!(response[1].visible);
    for value in &response[2..4] {
        assert_eq!(value.curated_scope, Some(ToolScope::Read));
        assert!(value.visible);
    }
    assert_eq!(response[4].scope, ToolScope::Admin);
    assert_eq!(response[5].scope, ToolScope::Write);
    assert_eq!(response[6].scope, ToolScope::Read);
    assert!(!response[7].visible);
    assert!(response[8].visible);
}

#[test]
fn action_aliases_are_compared_inside_the_module() {
    use tinyconnectors_bus::vocabulary::UserScopePref;
    let response = evaluate_actions(vec![ActionVocabularyRequest {
        slug: "GOOGLE_CALENDAR_CREATE_EVENT".into(),
        preference: UserScopePref::default(),
        unknown_scope_profile: UnknownScopeProfile::SubstringCompatibility,
        compare_with: Some("googlecalendar_create_event".into()),
    }]);
    assert_eq!(response[0].toolkit.as_deref(), Some("googlecalendar"));
    assert_eq!(
        response[0].canonical_slug.as_deref(),
        Some("GOOGLECALENDAR_CREATE_EVENT")
    );
    // Matching treats the first slug as a catalog candidate, preserving the extracted asymmetry.
    assert_eq!(response[0].matches, Some(false));
}

#[test]
fn unknown_toolkits_retain_fallback_descriptions_without_a_catalog() {
    let response = get_vocabulary(VocabularyRequest {
        toolkits: vec!["unknown".into()],
    });
    assert!(response.toolkits[0].actions.is_none());
    assert!(!response.toolkits[0].native_provider);
    assert_ne!(response.toolkits[0].description, "");
}

#[test]
fn toolkit_descriptions_preserve_the_extracted_prompt_vocabulary() -> serde_json::Result<()> {
    let golden: std::collections::BTreeMap<String, String> = serde_json::from_str(include_str!(
        "../../../tinyconnectors-sync/src/toolkits/catalog/fixtures/descriptions.json"
    ))?;
    let response = get_vocabulary(VocabularyRequest {
        toolkits: golden.keys().cloned().collect(),
    });
    for toolkit in response.toolkits {
        assert_eq!(toolkit.description, golden[&toolkit.toolkit]);
    }
    Ok(())
}

#[test]
fn identity_metadata_is_computed_by_the_module() {
    use tinyconnectors_bus::vocabulary::{IdentityKind, IdentityValueRequest};
    let response = normalize_identities(NormalizeIdentitiesRequest {
        values: vec![IdentityValueRequest {
            kind: IdentityKind::Email,
            value: " A@B.COM ".into(),
        }],
        connection_identifiers: vec![" Conn ".into()],
        identities: vec![],
    });
    assert_eq!(response.values[0].value.as_deref(), Some("a@b.com"));
    assert!((response.values[0].confidence - 0.95).abs() < f64::EPSILON);
    assert!(response.values[0].matchable);
    assert_eq!(response.connection_identifiers, ["conn"]);
    assert_eq!(response.prompt_section, "");
}

#[test]
fn every_curated_catalog_is_reported_ready() -> serde_json::Result<()> {
    let golden: std::collections::BTreeMap<String, serde_json::Value> =
        serde_json::from_str(include_str!(
            "../../../tinyconnectors-sync/src/toolkits/catalog/fixtures/catalog-actions.json"
        ))?;
    let catalogs = get_vocabulary(VocabularyRequest {
        toolkits: golden.keys().cloned().collect(),
    });
    let ready = get_vocabulary(VocabularyRequest {
        toolkits: catalogs.agent_ready_toolkits,
    });
    for toolkit in catalogs.toolkits {
        assert!(
            ready
                .toolkits
                .iter()
                .any(|entry| entry.toolkit == toolkit.toolkit),
            "{} has a compiled catalog but no ready-list entry",
            toolkit.toolkit
        );
    }
    Ok(())
}

#[test]
fn recognized_aliases_use_canonical_catalog_scope_and_visibility() {
    for profile in [
        UnknownScopeProfile::SubstringCompatibility,
        UnknownScopeProfile::LeadingVerb,
    ] {
        for (alias, canonical) in [
            (
                "GOOGLE_CALENDAR_CREATE_EVENT",
                "GOOGLECALENDAR_CREATE_EVENT",
            ),
            ("google_drive_find_file", "GOOGLEDRIVE_FIND_FILE"),
            ("MICROSOFT_OUTLOOK_SEND_EMAIL", "OUTLOOK_SEND_EMAIL"),
        ] {
            for preference in [
                tinyconnectors_bus::vocabulary::UserScopePref::default(),
                tinyconnectors_bus::vocabulary::UserScopePref {
                    read: true,
                    write: false,
                    admin: false,
                },
            ] {
                let request = |slug: &str| ActionVocabularyRequest {
                    slug: slug.into(),
                    preference,
                    unknown_scope_profile: profile,
                    compare_with: None,
                };
                let response = evaluate_actions(vec![request(alias), request(canonical)]);
                assert!(response[1].curated_scope.is_some(), "{canonical}");
                assert_eq!(
                    response[0].curated_scope, response[1].curated_scope,
                    "{alias}"
                );
                assert_eq!(response[0].scope, response[1].scope, "{alias}");
                assert_eq!(response[0].visible, response[1].visible, "{alias}");
                assert_eq!(response[0].toolkit, response[1].toolkit, "{alias}");
            }
        }
    }
}

#[test]
fn identifiers_without_canonical_characters_are_rejected() {
    use tinyconnectors_bus::vocabulary::{IdentityKind, IdentityValueRequest};
    let response = normalize_identities(NormalizeIdentitiesRequest {
        values: vec![
            IdentityValueRequest {
                kind: IdentityKind::Phone,
                value: " --- ".into(),
            },
            IdentityValueRequest {
                kind: IdentityKind::Handle,
                value: " @@@ ".into(),
            },
        ],
        connection_identifiers: vec![],
        identities: vec![],
    });
    for value in response.values {
        assert_eq!(value.value, None);
    }
}

#[test]
fn toolkit_casing_and_padding_preserve_consistent_metadata() {
    let response = get_vocabulary(VocabularyRequest {
        toolkits: vec!["gmail".into(), "GMAIL".into(), "  gmail  ".into()],
    });
    let canonical = &response.toolkits[0];
    assert!(canonical.native_provider);
    for (variant, original) in response.toolkits[1..].iter().zip(["GMAIL", "  gmail  "]) {
        assert_eq!(variant.toolkit, original);
        assert_eq!(variant.description, canonical.description);
        assert_eq!(variant.native_provider, canonical.native_provider);
        assert_eq!(variant.actions, canonical.actions);
    }
}

#[test]
fn phones_without_digits_have_no_canonical_identity() {
    use tinyconnectors_bus::vocabulary::{IdentityKind, IdentityValueRequest};
    let response = normalize_identities(NormalizeIdentitiesRequest {
        values: ["+", "+++", "---+---", "abc+", "１２３+"]
            .into_iter()
            .map(|value| IdentityValueRequest {
                kind: IdentityKind::Phone,
                value: value.into(),
            })
            .collect(),
        connection_identifiers: vec![],
        identities: vec![],
    });
    for value in response.values {
        assert_eq!(value.value, None);
    }
}

#[test]
fn curated_catalog_lists_one_toolkit_in_catalog_order() {
    let listed = list_curated_catalog(CuratedCatalogRequest {
        toolkit: Some(" Gmail ".into()),
    });
    let vocab = get_vocabulary(VocabularyRequest {
        toolkits: vec!["gmail".into()],
    });
    let expected = vocab.toolkits[0].actions.clone().expect("gmail is curated");
    assert_eq!(listed.len(), expected.len());
    for (tool, action) in listed.iter().zip(&expected) {
        assert_eq!(tool.toolkit, "gmail");
        assert_eq!(tool.slug, action.slug);
        assert_eq!(tool.scope, action.scope);
    }
}

#[test]
fn curated_catalog_without_a_toolkit_lists_every_curated_toolkit_once() {
    let listed = list_curated_catalog(CuratedCatalogRequest::default());
    for toolkit in CURATED_TOOLKITS {
        assert!(
            listed.iter().any(|t| t.toolkit == *toolkit),
            "{toolkit} missing from the full listing"
        );
    }
    let mut keys: Vec<(&str, &str)> = listed
        .iter()
        .map(|t| (t.toolkit.as_str(), t.slug.as_str()))
        .collect();
    let before = keys.len();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(before, keys.len());
}

#[test]
fn curated_catalog_for_an_uncurated_toolkit_is_empty() {
    let listed = list_curated_catalog(CuratedCatalogRequest {
        toolkit: Some("definitely-not-a-toolkit".into()),
    });
    assert_eq!(listed, Vec::new());
}

#[test]
fn visible_tools_follow_the_scope_preference() {
    let default = list_visible_tools("gmail", UserScopePref::default());
    assert!(default.has_catalog);
    assert_ne!(default.tools, Vec::new());
    assert!(default.tools.iter().all(|t| t.scope != ToolScope::Admin));
    assert!(default.tools.iter().any(|t| t.scope == ToolScope::Write));

    let read_only = list_visible_tools(
        "gmail",
        UserScopePref {
            read: true,
            write: false,
            admin: false,
        },
    );
    assert!(read_only.tools.iter().all(|t| t.scope == ToolScope::Read));
    assert!(read_only.tools.len() < default.tools.len());

    let everything = list_visible_tools(
        "gmail",
        UserScopePref {
            read: true,
            write: true,
            admin: true,
        },
    );
    let full = list_curated_catalog(CuratedCatalogRequest {
        toolkit: Some("gmail".into()),
    });
    assert_eq!(
        everything.tools, full,
        "all scopes on lists the whole catalog"
    );
}

#[test]
fn visible_tools_for_an_uncurated_toolkit_pass_through() {
    let listed = list_visible_tools("not-a-toolkit", UserScopePref::default());
    assert!(!listed.has_catalog);
    assert_eq!(listed.tools, Vec::new());
}
