//! Unit tests for the direct route's trigger members, with webhooks opted in.
//!
//! Every test scripts the exact sequence of requests it expects, so a test
//! fails both when a reply is mapped wrongly and when the route asks Composio
//! for something it should not — above all, for another user's rows.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{Value, json};

use super::{DirectRoute, Route};
use crate::client::Transport;
use crate::{Error, Result};

/// One scripted step: the method and full path expected, and the reply to it.
type Step = (&'static str, String, std::result::Result<Value, String>);

/// A transport that answers a scripted sequence of requests, in order.
///
/// Each step names the method and the full path it expects, so an unexpected
/// request fails the test at the point it is made rather than being answered.
#[derive(Debug, Default)]
struct ScriptedTransport {
    steps: Mutex<VecDeque<Step>>,
    /// The body of every request made, in order; `None` for a GET or DELETE.
    log: Mutex<Vec<Option<Value>>>,
}

impl ScriptedTransport {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn expect(self: &Arc<Self>, method: &'static str, path: &str, reply: Value) -> Arc<Self> {
        self.steps
            .lock()
            .unwrap()
            .push_back((method, path.to_string(), Ok(reply)));
        self.clone()
    }

    fn expect_failure(self: &Arc<Self>, method: &'static str, path: &str, message: &str) {
        self.steps
            .lock()
            .unwrap()
            .push_back((method, path.to_string(), Err(message.to_string())));
    }

    fn answer(
        &self,
        method: &'static str,
        path: &str,
        body: Option<&Value>,
    ) -> Result<serde_json::Value> {
        self.log.lock().unwrap().push(body.cloned());
        let (want_method, want_path, reply) = self
            .steps
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| panic!("unexpected request: {method} {path}"));
        assert_eq!(
            (method, path),
            (want_method, want_path.as_str()),
            "request out of script"
        );
        reply.map_err(|message| Error::Transport {
            path: path.to_string(),
            message,
        })
    }

    fn requests(&self) -> Vec<Option<Value>> {
        self.log.lock().unwrap().clone()
    }

    fn body_of(&self, index: usize) -> Value {
        self.requests()[index].clone().expect("a request body")
    }

    fn assert_script_done(&self) {
        assert!(
            self.steps.lock().unwrap().is_empty(),
            "the route stopped before making every scripted request"
        );
    }
}

#[async_trait]
impl Transport for ScriptedTransport {
    async fn get(&self, path: &str) -> Result<serde_json::Value> {
        self.answer("GET", path, None)
    }

    async fn post(&self, path: &str, body: &serde_json::Value) -> Result<serde_json::Value> {
        self.answer("POST", path, Some(body))
    }

    async fn delete(&self, path: &str) -> Result<serde_json::Value> {
        self.answer("DELETE", path, None)
    }
}

fn route(transport: &Arc<ScriptedTransport>) -> DirectRoute {
    DirectRoute::new(transport.clone(), "sk-test", "entity-1").with_webhooks(true)
}

const ACTIVE_PATH: &str =
    "/trigger_instances/active?user_ids=entity-1&show_disabled=false&limit=50";

#[tokio::test]
async fn lists_available_triggers_from_the_v3_catalog() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/triggers_types?toolkit_slugs=gmail&limit=50&toolkit_versions=latest",
        json!({
            "items": [
                {
                    "slug": "GMAIL_NEW_GMAIL_MESSAGE",
                    "type": "poll",
                    "toolkit": { "slug": "gmail" },
                    "config": {
                        "type": "object",
                        "properties": {
                            "labelIds": { "type": "string", "default": "INBOX" },
                            "interval": { "type": "number", "default": 1 },
                            "query": { "type": "string" }
                        },
                        "required": ["labelIds"]
                    }
                },
                { "slug": "GMAIL_EMAIL_SENT", "type": "webhook", "config": {} },
                { "slug": "  ", "type": "webhook" }
            ],
            "next_cursor": null
        }),
    );

    let response = route(&transport)
        .list_available_triggers("gmail", Some("c1"))
        .await
        .unwrap();

    assert_eq!(response.triggers.len(), 2, "a row with no slug is dropped");
    let first = &response.triggers[0];
    assert_eq!(first.slug, "GMAIL_NEW_GMAIL_MESSAGE");
    assert_eq!(first.scope, "static");
    assert_eq!(
        first.default_config,
        Some(json!({ "labelIds": "INBOX", "interval": 1 })),
        "only properties with a default contribute"
    );
    assert_eq!(
        first.required_config_keys,
        Some(vec!["labelIds".to_string()])
    );
    assert!(first.repo.is_none());

    let second = &response.triggers[1];
    assert!(second.default_config.is_none(), "no defaults means omitted");
    assert!(second.required_config_keys.is_none());
    transport.assert_script_done();
}

#[tokio::test]
async fn reads_a_field_map_config_with_per_field_required_flags() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/triggers_types?toolkit_slugs=github&limit=50&toolkit_versions=latest",
        json!({
            "items": [{
                "slug": "GITHUB_COMMIT_EVENT",
                "config": {
                    "owner": { "type": "string", "required": true },
                    "repo": { "type": "string", "required": true },
                    "branch": { "type": "string", "required": false, "default": "main" }
                }
            }]
        }),
    );

    let response = route(&transport)
        .list_available_triggers("github", Some("c1"))
        .await
        .unwrap();

    let trigger = &response.triggers[0];
    assert_eq!(
        trigger.required_config_keys,
        Some(vec!["owner".to_string(), "repo".to_string()])
    );
    assert_eq!(trigger.default_config, Some(json!({ "branch": "main" })));
    transport.assert_script_done();
}

#[tokio::test]
async fn follows_the_catalog_cursor_to_its_last_page() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest",
            json!({ "items": [{ "slug": "SLACK_A" }], "next_cursor": "page 2" }),
        )
        .expect(
            "GET",
            "/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest&cursor=page%202",
            json!({ "items": [{ "slug": "SLACK_B" }], "next_cursor": "" }),
        );

    let response = route(&transport)
        .list_available_triggers("slack", None)
        .await
        .unwrap();

    let slugs: Vec<_> = response.triggers.iter().map(|t| t.slug.as_str()).collect();
    assert_eq!(slugs, ["SLACK_A", "SLACK_B"]);
    transport.assert_script_done();
}

#[tokio::test]
async fn a_repeated_cursor_is_an_incomplete_listing_not_a_finished_one() {
    // A cursor that does not advance would otherwise loop forever.
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest",
            json!({ "items": [{ "slug": "SLACK_A" }], "next_cursor": "c" }),
        )
        .expect(
            "GET",
            "/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest&cursor=c",
            json!({ "items": [{ "slug": "SLACK_B" }], "next_cursor": "c" }),
        );

    let error = route(&transport)
        .list_available_triggers("slack", None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::ListingIncomplete { .. }), "{error}");
    transport.assert_script_done();
}

#[tokio::test]
async fn lists_only_this_entitys_active_triggers() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        ACTIVE_PATH,
        json!({
            "items": [
                {
                    "id": "ti_1",
                    "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                    "connected_account_id": "c1",
                    "user_id": "entity-1",
                    "trigger_config": { "labelIds": "INBOX" },
                    "disabled_at": null
                },
                {
                    "id": "ti_2",
                    "trigger_name": "SLACK_RECEIVE_MESSAGE",
                    "connected_account_id": "c2",
                    "user_id": "entity-1",
                    "disabled_at": "2026-10-01T00:00:00Z"
                },
                {
                    "id": "ti_3",
                    "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                    "connected_account_id": "c9",
                    "user_id": "someone-else"
                },
                { "trigger_name": "GMAIL_NO_ID", "user_id": "entity-1" }
            ]
        }),
    );
    transport.expect(
        "GET",
        "/connected_accounts?limit=200",
        json!({ "items": [] }),
    );

    let response = route(&transport).list_triggers(None).await.unwrap();

    let ids: Vec<_> = response.triggers.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(
        ids,
        ["ti_1", "ti_2"],
        "another user's row and a row with no id are dropped"
    );
    let first = &response.triggers[0];
    assert_eq!(first.slug, "GMAIL_NEW_GMAIL_MESSAGE");
    assert_eq!(first.toolkit, "gmail");
    assert_eq!(first.connection_id, "c1");
    assert_eq!(first.trigger_config, Some(json!({ "labelIds": "INBOX" })));
    assert_eq!(first.state.as_deref(), Some("active"));
    assert_eq!(response.triggers[1].toolkit, "slack");
    assert_eq!(response.triggers[1].state.as_deref(), Some("disabled"));
    transport.assert_script_done();
}

#[tokio::test]
async fn filters_active_triggers_by_toolkit_including_one_with_an_underscore() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        ACTIVE_PATH,
        json!({
            "items": [
                { "id": "ti_1", "trigger_name": "MICROSOFT_TEAMS_NEW_MESSAGE",
                  "connected_account_id": "c1", "user_id": "entity-1" },
                { "id": "ti_2", "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                  "connected_account_id": "c2", "user_id": "entity-1" },
                { "id": "ti_3", "trigger_name": "OUTLOOK_NEW_MAIL",
                  "toolkit": { "slug": "outlook" },
                  "connected_account_id": "c3", "user_id": "entity-1" }
            ]
        }),
    );

    let response = route(&transport)
        .list_triggers(Some("microsoft_teams"))
        .await
        .unwrap();

    assert_eq!(response.triggers.len(), 1);
    assert_eq!(response.triggers[0].id, "ti_1");
    assert_eq!(
        response.triggers[0].toolkit, "microsoft_teams",
        "the filter names the toolkit better than the slug's first word"
    );
    transport.assert_script_done();
}

#[tokio::test]
async fn an_explicit_toolkit_on_an_active_row_wins_over_the_slug_prefix() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        ACTIVE_PATH,
        json!({ "items": [
            { "id": "ti_3", "trigger_name": "OUTLOOK_NEW_MAIL", "toolkit": { "slug": "outlook" },
              "connected_account_id": "c3", "user_id": "entity-1" }
        ]}),
    );

    let response = route(&transport)
        .list_triggers(Some("outlook"))
        .await
        .unwrap();

    assert_eq!(response.triggers.len(), 1);
    assert_eq!(response.triggers[0].toolkit, "outlook");
}

#[tokio::test]
async fn creates_a_trigger_as_the_entity_on_its_own_connection() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/connected_accounts/c1",
            json!({ "id": "c1", "user_id": "entity-1" }),
        )
        .expect(
            "POST",
            "/trigger_instances/GMAIL_NEW_GMAIL_MESSAGE/upsert",
            json!({ "trigger_id": "ti_1" }),
        );

    let response = route(&transport)
        .create_trigger(
            "GMAIL_NEW_GMAIL_MESSAGE",
            Some("c1"),
            Some(json!({ "labelIds": "INBOX" })),
        )
        .await
        .unwrap();

    assert_eq!(response.trigger_id, "ti_1");
    assert_eq!(
        transport.body_of(1),
        json!({
            "user_id": "entity-1",
            "toolkit_versions": "latest",
            "connected_account_id": "c1",
            "trigger_config": { "labelIds": "INBOX" }
        })
    );
    transport.assert_script_done();
}

#[tokio::test]
async fn creates_a_trigger_without_a_connection_or_config() {
    let transport = ScriptedTransport::new().expect(
        "POST",
        "/trigger_instances/GMAIL_NEW_GMAIL_MESSAGE/upsert",
        json!({ "trigger_id": "ti_1", "status": "active" }),
    );

    let response = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", None, None)
        .await
        .unwrap();

    assert_eq!(response.trigger_id, "ti_1");
    assert_eq!(response.status.as_deref(), Some("active"));
    assert_eq!(
        transport.body_of(0),
        json!({ "user_id": "entity-1", "toolkit_versions": "latest" }),
        "nothing absent is sent as null"
    );
}

#[tokio::test]
async fn enables_a_trigger_and_reports_what_was_enabled() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/connected_accounts/c1",
            json!({ "id": "c1", "user_id": "entity-1" }),
        )
        .expect(
            "POST",
            "/trigger_instances/SLACK_RECEIVE_MESSAGE/upsert",
            json!({ "trigger_id": "ti_7" }),
        );

    let response = route(&transport)
        .enable_trigger("c1", "SLACK_RECEIVE_MESSAGE", None)
        .await
        .unwrap();

    assert_eq!(response.trigger_id, "ti_7");
    assert_eq!(response.slug, "SLACK_RECEIVE_MESSAGE");
    assert_eq!(response.connection_id, "c1");
    assert_eq!(
        transport.body_of(1),
        json!({ "user_id": "entity-1", "toolkit_versions": "latest", "connected_account_id": "c1" })
    );
    transport.assert_script_done();
}

#[tokio::test]
async fn refuses_to_create_a_trigger_on_another_users_connection() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/connected_accounts/c9",
        json!({ "id": "c9", "user_id": "someone-else" }),
    );

    let error = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", Some("c9"), None)
        .await
        .unwrap_err();

    assert_eq!(
        error,
        Error::ConnectionNotOwned {
            connection_id: "c9".to_string()
        }
    );
    assert_eq!(transport.requests().len(), 1, "no subscription was made");
}

#[tokio::test]
async fn refuses_to_enable_a_trigger_on_another_users_connection() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/connected_accounts/c9",
        json!({ "id": "c9", "user_id": "someone-else" }),
    );

    let error = route(&transport)
        .enable_trigger("c9", "GMAIL_NEW_GMAIL_MESSAGE", None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::ConnectionNotOwned { .. }), "{error}");
    assert_eq!(transport.requests().len(), 1, "no subscription was made");
}

#[tokio::test]
async fn refuses_a_subscription_when_the_owner_lookup_fails() {
    // A subscription routes the account's events to whoever holds the project
    // webhook, so it is made only on an account this entity is known to own.
    // Composio checks the upsert's `user_id` against the account only on
    // projects with 2FA enabled, so it cannot be relied on to catch this.
    let transport = ScriptedTransport::new();
    transport.expect_failure("GET", "/connected_accounts/c1", "HTTP 500");

    let error = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", Some("c1"), None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::ConnectionNotOwned { .. }), "{error}");
    assert_eq!(transport.requests().len(), 1, "no subscription was made");
}

#[tokio::test]
async fn refuses_a_subscription_on_an_account_that_names_no_owner() {
    let transport =
        ScriptedTransport::new().expect("GET", "/connected_accounts/c1", json!({ "id": "c1" }));

    let error = route(&transport)
        .enable_trigger("c1", "GMAIL_NEW_GMAIL_MESSAGE", None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::ConnectionNotOwned { .. }), "{error}");
    assert_eq!(transport.requests().len(), 1, "no subscription was made");
}

#[tokio::test]
async fn an_upsert_reply_without_a_trigger_id_is_a_decode_error() {
    let transport = ScriptedTransport::new().expect(
        "POST",
        "/trigger_instances/GMAIL_NEW_GMAIL_MESSAGE/upsert",
        json!({ "trigger_id": "  " }),
    );
    transport.expect("GET", ACTIVE_PATH, json!({ "items": [] }));

    let error = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", None, None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Decode { .. }), "{error}");
}

#[tokio::test]
async fn an_empty_upsert_reply_recovers_the_instance_from_the_active_list() {
    // The documented 204: the transport hands back null.
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/connected_accounts/c1",
            json!({ "id": "c1", "user_id": "entity-1" }),
        )
        .expect(
            "POST",
            "/trigger_instances/GMAIL_NEW_GMAIL_MESSAGE/upsert",
            Value::Null,
        )
        .expect(
            "GET",
            ACTIVE_PATH,
            json!({ "items": [
                { "id": "ti_other", "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                  "connected_account_id": "c2", "user_id": "entity-1" },
                { "id": "ti_7", "trigger_name": "gmail_new_gmail_message",
                  "connected_account_id": "c1", "user_id": "entity-1" }
            ] }),
        );

    let response = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", Some("c1"), None)
        .await
        .unwrap();

    assert_eq!(response.trigger_id, "ti_7");
}

#[tokio::test]
async fn disables_a_trigger_by_removing_this_entitys_subscription() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            "/trigger_instances/active?trigger_ids=ti_1&user_ids=entity-1&show_disabled=true&limit=50",
            json!({ "items": [{ "id": "ti_1", "user_id": "entity-1",
                                "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE" }] }),
        )
        .expect(
            "DELETE",
            "/trigger_instances/manage/ti_1",
            json!({ "trigger_id": "ti_1" }),
        );

    let response = route(&transport).disable_trigger("ti_1").await.unwrap();

    assert!(response.deleted);
    transport.assert_script_done();
}

#[tokio::test]
async fn refuses_to_disable_a_trigger_this_entity_does_not_have() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/trigger_instances/active?trigger_ids=ti_9&user_ids=entity-1&show_disabled=true&limit=50",
        json!({ "items": [] }),
    );

    let error = route(&transport).disable_trigger("ti_9").await.unwrap_err();

    assert_eq!(
        error,
        Error::TriggerNotOwned {
            trigger_id: "ti_9".to_string()
        }
    );
    assert_eq!(transport.requests().len(), 1, "nothing was deleted");
}

#[tokio::test]
async fn refuses_to_disable_another_users_trigger_even_if_composio_returns_it() {
    // Composio filters by `user_ids`; the route checks again rather than trust
    // a filter it cannot see applied.
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/trigger_instances/active?trigger_ids=ti_9&user_ids=entity-1&show_disabled=true&limit=50",
        json!({ "items": [{ "id": "ti_9", "user_id": "someone-else" }] }),
    );

    let error = route(&transport).disable_trigger("ti_9").await.unwrap_err();

    assert!(matches!(error, Error::TriggerNotOwned { .. }), "{error}");
    assert_eq!(transport.requests().len(), 1, "nothing was deleted");
}

#[tokio::test]
async fn still_refuses_the_github_repository_listing() {
    // The repository picker has no verified v3 request shape; GitHub triggers
    // take their repository as ordinary required configuration instead.
    let transport = ScriptedTransport::new();

    let error = route(&transport).list_github_repos(None).await.unwrap_err();

    assert!(matches!(
        error,
        Error::UnsupportedByRoute {
            route: "direct",
            member: "ListGithubRepos"
        }
    ));
    assert_eq!(transport.requests(), Vec::new());
}

#[tokio::test]
async fn a_rejected_key_on_a_trigger_call_counts_toward_the_gate() {
    let transport = ScriptedTransport::new();
    for _ in 0..super::INVALID_API_KEY_THRESHOLD {
        transport.expect_failure("GET", ACTIVE_PATH, "401 invalid api key");
    }
    let direct = route(&transport);

    for _ in 0..super::INVALID_API_KEY_THRESHOLD {
        assert!(direct.list_triggers(None).await.is_err());
    }
    let error = direct.list_triggers(None).await.unwrap_err();

    assert!(matches!(error, Error::DirectAuthGated { .. }), "{error}");
    transport.assert_script_done();
}

#[tokio::test]
async fn refuses_to_disable_a_trigger_whose_row_names_no_owner() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        "/trigger_instances/active?trigger_ids=ti_9&user_ids=entity-1&show_disabled=true&limit=50",
        json!({ "items": [{ "id": "ti_9" }] }),
    );

    let error = route(&transport).disable_trigger("ti_9").await.unwrap_err();

    assert!(matches!(error, Error::TriggerNotOwned { .. }), "{error}");
    transport.assert_script_done();
}

#[tokio::test]
async fn an_ambiguous_empty_upsert_recovery_is_refused_rather_than_guessed() {
    let transport = ScriptedTransport::new()
        .expect(
            "POST",
            "/trigger_instances/GMAIL_NEW_GMAIL_MESSAGE/upsert",
            Value::Null,
        )
        .expect(
            "GET",
            ACTIVE_PATH,
            json!({ "items": [
                { "id": "ti_a", "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                  "connected_account_id": "c1", "user_id": "entity-1" },
                { "id": "ti_b", "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                  "connected_account_id": "c2", "user_id": "entity-1" }
            ] }),
        );

    let error = route(&transport)
        .create_trigger("GMAIL_NEW_GMAIL_MESSAGE", None, None)
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Decode { .. }), "{error}");
}

#[tokio::test]
async fn empty_upsert_recovery_matches_the_requested_configuration() {
    let transport = ScriptedTransport::new()
        .expect(
            "POST",
            "/trigger_instances/GITHUB_COMMIT_EVENT/upsert",
            Value::Null,
        )
        .expect(
            "GET",
            ACTIVE_PATH,
            json!({ "items": [
                { "id": "ti_a", "trigger_name": "GITHUB_COMMIT_EVENT",
                  "trigger_config": { "repo": "a" }, "user_id": "entity-1" },
                { "id": "ti_b", "trigger_name": "GITHUB_COMMIT_EVENT",
                  "trigger_config": { "repo": "b" }, "user_id": "entity-1" }
            ] }),
        );

    let response = route(&transport)
        .create_trigger("GITHUB_COMMIT_EVENT", None, Some(json!({ "repo": "b" })))
        .await
        .unwrap();

    assert_eq!(response.trigger_id, "ti_b");
}

#[tokio::test]
async fn a_listing_that_never_ends_is_an_error_not_a_partial_list() {
    let transport = ScriptedTransport::new();
    transport.expect(
        "GET",
        "/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest",
        json!({ "items": [{ "slug": "SLACK_0" }], "next_cursor": "p1" }),
    );
    for page in 1..super::MAX_PAGES {
        transport.expect(
            "GET",
            &format!("/triggers_types?toolkit_slugs=slack&limit=50&toolkit_versions=latest&cursor=p{page}"),
            json!({ "items": [{ "slug": "SLACK_N" }], "next_cursor": format!("p{}", page + 1) }),
        );
    }

    let error = route(&transport)
        .list_available_triggers("slack", Some("c1"))
        .await
        .unwrap_err();

    assert!(matches!(error, Error::ListingIncomplete { .. }), "{error}");
    transport.assert_script_done();
}

#[tokio::test]
async fn an_unfiltered_listing_labels_rows_with_their_connections_toolkit() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            ACTIVE_PATH,
            json!({ "items": [{
                "id": "ti_1",
                "trigger_name": "MICROSOFT_TEAMS_NEW_MESSAGE",
                "connected_account_id": "c1",
                "user_id": "entity-1"
            }] }),
        )
        .expect(
            "GET",
            "/connected_accounts?limit=200",
            json!({ "items": [{ "id": "c1", "toolkit": { "slug": "microsoft_teams" },
                                "status": "ACTIVE" }] }),
        );

    let response = route(&transport).list_triggers(None).await.unwrap();

    assert_eq!(response.triggers[0].toolkit, "microsoft_teams");
    transport.assert_script_done();
}

#[tokio::test]
async fn a_failed_toolkit_lookup_falls_back_to_the_slugs_first_word() {
    let transport = ScriptedTransport::new().expect(
        "GET",
        ACTIVE_PATH,
        json!({ "items": [{ "id": "ti_1", "trigger_name": "GMAIL_NEW_GMAIL_MESSAGE",
                            "connected_account_id": "c1", "user_id": "entity-1" }] }),
    );
    transport.expect_failure("GET", "/connected_accounts?limit=200", "boom");

    let response = route(&transport).list_triggers(None).await.unwrap();

    assert_eq!(response.triggers[0].toolkit, "gmail");
    transport.assert_script_done();
}

#[tokio::test]
async fn the_toolkit_lookup_follows_connection_pages() {
    let transport = ScriptedTransport::new()
        .expect(
            "GET",
            ACTIVE_PATH,
            json!({ "items": [{ "id": "ti_1", "trigger_name": "MICROSOFT_TEAMS_NEW_MESSAGE",
                                "connected_account_id": "c300", "user_id": "entity-1" }] }),
        )
        .expect(
            "GET",
            "/connected_accounts?limit=200",
            json!({ "items": [{ "id": "c1", "toolkit": "gmail" }], "next_cursor": "p2" }),
        )
        .expect(
            "GET",
            "/connected_accounts?limit=200&cursor=p2",
            json!({ "items": [{ "id": "c300", "toolkit": "microsoft_teams" }] }),
        );

    let response = route(&transport).list_triggers(None).await.unwrap();

    assert_eq!(response.triggers[0].toolkit, "microsoft_teams");
    transport.assert_script_done();
}
