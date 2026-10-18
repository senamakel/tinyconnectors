//! Tests for webhook signature verification and delivery decoding.
//!
//! One signature is a known answer computed outside Rust (Python's `hmac`), so
//! the scheme is pinned against an independent implementation; the rest are
//! signed here with the same scheme to exercise each way a delivery can fail.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use hmac::{Hmac, Mac};
use serde_json::json;
use sha2::Sha256;

use super::*;

const SECRET: &str = "whsec_dGVzdA==";
const ID: &str = "msg_2Lh9";
const TIMESTAMP: u64 = 1_760_000_000;
const BODY: &[u8] = br#"{"type":"composio.trigger.message"}"#;

/// Sign `body` the way Composio does, for `secret`, `id` and `timestamp`.
fn sign(secret: &str, id: &str, timestamp: u64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(format!("{id}.{timestamp}.").as_bytes());
    mac.update(body);
    base64::engine::general_purpose::STANDARD.encode(mac.finalize().into_bytes())
}

fn check(signature: &str, body: &[u8], now: u64) -> Result<(), WebhookError> {
    let timestamp = TIMESTAMP.to_string();
    let headers = WebhookHeaders {
        id: ID,
        timestamp: &timestamp,
        signature,
    };
    verify(SECRET, &headers, body, now, DEFAULT_TOLERANCE_SECS)
}

#[test]
fn accepts_a_signature_computed_by_an_independent_implementation() {
    // `hmac.new(b"whsec_dGVzdA==", b"msg_2Lh9.1760000000.<body>", sha256)`,
    // base64-encoded. The secret is keyed as its raw bytes: no `whsec_`
    // stripping and no base64 decoding.
    let signature = "v1,u3rhivKxA1i5wJshDB8nGZDxVEAR9ymjDSxmeO8/8I0=";
    assert_eq!(check(signature, BODY, TIMESTAMP), Ok(()));
    assert_eq!(
        sign(SECRET, ID, TIMESTAMP, BODY),
        "u3rhivKxA1i5wJshDB8nGZDxVEAR9ymjDSxmeO8/8I0="
    );
}

#[test]
fn accepts_a_bare_signature_without_the_version_prefix() {
    let signature = sign(SECRET, ID, TIMESTAMP, BODY);
    assert_eq!(check(&signature, BODY, TIMESTAMP), Ok(()));
}

#[test]
fn accepts_when_any_one_of_several_signatures_matches() {
    // Several signatures appear while a secret is being rotated.
    let good = sign(SECRET, ID, TIMESTAMP, BODY);
    let stale = sign("an-old-secret", ID, TIMESTAMP, BODY);
    let header = format!("v1,{stale} v1a,{good}x v1,{good}");
    assert_eq!(check(&header, BODY, TIMESTAMP), Ok(()));
}

#[test]
fn ignores_a_signature_under_another_scheme() {
    // `v1a` is an asymmetric scheme this secret cannot check; a valid HMAC
    // filed under it is not a `v1` signature.
    let good = sign(SECRET, ID, TIMESTAMP, BODY);
    assert_eq!(
        check(&format!("v1a,{good}"), BODY, TIMESTAMP),
        Err(WebhookError::SignatureMismatch)
    );
}

#[test]
fn rejects_a_tampered_body() {
    let signature = format!("v1,{}", sign(SECRET, ID, TIMESTAMP, BODY));
    assert_eq!(
        check(
            &signature,
            br#"{"type":"composio.trigger.messagE"}"#,
            TIMESTAMP
        ),
        Err(WebhookError::SignatureMismatch)
    );
}

#[test]
fn rejects_a_signature_made_with_another_secret() {
    let signature = format!("v1,{}", sign("whsec_other", ID, TIMESTAMP, BODY));
    assert_eq!(
        check(&signature, BODY, TIMESTAMP),
        Err(WebhookError::SignatureMismatch)
    );
}

#[test]
fn rejects_a_signature_for_another_message_id() {
    // The id is signed, so a captured delivery cannot be replayed under a
    // fresh id to slip past a host's deduplication.
    let signature = format!("v1,{}", sign(SECRET, "msg_other", TIMESTAMP, BODY));
    assert_eq!(
        check(&signature, BODY, TIMESTAMP),
        Err(WebhookError::SignatureMismatch)
    );
}

#[test]
fn rejects_a_stale_timestamp_even_when_correctly_signed() {
    let signature = format!("v1,{}", sign(SECRET, ID, TIMESTAMP, BODY));
    assert_eq!(check(&signature, BODY, TIMESTAMP + 300), Ok(()));
    assert_eq!(
        check(&signature, BODY, TIMESTAMP + 301),
        Err(WebhookError::StaleTimestamp {
            skew_secs: 301,
            tolerance_secs: 300
        })
    );
}

#[test]
fn rejects_a_timestamp_from_the_future() {
    let signature = format!("v1,{}", sign(SECRET, ID, TIMESTAMP, BODY));
    assert_eq!(check(&signature, BODY, TIMESTAMP - 300), Ok(()));
    assert_eq!(
        check(&signature, BODY, TIMESTAMP - 301),
        Err(WebhookError::StaleTimestamp {
            skew_secs: 301,
            tolerance_secs: 300
        })
    );
}

#[test]
fn names_each_missing_header() {
    let timestamp = TIMESTAMP.to_string();
    let full = WebhookHeaders {
        id: ID,
        timestamp: &timestamp,
        signature: "v1,abc=",
    };
    for (headers, missing) in [
        (WebhookHeaders { id: " ", ..full }, HEADER_ID),
        (
            WebhookHeaders {
                timestamp: "",
                ..full
            },
            HEADER_TIMESTAMP,
        ),
        (
            WebhookHeaders {
                signature: "",
                ..full
            },
            HEADER_SIGNATURE,
        ),
    ] {
        assert_eq!(
            verify(SECRET, &headers, BODY, TIMESTAMP, DEFAULT_TOLERANCE_SECS),
            Err(WebhookError::MissingHeader { header: missing })
        );
    }
}

#[test]
fn rejects_a_timestamp_that_is_not_unix_seconds() {
    let headers = WebhookHeaders {
        id: ID,
        timestamp: "2026-10-15T00:00:00Z",
        signature: "v1,abc=",
    };
    assert_eq!(
        verify(SECRET, &headers, BODY, TIMESTAMP, DEFAULT_TOLERANCE_SECS),
        Err(WebhookError::MalformedHeader {
            header: HEADER_TIMESTAMP
        })
    );
}

#[test]
fn rejects_a_signature_header_with_nothing_decodable_in_it() {
    assert_eq!(
        check("v1,not*base64! ***", BODY, TIMESTAMP),
        Err(WebhookError::MalformedHeader {
            header: HEADER_SIGNATURE
        })
    );
}

#[test]
fn refuses_to_verify_with_an_empty_secret() {
    // Anyone can compute an HMAC under an empty key, so accepting it would
    // accept a forgery.
    let signature = sign("", ID, TIMESTAMP, BODY);
    let timestamp = TIMESTAMP.to_string();
    let headers = WebhookHeaders {
        id: ID,
        timestamp: &timestamp,
        signature: &signature,
    };
    assert_eq!(
        verify("  ", &headers, BODY, TIMESTAMP, DEFAULT_TOLERANCE_SECS),
        Err(WebhookError::EmptySecret)
    );
}

#[test]
fn every_error_renders_a_lowercase_message() {
    for error in [
        WebhookError::MissingHeader { header: HEADER_ID },
        WebhookError::MalformedHeader {
            header: HEADER_SIGNATURE,
        },
        WebhookError::EmptySecret,
        WebhookError::StaleTimestamp {
            skew_secs: 400,
            tolerance_secs: 300,
        },
        WebhookError::SignatureMismatch,
        WebhookError::UndecodableBody {
            reason: "x".to_string(),
        },
    ] {
        let message = error.to_string();
        assert!(
            message.starts_with(|c: char| c.is_ascii_lowercase()),
            "{message}"
        );
        assert!(!message.ends_with('.'), "{message}");
    }
}

#[test]
fn decodes_a_v3_delivery() {
    let body = json!({
        "id": "msg_2Lh9",
        "type": "composio.trigger.message",
        "timestamp": "2026-10-15T00:00:00Z",
        "metadata": {
            "log_id": "log_1",
            "trigger_slug": "GMAIL_NEW_GMAIL_MESSAGE",
            "trigger_id": "ti_1",
            "connected_account_id": "ca_1",
            "user_id": "entity-1",
            "auth_config_id": "ac_1"
        },
        "data": { "subject": "hi", "messageId": "m1" }
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.trigger, "GMAIL_NEW_GMAIL_MESSAGE");
    assert_eq!(event.toolkit, "gmail", "derived from the slug's first word");
    assert_eq!(event.payload, json!({ "subject": "hi", "messageId": "m1" }));
    assert_eq!(event.metadata.id, "msg_2Lh9");
    assert_eq!(event.metadata.uuid, "log_1");
    assert_eq!(event.metadata.trigger_id, "ti_1");
    assert_eq!(event.metadata.connected_account_id, "ca_1");
    assert_eq!(event.metadata.user_id, "entity-1");
}

#[test]
fn a_v3_delivery_prefers_an_explicit_toolkit_over_the_slug() {
    let body = json!({
        "id": "msg_1",
        "type": "composio.trigger.message",
        "metadata": {
            "trigger_slug": "MICROSOFT_TEAMS_NEW_MESSAGE",
            "toolkit_slug": "microsoft_teams"
        },
        "data": {}
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.toolkit, "microsoft_teams");
}

#[test]
fn decodes_a_legacy_v2_delivery() {
    let body = json!({
        "type": "slack_receive_message",
        "timestamp": "2026-10-15T00:00:00Z",
        "log_id": "log_2",
        "data": {
            "text": "hello",
            "connection_id": "ca_2",
            "trigger_nano_id": "ti_2",
            "user_id": "entity-2"
        }
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.trigger, "SLACK_RECEIVE_MESSAGE");
    assert_eq!(event.toolkit, "slack");
    assert_eq!(
        event.payload["text"], "hello",
        "the payload is `data`, metadata keys and all"
    );
    assert_eq!(event.payload["connection_id"], "ca_2");
    assert_eq!(event.metadata.uuid, "log_2");
    assert_eq!(event.metadata.connected_account_id, "ca_2");
    assert_eq!(event.metadata.trigger_id, "ti_2");
    assert_eq!(event.metadata.user_id, "entity-2");
}

#[test]
fn a_v2_delivery_may_name_its_trigger_id_directly() {
    let body = json!({
        "type": "github_star_added_event",
        "timestamp": "2026-10-15T00:00:00Z",
        "data": { "trigger_id": "ti_3", "connected_account_id": "ca_3" }
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.metadata.trigger_id, "ti_3");
    assert_eq!(event.metadata.connected_account_id, "ca_3");
    assert!(event.metadata.uuid.is_empty());
}

#[test]
fn refuses_a_body_that_is_not_a_trigger_delivery() {
    for body in [
        &b"not json at all"[..],
        b"[1, 2, 3]",
        b"{}",
        br#"{"type": "  "}"#,
        // A v3 event that is not a trigger message carries no trigger slug.
        br#"{"type": "composio.connected_account.expired", "metadata": {}, "data": {}}"#,
    ] {
        let error = decode(body).unwrap_err();
        assert!(
            matches!(error, WebhookError::UndecodableBody { .. }),
            "{error}"
        );
    }
}

#[test]
fn a_known_multiword_toolkit_wins_over_the_slugs_first_word() {
    let body = json!({
        "id": "msg_1",
        "type": "composio.trigger.message",
        "metadata": { "trigger_slug": "MICROSOFT_TEAMS_NEW_MESSAGE" },
        "data": {}
    });
    let bytes = body.to_string();

    let event = decode_with_toolkits(bytes.as_bytes(), &["microsoft", "microsoft_teams"]).unwrap();
    assert_eq!(event.toolkit, "microsoft_teams", "the longest known prefix");

    let event = decode_with_toolkits(bytes.as_bytes(), &["gmail"]).unwrap();
    assert_eq!(event.toolkit, "microsoft", "no known match falls back");
}

#[test]
fn a_known_toolkit_also_resolves_a_legacy_v2_delivery() {
    let body = json!({
        "type": "microsoft_teams_new_message",
        "timestamp": "2026-10-15T00:00:00Z",
        "data": { "connection_id": "ca_1" }
    });

    let event = decode_with_toolkits(body.to_string().as_bytes(), &["microsoft_teams"]).unwrap();

    assert_eq!(event.toolkit, "microsoft_teams");
}

#[test]
fn a_non_trigger_delivery_with_metadata_is_not_decoded_as_a_trigger() {
    let body = json!({
        "type": "composio.connected_account.expired",
        "metadata": { "trigger_slug": "GMAIL_NEW_GMAIL_MESSAGE" },
        "data": {}
    });

    let error = decode(body.to_string().as_bytes()).unwrap_err();

    assert!(matches!(error, WebhookError::UndecodableBody { .. }));
}

#[test]
fn decodes_a_legacy_v1_delivery() {
    let body = json!({
        "trigger_name": "gmail_new_gmail_message",
        "trigger_id": "ti_1",
        "connection_id": "ca_1",
        "payload": { "subject": "hi" }
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.trigger, "GMAIL_NEW_GMAIL_MESSAGE");
    assert_eq!(event.toolkit, "gmail");
    assert_eq!(event.metadata.trigger_id, "ti_1");
    assert_eq!(event.metadata.connected_account_id, "ca_1");
    assert_eq!(event.payload, json!({ "subject": "hi" }));
}

#[test]
fn a_body_with_no_trigger_in_any_version_is_undecodable() {
    let error = decode(br#"{"payload":{}}"#).unwrap_err();

    assert!(matches!(error, WebhookError::UndecodableBody { .. }));
}

#[test]
fn a_v2_delivery_prefers_the_connection_nano_id() {
    let body = json!({
        "type": "gmail_new_gmail_message",
        "timestamp": "2026-10-15T00:00:00Z",
        "data": { "connection_id": "uuid-old", "connection_nano_id": "ca_9" }
    });

    let event = decode(body.to_string().as_bytes()).unwrap();

    assert_eq!(event.metadata.connected_account_id, "ca_9");
}

#[test]
fn refuses_bodies_that_only_look_like_a_legacy_delivery() {
    for body in [
        // A type alone is not a V2 trigger.
        json!({ "type": "not-a-trigger" }),
        // V2 needs a timestamp...
        json!({ "type": "x_y", "data": { "connection_id": "ca_1" } }),
        // ...data that is an object...
        json!({ "type": "x_y", "timestamp": "t", "data": "text" }),
        // ...and a connection.
        json!({ "type": "x_y", "timestamp": "t", "data": {} }),
        // V1 needs its connection, trigger id and payload as well as a name.
        json!({ "trigger_name": "X_Y" }),
        json!({ "trigger_name": "X_Y", "connection_id": "ca_1", "trigger_id": "ti_1" }),
    ] {
        let error = decode(body.to_string().as_bytes()).unwrap_err();
        assert!(
            matches!(error, WebhookError::UndecodableBody { .. }),
            "{body}"
        );
    }
}
