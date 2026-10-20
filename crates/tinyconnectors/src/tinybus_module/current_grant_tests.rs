//! Linked module tests for access through the current TinyBus storage grants.

use super::*;

use std::sync::atomic::Ordering;

use serde_json::json;
use tinyconnectors_bus::storage::PREFS_NAMESPACE;

#[path = "current_grant_storage_tests.rs"]
mod grant_support;
use grant_support::{CurrentGrantHost, GrantAttempts, serve_current_grants, stop_setup};

async fn assert_unknown_resources_are_rejected(storage: &CurrentGrantHost) {
    let unknown_document = storage
        .scoped
        .documents()
        .get("ungranted.collection", "probe")
        .await;
    assert!(unknown_document.is_err());
    let unknown_streams = storage.scoped.streams().streams("ungranted/").await;
    assert!(unknown_streams.is_err());
    assert_eq!(storage.attempts.unknown_documents.load(Ordering::SeqCst), 1);
    assert_eq!(storage.attempts.unknown_streams.load(Ordering::SeqCst), 1);
}

async fn assert_absent_reads_use_current_grants(
    proxy: &tinybus::Proxy,
    attempts: &GrantAttempts,
) -> tinybus::Result<()> {
    let absent: ComposioUserScopesResponse = proxy
        .call(
            names::methods::GET_USER_SCOPES,
            (ComposioGetUserScopesRequest {
                toolkit: "gmail".into(),
            },),
        )
        .await?;
    assert_eq!(absent.scopes, ComposioUserScopes::default());
    assert_eq!(attempts.legacy_documents.load(Ordering::SeqCst), 0);

    let empty_history: ComposioTriggerHistoryResult = proxy
        .call(
            names::methods::LIST_TRIGGER_HISTORY,
            (ComposioListTriggerHistoryRequest { limit: Some(10) },),
        )
        .await?;
    assert!(empty_history.entries.is_empty());
    assert_eq!(attempts.legacy_streams.load(Ordering::SeqCst), 0);
    Ok(())
}

async fn assert_current_grants_and_stop_second_setup(
    storage: &CurrentGrantHost,
    proxy: tinybus::Proxy,
    second: tinybus::Connection,
) -> tinybus::Result<()> {
    assert!(storage.attempts.current_documents.load(Ordering::SeqCst) > 0);
    assert_eq!(storage.attempts.legacy_documents.load(Ordering::SeqCst), 0);
    assert!(storage.attempts.current_streams.load(Ordering::SeqCst) > 0);
    assert_eq!(storage.attempts.legacy_streams.load(Ordering::SeqCst), 0);
    drop(proxy);
    stop_setup(&second).await?;
    drop(second);
    Ok(())
}

#[tokio::test]
async fn module_preferences_and_trigger_history_use_only_current_storage_grants()
-> tinybus::Result<()> {
    // This exercises the real TinyBus and storage-bus route through the linked
    // service setup; it does not load or qualify the compiled cdylib.
    let bus = MemoryBus::new();
    Broker::new().spawn(bus.clone());
    let storage = serve_current_grants(&bus).await?;
    assert_unknown_resources_are_rejected(&storage).await;

    let (first, proxy) = setup_on(&bus, ModuleConfig::default()).await?;
    assert_absent_reads_use_current_grants(&proxy, &storage.attempts).await?;

    let documents_before_set = storage.attempts.current_documents.load(Ordering::SeqCst);

    let saved: ComposioUserScopesResponse = proxy
        .call(
            names::methods::SET_USER_SCOPES,
            (ComposioSetUserScopesRequest {
                toolkit: "gmail".into(),
                scopes: ComposioUserScopes {
                    read: true,
                    write: false,
                    admin: false,
                },
            },),
        )
        .await?;
    assert!(!saved.scopes.write);
    assert!(
        storage.attempts.current_documents.load(Ordering::SeqCst) > documents_before_set,
        "SetUserScopes must reach the current preference collection"
    );

    let read: ComposioUserScopesResponse = proxy
        .call(
            names::methods::GET_USER_SCOPES,
            (ComposioGetUserScopesRequest {
                toolkit: "gmail".into(),
            },),
        )
        .await?;
    assert_eq!(read.scopes, saved.scopes);

    let key = tinystoragedrivers_core::Key::of(["gmail"]).expect("preference key");
    let stored = storage
        .scoped
        .documents()
        .get(PREFS_NAMESPACE, key.as_str())
        .await
        .expect("reads current preference document")
        .expect("current preference document was persisted");
    assert_eq!(
        stored.doc,
        json!({"read": true, "write": false, "admin": false})
    );

    drop(proxy);
    stop_setup(&first).await?;
    drop(first);
    let (second, proxy) = setup_on(&bus, ModuleConfig::default()).await?;
    let reopened: ComposioUserScopesResponse = proxy
        .call(
            names::methods::GET_USER_SCOPES,
            (ComposioGetUserScopesRequest {
                toolkit: "gmail".into(),
            },),
        )
        .await?;
    assert_eq!(reopened.scopes, saved.scopes);

    let archive = crate::triggers::TriggerArchive::new(storage.scoped.streams().clone());
    archive
        .record(
            "gmail",
            "GMAIL_NEW_GMAIL_MESSAGE",
            "evt-current",
            "u",
            &json!({"subject": "hello"}),
        )
        .await
        .expect("records current trigger history");
    let stream_reads_before_history = storage.attempts.current_streams.load(Ordering::SeqCst);
    let history: ComposioTriggerHistoryResult = proxy
        .call(
            names::methods::LIST_TRIGGER_HISTORY,
            (ComposioListTriggerHistoryRequest { limit: Some(10) },),
        )
        .await?;
    assert_eq!(history.entries.len(), 1);
    assert_eq!(history.entries[0].metadata_id, "evt-current");
    assert!(
        storage.attempts.current_streams.load(Ordering::SeqCst) > stream_reads_before_history,
        "ListTriggerHistory must read the current trigger stream prefix"
    );

    assert_current_grants_and_stop_second_setup(&storage, proxy, second).await?;
    Ok(())
}
