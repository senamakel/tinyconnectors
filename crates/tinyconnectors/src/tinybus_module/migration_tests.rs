use super::*;
use serde_json::json;
use tinystoragedrivers_core::schema::{Item, StreamPlanRequest, TransformKind};
use tinystoragedrivers_core::{Key, Meta};
use tinystoragedrivers_tinybus::migration::{
    DocumentsTransformRequest, Expiry, MigrationPayload, MigrationPayloadReply, SourceRecord,
};

#[test]
fn declaration_exports_the_implementation_schema_and_explicit_operations() {
    let result = declaration();
    assert!(result.is_ok(), "the owner schema has external operations");
    let Some(declaration) = result.ok() else {
        return;
    };
    assert_eq!(declaration.owner.as_str(), "connectors");
    assert!(declaration.items.iter().any(|item| matches!(
        item,
        Item::Collection { name, version: 2, .. } if name == "connectors.user_scopes"
    )));
    assert!(declaration.items.iter().any(|item| matches!(
        item,
        Item::Stream { name, .. } if name == "connectors.triggers"
    )));

    let documents = declaration
        .steps
        .iter()
        .find(|step| step.id == "user_scopes_unwrap_value");
    assert!(
        documents.is_some(),
        "the implementation declares the preferences transform"
    );
    let Some(documents) = documents else {
        return;
    };
    assert_eq!(documents.item, "connectors.user_scopes");
    assert_eq!(documents.from, 1);
    assert_eq!(documents.to, 2);
    assert_eq!(documents.source.as_deref(), Some("composio-user-scopes"));
    assert_eq!(documents.transform.operation, USER_SCOPES_OPERATION);
    assert_eq!(documents.transform.version, USER_SCOPES_OPERATION_VERSION);
    assert_eq!(documents.transform.kind, TransformKind::Documents);

    let stream = declaration
        .steps
        .iter()
        .find(|step| step.id == "triggers_rename");
    assert!(
        stream.is_some(),
        "the implementation declares the trigger transform"
    );
    let Some(stream) = stream else {
        return;
    };
    assert_eq!(stream.item, "connectors.triggers");
    assert_eq!(stream.from, 1);
    assert_eq!(stream.to, 2);
    assert_eq!(stream.source.as_deref(), Some("triggers/"));
    assert_eq!(stream.transform.operation, TRIGGERS_OPERATION);
    assert_eq!(stream.transform.version, TRIGGERS_OPERATION_VERSION);
    assert_eq!(stream.transform.kind, TransformKind::Stream);
}

#[test]
fn migration_handler_reuses_owner_transforms_and_preserves_document_history() {
    let handler = ConnectorMigration;
    let reply = handler.transform(MigrationPayload::Documents(DocumentsTransformRequest {
        operation: USER_SCOPES_OPERATION.to_owned(),
        version: USER_SCOPES_OPERATION_VERSION,
        records: vec![SourceRecord {
            id: "gmail".to_owned(),
            body: json!({"value": {"scopes": ["read"]}}),
            meta: Meta {
                schema: 1,
                created_ms: 123,
                updated_ms: 456,
                expires_ms: Some(789),
            },
        }],
    }));
    let records = match reply {
        MigrationPayloadReply::Documents(Ok(records)) => Some(records),
        _ => None,
    };
    assert!(records.is_some(), "preferences transform must succeed");
    let Some(records) = records else {
        return;
    };
    assert_eq!(records.len(), 1);
    let Some(record) = records.first() else {
        return;
    };
    assert_eq!(record.collection, "connectors.user_scopes");
    let expected_id = Key::of(["gmail"]).map(|key| key.as_str().to_owned());
    assert!(expected_id.is_ok());
    assert_eq!(
        Some(record.id.as_str()),
        expected_id.as_ref().ok().map(String::as_str)
    );
    assert_eq!(record.body, json!({"scopes": ["read"]}));
    assert_eq!(record.created_ms, Some(123));
    assert_eq!(record.expires, Expiry::At(789));

    let reply = handler.transform(MigrationPayload::StreamPlan(StreamPlanRequest {
        operation: TRIGGERS_OPERATION.to_owned(),
        version: TRIGGERS_OPERATION_VERSION,
        source: "triggers/2026-10-21".to_owned(),
    }));
    let plan = match reply {
        MigrationPayloadReply::StreamPlan(Ok(Some(plan))) => Some(plan),
        _ => None,
    };
    assert!(
        plan.is_some(),
        "trigger stream transform must produce a plan"
    );
    let Some(plan) = plan else {
        return;
    };
    assert_eq!(plan.target, "connectors.triggers/2026-10-21");
    assert_eq!(plan.mode, StreamMapMode::Identity);
}

#[test]
fn migration_handler_rejects_unknown_operations_and_versions() {
    let handler = ConnectorMigration;
    let reply = handler.transform(MigrationPayload::StreamPlan(StreamPlanRequest {
        operation: TRIGGERS_OPERATION.to_owned(),
        version: TRIGGERS_OPERATION_VERSION + 1,
        source: "triggers/2026-10-21".to_owned(),
    }));
    assert!(matches!(
        reply,
        MigrationPayloadReply::StreamPlan(Err(TransformError {
            kind: TransformErrorKind::UnsupportedVersion,
            ..
        }))
    ));
}
