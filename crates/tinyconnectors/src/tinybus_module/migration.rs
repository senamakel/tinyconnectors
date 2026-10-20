//! Compiled connector schema exchange and owner transforms.

use std::sync::Arc;

use tinystoragedrivers_core::schema::{
    Schema, SchemaDeclaration, ScopeClass, StreamMapMode, StreamPlan, TransformError,
    TransformErrorKind, declaration_from_schema,
};
use tinystoragedrivers_tinybus::migration::{
    MigrationPayload, MigrationPayloadHandler, MigrationPayloadReply, serve_module_migration,
};

use crate::state::{
    TRIGGERS_OPERATION, TRIGGERS_OPERATION_VERSION, USER_SCOPES_OPERATION,
    USER_SCOPES_OPERATION_VERSION, rename_trigger_stream, storage_schema, unwrap_user_scopes,
};

fn declaration() -> tinybus::Result<SchemaDeclaration> {
    schema_declaration(&storage_schema())
}

fn schema_declaration(schema: &Schema) -> tinybus::Result<SchemaDeclaration> {
    declaration_from_schema(schema, |step| match step.id {
        "user_scopes_unwrap_value" => Some((
            USER_SCOPES_OPERATION.to_owned(),
            USER_SCOPES_OPERATION_VERSION,
        )),
        "triggers_rename" => Some((TRIGGERS_OPERATION.to_owned(), TRIGGERS_OPERATION_VERSION)),
        _ => None,
    })
    .map_err(|_| tinybus::Error::failed("connector migration declaration is invalid"))
}

fn validate_module_scope(declaration: &SchemaDeclaration) -> tinybus::Result<()> {
    if declaration
        .items
        .iter()
        .any(|item| item.scope() != ScopeClass::Module)
    {
        return Err(tinybus::Error::failed(
            "connector migration schema contains a non-module item",
        ));
    }
    Ok(())
}

struct ConnectorMigration;

impl MigrationPayloadHandler for ConnectorMigration {
    fn transform(&self, payload: MigrationPayload) -> MigrationPayloadReply {
        match payload {
            MigrationPayload::Documents(request)
                if request.operation == USER_SCOPES_OPERATION
                    && request.version == USER_SCOPES_OPERATION_VERSION =>
            {
                let expiries = request
                    .records
                    .iter()
                    .map(|record| record.meta.expires_ms)
                    .collect::<Vec<_>>();
                let result = unwrap_user_scopes(request.records)
                    .map(|records| {
                        records
                            .into_iter()
                            .zip(expiries)
                            .map(|(record, expires_ms)| {
                                let mut target =
                                    tinystoragedrivers_tinybus::migration::TargetRecord::new(
                                        record.collection,
                                        record.id,
                                        record.body,
                                    );
                                if let Some(created_ms) = record.created_ms {
                                    target = target.created_ms(created_ms);
                                }
                                target.expires_ms(expires_ms)
                            })
                            .collect()
                    })
                    .map_err(|_| invalid_transform("connector preference record is invalid"));
                MigrationPayloadReply::Documents(result)
            }
            MigrationPayload::StreamPlan(request)
                if request.operation == TRIGGERS_OPERATION
                    && request.version == TRIGGERS_OPERATION_VERSION =>
            {
                let plan = rename_trigger_stream(&request.source, Vec::new()).map(|(target, _)| {
                    StreamPlan {
                        target,
                        mode: StreamMapMode::Identity,
                    }
                });
                MigrationPayloadReply::StreamPlan(Ok(plan))
            }
            MigrationPayload::Documents(request) => MigrationPayloadReply::Documents(Err(
                unsupported_operation(&request.operation, request.version),
            )),
            MigrationPayload::StreamPlan(request) => MigrationPayloadReply::StreamPlan(Err(
                unsupported_operation(&request.operation, request.version),
            )),
            MigrationPayload::BlobTarget(request) => MigrationPayloadReply::BlobTarget(Err(
                unsupported_operation(&request.operation, request.version),
            )),
            MigrationPayload::StreamEntries(request) => MigrationPayloadReply::StreamEntries(Err(
                unsupported_operation(&request.operation, request.version),
            )),
        }
    }
}

fn unsupported_operation(operation: &str, version: u32) -> TransformError {
    let known = (operation == USER_SCOPES_OPERATION && version != USER_SCOPES_OPERATION_VERSION)
        || (operation == TRIGGERS_OPERATION && version != TRIGGERS_OPERATION_VERSION);
    TransformError {
        kind: if known {
            TransformErrorKind::UnsupportedVersion
        } else {
            TransformErrorKind::UnsupportedOperation
        },
        message: "connector migration operation is unsupported".to_owned(),
    }
}

fn invalid_transform(message: &str) -> TransformError {
    TransformError {
        kind: TransformErrorKind::InvalidInput,
        message: message.to_owned(),
    }
}

pub(super) async fn setup(context: &tinybus_module::ModuleSetupContext) -> tinybus::Result<()> {
    let declaration = declaration()?;
    validate_module_scope(&declaration)?;
    serve_module_migration(context, declaration, Arc::new(ConnectorMigration)).await?;
    context
        .connection()
        .request_name(tinystoragedrivers_tinybus::migration::MIGRATION_INTERFACE)
        .await?;
    Ok(())
}

#[cfg(test)]
#[path = "migration_tests.rs"]
mod tests;
