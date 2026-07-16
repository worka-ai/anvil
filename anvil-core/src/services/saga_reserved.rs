use crate::anvil_api::{NativeMutationContext, PublicMutationContext, WriteOptions, write_options};
use tonic::Status;

pub(crate) const SAGA_RESERVED_MESSAGE: &str =
    "saga API is reserved in this release and is not implemented";

pub(crate) fn saga_reserved_status() -> Status {
    Status::unimplemented(SAGA_RESERVED_MESSAGE)
}

pub(crate) fn write_options_transaction_id(
    options: Option<&WriteOptions>,
) -> Result<Option<&str>, Status> {
    let Some(options) = options else {
        return Ok(None);
    };
    match options.execution.as_ref() {
        None => Ok(None),
        Some(write_options::Execution::TransactionId(transaction_id)) => {
            validate_transaction_id(transaction_id).map(Some)
        }
        Some(write_options::Execution::SagaOperation(_))
        | Some(write_options::Execution::SagaCompensationOperation(_)) => {
            Err(saga_reserved_status())
        }
    }
}

pub(crate) fn write_options_is_transactional(options: Option<&WriteOptions>) -> bool {
    matches!(
        options.and_then(|options| options.execution.as_ref()),
        Some(write_options::Execution::TransactionId(_))
    )
}

pub(crate) fn native_context_transaction_id(
    context: Option<&NativeMutationContext>,
) -> Result<Option<&str>, Status> {
    let Some(context) = context else {
        return Ok(None);
    };
    reject_native_saga_context(context)?;
    match context.transaction_id.as_deref() {
        None => Ok(None),
        Some(transaction_id) => validate_transaction_id(transaction_id).map(Some),
    }
}

pub(crate) fn public_context_transaction_id(
    context: &PublicMutationContext,
) -> Result<Option<&str>, Status> {
    reject_public_saga_context(context)?;
    match context.transaction_id.as_deref() {
        None => Ok(None),
        Some(transaction_id) => validate_transaction_id(transaction_id).map(Some),
    }
}

pub(crate) fn reject_native_saga_context(context: &NativeMutationContext) -> Result<(), Status> {
    if context.saga_operation.is_some() || context.saga_compensation_operation.is_some() {
        return Err(saga_reserved_status());
    }
    Ok(())
}

pub(crate) fn reject_public_saga_context(context: &PublicMutationContext) -> Result<(), Status> {
    if context.saga_operation.is_some() || context.saga_compensation_operation.is_some() {
        return Err(saga_reserved_status());
    }
    Ok(())
}

fn validate_transaction_id(transaction_id: &str) -> Result<&str, Status> {
    if transaction_id.trim().is_empty() {
        return Err(Status::invalid_argument("transaction_id must not be empty"));
    }
    Ok(transaction_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anvil_api::{
        NativeMutationContext, PublicMutationContext, SagaCompensationOperationContext,
        SagaOperationContext, WriteOptions, write_options,
    };

    #[test]
    fn write_options_rejects_saga_execution_contexts() {
        let options = WriteOptions {
            idempotency_key: String::new(),
            consistency: 0,
            wait_for_finalization: false,
            preconditions: Vec::new(),
            boundary_values: Vec::new(),
            execution: Some(write_options::Execution::SagaOperation(
                SagaOperationContext::default(),
            )),
        };

        let err = write_options_transaction_id(Some(&options)).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unimplemented);
    }

    #[test]
    fn mutation_contexts_reject_saga_execution_contexts() {
        let native = NativeMutationContext {
            tenant_id: 1,
            bucket_id: 2,
            principal: "principal".to_string(),
            request_id: "request".to_string(),
            precondition: "none".to_string(),
            authz_zookie_optional: String::new(),
            idempotency_key: "idem".to_string(),
            transaction_id: None,
            saga_operation: Some(SagaOperationContext::default()),
            saga_compensation_operation: None,
            write_visibility: None,
        };
        let public = PublicMutationContext {
            request_id: "public-request".to_string(),
            idempotency_key: "public-idem".to_string(),
            expected_generation: 1,
            transaction_id: None,
            saga_operation: None,
            saga_compensation_operation: Some(SagaCompensationOperationContext::default()),
        };

        assert_eq!(
            native_context_transaction_id(Some(&native))
                .unwrap_err()
                .code(),
            tonic::Code::Unimplemented
        );
        assert_eq!(
            public_context_transaction_id(&public).unwrap_err().code(),
            tonic::Code::Unimplemented
        );
    }
}
