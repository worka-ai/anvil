use crate::AppState;
use crate::anvil_api::saga_service_server::SagaService;
use crate::anvil_api::*;
use crate::services::saga_reserved::saga_reserved_status;
use futures_core::Stream;
use std::pin::Pin;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl SagaService for AppState {
    type WatchSagaStream = Pin<Box<dyn Stream<Item = Result<SagaEvent, Status>> + Send>>;

    async fn start_saga(
        &self,
        _request: Request<StartSagaRequest>,
    ) -> Result<Response<StartSagaResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn begin_saga_transaction(
        &self,
        _request: Request<BeginSagaTransactionRequest>,
    ) -> Result<Response<BeginSagaTransactionResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn seal_saga_transaction(
        &self,
        _request: Request<SealSagaTransactionRequest>,
    ) -> Result<Response<SealSagaTransactionResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn start_saga_compensation_program(
        &self,
        _request: Request<StartSagaCompensationProgramRequest>,
    ) -> Result<Response<StartSagaCompensationProgramResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn begin_saga_compensation_transaction(
        &self,
        _request: Request<BeginSagaCompensationTransactionRequest>,
    ) -> Result<Response<BeginSagaCompensationTransactionResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn seal_saga_compensation_transaction(
        &self,
        _request: Request<SealSagaCompensationTransactionRequest>,
    ) -> Result<Response<SealSagaCompensationTransactionResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn seal_saga_compensation_program(
        &self,
        _request: Request<SealSagaCompensationProgramRequest>,
    ) -> Result<Response<SealSagaCompensationProgramResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn apply_saga(
        &self,
        _request: Request<ApplySagaRequest>,
    ) -> Result<Response<ApplySagaResponse>, Status> {
        Err(saga_reserved_status())
    }

    async fn get_saga(
        &self,
        _request: Request<GetSagaRequest>,
    ) -> Result<Response<SagaStatus>, Status> {
        Err(saga_reserved_status())
    }

    async fn watch_saga(
        &self,
        _request: Request<WatchSagaRequest>,
    ) -> Result<Response<Self::WatchSagaStream>, Status> {
        Err(saga_reserved_status())
    }

    async fn cancel_saga(
        &self,
        _request: Request<CancelSagaRequest>,
    ) -> Result<Response<SagaStatus>, Status> {
        Err(saga_reserved_status())
    }

    async fn abort_saga_draft(
        &self,
        _request: Request<AbortSagaDraftRequest>,
    ) -> Result<Response<SagaStatus>, Status> {
        Err(saga_reserved_status())
    }

    async fn resolve_blocked_saga(
        &self,
        _request: Request<ResolveBlockedSagaRequest>,
    ) -> Result<Response<SagaStatus>, Status> {
        Err(saga_reserved_status())
    }
}
