use crate::AppState;
use crate::anvil_api::object_service_server::ObjectService as ObjectServiceTrait;
use crate::anvil_api::stream_service_server::StreamService;
use crate::anvil_api::*;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl StreamService for AppState {
    type TailStreamStream = <AppState as ObjectServiceTrait>::TailAppendStreamStream;

    async fn create_stream(
        &self,
        request: Request<CreateAppendStreamRequest>,
    ) -> Result<Response<CreateAppendStreamResponse>, Status> {
        <AppState as ObjectServiceTrait>::create_append_stream(self, request).await
    }

    async fn append_record(
        &self,
        request: Request<AppendStreamRecordRequest>,
    ) -> Result<Response<AppendStreamRecordResponse>, Status> {
        <AppState as ObjectServiceTrait>::append_stream_record(self, request).await
    }

    async fn read_stream(
        &self,
        request: Request<ReadAppendStreamRequest>,
    ) -> Result<Response<ReadAppendStreamResponse>, Status> {
        <AppState as ObjectServiceTrait>::read_append_stream(self, request).await
    }

    async fn tail_stream(
        &self,
        request: Request<TailAppendStreamRequest>,
    ) -> Result<Response<Self::TailStreamStream>, Status> {
        <AppState as ObjectServiceTrait>::tail_append_stream(self, request).await
    }

    async fn seal_segment(
        &self,
        request: Request<SealAppendStreamSegmentRequest>,
    ) -> Result<Response<SealAppendStreamSegmentResponse>, Status> {
        <AppState as ObjectServiceTrait>::seal_append_stream_segment(self, request).await
    }
}
