use std::fmt;

use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

mod generated {
    tonic::include_proto!("anvil");
}

pub mod proto {
    pub use super::generated::{
        AbortMultipartRequest, AbortMultipartResponse, AcquireTaskLeaseRequest,
        AppendStreamRecordRequest, AppendStreamRecordResponse, ApplyAuthzSchemaRequest,
        ApplyAuthzSchemaResponse, AuthzNamespaceSchema, AuthzRelationRule, AuthzRelationSchema,
        AuthzSchemaRef, AuthzScope, AuthzSubject, AuthzTuple, AuthzTupleMutation,
        BindAuthzSchemaRequest, BindAuthzSchemaResponse, Bucket, CancelHfIngestionRequest,
        CancelHfIngestionResponse, CheckPermissionRequest, CheckPermissionResponse,
        CheckPermissionsRequest, CheckPermissionsResponse, CheckpointTaskLeaseRequest,
        CommitTaskLeaseRequest, CommitTaskLeaseResponse, CompareAndSwapManifestRequest,
        CompareAndSwapManifestResponse, CompleteMultipartPart, CompleteMultipartRequest,
        CompleteMultipartResponse, ComposeObjectRequest, ComposeObjectResponse,
        ComposeObjectSource, CopyObjectRequest, CopyObjectResponse, CreateAppendStreamRequest,
        CreateAppendStreamResponse, CreateBucketRequest, CreateBucketResponse, CreateHfKeyRequest,
        CreateHfKeyResponse, CreateIndexRequest, CreatePersonalDbGroupRequest,
        CreatePersonalDbProjectionRequest, DType, DeleteBucketRequest, DeleteBucketResponse,
        DeleteHfKeyRequest, DeleteHfKeyResponse, DeleteObjectRequest, DeleteObjectResponse,
        DisableIndexRequest, DropIndexRequest, DropIndexResponse, ForceReleaseTaskLeaseRequest,
        ForceReleaseTaskLeaseResponse, GetAccessTokenRequest, GetAccessTokenResponse,
        GetAuthzSchemaBindingRequest, GetAuthzSchemaBindingResponse, GetAuthzSchemaRequest,
        GetAuthzSchemaResponse, GetBucketPolicyRequest, GetBucketPolicyResponse,
        GetGitBlobByPathRequest, GetGitBlobByPathResponse, GetGitObjectRequest,
        GetGitObjectResponse, GetHfIngestionStatusRequest, GetHfIngestionStatusResponse,
        GetObjectRequest, GetObjectResponse, GetPersonalDbGroupRequest,
        GetPersonalDbProjectionRequest, GetTensorChunk, GetTensorRequest, GetTensorsRequest,
        GitBlobLocation, GitPackMetadata, GitTreeEntryRecord, GrantAccessRequest,
        GrantAccessResponse, HeadObjectRequest, HeadObjectResponse, HfKey, IndexBuildRecord,
        IndexDefinitionRecord, IndexDefinitionResponse, IndexDiagnosticRecord, IndexKind,
        IndexQueryHit, InitiateMultipartRequest, InitiateMultipartResponse,
        ListAuthzObjectsRequest, ListAuthzObjectsResponse, ListAuthzSubjectsRequest,
        ListAuthzSubjectsResponse, ListBucketsRequest, ListBucketsResponse, ListGitTreeRequest,
        ListGitTreeResponse, ListHfKeysRequest, ListHfKeysResponse, ListIndexDiagnosticsRequest,
        ListIndexDiagnosticsResponse, ListIndexesRequest, ListIndexesResponse,
        ListObjectVersionsRequest, ListObjectVersionsResponse, ListObjectsRequest,
        ListObjectsResponse, ListRepairFindingsRequest, ListRepairFindingsResponse,
        ListTensorsRequest, ListTensorsResponse, ModelManifest, NativeMutationContext, ObjectInfo,
        ObjectMetadata, ObjectRef, ObjectSummary, ObjectVersionSummary, PatchJsonObjectRequest,
        PatchJsonObjectResponse, PersonalDbCatchUpEntry, PersonalDbCatchUpRequest,
        PersonalDbCatchUpResponse, PersonalDbCommitCertificateRecord,
        PersonalDbCommittedHeadRecord, PersonalDbGroupManifestRecord, PersonalDbGroupResponse,
        PersonalDbLogRecord, PersonalDbProjectionResponse, PersonalDbSnapshotsHeadRecord,
        PersonalDbVoterAck, PutAuthzSchemaRequest, PutAuthzSchemaResponse, PutBucketPolicyRequest,
        PutBucketPolicyResponse, PutGitPackRequest, PutGitPackResponse, PutModelManifestRequest,
        PutModelManifestResponse, PutObjectRequest, PutObjectResponse, QueryIndexRequest,
        QueryIndexResponse, ReadAuthzTuplesRequest, ReadAuthzTuplesResponse, ReadTaskLeaseRequest,
        ReadTaskLeaseResponse, RepairAuthzDerivedIndexRequest, RepairAuthzDerivedIndexResponse,
        RepairDirectoryIndexRequest, RepairDirectoryIndexResponse, RepairFindingRecord,
        RepairIndexRequest, RepairIndexResponse, RepairPersonalDbLogChainRequest,
        RepairPersonalDbLogChainResponse, RepairSubjectRecord, RevokeAccessRequest,
        RevokeAccessResponse, SealAppendStreamSegmentRequest, SealAppendStreamSegmentResponse,
        SetPublicAccessRequest, SetPublicAccessResponse, StartHfIngestionRequest,
        StartHfIngestionResponse, SubmitPersonalDbChangesetRequest,
        SubmitPersonalDbChangesetResponse, TaskLease, TaskLeaseResponse, TenantScope,
        TensorIndexRow, UpdateIndexRequest, UploadPartMetadata, UploadPartRequest,
        UploadPartResponse, WatchAuthzDerivedLagRequest, WatchAuthzDerivedLagResponse,
        WatchAuthzNamespaceRequest, WatchAuthzNamespaceResponse, WatchAuthzTupleLogRequest,
        WatchAuthzTupleLogResponse, WatchBucketMetadataRequest, WatchBucketMetadataResponse,
        WatchEventEnvelope, WatchGitSourceRequest, WatchGitSourceResponse,
        WatchIndexDefinitionRequest, WatchIndexDefinitionResponse, WatchIndexPartitionRequest,
        WatchIndexPartitionResponse, WatchPersonalDbGroupRequest, WatchPersonalDbGroupResponse,
        WatchPersonalDbProjectionRequest, WatchPersonalDbProjectionResponse, WatchPrefixRequest,
        WatchPrefixResponse, WriteAuthzTupleRequest, WriteAuthzTupleResponse,
        WriteAuthzTuplesRequest, WriteAuthzTuplesResponse, auth_service_client,
        bucket_service_client, coordination_service_client, get_object_response,
        git_source_service_client, hf_ingestion_service_client, hugging_face_key_service_client,
        index_service_client, model_manifest, model_service_client, object_service_client,
        personal_db_service_client, put_git_pack_request, put_object_request,
        repair_service_client, upload_part_request,
    };
}

#[derive(Clone, Default)]
pub struct BearerInterceptor {
    authorization: Option<MetadataValue<Ascii>>,
}

impl BearerInterceptor {
    pub fn new(
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        Ok(Self {
            authorization: Some(bearer_metadata(token)?),
        })
    }

    pub fn anonymous() -> Self {
        Self {
            authorization: None,
        }
    }
}

impl Interceptor for BearerInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = &self.authorization {
            request
                .metadata_mut()
                .insert("authorization", value.clone());
        }
        Ok(request)
    }
}

pub fn bearer_metadata(
    token: impl AsRef<str>,
) -> Result<MetadataValue<Ascii>, tonic::metadata::errors::InvalidMetadataValue> {
    let mut value: MetadataValue<Ascii> = format!("Bearer {}", token.as_ref()).parse()?;
    value.set_sensitive(true);
    Ok(value)
}

pub type InterceptedChannel =
    tonic::service::interceptor::InterceptedService<Channel, BearerInterceptor>;

impl fmt::Debug for BearerInterceptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BearerInterceptor")
            .field(
                "authorization",
                &self.authorization.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

#[derive(Clone)]
pub struct AnvilClient {
    channel: Channel,
    interceptor: BearerInterceptor,
}

impl fmt::Debug for AnvilClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AnvilClient")
            .field("channel", &"<channel>")
            .field("interceptor", &self.interceptor)
            .finish()
    }
}

impl AnvilClient {
    pub async fn connect<D>(endpoint: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let channel = Endpoint::new(endpoint)?.connect().await?;
        Ok(Self::from_channel(channel))
    }

    pub async fn connect_with_bearer<D>(
        endpoint: D,
        token: impl AsRef<str>,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let channel = Endpoint::new(endpoint)?.connect().await?;
        Ok(Self::from_channel_with_bearer(channel, token)?)
    }

    pub fn from_channel(channel: Channel) -> Self {
        Self {
            channel,
            interceptor: BearerInterceptor::anonymous(),
        }
    }

    pub fn from_channel_with_bearer(
        channel: Channel,
        token: impl AsRef<str>,
    ) -> Result<Self, tonic::metadata::errors::InvalidMetadataValue> {
        Ok(Self {
            channel,
            interceptor: BearerInterceptor::new(token)?,
        })
    }

    pub fn auth(&self) -> proto::auth_service_client::AuthServiceClient<InterceptedChannel> {
        proto::auth_service_client::AuthServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn coordination(
        &self,
    ) -> proto::coordination_service_client::CoordinationServiceClient<InterceptedChannel> {
        proto::coordination_service_client::CoordinationServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn buckets(&self) -> proto::bucket_service_client::BucketServiceClient<InterceptedChannel> {
        proto::bucket_service_client::BucketServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn objects(&self) -> proto::object_service_client::ObjectServiceClient<InterceptedChannel> {
        proto::object_service_client::ObjectServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn indexes(&self) -> proto::index_service_client::IndexServiceClient<InterceptedChannel> {
        proto::index_service_client::IndexServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn git_sources(
        &self,
    ) -> proto::git_source_service_client::GitSourceServiceClient<InterceptedChannel> {
        proto::git_source_service_client::GitSourceServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn personaldb(
        &self,
    ) -> proto::personal_db_service_client::PersonalDbServiceClient<InterceptedChannel> {
        proto::personal_db_service_client::PersonalDbServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn repair(&self) -> proto::repair_service_client::RepairServiceClient<InterceptedChannel> {
        proto::repair_service_client::RepairServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn hugging_face_keys(
        &self,
    ) -> proto::hugging_face_key_service_client::HuggingFaceKeyServiceClient<InterceptedChannel>
    {
        proto::hugging_face_key_service_client::HuggingFaceKeyServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn hf_ingestion(
        &self,
    ) -> proto::hf_ingestion_service_client::HfIngestionServiceClient<InterceptedChannel> {
        proto::hf_ingestion_service_client::HfIngestionServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn models(&self) -> proto::model_service_client::ModelServiceClient<InterceptedChannel> {
        proto::model_service_client::ModelServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }
}
