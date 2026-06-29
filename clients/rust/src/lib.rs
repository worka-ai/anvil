use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

pub mod proto {
    tonic::include_proto!("anvil");
}

#[derive(Clone, Debug, Default)]
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
    format!("Bearer {}", token.as_ref()).parse()
}

pub type InterceptedChannel =
    tonic::service::interceptor::InterceptedService<Channel, BearerInterceptor>;

#[derive(Clone, Debug)]
pub struct AnvilClient {
    channel: Channel,
    interceptor: BearerInterceptor,
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

    pub fn internal(
        &self,
    ) -> proto::internal_anvil_service_client::InternalAnvilServiceClient<InterceptedChannel> {
        proto::internal_anvil_service_client::InternalAnvilServiceClient::with_interceptor(
            self.channel.clone(),
            self.interceptor.clone(),
        )
    }

    pub fn auth(&self) -> proto::auth_service_client::AuthServiceClient<InterceptedChannel> {
        proto::auth_service_client::AuthServiceClient::with_interceptor(
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
