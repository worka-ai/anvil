use crate::persistence::Object;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const LINK_METADATA_CONTENT_TYPE: &str = "application/vnd.anvil.object-link+json";
pub const MAX_LINK_RESOLUTION_DEPTH: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectEntryKind {
    #[default]
    Blob,
    Link,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ObjectLinkResolution {
    #[default]
    Follow,
    Redirect,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLinkTarget {
    pub target_key: String,
    pub target_version: Option<uuid::Uuid>,
    pub resolution: ObjectLinkResolution,
    pub generation: u64,
    pub created_at: DateTime<Utc>,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLinkDescriptor {
    pub schema: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub link_key: String,
    pub target_key: String,
    pub target_version: Option<String>,
    pub resolution: ObjectLinkResolution,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub created_by: String,
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PutObjectLinkRequest {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub link_key: String,
    pub target_key: String,
    pub target_version: Option<uuid::Uuid>,
    pub resolution: ObjectLinkResolution,
    pub expected_generation: Option<u64>,
    pub create_only: bool,
    pub allow_dangling: bool,
    pub idempotency_key: String,
    pub created_by: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectLinkRequest {
    pub tenant_id: i64,
    pub bucket_id: i64,
    pub link_key: String,
    pub expected_generation: u64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteObjectLinkResult {
    pub link_key: String,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectLinkMutation {
    pub link: Object,
    pub descriptor: ObjectLinkDescriptor,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FollowedObjectLink {
    pub descriptor: ObjectLinkDescriptor,
    pub response_etag: String,
    pub target_version: uuid::Uuid,
}

#[derive(Debug, thiserror::Error)]
pub enum ObjectLinkError {
    #[error("bucket not found")]
    BucketNotFound,
    #[error("bucket does not belong to tenant")]
    BucketTenantMismatch,
    #[error("invalid link key")]
    InvalidLinkKey,
    #[error("invalid target key")]
    InvalidTargetKey,
    #[error("object link already exists")]
    AlreadyExists,
    #[error("object link not found")]
    NotFound,
    #[error("existing object is not an object link")]
    ExistingObjectIsNotLink,
    #[error("expected link generation is required")]
    MissingExpectedGeneration,
    #[error("object link generation conflict: expected {expected}, actual {actual}")]
    GenerationConflict { expected: u64, actual: u64 },
    #[error("object link target does not exist")]
    DanglingObjectLink,
    #[error("object link target must be a blob")]
    TargetNotBlob,
    #[error("object link loop detected")]
    LinkLoop,
    #[error("object link resolution depth exceeded")]
    LinkDepthExceeded,
    #[error("internal object-link error: {0}")]
    Internal(String),
}

impl From<anyhow::Error> for ObjectLinkError {
    fn from(error: anyhow::Error) -> Self {
        Self::Internal(error.to_string())
    }
}

pub fn link_descriptor(bucket_name: &str, link: &Object) -> Option<ObjectLinkDescriptor> {
    let target = link.link.as_ref()?;
    Some(ObjectLinkDescriptor {
        schema: "anvil.object_link.v1".to_string(),
        tenant_id: link.tenant_id.to_string(),
        bucket_name: bucket_name.to_string(),
        link_key: link.key.clone(),
        target_key: target.target_key.clone(),
        target_version: target.target_version.map(|version| version.to_string()),
        resolution: target.resolution,
        created_at: target.created_at,
        updated_at: link.created_at,
        created_by: target.created_by.clone(),
        generation: target.generation,
    })
}

pub fn link_generation(link: &Object) -> Option<u64> {
    link.link.as_ref().map(|target| target.generation)
}

pub fn link_metadata_hash(descriptor: &ObjectLinkDescriptor) -> String {
    blake3::hash(&serde_json::to_vec(descriptor).expect("object link descriptor serializes"))
        .to_hex()
        .to_string()
}

pub fn link_metadata_etag(descriptor: &ObjectLinkDescriptor) -> String {
    format!("link-meta-{}", link_metadata_hash(descriptor))
}

pub fn followed_link_etag(link: &Object, target: &Object) -> Option<String> {
    let descriptor = link_descriptor("", link)?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.object_link.followed_etag.v1");
    hasher.update(link.key.as_bytes());
    hasher.update(&descriptor.generation.to_le_bytes());
    hasher.update(descriptor.target_key.as_bytes());
    if let Some(target_version) = descriptor.target_version.as_ref() {
        hasher.update(target_version.as_bytes());
    }
    hasher.update(target.key.as_bytes());
    hasher.update(target.version_id.as_bytes());
    hasher.update(target.etag.as_bytes());
    Some(format!("link-follow-{}", hasher.finalize().to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::persistence::Persistence;
    use serde_json::json;
    use tempfile::tempdir;

    fn test_config(storage_path: &std::path::Path) -> Config {
        Config {
            jwt_secret: "test-secret".to_string(),
            anvil_secret_encryption_key:
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            public_api_addr: "test-node".to_string(),
            api_listen_addr: "127.0.0.1:0".to_string(),
            region: "test-region".to_string(),
            storage_path: storage_path.to_string_lossy().to_string(),
            ..Config::default()
        }
    }

    async fn seeded() -> (tempfile::TempDir, Persistence, crate::persistence::Bucket) {
        let temp = tempdir().unwrap();
        let persistence = Persistence::new(&test_config(temp.path()), None).unwrap();
        persistence.create_region("test-region").await.unwrap();
        let tenant = persistence
            .create_tenant("tenant-a", "unused")
            .await
            .unwrap();
        let bucket = persistence
            .create_bucket(tenant.id, "releases", "test-region")
            .await
            .unwrap();
        persistence
            .create_object(
                tenant.id,
                bucket.id,
                "versions/app-v1.bin",
                "payload-hash-v1",
                11,
                "etag-v1",
                Some("application/octet-stream"),
                Some(json!({"channel": "stable"})),
                None,
                Some(b"hello world".to_vec()),
            )
            .await
            .unwrap();
        persistence
            .create_object(
                tenant.id,
                bucket.id,
                "versions/app-v2.bin",
                "payload-hash-v2",
                12,
                "etag-v2",
                Some("application/octet-stream"),
                None,
                None,
                Some(b"hello again".to_vec()),
            )
            .await
            .unwrap();
        (temp, persistence, bucket)
    }

    fn link_request(
        bucket: &crate::persistence::Bucket,
        link_key: &str,
        target_key: &str,
    ) -> PutObjectLinkRequest {
        PutObjectLinkRequest {
            tenant_id: bucket.tenant_id,
            bucket_id: bucket.id,
            link_key: link_key.to_string(),
            target_key: target_key.to_string(),
            target_version: None,
            resolution: ObjectLinkResolution::Follow,
            expected_generation: None,
            create_only: true,
            allow_dangling: false,
            idempotency_key: format!("idem-{link_key}"),
            created_by: "principal:test".to_string(),
        }
    }

    #[tokio::test]
    async fn many_links_can_point_to_same_target() {
        let (_temp, persistence, bucket) = seeded().await;
        let first = persistence
            .put_object_link(link_request(&bucket, "latest.bin", "versions/app-v1.bin"))
            .await
            .unwrap();
        let second = persistence
            .put_object_link(link_request(&bucket, "stable.bin", "versions/app-v1.bin"))
            .await
            .unwrap();

        assert_eq!(first.descriptor.target_key, "versions/app-v1.bin");
        assert_eq!(second.descriptor.target_key, "versions/app-v1.bin");
        assert_ne!(first.link.key, second.link.key);
    }

    #[tokio::test]
    async fn link_update_is_generation_checked() {
        let (_temp, persistence, bucket) = seeded().await;
        let created = persistence
            .put_object_link(link_request(&bucket, "latest.bin", "versions/app-v1.bin"))
            .await
            .unwrap();
        let mut stale = link_request(&bucket, "latest.bin", "versions/app-v2.bin");
        stale.create_only = false;
        stale.expected_generation = Some(created.descriptor.generation + 1);

        let err = persistence.put_object_link(stale).await.unwrap_err();
        assert!(matches!(
            err,
            ObjectLinkError::GenerationConflict {
                expected: 2,
                actual: 1
            }
        ));

        let mut update = link_request(&bucket, "latest.bin", "versions/app-v2.bin");
        update.create_only = false;
        update.expected_generation = Some(created.descriptor.generation);
        let updated = persistence.put_object_link(update).await.unwrap();
        assert_eq!(updated.descriptor.generation, 2);
        assert_eq!(updated.descriptor.target_key, "versions/app-v2.bin");
    }

    #[tokio::test]
    async fn deleting_link_does_not_delete_target() {
        let (_temp, persistence, bucket) = seeded().await;
        persistence
            .put_object_link(link_request(&bucket, "latest.bin", "versions/app-v1.bin"))
            .await
            .unwrap();

        persistence
            .soft_delete_object(bucket.id, "latest.bin")
            .await
            .unwrap()
            .expect("link delete marker");

        assert!(
            persistence
                .get_object(bucket.id, "versions/app-v1.bin")
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            persistence
                .get_object(bucket.id, "latest.bin")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn dangling_targets_are_structured_errors() {
        let (_temp, persistence, bucket) = seeded().await;
        let mut dangling = link_request(&bucket, "missing.bin", "versions/missing.bin");
        dangling.allow_dangling = true;
        persistence.put_object_link(dangling).await.unwrap();

        let err = persistence
            .resolve_object_link_target(bucket.id, "missing.bin")
            .await
            .unwrap_err();
        assert!(matches!(err, ObjectLinkError::DanglingObjectLink));
    }

    #[tokio::test]
    async fn followed_link_etag_changes_with_link_generation() {
        let (_temp, persistence, bucket) = seeded().await;
        let created = persistence
            .put_object_link(link_request(&bucket, "latest.bin", "versions/app-v1.bin"))
            .await
            .unwrap();
        let target_v1 = persistence
            .get_object(bucket.id, "versions/app-v1.bin")
            .await
            .unwrap()
            .unwrap();
        let etag_v1 = followed_link_etag(&created.link, &target_v1).unwrap();

        let mut update = link_request(&bucket, "latest.bin", "versions/app-v2.bin");
        update.create_only = false;
        update.expected_generation = Some(created.descriptor.generation);
        let updated = persistence.put_object_link(update).await.unwrap();
        let target_v2 = persistence
            .get_object(bucket.id, "versions/app-v2.bin")
            .await
            .unwrap()
            .unwrap();
        let etag_v2 = followed_link_etag(&updated.link, &target_v2).unwrap();

        assert_ne!(etag_v1, etag_v2);
    }
}
