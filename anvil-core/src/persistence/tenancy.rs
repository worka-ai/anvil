use super::*;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct ActiveIndexPolicySnapshotProto {
    #[prost(message, repeated, tag = "1")]
    definitions: Vec<ActiveIndexPolicyDefinitionProto>,
}

#[derive(Clone, PartialEq, Message)]
struct ActiveIndexPolicyDefinitionProto {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    kind: String,
    #[prost(int64, tag = "3")]
    version: i64,
}

impl Persistence {
    pub async fn get_tenant_by_name(&self, name: &str) -> Result<Option<Tenant>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .tenant_by_name(name))
    }

    pub async fn list_tenants(&self) -> Result<Vec<Tenant>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .tenants())
    }

    pub async fn get_app_by_client_id(&self, client_id: &str) -> Result<Option<AppDetails>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_details_by_client_id(client_id))
    }

    pub async fn create_tenant(&self, name: &str, idempotency_key: &str) -> Result<Tenant> {
        let permit = self.control_write_permit().await?;
        let tenant = control_journal::create_tenant_with_permit(
            &self.storage,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.write_mesh_tenant_locators(&tenant, idempotency_key)
            .await?;
        Ok(tenant)
    }

    pub async fn create_app(
        &self,
        tenant_id: i64,
        name: &str,
        client_id: &str,
        encrypted_secret: &[u8],
    ) -> Result<App> {
        let permit = self.control_write_permit().await?;
        control_journal::create_app_with_permit(
            &self.storage,
            tenant_id,
            name,
            client_id,
            encrypted_secret,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn get_app_by_id(&self, id: i64) -> Result<Option<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_by_id(id))
    }

    pub async fn get_app_by_name(&self, name: &str) -> Result<Option<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .app_by_name(name))
    }

    pub async fn list_apps_for_tenant(&self, tenant_id: i64) -> Result<Vec<App>> {
        Ok(control_journal::read_control_state(&self.storage)
            .await?
            .apps_for_tenant(tenant_id))
    }

    pub async fn update_app_secret(&self, app_id: i64, new_encrypted_secret: &[u8]) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::update_app_secret_with_permit(
            &self.storage,
            app_id,
            new_encrypted_secret,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn delete_app(&self, app_id: i64) -> Result<()> {
        let permit = self.control_write_permit().await?;
        control_journal::delete_app_with_permit(
            &self.storage,
            app_id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn create_bucket(
        &self,
        tenant_id: i64,
        name: &str,
        region: &str,
    ) -> Result<Bucket, tonic::Status> {
        let total_start = std::time::Instant::now();
        let step_start = std::time::Instant::now();
        crate::mesh_lifecycle::ensure_new_writable_placement(
            &self.storage,
            region,
            &self.cell_id,
            &self.owner_node_id,
        )
        .await
        .map_err(|err| tonic::Status::failed_precondition(err.to_string()))?;
        crate::emit_test_timing(
            "persistence.create_bucket ensure_new_writable_placement",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        if bucket_journal::read_current_bucket(&self.storage, tenant_id, name)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?
            .is_some()
        {
            return Err(tonic::Status::already_exists(
                "A bucket with that name already exists.",
            ));
        }
        crate::emit_test_timing(
            "persistence.create_bucket read_current_bucket",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let bucket = Bucket {
            id: bucket_journal::next_bucket_id(&self.storage)
                .await
                .map_err(|e| tonic::Status::internal(e.to_string()))?,
            tenant_id,
            name: name.to_string(),
            region: region.to_string(),
            created_at: Utc::now(),
            is_public_read: false,
        };
        crate::emit_test_timing(
            "persistence.create_bucket next_bucket_id",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let tenant_permit = self
            .bucket_tenant_write_permit(tenant_id)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "persistence.create_bucket tenant_write_permit",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        let global_permit = self
            .bucket_global_write_permit()
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "persistence.create_bucket global_write_permit",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &bucket,
            BucketJournalMutation::Create,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
        )
        .await
        .map_err(|e| tonic::Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "persistence.create_bucket append_bucket_mutation",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        self.write_mesh_bucket_locator(&bucket)
            .await
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        crate::emit_test_timing(
            "persistence.create_bucket write_mesh_bucket_locator",
            step_start.elapsed(),
        );
        let step_start = std::time::Instant::now();
        self.cache
            .insert_bucket(tenant_id, name.to_string(), bucket.clone())
            .await;
        self.publish_event(MetadataEvent::BucketUpdated {
            tenant_id,
            name: name.to_string(),
        })
        .await;
        crate::emit_test_timing(
            "persistence.create_bucket cache_and_publish",
            step_start.elapsed(),
        );
        crate::emit_test_timing("persistence.create_bucket total", total_start.elapsed());
        Ok(bucket)
    }

    pub async fn get_bucket_by_name(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        if let Some(bucket) = self.cache.get_bucket(tenant_id, name).await {
            return Ok(Some(bucket));
        }
        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, name).await?;
        if let Some(bucket) = bucket.clone() {
            self.cache
                .insert_bucket(tenant_id, name.to_string(), bucket)
                .await;
        }
        Ok(bucket)
    }

    pub async fn set_bucket_public_access(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        is_public: bool,
    ) -> Result<Bucket> {
        let mut out = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        out.is_public_read = is_public;
        let tenant_permit = self.bucket_tenant_write_permit(out.tenant_id).await?;
        let global_permit = self.bucket_global_write_permit().await?;
        bucket_journal::append_bucket_mutation_with_permits(
            &self.storage,
            &out,
            BucketJournalMutation::Update,
            &tenant_permit,
            &global_permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.cache.invalidate_bucket(tenant_id, bucket_name).await;
        Ok(out)
    }

    pub async fn soft_delete_bucket(&self, tenant_id: i64, name: &str) -> Result<Option<Bucket>> {
        let deleted = bucket_journal::read_current_bucket(&self.storage, tenant_id, name).await?;
        if let Some(bucket) = &deleted {
            let tenant_permit = self.bucket_tenant_write_permit(bucket.tenant_id).await?;
            let global_permit = self.bucket_global_write_permit().await?;
            bucket_journal::append_bucket_mutation_with_permits(
                &self.storage,
                bucket,
                BucketJournalMutation::Delete,
                &tenant_permit,
                &global_permit,
                &self.partition_owner_signing_key,
            )
            .await?;
            self.mark_mesh_bucket_locator_deleted(bucket).await?;
        }
        self.cache.invalidate_bucket(tenant_id, name).await;
        Ok(deleted)
    }

    pub async fn bucket_has_retained_objects_or_uploads(&self, bucket_id: i64) -> Result<bool> {
        let has_objects = if let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        {
            !metadata_journal::read_object_versions(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
                "",
                "",
                None,
                1,
            )
            .await?
            .versions
            .is_empty()
        } else {
            false
        };
        if has_objects {
            return Ok(true);
        }
        multipart_journal::has_active_multipart_upload(&self.storage, bucket_id).await
    }

    pub async fn hard_delete_bucket_if_empty(&self, bucket_id: i64) -> Result<bool> {
        if self
            .bucket_has_retained_objects_or_uploads(bucket_id)
            .await?
        {
            return Ok(false);
        }
        Ok(true)
    }

    pub async fn create_bucket_metadata_event(
        &self,
        tenant_id: i64,
        bucket: &Bucket,
        event_type: &str,
        bucket_metadata: JsonValue,
    ) -> Result<BucketMetadataEvent> {
        bucket_journal::latest_bucket_metadata_event(&self.storage, tenant_id, &bucket.name)
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "bucket metadata event not found after {event_type}: {}",
                    bucket_metadata
                )
            })
    }

    pub async fn list_bucket_metadata_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<BucketMetadataEvent>> {
        bucket_journal::list_bucket_metadata_events_by_bucket_id(
            &self.storage,
            tenant_id,
            bucket_id,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }

    pub async fn list_buckets_for_tenant(&self, tenant_id: i64) -> Result<Vec<Bucket>> {
        let mut buckets = bucket_journal::read_current_buckets(&self.storage, tenant_id).await?;
        buckets.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(buckets)
    }

    pub async fn active_index_policy_snapshot_hash(
        &self,
        tenant_id: i64,
        bucket_id: i64,
    ) -> Result<String> {
        let defs = index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            false,
        )
        .await?;
        let snapshot = ActiveIndexPolicySnapshotProto {
            definitions: defs
                .iter()
                .map(|definition| ActiveIndexPolicyDefinitionProto {
                    name: definition.name.clone(),
                    kind: definition.kind.clone(),
                    version: definition.version,
                })
                .collect(),
        };
        Ok(
            blake3::hash(&crate::core_store::encode_deterministic_proto(&snapshot))
                .to_hex()
                .to_string(),
        )
    }

    pub async fn latest_authz_revision(&self, tenant_id: i64) -> Result<i64> {
        authz_journal::latest_authz_revision(&self.storage, tenant_id).await
    }
}
