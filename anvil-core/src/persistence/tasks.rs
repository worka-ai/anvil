use super::*;
use anyhow::Context;

const AUTHZ_MATERIALIZATION_DERIVED_INDEX_KIND: &str = "userset";

impl Persistence {
    pub async fn hard_delete_object(&self, _object_id: i64) -> Result<()> {
        // Object metadata is append-only in the native journal. Physical shard cleanup
        // must not erase the metadata history needed for watches, indexes, and audit.
        Ok(())
    }

    pub async fn enqueue_task(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<()> {
        let permit = self.task_queue_write_permit().await?;
        task_journal::enqueue_task_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        self.notify_task_enqueued();
        Ok(())
    }

    pub async fn enqueue_task_if_absent(
        &self,
        task_type: crate::tasks::TaskType,
        payload: JsonValue,
        priority: i32,
    ) -> Result<bool> {
        let permit = self.task_queue_write_permit().await?;
        let enqueued = task_journal::enqueue_task_if_absent_with_permit(
            &self.storage,
            task_type,
            payload,
            priority,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await?;
        if enqueued {
            self.notify_task_enqueued();
        }
        Ok(enqueued)
    }

    pub(super) async fn enqueue_index_build_task(
        &self,
        payload: JsonValue,
        priority: i32,
    ) -> Result<bool> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::enqueue_index_build_task_with_permit(
                &self.storage,
                payload.clone(),
                priority,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(result) => {
                    if result {
                        self.notify_task_enqueued();
                    }
                    return Ok(result);
                }
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("index build task enqueue retry exhausted")))
    }

    pub(crate) async fn enqueue_authz_materialization(
        &self,
        tenant_id: i64,
        target_revision: u64,
    ) -> Result<bool> {
        let payload = serde_json::json!({
            "tenant_id": tenant_id,
            "target_revision": target_revision,
        });
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::enqueue_authz_materialization_task_with_permit(
                &self.storage,
                payload.clone(),
                30,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(result) => {
                    if result {
                        self.notify_task_enqueued();
                    }
                    return Ok(result);
                }
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("authz materialization enqueue retry exhausted")))
    }

    pub(crate) async fn run_authz_materialization_task(
        &self,
        tenant_id: i64,
        requested_revision: u64,
    ) -> Result<authz_journal::AuthzMaterializationOutcome> {
        let latest_revision =
            authz_journal::latest_authz_tuple_revision(&self.storage, tenant_id).await?;
        let latest_revision = u64::try_from(latest_revision.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        let target_revision = requested_revision.max(latest_revision);
        let source_fence_token =
            authz_journal::latest_authz_journal_fence_token(&self.storage, tenant_id).await?;

        let outcome = authz_journal::materialize_authz_derived_state_at_revision(
            &self.storage,
            tenant_id,
            target_revision,
            source_fence_token,
        )
        .await?;

        let latest_after =
            authz_journal::latest_authz_tuple_revision(&self.storage, tenant_id).await?;
        let latest_after = u64::try_from(latest_after.max(0))
            .context("authorization tuple revision exceeds supported range")?;
        append_authz_materialization_lag_watch(&self.storage, tenant_id, latest_after, &outcome)
            .await?;
        if latest_after > outcome.processed_revision {
            self.enqueue_authz_materialization(tenant_id, latest_after)
                .await?;
        }

        Ok(outcome)
    }

    pub async fn acquire_task_execution_lease(
        &self,
        task: &TaskRecord,
    ) -> Result<task_lease::TaskLease> {
        let target = self.task_lease_target(task).await?;
        let now_nanos = current_time_nanos()?;
        let ttl_nanos = self.task_lease_ttl_nanos()?;
        task_lease::acquire_task_lease(
            &self.storage,
            task_lease::TaskLeaseAcquire {
                task_id: task_lease_id(task.id)?,
                task_kind: task.task_type.as_str().to_string(),
                partition_family: target.partition_family,
                partition_id: target.partition_id,
                owner: task_lease::TaskLeaseOwner::node(self.owner_node_id.clone()),
                source_cursor: target.source_cursor,
                now_nanos,
                ttl_nanos,
            },
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn checkpoint_task_execution_lease(
        &self,
        lease: &task_lease::TaskLease,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            &lease.task_id,
            &task_lease::TaskLeaseOwner::node(self.owner_node_id.clone()),
            lease.fence_token,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn acquire_named_task_lease(
        &self,
        request: task_lease::TaskLeaseAcquire,
    ) -> Result<task_lease::TaskLease> {
        task_lease::acquire_task_lease(&self.storage, request, &self.partition_owner_signing_key)
            .await
    }

    pub async fn checkpoint_named_task_lease(
        &self,
        task_id: &str,
        owner: &task_lease::TaskLeaseOwner,
        fence_token: u64,
        checkpoint_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::checkpoint_task_lease(
            &self.storage,
            task_id,
            owner,
            fence_token,
            checkpoint_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn commit_named_task_lease(
        &self,
        task_id: &str,
        owner: &task_lease::TaskLeaseOwner,
        fence_token: u64,
        committed_cursor: u128,
    ) -> Result<task_lease::TaskLease> {
        task_lease::commit_task_lease(
            &self.storage,
            task_id,
            owner,
            fence_token,
            committed_cursor,
            current_time_nanos()?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn force_release_named_task_lease(
        &self,
        tenant_id: i64,
        task_id: &str,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::force_release_task_lease(
            &self.storage,
            tenant_id,
            task_id,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn read_task_execution_lease(
        &self,
        task_id: i64,
    ) -> Result<Option<task_lease::TaskLease>> {
        task_lease::read_task_lease(
            &self.storage,
            0,
            &task_lease_id(task_id)?,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub(super) async fn task_lease_target(&self, task: &TaskRecord) -> Result<TaskLeaseTarget> {
        match task.task_type {
            crate::tasks::TaskType::ObjectMetadataCompaction => {
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let bucket = bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id)
                    .await?
                    .ok_or_else(|| anyhow!("object metadata compaction bucket not found"))?;
                let stats = metadata_journal::active_object_journal_stats(
                    &self.storage,
                    &bucket,
                    &self.partition_owner_signing_key,
                )
                .await?;
                Ok(TaskLeaseTarget {
                    partition_family: "object_metadata".to_string(),
                    partition_id: hex::encode(metadata_journal::object_metadata_partition_id(
                        bucket.tenant_id,
                        bucket.id,
                    )),
                    source_cursor: u128::from(stats.last_sequence),
                })
            }
            crate::tasks::TaskType::IndexBuild => {
                let tenant_id = task_payload_i64(task, "tenant_id")?;
                let bucket_id = task_payload_i64(task, "bucket_id")?;
                let index_id = task_payload_i64(task, "index_id")?;
                let source_cursor = task_payload_u128(task, "source_cursor")?;
                Ok(TaskLeaseTarget {
                    partition_family: "index".to_string(),
                    partition_id: hex::encode(crate::formats::hash32(
                        format!("tenant/{tenant_id}/bucket/{bucket_id}/index/{index_id}")
                            .as_bytes(),
                    )),
                    source_cursor,
                })
            }
            crate::tasks::TaskType::AuthzMaterialization => {
                let tenant_id = task_payload_i64(task, "tenant_id")?;
                let source_cursor = task_payload_u128(task, "target_revision")?;
                Ok(TaskLeaseTarget {
                    partition_family: "authz_materialization".to_string(),
                    partition_id: hex::encode(crate::formats::hash32(
                        format!("tenant/{tenant_id}/authz").as_bytes(),
                    )),
                    source_cursor,
                })
            }
            _ => Ok(TaskLeaseTarget {
                partition_family: "task_queue".to_string(),
                partition_id: hex::encode(task_journal::task_queue_partition_id()),
                source_cursor: task.id.max(0) as u128,
            }),
        }
    }

    pub(super) fn task_lease_ttl_nanos(&self) -> Result<i64> {
        if self.task_lease_ttl_secs == 0 {
            return Err(anyhow!("task lease ttl must be nonzero"));
        }
        let ttl = self
            .task_lease_ttl_secs
            .checked_mul(1_000_000_000)
            .ok_or_else(|| anyhow!("task lease ttl overflow"))?;
        i64::try_from(ttl).map_err(|_| anyhow!("task lease ttl cannot fit i64 nanoseconds"))
    }

    pub async fn claim_pending_tasks(&self, limit: i64) -> Result<Vec<TaskRecord>> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::claim_pending_tasks_with_permit(
                &self.storage,
                limit,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(tasks) => return Ok(tasks),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task claim retry exhausted")))
    }

    pub async fn has_due_task_work(&self) -> Result<bool> {
        task_journal::has_due_tasks(&self.storage).await
    }

    pub async fn list_tasks_page(
        &self,
        after_tuple_key: Option<&[u8]>,
        page_size: usize,
    ) -> Result<TaskPage> {
        task_journal::list_tasks_page(&self.storage, after_tuple_key, page_size).await
    }

    pub async fn update_task_status(
        &self,
        task_id: i64,
        status: crate::tasks::TaskStatus,
    ) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::update_task_status_with_permit(
                &self.storage,
                task_id,
                status,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task status update retry exhausted")))
    }

    pub async fn fail_task(&self, task_id: i64, error: &str) -> Result<()> {
        let mut last_error = None;
        for _ in 0..5 {
            let permit = match self.task_queue_write_permit().await {
                Ok(permit) => permit,
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match task_journal::fail_task_with_permit(
                &self.storage,
                task_id,
                error,
                &permit,
                &self.partition_owner_signing_key,
            )
            .await
            {
                Ok(()) => return Ok(()),
                Err(error) if is_retryable_partition_fence_error(&error) => {
                    last_error = Some(error);
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("task failure update retry exhausted")))
    }

    pub async fn hf_create_key(
        &self,
        tenant_id: i64,
        name: &str,
        token_encrypted: &[u8],
        note: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::create_key_with_permit(
            &self.storage,
            tenant_id,
            name,
            token_encrypted,
            note,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_delete_key(&self, tenant_id: i64, name: &str) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::delete_key_with_permit(
            &self.storage,
            tenant_id,
            name,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_key_encrypted(
        &self,
        tenant_id: i64,
        name: &str,
    ) -> Result<Option<(i64, Vec<u8>)>> {
        hf_journal::get_key_encrypted(&self.storage, tenant_id, name).await
    }

    pub async fn hf_get_key_encrypted_by_id(
        &self,
        tenant_id: i64,
        id: i64,
    ) -> Result<Option<Vec<u8>>> {
        hf_journal::get_key_encrypted_by_id(&self.storage, tenant_id, id).await
    }

    pub(crate) async fn hf_list_encrypted_key_page(
        &self,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfKeyPage> {
        hf_journal::list_encrypted_key_page(&self.storage, after_cursor, limit).await
    }

    pub async fn hf_update_key_encrypted(&self, id: i64, token_encrypted: &[u8]) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_key_encrypted_with_permit(
            &self.storage,
            id,
            token_encrypted,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_list_key_page(
        &self,
        tenant_id: i64,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfKeyPage> {
        hf_journal::list_key_page(&self.storage, tenant_id, after_cursor, limit).await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn hf_create_ingestion(
        &self,
        key_id: i64,
        tenant_id: i64,
        requester_app_id: i64,
        repo: &str,
        revision: Option<&str>,
        target_bucket: &str,
        target_region: &str,
        target_prefix: Option<&str>,
        include_globs: &[String],
        exclude_globs: &[String],
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::create_ingestion_with_permit(
            &self.storage,
            key_id,
            tenant_id,
            requester_app_id,
            repo,
            revision,
            target_bucket,
            target_region,
            target_prefix,
            include_globs,
            exclude_globs,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_get_ingestion_job(&self, id: i64) -> Result<Option<HfIngestionJob>> {
        hf_journal::get_ingestion_job(&self.storage, id).await
    }

    pub async fn hf_update_ingestion_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_ingestion_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_cancel_ingestion(&self, id: i64) -> Result<u64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::cancel_ingestion_with_permit(
            &self.storage,
            id,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_add_item(
        &self,
        ingestion_id: i64,
        path: &str,
        size: Option<i64>,
        etag: Option<&str>,
    ) -> Result<i64> {
        let permit = self.hf_write_permit().await?;
        hf_journal::add_item_with_permit(
            &self.storage,
            ingestion_id,
            path,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_state(
        &self,
        id: i64,
        state_value: crate::tasks::HFIngestionItemState,
        error: Option<&str>,
    ) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_state_with_permit(
            &self.storage,
            id,
            state_value,
            error,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_update_item_success(&self, id: i64, size: i64, etag: &str) -> Result<()> {
        let permit = self.hf_write_permit().await?;
        hf_journal::update_item_success_with_permit(
            &self.storage,
            id,
            size,
            etag,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn hf_list_stored_ingestion_item_page(
        &self,
        ingestion_id: i64,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfStoredItemPage> {
        hf_journal::list_stored_ingestion_item_page(
            &self.storage,
            ingestion_id,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn hf_list_stored_target_item_page(
        &self,
        tenant_id: i64,
        bucket: &str,
        prefix: &str,
        after_cursor: Option<&[u8]>,
        limit: usize,
    ) -> Result<hf_journal::HfStoredItemPage> {
        hf_journal::list_stored_target_item_page(
            &self.storage,
            tenant_id,
            bucket,
            prefix,
            after_cursor,
            limit,
        )
        .await
    }

    pub async fn hf_get_ingestion_status(&self, id: i64) -> Result<hf_journal::HfIngestionStatus> {
        hf_journal::get_ingestion_status(&self.storage, id).await
    }
}

async fn append_authz_materialization_lag_watch(
    storage: &Storage,
    tenant_id: i64,
    latest_revision: u64,
    outcome: &authz_journal::AuthzMaterializationOutcome,
) -> Result<()> {
    let derived_index_id = crate::authz_userset_index::DEFAULT_DERIVED_USERSET_INDEX_ID.to_string();
    if let Some(latest_event) =
        crate::authz_derived_lag_watch::latest_authz_derived_lag_watch_event(
            storage,
            tenant_id,
            &derived_index_id,
        )
        .await?
        && latest_event.payload.processed_revision >= outcome.processed_revision
    {
        return Ok(());
    }
    crate::authz_derived_lag_watch::append_authz_derived_lag_watch_record(
        storage,
        tenant_id,
        authz_materialization_mutation_id(
            tenant_id,
            outcome.processed_revision,
            &outcome.source_records_hash,
        ),
        crate::authz_derived_lag_watch::AuthzDerivedLagWatchPayload {
            derived_index_id,
            derived_index_kind: AUTHZ_MATERIALIZATION_DERIVED_INDEX_KIND.to_string(),
            processed_revision: outcome.processed_revision,
            latest_revision,
            source_cursor: u128::from(outcome.processed_revision),
            source_manifest_hash: outcome.source_records_hash.clone(),
            generation: outcome.generation,
            emitted_at: Utc::now().to_rfc3339(),
        },
    )
    .await
}

fn authz_materialization_mutation_id(
    tenant_id: i64,
    processed_revision: u64,
    source_records_hash: &str,
) -> [u8; 16] {
    let hash = crate::formats::hash32(
        format!("authz-materialization:{tenant_id}:{processed_revision}:{source_records_hash}")
            .as_bytes(),
    );
    let mut mutation_id = [0; 16];
    mutation_id.copy_from_slice(&hash[..16]);
    mutation_id
}
