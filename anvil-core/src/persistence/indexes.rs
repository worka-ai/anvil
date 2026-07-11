use super::*;

impl Persistence {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
        kind: &str,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: &str,
        build_policy: JsonValue,
    ) -> Result<IndexDefinition> {
        let now = Utc::now();
        Ok(IndexDefinition {
            id: index_journal::next_index_definition_id(&self.storage, tenant_id, bucket_id)
                .await?,
            tenant_id,
            bucket_id,
            name: name.to_string(),
            kind: kind.to_string(),
            selector,
            extractor,
            authorization_mode: authorization_mode.to_string(),
            build_policy,
            enabled: true,
            version: 1,
            created_at: now,
            updated_at: now,
        })
    }

    pub async fn update_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
        selector: JsonValue,
        extractor: JsonValue,
        authorization_mode: &str,
        build_policy: JsonValue,
    ) -> Result<Option<IndexDefinition>> {
        let Some(mut index) =
            index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
                .await?
        else {
            return Ok(None);
        };
        index.selector = selector;
        index.extractor = extractor;
        index.authorization_mode = authorization_mode.to_string();
        index.build_policy = build_policy;
        index.version += 1;
        index.updated_at = Utc::now();
        Ok(Some(index))
    }

    pub async fn get_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
            .await
    }

    pub async fn disable_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        let Some(mut index) =
            index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
                .await?
        else {
            return Ok(None);
        };
        index.enabled = false;
        index.version += 1;
        index.updated_at = Utc::now();
        Ok(Some(index))
    }

    pub async fn drop_index_definition(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        name: &str,
    ) -> Result<Option<IndexDefinition>> {
        index_journal::read_current_index_definition(&self.storage, tenant_id, bucket_id, name)
            .await
    }

    pub async fn list_index_definitions(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        include_disabled: bool,
    ) -> Result<Vec<IndexDefinition>> {
        index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            include_disabled,
        )
        .await
    }

    pub async fn create_index_definition_event(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index: &IndexDefinition,
        event_type: &str,
    ) -> Result<IndexDefinitionEvent> {
        self.create_index_definition_event_with_transaction(
            tenant_id,
            bucket_id,
            bucket_name,
            index,
            event_type,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_definition_event_with_transaction(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index: &IndexDefinition,
        event_type: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<IndexDefinitionEvent> {
        let event = IndexDefinitionEvent {
            id: index_journal::read_index_definition_events(
                &self.storage,
                tenant_id,
                bucket_id,
                0,
                0,
            )
            .await?
            .into_iter()
            .map(|event| event.id)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or_else(|| anyhow!("index definition cursor overflow"))?,
            tenant_id,
            bucket_id,
            bucket_name: bucket_name.to_string(),
            index_id: index.id,
            index_name: index.name.clone(),
            event_type: event_type.to_string(),
            index_version: index.version,
            mutation_id: uuid::Uuid::new_v4(),
            definition: serde_json::json!({
                "index_id": index.id,
                "bucket_name": bucket_name,
                "name": index.name,
                "kind": index.kind,
                "selector_json": index.selector.to_string(),
                "extractor_json": index.extractor.to_string(),
                "authorization_mode": index.authorization_mode,
                "build_policy_json": index.build_policy.to_string(),
                "enabled": index.enabled,
                "version": index.version,
                "created_at": index.created_at.to_rfc3339(),
                "updated_at": index.updated_at.to_rfc3339(),
            }),
            created_at: Utc::now(),
        };
        let permit = self
            .index_definition_write_permit(tenant_id, bucket_id)
            .await?;
        index_journal::append_index_definition_event_with_permit_in_transaction(
            &self.storage,
            &event,
            &permit,
            &self.partition_owner_signing_key,
            transaction_id,
            transaction_principal,
        )
        .await?;
        Ok(event)
    }

    pub async fn list_index_definition_events(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<IndexDefinitionEvent>> {
        index_journal::read_index_definition_events(
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

    pub async fn enqueue_index_build_for_index(
        &self,
        bucket: &Bucket,
        index: &IndexDefinition,
    ) -> Result<bool> {
        if !index.enabled
            || !matches!(
                index.kind.as_str(),
                "path" | "metadata_filter" | "full_text" | "vector" | "hybrid" | "typed_json"
            )
        {
            return Ok(false);
        }
        let typed_json_source_kind = if index.kind == "typed_json" {
            index
                .build_policy
                .get("source_kind")
                .or_else(|| index.build_policy.get("source"))
                .and_then(JsonValue::as_str)
                .unwrap_or("object_current")
        } else {
            "object_current"
        };
        let source_cursor = if index.kind == "typed_json"
            && typed_json_source_kind == "append_record"
        {
            append_journal::append_record_source_cursor(&self.storage, bucket.tenant_id, bucket.id)
                .await?
        } else {
            let stats = metadata_journal::active_object_journal_stats(
                &self.storage,
                bucket,
                &self.partition_owner_signing_key,
            )
            .await?;
            index_repair::source_cursor_from_stats(stats)
        };
        if source_cursor == 0 {
            return Ok(false);
        }
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let checkpoint = watch_checkpoint::read_watch_checkpoint(
            &self.storage,
            "object_metadata",
            &index_storage_id,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_manifest_hash =
            if index.kind == "typed_json" && typed_json_source_kind == "append_record" {
                blake3::hash(
                    format!(
                        "append_record:{}:{}:{}",
                        bucket.tenant_id, bucket.id, source_cursor
                    )
                    .as_bytes(),
                )
                .to_hex()
                .to_string()
            } else {
                metadata_journal::object_metadata_source_checkpoint_hash(
                    &self.storage,
                    bucket,
                    &self.partition_owner_signing_key,
                    source_cursor,
                )
                .await?
            };
        let latest_proof = crate::derived_index_proof::read_latest_derived_index_proof(
            &self.storage,
            &index_storage_id,
            &self.partition_owner_signing_key,
        )
        .await
        .ok()
        .flatten();
        let catch_up_plan = crate::derived_index_catchup::plan_derived_index_catch_up(
            crate::derived_index_catchup::DerivedIndexCatchUpInput {
                index_id: index_storage_id.clone(),
                consumer_id: index_storage_id.clone(),
                watch_stream_id: "object_metadata".to_string(),
                checkpoint_cursor: checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.cursor)
                    .unwrap_or(0),
                retained_start_cursor: 0,
                latest_cursor: source_cursor,
                manifest_checkpoint_cursor: 0,
                source_manifest_hash: source_manifest_hash.clone(),
                required_source_cursor: source_cursor,
                min_generation: index.version.max(1) as u64,
                latest_proof,
            },
            &self.partition_owner_signing_key,
        )?;
        if matches!(
            catch_up_plan,
            crate::derived_index_catchup::DerivedIndexCatchUpPlan::UpToDate { .. }
        ) {
            return Ok(false);
        }
        self.enqueue_index_build_task(
            serde_json::json!({
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "index_id": index.id,
                "index_version": index.version,
                "source_cursor": source_cursor,
                "source_manifest_hash": source_manifest_hash,
                "catch_up_plan": catch_up_plan,
            }),
            40,
        )
        .await
    }

    pub async fn enqueue_index_builds_for_bucket(&self, bucket: &Bucket) -> Result<usize> {
        let indexes = index_journal::read_current_index_definitions(
            &self.storage,
            bucket.tenant_id,
            bucket.id,
            false,
        )
        .await?;
        let mut scheduled = 0usize;
        for index in indexes {
            if self.enqueue_index_build_for_index(bucket, &index).await? {
                scheduled = scheduled.saturating_add(1);
            }
        }
        Ok(scheduled)
    }

    pub async fn build_index_task(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_id: i64,
        index_version: i64,
        source_cursor: u128,
    ) -> Result<Option<index_builder::IndexBuildOutcome>> {
        let Some(bucket) =
            bucket_journal::read_current_bucket_by_id(&self.storage, bucket_id).await?
        else {
            return Ok(None);
        };
        if bucket.tenant_id != tenant_id {
            return Err(anyhow!("index build bucket tenant mismatch"));
        }
        let Some(index) = index_journal::read_current_index_definitions(
            &self.storage,
            tenant_id,
            bucket_id,
            true,
        )
        .await?
        .into_iter()
        .find(|index| index.id == index_id) else {
            return Ok(None);
        };
        if !index.enabled || index.version != index_version {
            return Ok(None);
        }
        let index_storage_id = index_journal::index_storage_id(tenant_id, bucket_id, index.id);
        self.ensure_index_build_ownership_fence(tenant_id, bucket_id, &index_storage_id)
            .await?;
        let outcome = match index.kind.as_str() {
            "path" | "metadata_filter" => {
                index_builder::build_metadata_backed_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            "full_text" => {
                index_builder::build_full_text_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            "vector" => {
                index_builder::build_vector_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                    &self.embedding_providers,
                )
                .await?
            }
            "hybrid" => {
                index_builder::build_hybrid_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                    &self.embedding_providers,
                )
                .await?
            }
            "typed_json" => {
                index_builder::build_typed_json_index(
                    &self.storage,
                    &bucket,
                    &index,
                    &self.partition_owner_signing_key,
                    source_cursor,
                    &self.owner_node_id,
                )
                .await?
            }
            _ => return Ok(None),
        };
        for diagnostic in &outcome.diagnostics {
            self.create_index_diagnostic(
                tenant_id,
                bucket_id,
                &bucket.name,
                Some(index.id),
                &index.name,
                &diagnostic.object_key,
                diagnostic.version_id,
                &diagnostic.severity,
                &diagnostic.code,
                &diagnostic.message,
                diagnostic.details.clone(),
            )
            .await?;
        }
        Ok(Some(outcome))
    }

    pub async fn repair_index_from_base_journal(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        index_name: &str,
        rebuild: bool,
    ) -> Result<index_repair::IndexRepairReport> {
        let bucket = self
            .get_bucket_by_name(tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        let index = self
            .get_index_definition(tenant_id, bucket.id, index_name)
            .await?
            .filter(|index| index.enabled)
            .ok_or_else(|| anyhow!("index definition not found"))?;
        if !matches!(
            index.kind.as_str(),
            "path" | "metadata_filter" | "full_text" | "vector" | "hybrid" | "typed_json"
        ) {
            return Err(anyhow!(
                "index kind does not have a repairable derived index"
            ));
        }

        let stats = metadata_journal::active_object_journal_stats(
            &self.storage,
            &bucket,
            &self.partition_owner_signing_key,
        )
        .await?;
        let source_cursor = index_repair::source_cursor_from_stats(stats);
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let source_manifest_hash = if source_cursor == 0 {
            String::new()
        } else {
            metadata_journal::object_metadata_source_checkpoint_hash(
                &self.storage,
                &bucket,
                &self.partition_owner_signing_key,
                source_cursor,
            )
            .await?
        };

        let mut status = index_repair::assess_derived_index(
            &self.storage,
            &index,
            &index_storage_id,
            source_cursor,
            &source_manifest_hash,
            &self.partition_owner_signing_key,
        )
        .await?;
        let mut build = None;
        let mut finding = None;

        if let index_repair::IndexRepairStatus::NeedsRepair(reason) = status.clone() {
            let permit = self
                .object_metadata_write_permit(bucket.tenant_id, bucket.id)
                .await?;
            if rebuild {
                build = self
                    .build_index_task(tenant_id, bucket.id, index.id, index.version, source_cursor)
                    .await?;
                status = index_repair::IndexRepairStatus::Rebuilt(reason.clone());
            }

            let finding_status = if rebuild {
                repair_finding::RepairFindingStatus::RebuiltDerivedIndex
            } else {
                repair_finding::RepairFindingStatus::Open
            };
            let write = index_repair::repair_finding_write(
                &bucket,
                &index,
                &index_storage_id,
                source_cursor,
                &source_manifest_hash,
                &reason,
                finding_status,
                permit.fence_token,
            )?;
            finding = Some(
                repair_finding::write_repair_finding(
                    &self.storage,
                    write,
                    &self.partition_owner_signing_key,
                )
                .await?,
            );
        }

        Ok(index_repair::IndexRepairReport {
            status,
            bucket_name: bucket.name,
            index_name: index.name,
            index_storage_id,
            source_cursor,
            finding,
            build,
        })
    }

    pub async fn repair_directory_index(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        rebuild: bool,
    ) -> Result<directory_repair::DirectoryIndexRepairReport> {
        let bucket = self
            .get_bucket_by_name(tenant_id, bucket_name)
            .await?
            .ok_or_else(|| anyhow!("bucket not found"))?;
        let permit = self
            .object_metadata_write_permit(bucket.tenant_id, bucket.id)
            .await?;
        directory_repair::repair_directory_index(
            &self.storage,
            &bucket,
            rebuild,
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_repair_findings(
        &self,
        scope_kind: &str,
        scope_id: &str,
        limit: usize,
    ) -> Result<Vec<repair_finding::RepairFinding>> {
        let mut findings = repair_finding::list_repair_findings(
            &self.storage,
            scope_kind,
            scope_id,
            &self.partition_owner_signing_key,
        )
        .await?;
        if limit > 0 && findings.len() > limit {
            findings.truncate(limit);
        }
        Ok(findings)
    }

    pub async fn repair_authz_derived_userset_index(
        &self,
        tenant_id: i64,
        derived_index_id: &str,
        rebuild: bool,
    ) -> Result<authz_repair::AuthzDerivedIndexRepairReport> {
        let permit = self.authz_write_permit(tenant_id).await?;
        authz_repair::repair_authz_derived_userset_index(
            &self.storage,
            tenant_id,
            derived_index_id,
            rebuild,
            permit.fence_token,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn repair_personaldb_log_chain(
        &self,
        tenant_id: i64,
        database_id: &str,
    ) -> Result<personaldb_repair::PersonalDbLogChainRepairReport> {
        let scope_id = format!("tenant-{tenant_id}-database-{database_id}");
        let permit = self.repair_write_permit("personaldb", &scope_id).await?;
        personaldb_repair::repair_personaldb_log_chain(
            &self.storage,
            tenant_id,
            database_id,
            permit.fence_token,
            &self.personaldb_signing_key,
            &self.partition_owner_signing_key,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn create_index_diagnostic(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        bucket_name: &str,
        index_id: Option<i64>,
        index_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        severity: &str,
        code: &str,
        message: &str,
        details: JsonValue,
    ) -> Result<IndexDiagnostic> {
        let permit = self
            .index_diagnostic_write_permit(tenant_id, bucket_id)
            .await?;
        index_diagnostic_journal::write_index_diagnostic_with_permit(
            &self.storage,
            IndexDiagnostic {
                id: 0,
                tenant_id,
                bucket_id,
                bucket_name: bucket_name.to_string(),
                index_id,
                index_name: index_name.to_string(),
                object_key: object_key.to_string(),
                version_id,
                severity: severity.to_string(),
                code: code.to_string(),
                message: message.to_string(),
                details,
                created_at: Utc::now(),
            },
            &permit,
            &self.partition_owner_signing_key,
        )
        .await
    }

    pub async fn list_index_diagnostics(
        &self,
        tenant_id: i64,
        bucket_id: i64,
        index_name: &str,
        severity: &str,
        after_cursor: i64,
        limit: i32,
    ) -> Result<Vec<IndexDiagnostic>> {
        index_diagnostic_journal::read_index_diagnostics(
            &self.storage,
            tenant_id,
            bucket_id,
            index_name,
            severity,
            after_cursor,
            if limit == 0 {
                1000
            } else {
                limit.max(1) as usize
            },
        )
        .await
    }
}
