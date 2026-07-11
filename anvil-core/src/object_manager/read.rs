use super::*;
use crate::core_store::ReadLogicalRangeRequest;
use crate::query_planner::{
    AuthzCandidateRequest, BoundaryCandidateRequest, CandidateSet, CandidateSetKind,
    CandidateSetScope, CoreDocId, IndexCandidateRequest, ObjectAuthzKey, OrderedDocTuple,
    QueryPlanRequest, RangePlanRequest, ReadRangePlan, stable_doc_ordinal,
};
use crate::query_planner::{BoundaryCandidateReader, IndexCandidateReader};
use std::collections::BTreeMap;

impl ObjectManager {
    pub async fn get_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
        range: Option<CoreByteRange>,
    ) -> Result<
        (
            Object,
            Pin<Box<dyn Stream<Item = Result<Vec<u8>, Status>> + Send + 'static>>,
            u64,
        ),
        Status,
    > {
        let result = self
            .get_object_with_link_mode(
                claims,
                bucket_name,
                object_key,
                version_id,
                range,
                ObjectLinkReadMode::Follow,
            )
            .await?;
        Ok((result.object, result.stream, result.range_start))
    }

    pub async fn get_object_with_link_mode(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
        range: Option<CoreByteRange>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectReadResult, Status> {
        self.get_object_with_link_mode_for_tenant(
            claims,
            None,
            bucket_name,
            object_key,
            version_id,
            range,
            link_mode,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn get_object_with_link_mode_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: String,
        object_key: String,
        version_id: Option<uuid::Uuid>,
        range: Option<CoreByteRange>,
        link_mode: ObjectLinkReadMode,
        consistency: ObjectReadConsistency,
    ) -> Result<ObjectReadResult, Status> {
        let _latency = self
            .observability
            .latency_guard(OBJECT_READ_LATENCY, &[("api", "native")]);
        if !validation::is_valid_bucket_name(&bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(&object_key) {
            self.record_reserved_namespace_rejection("get_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(&object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, &bucket_name)
            .await?;

        self.require_object_read_access(
            claims.as_ref(),
            &bucket,
            &object_key,
            consistency.authz_revision(),
        )
        .await?;

        let mut object = match version_id {
            Some(version_id) => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_object_version_metadata_at_generation(
                            &bucket,
                            &object_key,
                            version_id,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_object_version_metadata(&bucket, &object_key, version_id)
                        .await
                }
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_current_object_metadata_at_generation(
                            &bucket,
                            &object_key,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_current_object_metadata(&bucket, &object_key)
                        .await
                };
                object
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object not found"))?
            }
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref(), consistency)
                .await?;
            object = target;
            followed_link = Some(link);
        }

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();
        let range_start = range.map(|range| range.start).unwrap_or(0);
        let logical_authz_scope = AuthzScopeRef {
            anvil_storage_tenant_id: bucket.tenant_id.to_string(),
            authz_realm_id: format!("bucket:{}", bucket.name),
        };

        tokio::spawn(async move {
            let data_target = match object_clone
                .shard_map
                .as_ref()
                .ok_or_else(|| anyhow!("object shard map is missing"))
                .and_then(object_data_target_from_shard_map)
            {
                Ok(data_target) => data_target,
                Err(error) => {
                    let _ = tx
                        .send(Err(Status::not_found(format!(
                            "Object data unavailable: {error}"
                        ))))
                        .await;
                    return;
                }
            };

            let read_result = match data_target {
                ObjectDataTarget::LogicalFile(locator) => {
                    let manifest = match app_state
                        .core_store
                        .read_logical_file_manifest(&locator)
                        .await
                    {
                        Ok(manifest) => manifest,
                        Err(error) => {
                            let _ = tx.send(Err(Status::not_found(error.to_string()))).await;
                            return;
                        }
                    };
                    let read_range = range.unwrap_or(CoreByteRange {
                        start: 0,
                        end_exclusive: manifest.logical_size,
                    });
                    app_state
                        .core_store
                        .read_logical_range_chunks(
                            ReadLogicalRangeRequest {
                                manifest,
                                ranges: vec![read_range],
                                authz_scope: logical_authz_scope,
                                expected_boundary: None,
                                prefetch_policy: CorePrefetchPolicy::default(),
                                trace_context: Default::default(),
                            },
                            1024 * 64,
                            |chunk| {
                                let tx = tx.clone();
                                async move {
                                    tx.send(Ok(chunk))
                                        .await
                                        .map_err(|_| anyhow!("object read response stream closed"))
                                }
                            },
                        )
                        .await
                }
                ObjectDataTarget::ObjectRef(object_ref) => {
                    app_state
                        .core_store
                        .read_object_ref_chunks(object_ref, range, 1024 * 64, |chunk| {
                            let tx = tx.clone();
                            async move {
                                tx.send(Ok(chunk))
                                    .await
                                    .map_err(|_| anyhow!("object read response stream closed"))
                            }
                        })
                        .await
                }
            };

            match read_result {
                Ok(()) => {}
                Err(error) => {
                    let _ = tx.send(Err(Status::not_found(error.to_string()))).await;
                }
            }
        });

        Ok(ObjectReadResult {
            object,
            stream: Box::pin(ReceiverStream::new(rx)),
            followed_link,
            range_start,
        })
    }

    pub async fn delete_object(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<Object, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            self.record_reserved_namespace_rejection("delete_object");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        access_control::require_object_permission(
            &self.storage,
            claims,
            &bucket,
            object_key,
            "delete",
        )
        .await?;

        let delete_marker = self
            .persistence
            .soft_delete_object_in_transaction(
                bucket.id,
                object_key,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;
        if transaction_id.is_none() {
            self.publish_object_watch_event(tenant_id, &bucket, &delete_marker, "delete", true)
                .await?;
        }

        Ok(delete_marker)
    }

    pub async fn delete_object_version(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        version_id: uuid::Uuid,
        transaction_id: Option<&str>,
        transaction_principal: Option<&str>,
    ) -> Result<Object, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let tenant_id = claims.tenant_id;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        access_control::require_object_permission(
            &self.storage,
            claims,
            &bucket,
            object_key,
            "delete",
        )
        .await?;
        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }

        let deleted = self
            .persistence
            .delete_object_version_in_transaction(
                bucket.id,
                object_key,
                version_id,
                transaction_id,
                transaction_principal,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object version not found"))?;
        if transaction_id.is_none() {
            self.publish_object_watch_event(
                tenant_id,
                &bucket,
                &deleted,
                "delete_version",
                deleted.deleted_at.is_some(),
            )
            .await?;
        }

        Ok(deleted)
    }

    pub async fn head_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<Object, Status> {
        self.head_object_with_consistency(
            claims,
            bucket_name,
            object_key,
            version_id,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn head_object_with_consistency(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        consistency: ObjectReadConsistency,
    ) -> Result<Object, Status> {
        Ok(self
            .head_object_with_link_mode_for_tenant(
                claims,
                None,
                bucket_name,
                object_key,
                version_id,
                ObjectLinkReadMode::Follow,
                consistency,
            )
            .await?
            .object)
    }

    pub async fn head_object_with_link_mode(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
    ) -> Result<ObjectHeadResult, Status> {
        self.head_object_with_link_mode_for_tenant(
            claims,
            None,
            bucket_name,
            object_key,
            version_id,
            link_mode,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn head_object_with_link_mode_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        link_mode: ObjectLinkReadMode,
        consistency: ObjectReadConsistency,
    ) -> Result<ObjectHeadResult, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;

        self.require_object_read_access(
            claims.as_ref(),
            &bucket,
            object_key,
            consistency.authz_revision(),
        )
        .await?;

        let mut object = match version_id {
            Some(version_id) => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_object_version_metadata_at_generation(
                            &bucket,
                            object_key,
                            version_id,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_object_version_metadata(&bucket, object_key, version_id)
                        .await
                }
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_current_object_metadata_at_generation(
                            &bucket,
                            object_key,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_current_object_metadata(&bucket, object_key)
                        .await
                };
                object
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object not found"))?
            }
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref(), consistency)
                .await?;
            object = target;
            followed_link = Some(link);
        }
        Ok(ObjectHeadResult {
            object,
            followed_link,
        })
    }

    pub async fn read_object_link(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<object_links::ObjectLinkDescriptor, Status> {
        self.read_object_link_for_tenant(
            claims,
            None,
            bucket_name,
            object_key,
            version_id,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn read_object_link_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
        consistency: ObjectReadConsistency,
    ) -> Result<object_links::ObjectLinkDescriptor, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        self.require_object_read_access(
            claims.as_ref(),
            &bucket,
            object_key,
            consistency.authz_revision(),
        )
        .await?;

        let object = match version_id {
            Some(version_id) => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_object_version_metadata_at_generation(
                            &bucket,
                            object_key,
                            version_id,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_object_version_metadata(&bucket, object_key, version_id)
                        .await
                };
                object
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object link not found"))?
            }
            None => {
                let object = if let Some(root_generation) = consistency.root_generation() {
                    self.core_store
                        .read_current_object_metadata_at_generation(
                            &bucket,
                            object_key,
                            root_generation,
                        )
                        .await
                } else {
                    self.core_store
                        .read_current_object_metadata(&bucket, object_key)
                        .await
                };
                object
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Object link not found"))?
            }
        };
        if object.deleted_at.is_some() || object.kind != object_links::ObjectEntryKind::Link {
            return Err(Status::not_found("Object link not found"));
        }
        object_links::link_descriptor(&bucket.name, &object)
            .ok_or_else(|| Status::internal("Object link descriptor missing"))
    }

    pub async fn list_objects(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        self.list_objects_for_tenant(
            claims,
            None,
            bucket_name,
            prefix,
            start_after,
            limit,
            delimiter,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn list_objects_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
        consistency: ObjectReadConsistency,
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        let _latency = self
            .observability
            .latency_guard(PREFIX_LIST_LATENCY, &[("api", "native")]);
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            self.record_reserved_namespace_rejection("list_objects");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        let reader_claims = self
            .authorized_bucket_reader_claims(claims.as_ref(), &bucket, consistency.authz_revision())
            .await?;

        self.planner_backed_object_listing(
            &reader_claims,
            &bucket,
            prefix,
            start_after,
            limit,
            delimiter,
            consistency,
        )
        .await
    }

    async fn planner_backed_object_listing(
        &self,
        claims: &auth::Claims,
        bucket: &Bucket,
        prefix: &str,
        start_after: &str,
        limit: i32,
        delimiter: &str,
        consistency: ObjectReadConsistency,
    ) -> Result<(Vec<Object>, Vec<String>), Status> {
        let mut objects = match consistency.root_generation() {
            Some(root_generation) => {
                self.core_store
                    .list_current_object_metadata_at_generation(bucket, root_generation)
                    .await
            }
            None => self.core_store.list_current_object_metadata(bucket).await,
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        objects.retain(|object| {
            object.key.starts_with(prefix)
                && object.key.as_str() > start_after
                && !validation::is_reserved_internal_key(&object.key)
                && object.deleted_at.is_none()
        });
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        let system_revision = self
            .object_listing_authz_revision(consistency)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let root_generation = consistency
            .root_generation()
            .unwrap_or_else(|| object_listing_root_generation(&objects));
        let docs = object_listing_docs(bucket, objects, "object-list-current");
        let plan = self
            .execute_object_listing_plan(
                claims,
                bucket,
                docs,
                root_generation,
                system_revision,
                "object-list-current",
                prefix,
                start_after,
                delimiter,
                normalized_list_limit(limit) as u32,
            )
            .await?;
        let objects = object_listing_objects_from_plan(&plan);
        Ok(shape_object_listing(objects, prefix, delimiter, limit))
    }

    pub async fn list_object_versions(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: &str,
        limit: i32,
    ) -> Result<crate::persistence::ObjectVersionsPage, Status> {
        self.list_object_versions_for_tenant(
            claims,
            None,
            bucket_name,
            prefix,
            key_marker,
            version_id_marker,
            limit,
            ObjectReadConsistency::Latest,
        )
        .await
    }

    pub async fn list_object_versions_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        prefix: &str,
        key_marker: &str,
        version_id_marker: &str,
        limit: i32,
        consistency: ObjectReadConsistency,
    ) -> Result<crate::persistence::ObjectVersionsPage, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(prefix) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !prefix.is_empty() && !validation::is_valid_object_key(prefix) {
            return Err(Status::invalid_argument("Invalid object key prefix"));
        }
        if !key_marker.is_empty() && !validation::is_valid_object_key(key_marker) {
            return Err(Status::invalid_argument("Invalid key marker"));
        }
        let version_id_marker = if version_id_marker.is_empty() {
            None
        } else if key_marker.is_empty() {
            return Err(Status::invalid_argument(
                "version id marker requires key marker",
            ));
        } else {
            Some(
                uuid::Uuid::parse_str(version_id_marker)
                    .map_err(|_| Status::invalid_argument("Invalid version id marker"))?,
            )
        };

        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        let reader_claims = self
            .authorized_bucket_reader_claims(claims.as_ref(), &bucket, consistency.authz_revision())
            .await?;
        let unbounded_limit = i32::MAX;
        let page = match consistency.root_generation() {
            Some(root_generation) => {
                self.core_store
                    .list_object_versions_metadata_at_generation(
                        &bucket,
                        prefix,
                        key_marker,
                        version_id_marker,
                        unbounded_limit,
                        root_generation,
                    )
                    .await
            }
            None => {
                self.core_store
                    .list_object_versions_metadata(
                        &bucket,
                        prefix,
                        key_marker,
                        version_id_marker,
                        unbounded_limit,
                    )
                    .await
            }
        }
        .map_err(|e| Status::internal(e.to_string()))?;
        let versions = page
            .versions
            .into_iter()
            .filter(|version| !validation::is_reserved_internal_key(&version.object.key))
            .collect::<Vec<_>>();
        let system_revision = self
            .object_listing_authz_revision(consistency)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let root_generation = consistency
            .root_generation()
            .unwrap_or_else(|| object_version_listing_root_generation(&versions));
        let docs = object_version_listing_docs(&bucket, versions, "object-list-versions");
        let plan = self
            .execute_object_listing_plan(
                &reader_claims,
                &bucket,
                docs,
                root_generation,
                system_revision,
                "object-list-versions",
                prefix,
                key_marker,
                version_id_marker
                    .as_ref()
                    .map(uuid::Uuid::to_string)
                    .as_deref()
                    .unwrap_or(""),
                normalized_list_limit(limit).saturating_add(1) as u32,
            )
            .await?;
        let versions = object_listing_versions_from_plan(&plan);
        Ok(shape_object_version_listing(
            versions,
            normalized_list_limit(limit),
        ))
    }

    async fn object_listing_authz_revision(
        &self,
        consistency: ObjectReadConsistency,
    ) -> AnyhowResult<u64> {
        if let Some(revision) = consistency.authz_revision() {
            return Ok(u64::try_from(revision.max(0))?);
        }
        Ok(crate::authz_segment::read_latest_authz_tuple_segment(
            &self.storage,
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
        )
        .await?
        .map(|segment| segment.header.generation)
        .unwrap_or(0))
    }

    #[allow(clippy::too_many_arguments)]
    async fn execute_object_listing_plan(
        &self,
        claims: &auth::Claims,
        bucket: &Bucket,
        docs: Vec<ObjectListingPlanDoc>,
        root_generation: u64,
        system_revision: u64,
        family: &str,
        predicate_a: &str,
        predicate_b: &str,
        order: &str,
        limit: u32,
    ) -> Result<ObjectListingPlanOutput, Status> {
        if !matches!(family, "object-list-current" | "object-list-versions") {
            return Err(Status::failed_precondition(
                "IndexCapabilityMissing: object listing requires a planner-backed path/object-list candidate reader",
            ));
        }
        let partition_id = object_listing_partition_id(bucket, family);
        let object_namespace =
            access_control::system_realm_namespace(crate::system_realm::SYSTEM_OBJECT_NAMESPACE);
        let scope = CandidateSetScope {
            root_key_hash: object_listing_hash(&[
                "root",
                &bucket.tenant_id.to_string(),
                &bucket.id.to_string(),
                &root_generation.to_string(),
            ]),
            root_generation,
            index_id: format!("{family}:{}:{}", bucket.tenant_id, bucket.id),
            index_generation: root_generation,
            authz_realm_id: crate::system_realm::SYSTEM_REALM_ID.to_string(),
            authz_scope_hash: object_listing_hash(&[
                "authz-scope",
                &bucket.tenant_id.to_string(),
                &bucket.id.to_string(),
                &system_revision.to_string(),
            ]),
            authz_object_namespace: object_namespace.clone(),
            authz_relation: "get".to_string(),
            authz_principal_hash: object_listing_hash(&["principal", &claims.sub]),
            authz_revision: system_revision,
            boundary_schema_generation_hash: object_listing_hash(&[
                "boundary",
                &bucket.tenant_id.to_string(),
                &bucket.id.to_string(),
            ]),
            predicate_hash: object_listing_hash(&["predicate", family, predicate_a, predicate_b]),
            order_hash: object_listing_hash(&["order", family, order]),
        };
        let reader = ObjectListingCandidateReader::new(scope.clone(), partition_id, docs);
        let authz_reader = crate::authz_segment::AuthzSegmentCandidateReader::new(
            self.storage.clone(),
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
        );
        let planner = crate::query_planner::CoreStoreQueryPlanner {
            boundary_reader: &reader,
            authz_reader: &authz_reader,
            index_reader: &reader,
        };
        let request = QueryPlanRequest {
            boundary: BoundaryCandidateRequest {
                root_key_hash: scope.root_key_hash.clone(),
                root_generation: scope.root_generation,
                bucket_name: bucket.name.clone(),
                boundary_schema_generation_hash: scope.boundary_schema_generation_hash.clone(),
                boundary_predicate_json: String::new(),
            },
            authz: AuthzCandidateRequest {
                authz_scope: scope.authz_scope_hash.clone(),
                candidate_scope: scope.clone(),
                partition_id,
                subject: format!("{}:{}", access_control::APP_SUBJECT_KIND, claims.sub),
                relation: "get".to_string(),
                object_namespace,
                revision: system_revision,
                system_revision,
                root_generation,
            },
            index: IndexCandidateRequest {
                index_id: scope.index_id.clone(),
                predicate_json: scope.predicate_hash.clone(),
                order_json: Some(scope.order_hash.clone()),
                generation: scope.index_generation,
                boundary_predicate_json: None,
            },
            limit,
            page_token: None,
        };
        let result = planner
            .plan(request)
            .await
            .map_err(|e| Status::failed_precondition(e.to_string()))?;
        let mut objects = Vec::new();
        for range in result.ranges {
            let index = usize::try_from(range.logical_start)
                .map_err(|_| Status::internal("Object listing range overflow"))?;
            if let Some(doc) = reader.docs.get(index) {
                objects.push(doc.clone());
            }
        }
        Ok(ObjectListingPlanOutput { docs: objects })
    }

    pub async fn current_object_for_write_precondition(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<Option<Object>, Status> {
        self.validate_write_request(claims, bucket_name, object_key)
            .await?;
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        self.core_store
            .read_current_object_metadata(&bucket, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn current_object_for_mutation_precondition(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        action: AnvilAction,
    ) -> Result<Option<Object>, Status> {
        match action {
            AnvilAction::ObjectRead | AnvilAction::ObjectWrite | AnvilAction::ObjectDelete => {
                self.validate_object_request(claims, bucket_name, object_key, action)
                    .await?;
            }
            AnvilAction::StreamCreate
            | AnvilAction::StreamAppend
            | AnvilAction::StreamSealSegment => {
                self.validate_object_path_only(bucket_name, object_key)?;
                access_control::require_action(
                    &self.storage,
                    &self.persistence,
                    claims,
                    action,
                    &format!("{bucket_name}/{object_key}"),
                )
                .await?;
            }
            _ => return Err(Status::internal("unsupported mutation precondition action")),
        }
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        self.core_store
            .read_current_object_metadata(&bucket, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn copy_object(
        &self,
        claims: auth::Claims,
        source_bucket_name: &str,
        source_object_key: &str,
        source_version_id: Option<uuid::Uuid>,
        destination_bucket_name: &str,
        destination_object_key: &str,
        transaction_id: Option<&str>,
    ) -> Result<Object, Status> {
        self.validate_write_request(&claims, destination_bucket_name, destination_object_key)
            .await?;
        let source_object = self
            .head_object(
                Some(claims.clone()),
                source_bucket_name,
                source_object_key,
                source_version_id,
            )
            .await?;
        let destination_bucket = self
            .get_tenant_bucket(claims.tenant_id, destination_bucket_name)
            .await?;
        let transaction_principal =
            crate::object_manager::transaction_principal_from_claims(&claims);

        let copied = self
            .persistence
            .create_object_with_storage_class(
                claims.tenant_id,
                destination_bucket.id,
                destination_object_key,
                &source_object.content_hash,
                source_object.size,
                &source_object.etag,
                source_object.content_type.as_deref(),
                source_object.user_meta,
                source_object.shard_map,
                None,
                transaction_id,
                Some(transaction_principal.as_str()),
                source_object.storage_class,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if transaction_id.is_none() {
            self.publish_object_watch_event(
                claims.tenant_id,
                &destination_bucket,
                &copied,
                "copy",
                false,
            )
            .await?;
        }

        Ok(copied)
    }

    pub async fn compose_object(
        &self,
        claims: auth::Claims,
        sources: Vec<ComposeSource>,
        destination_bucket_name: &str,
        destination_object_key: &str,
        transaction_id: Option<&str>,
    ) -> Result<Object, Status> {
        if sources.is_empty() {
            return Err(Status::invalid_argument(
                "ComposeObject requires at least one source",
            ));
        }

        let state = ComposeStreamState {
            manager: self.clone(),
            claims: claims.clone(),
            sources: sources.into_iter(),
            current: None,
        };
        let composed_stream = Box::pin(futures_util::stream::try_unfold(
            state,
            |mut state| async move {
                loop {
                    if let Some(current) = state.current.as_mut() {
                        match current.next().await {
                            Some(Ok(chunk)) => return Ok(Some((chunk, state))),
                            Some(Err(status)) => return Err(status),
                            None => {
                                state.current = None;
                                continue;
                            }
                        }
                    }

                    let Some(source) = state.sources.next() else {
                        return Ok(None);
                    };
                    let (_object, stream, _range_start) = state
                        .manager
                        .get_object(
                            Some(state.claims.clone()),
                            source.bucket_name,
                            source.object_key,
                            source.version_id,
                            None,
                        )
                        .await?;
                    state.current = Some(stream);
                }
            },
        ));

        self.put_object(
            &claims,
            destination_bucket_name,
            destination_object_key,
            composed_stream,
            ObjectWriteOptions {
                transaction_id: transaction_id.map(ToOwned::to_owned),
                ..Default::default()
            },
        )
        .await
    }

    pub async fn patch_json_object(
        &self,
        claims: auth::Claims,
        bucket_name: &str,
        object_key: &str,
        base_version_id: Option<uuid::Uuid>,
        merge_patch_json: &str,
        transaction_id: Option<&str>,
    ) -> Result<Object, Status> {
        let (_source_object, source_stream, _range_start) = self
            .get_object(
                Some(claims.clone()),
                bucket_name.to_string(),
                object_key.to_string(),
                base_version_id,
                None,
            )
            .await?;

        let source_bytes = collect_stream_bytes(source_stream).await?;
        let mut document: JsonValue = serde_json::from_slice(&source_bytes)
            .map_err(|e| Status::invalid_argument(format!("Object is not valid JSON: {}", e)))?;
        let patch: JsonValue = serde_json::from_str(merge_patch_json)
            .map_err(|e| Status::invalid_argument(format!("Patch is not valid JSON: {}", e)))?;

        apply_json_merge_patch(&mut document, patch);
        let patched_bytes = serde_json::to_vec(&document)
            .map_err(|e| Status::internal(format!("Failed to serialize patched JSON: {}", e)))?;

        self.put_object(
            &claims,
            bucket_name,
            object_key,
            tokio_stream::iter(vec![Ok(patched_bytes)]),
            ObjectWriteOptions {
                content_type: Some("application/json".to_string()),
                user_metadata: None,
                transaction_id: transaction_id.map(ToOwned::to_owned),
                transaction_principal: transaction_id
                    .map(|_| crate::object_manager::transaction_principal_from_claims(&claims)),
                storage_class_id: None,
            },
        )
        .await
    }

    async fn get_authorized_bucket(
        &self,
        claims: Option<&auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        if let (Some(claims), Some(route_tenant_id)) = (claims, route_tenant_id)
            && claims.tenant_id != route_tenant_id
        {
            return Err(Status::permission_denied(
                "Credentials are not valid for routed tenant",
            ));
        }

        let tenant_id = route_tenant_id.or_else(|| claims.map(|claims| claims.tenant_id));
        if let Some(tenant_id) = tenant_id
            && let Some(locator) = self
                .persistence
                .get_mesh_bucket_locator(tenant_id, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            && locator.status != crate::mesh_directory::BucketLocatorStatus::Deleted
            && locator.home_region.as_str() != self.region.as_str()
        {
            return Err(self.remote_bucket_status(locator.home_region.as_str()));
        }

        let tenant_id = tenant_id.ok_or_else(|| {
            Status::permission_denied(
                "Bucket reads require authenticated tenant claims or an explicit tenant route",
            )
        })?;
        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found for this tenant"))?;

        if bucket.region != self.region {
            return Err(self.remote_bucket_status(&bucket.region));
        }

        Ok(bucket)
    }

    fn remote_bucket_status(&self, bucket_region: &str) -> Status {
        let action = routing::remote_bucket_routing_action(self.cross_region_routing_policy, false);
        let (code, message, action_name) = match action {
            routing::RemoteBucketRoutingAction::Redirect => (
                tonic::Code::FailedPrecondition,
                format!("Bucket is in region {bucket_region}; redirect required"),
                "redirect",
            ),
            routing::RemoteBucketRoutingAction::Proxy => (
                tonic::Code::Unavailable,
                format!("Bucket is in region {bucket_region}; native proxy is unavailable"),
                "proxy_unavailable",
            ),
            routing::RemoteBucketRoutingAction::RejectLocalOnly => (
                tonic::Code::FailedPrecondition,
                format!("Bucket is in region {bucket_region}; cross-region routing is disabled"),
                "local_only",
            ),
            routing::RemoteBucketRoutingAction::ProxyUnavailable => (
                tonic::Code::Unavailable,
                format!("Bucket is in region {bucket_region}; cross-region proxy is unavailable"),
                "proxy_unavailable",
            ),
        };
        let mut status = Status::new(code, message);
        if let Ok(value) = MetadataValue::try_from(bucket_region) {
            status.metadata_mut().insert("x-anvil-bucket-region", value);
        }
        if let Ok(value) = MetadataValue::try_from(action_name) {
            status
                .metadata_mut()
                .insert("x-anvil-cross-region-action", value);
        }
        status
    }

    async fn resolve_followed_link(
        &self,
        bucket: &Bucket,
        initial_link: Object,
        claims: Option<&auth::Claims>,
        consistency: ObjectReadConsistency,
    ) -> Result<(Object, object_links::FollowedObjectLink), Status> {
        let initial_descriptor = object_links::link_descriptor(&bucket.name, &initial_link)
            .ok_or_else(|| Status::internal("Object link descriptor missing"))?;
        if initial_descriptor.resolution != object_links::ObjectLinkResolution::Follow {
            return Err(Status::failed_precondition("ObjectLinkRedirectRequired"));
        }

        let mut current_link = initial_link;
        let mut seen = std::collections::HashSet::new();
        for _ in 0..object_links::MAX_LINK_RESOLUTION_DEPTH {
            let Some(link) = current_link.link.clone() else {
                return Err(Status::failed_precondition("InvalidObjectLink"));
            };
            let seen_key = format!("{}:{}", current_link.key, current_link.version_id);
            if !seen.insert(seen_key) {
                return Err(Status::failed_precondition("ObjectLinkLoop"));
            }
            self.require_object_read_access(
                claims,
                bucket,
                &link.target_key,
                consistency.authz_revision(),
            )
            .await?;

            let target = match link.target_version {
                Some(version_id) => match consistency.root_generation() {
                    Some(root_generation) => {
                        self.core_store
                            .read_object_version_metadata_at_generation(
                                bucket,
                                &link.target_key,
                                version_id,
                                root_generation,
                            )
                            .await
                    }
                    None => {
                        self.core_store
                            .read_object_version_metadata(bucket, &link.target_key, version_id)
                            .await
                    }
                },
                None => match consistency.root_generation() {
                    Some(root_generation) => {
                        self.core_store
                            .read_current_object_metadata_at_generation(
                                bucket,
                                &link.target_key,
                                root_generation,
                            )
                            .await
                    }
                    None => {
                        self.core_store
                            .read_current_object_metadata(bucket, &link.target_key)
                            .await
                    }
                },
            }
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::failed_precondition("DanglingObjectLink"))?;
            if target.deleted_at.is_some() {
                return Err(Status::failed_precondition("DanglingObjectLink"));
            }
            match target.kind {
                object_links::ObjectEntryKind::Blob => {
                    let response_etag = object_links::followed_link_etag(&current_link, &target)
                        .ok_or_else(|| Status::internal("Object link ETag missing"))?;
                    let mut served = target;
                    served.etag = response_etag.clone();
                    let followed = object_links::FollowedObjectLink {
                        descriptor: initial_descriptor,
                        response_etag,
                        target_version: served.version_id,
                    };
                    return Ok((served, followed));
                }
                object_links::ObjectEntryKind::Link => {
                    current_link = target;
                }
            }
        }
        Err(Status::failed_precondition("ObjectLinkDepthExceeded"))
    }

    async fn object_read_allowed(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        authz_revision: Option<i64>,
    ) -> Result<bool, Status> {
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        self.object_read_allowed_for_bucket(claims, &bucket, object_key, authz_revision)
            .await
    }

    async fn object_read_allowed_for_bucket(
        &self,
        claims: &auth::Claims,
        bucket: &Bucket,
        object_key: &str,
        authz_revision: Option<i64>,
    ) -> Result<bool, Status> {
        let object_id = access_control::object_object_id(&bucket, object_key);
        if access_control::system_realm_relationship_allows(
            &self.storage,
            claims,
            crate::system_realm::SYSTEM_OBJECT_NAMESPACE,
            &object_id,
            "get",
            authz_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        {
            return Ok(true);
        }
        access_control::system_realm_relationship_allows(
            &self.storage,
            claims,
            crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
            &access_control::bucket_object_id(&bucket),
            "get_object",
            authz_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))
    }

    async fn require_object_read_access(
        &self,
        claims: Option<&auth::Claims>,
        bucket: &Bucket,
        object_key: &str,
        authz_revision: Option<i64>,
    ) -> Result<(), Status> {
        if let Some(claims) = claims
            && self
                .object_read_allowed_for_bucket(claims, bucket, object_key, authz_revision)
                .await?
        {
            return Ok(());
        }

        if bucket.is_public_read {
            let public_claims = access_control::public_read_claims(bucket.tenant_id);
            if self
                .object_read_allowed_for_bucket(&public_claims, bucket, object_key, authz_revision)
                .await?
            {
                return Ok(());
            }
        }

        Err(Status::permission_denied("Permission denied"))
    }

    async fn bucket_relation_allowed(
        &self,
        claims: &auth::Claims,
        bucket: &Bucket,
        relation: &str,
        authz_revision: Option<i64>,
    ) -> Result<bool, Status> {
        access_control::system_realm_relationship_allows(
            &self.storage,
            claims,
            crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
            &access_control::bucket_object_id(bucket),
            relation,
            authz_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))
    }

    async fn authorized_bucket_reader_claims(
        &self,
        claims: Option<&auth::Claims>,
        bucket: &Bucket,
        authz_revision: Option<i64>,
    ) -> Result<auth::Claims, Status> {
        if let Some(claims) = claims
            && self
                .bucket_relation_allowed(claims, bucket, "list_objects", authz_revision)
                .await?
        {
            return Ok(claims.clone());
        }

        if bucket.is_public_read {
            let public_claims = access_control::public_read_claims(bucket.tenant_id);
            if self
                .bucket_relation_allowed(&public_claims, bucket, "list_objects", authz_revision)
                .await?
            {
                return Ok(public_claims);
            }
        }

        Err(Status::permission_denied("Permission denied"))
    }

    pub(crate) async fn publish_object_watch_event(
        &self,
        tenant_id: i64,
        bucket: &Bucket,
        object: &Object,
        event_type: &str,
        is_delete_marker: bool,
    ) -> Result<(), Status> {
        let event = self
            .persistence
            .create_object_watch_event(
                tenant_id,
                bucket.id,
                &bucket.name,
                object,
                event_type,
                is_delete_marker,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        watch_log::append_object_watch_record(&self.storage, bucket, object, &event)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.watch_tx.send(event);
        Ok(())
    }

    pub(super) async fn validate_write_request(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<(), Status> {
        self.validate_object_request(claims, bucket_name, object_key, AnvilAction::ObjectWrite)
            .await
    }

    pub(super) fn validate_object_path_only(
        &self,
        bucket_name: &str,
        object_key: &str,
    ) -> Result<(), Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            self.record_reserved_namespace_rejection("object_path");
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }
        Ok(())
    }

    async fn validate_object_request(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        object_key: &str,
        action: AnvilAction,
    ) -> Result<(), Status> {
        self.validate_object_path_only(bucket_name, object_key)?;
        let bucket = self
            .get_tenant_bucket(claims.tenant_id, bucket_name)
            .await?;
        let relation = match action {
            AnvilAction::ObjectRead => "get",
            AnvilAction::ObjectWrite => "put",
            AnvilAction::ObjectDelete => "delete",
            _ => return Err(Status::internal("unsupported object action")),
        };
        access_control::require_object_permission(
            &self.storage,
            claims,
            &bucket,
            object_key,
            relation,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn get_tenant_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<Bucket, Status> {
        if let Some(locator) = self
            .persistence
            .get_mesh_bucket_locator(tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            && locator.status != crate::mesh_directory::BucketLocatorStatus::Deleted
            && locator.home_region.as_str() != self.region.as_str()
        {
            return Err(self.remote_bucket_status(locator.home_region.as_str()));
        }

        let bucket = bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))?;

        if bucket.region != self.region {
            return Err(self.remote_bucket_status(&bucket.region));
        }

        Ok(bucket)
    }
}

#[derive(Debug, Clone)]
struct ObjectListingPlanDoc {
    doc_id: CoreDocId,
    object: Object,
    version_is_latest: bool,
    is_delete_marker: bool,
    authz_key: ObjectAuthzKey,
    order_tuple: Vec<Vec<u8>>,
}

#[derive(Debug, Clone)]
struct ObjectListingPlanOutput {
    docs: Vec<ObjectListingPlanDoc>,
}

#[derive(Debug, Clone)]
struct ObjectListingCandidateReader {
    scope: CandidateSetScope,
    partition_id: u64,
    docs: Vec<ObjectListingPlanDoc>,
}

impl ObjectListingCandidateReader {
    fn new(scope: CandidateSetScope, partition_id: u64, docs: Vec<ObjectListingPlanDoc>) -> Self {
        Self {
            scope,
            partition_id,
            docs,
        }
    }
}

impl BoundaryCandidateReader for ObjectListingCandidateReader {
    async fn boundary_candidates(
        &self,
        request: BoundaryCandidateRequest,
    ) -> AnyhowResult<CandidateSet> {
        if self.scope.root_key_hash != request.root_key_hash
            || self.scope.root_generation != request.root_generation
            || self.scope.boundary_schema_generation_hash != request.boundary_schema_generation_hash
        {
            bail!("IndexGenerationMismatch");
        }
        Ok(CandidateSet::all_within_partition(
            self.scope.clone(),
            self.partition_id,
        ))
    }
}

impl IndexCandidateReader for ObjectListingCandidateReader {
    async fn predicate_candidates(
        &self,
        request: IndexCandidateRequest,
    ) -> AnyhowResult<CandidateSet> {
        if self.scope.index_id != request.index_id
            || self.scope.index_generation != request.generation
            || self.scope.predicate_hash != request.predicate_json
            || request
                .order_json
                .as_ref()
                .is_some_and(|order_hash| *order_hash != self.scope.order_hash)
        {
            bail!("IndexGenerationMismatch");
        }
        Ok(CandidateSet {
            scope: self.scope.clone(),
            kind: CandidateSetKind::OrderedTuples {
                partition_id: self.partition_id,
                tuples: self
                    .docs
                    .iter()
                    .map(|doc| OrderedDocTuple {
                        order_tuple: doc.order_tuple.clone(),
                        doc_id: doc.doc_id,
                    })
                    .collect(),
            },
        })
    }

    async fn range_plan(&self, request: RangePlanRequest) -> AnyhowResult<Vec<ReadRangePlan>> {
        request
            .candidates
            .scope
            .ensure_compatible_with(&self.scope)?;
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        Ok(self
            .docs
            .iter()
            .enumerate()
            .filter(|(_, doc)| request.candidates.contains_doc_id(doc.doc_id))
            .take(limit)
            .map(|(index, doc)| ReadRangePlan {
                manifest_hash: self.scope.index_id.clone(),
                logical_start: index as u64,
                logical_end: index as u64 + 1,
                doc_ids: vec![doc.doc_id],
                authz_keys: vec![doc.authz_key.clone()],
            })
            .collect())
    }
}

fn object_listing_docs(
    bucket: &Bucket,
    objects: Vec<Object>,
    family: &str,
) -> Vec<ObjectListingPlanDoc> {
    objects
        .into_iter()
        .map(|object| object_listing_doc(bucket, object, family, false))
        .collect()
}

fn object_version_listing_docs(
    bucket: &Bucket,
    versions: Vec<crate::persistence::ObjectVersion>,
    family: &str,
) -> Vec<ObjectListingPlanDoc> {
    versions
        .into_iter()
        .map(|version| {
            let mut doc = object_listing_doc(bucket, version.object, family, version.is_latest);
            doc.is_delete_marker = version.is_delete_marker;
            doc
        })
        .collect()
}

fn object_listing_doc(
    bucket: &Bucket,
    object: Object,
    family: &str,
    version_is_latest: bool,
) -> ObjectListingPlanDoc {
    let namespace =
        access_control::system_realm_namespace(crate::system_realm::SYSTEM_OBJECT_NAMESPACE);
    let object_id = access_control::object_object_id(bucket, &object.key);
    let authz_key = ObjectAuthzKey::realm_object(namespace, object_id);
    let partition_id = object_listing_partition_id(bucket, family);
    let doc_id = authz_key.doc_id(partition_id);
    let order_tuple = vec![
        object.key.as_bytes().to_vec(),
        object.created_at.to_rfc3339().as_bytes().to_vec(),
        object.version_id.as_bytes().to_vec(),
    ];
    let is_delete_marker = object.deleted_at.is_some();
    ObjectListingPlanDoc {
        doc_id,
        object,
        version_is_latest,
        is_delete_marker,
        authz_key,
        order_tuple,
    }
}

fn object_listing_root_generation(objects: &[Object]) -> u64 {
    objects
        .iter()
        .map(|object| object.id.max(0) as u64)
        .max()
        .unwrap_or(0)
}

fn object_version_listing_root_generation(versions: &[crate::persistence::ObjectVersion]) -> u64 {
    versions
        .iter()
        .map(|version| version.object.id.max(0) as u64)
        .max()
        .unwrap_or(0)
}

fn object_listing_partition_id(bucket: &Bucket, family: &str) -> u64 {
    stable_doc_ordinal(&[
        "object-list-partition",
        family,
        &bucket.tenant_id.to_string(),
        &bucket.id.to_string(),
    ])
}

fn object_listing_hash(parts: &[&str]) -> String {
    let mut hasher = blake3::Hasher::new();
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for byte in digest.as_bytes() {
        use std::fmt::Write as _;
        let _ = write!(&mut hex, "{byte:02x}");
    }
    format!("blake3:{hex}")
}

fn object_listing_objects_from_plan(plan: &ObjectListingPlanOutput) -> Vec<Object> {
    plan.docs.iter().map(|doc| doc.object.clone()).collect()
}

fn object_listing_versions_from_plan(
    plan: &ObjectListingPlanOutput,
) -> Vec<crate::persistence::ObjectVersion> {
    plan.docs
        .iter()
        .map(|doc| crate::persistence::ObjectVersion {
            object: doc.object.clone(),
            is_latest: doc.version_is_latest,
            is_delete_marker: doc.is_delete_marker,
        })
        .collect()
}

fn shape_object_listing(
    objects: Vec<Object>,
    prefix: &str,
    delimiter: &str,
    limit: i32,
) -> (Vec<Object>, Vec<String>) {
    let limit = normalized_list_limit(limit).max(1) as usize;
    if delimiter.is_empty() {
        return (objects.into_iter().take(limit).collect(), Vec::new());
    }

    enum ListingEntry {
        Object(Object),
        CommonPrefix(String),
    }

    let mut merged = BTreeMap::<String, ListingEntry>::new();
    for object in objects {
        let suffix = &object.key[prefix.len()..];
        if let Some(position) = suffix.find(delimiter) {
            let common_prefix = format!("{}{}", prefix, &suffix[..position + delimiter.len()]);
            merged
                .entry(common_prefix.clone())
                .or_insert(ListingEntry::CommonPrefix(common_prefix));
        } else {
            merged.insert(object.key.clone(), ListingEntry::Object(object));
        }
        if merged.len() >= limit {
            break;
        }
    }

    let mut listed = Vec::new();
    let mut common_prefixes = Vec::new();
    for (_, entry) in merged.into_iter().take(limit) {
        match entry {
            ListingEntry::Object(object) => listed.push(object),
            ListingEntry::CommonPrefix(prefix) => common_prefixes.push(prefix),
        }
    }
    (listed, common_prefixes)
}

fn shape_object_version_listing(
    mut versions: Vec<crate::persistence::ObjectVersion>,
    limit: i32,
) -> crate::persistence::ObjectVersionsPage {
    let limit = normalized_list_limit(limit).max(1) as usize;
    let is_truncated = versions.len() > limit;
    if is_truncated {
        versions.truncate(limit);
    }
    let (next_key_marker, next_version_id_marker) = if is_truncated {
        versions
            .last()
            .map(|version| {
                (
                    Some(version.object.key.clone()),
                    Some(version.object.version_id),
                )
            })
            .unwrap_or((None, None))
    } else {
        (None, None)
    };
    crate::persistence::ObjectVersionsPage {
        versions,
        is_truncated,
        next_key_marker,
        next_version_id_marker,
    }
}
