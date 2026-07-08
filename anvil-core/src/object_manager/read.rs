use super::*;

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

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, &bucket_name, &object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut object = match version_id {
            Some(version_id) => {
                let object = metadata_journal::read_object_version(
                    &self.storage,
                    &bucket,
                    &self.signing_key,
                    &object_key,
                    version_id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                &object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?,
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref())
                .await?;
            object = target;
            followed_link = Some(link);
        }

        let (tx, rx) = mpsc::channel(4);
        let app_state = self.clone();
        let object_clone = object.clone();
        let range_start = range.map(|range| range.start).unwrap_or(0);

        tokio::spawn(async move {
            let Some(object_ref) = object_clone
                .shard_map
                .as_ref()
                .and_then(core_object_ref_from_shard_map)
            else {
                let _ = tx
                    .send(Err(Status::not_found(
                        "Object data unavailable: object is not CoreStore-backed",
                    )))
                    .await;
                return;
            };

            let read_result = if let Some(range) = range {
                app_state
                    .core_store
                    .get_blob_range(GetBlobRange { object_ref, range })
                    .await
            } else {
                app_state.core_store.get_blob(GetBlob { object_ref }).await
            };

            match read_result {
                Ok(full_data) => {
                    for chunk in full_data.chunks(1024 * 64) {
                        if tx.send(Ok(chunk.to_vec())).await.is_err() {
                            return;
                        }
                    }
                }
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
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
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

        if !auth::is_authorized(
            AnvilAction::ObjectDelete,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;

        let delete_marker = self
            .persistence
            .soft_delete_object(bucket.id, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?;

        self.publish_object_watch_event(tenant_id, &bucket, &delete_marker, "delete", true)
            .await?;

        Ok(delete_marker)
    }

    pub async fn delete_object_version(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        version_id: uuid::Uuid,
        scopes: &[String],
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

        if !auth::is_authorized(
            AnvilAction::ObjectDelete,
            &format!("{}/{}", bucket_name, object_key),
            scopes,
        ) {
            return Err(Status::permission_denied("Permission denied"));
        }

        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        if bucket.region != self.region {
            return Err(Status::failed_precondition(format!(
                "Bucket is in region {}",
                bucket.region
            )));
        }

        let deleted = self
            .persistence
            .delete_object_version(bucket.id, object_key, version_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object version not found"))?;

        self.publish_object_watch_event(
            tenant_id,
            &bucket,
            &deleted,
            "delete_version",
            deleted.deleted_at.is_some(),
        )
        .await?;

        Ok(deleted)
    }

    pub async fn head_object(
        &self,
        claims: Option<auth::Claims>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
    ) -> Result<Object, Status> {
        Ok(self
            .head_object_with_link_mode(
                claims,
                bucket_name,
                object_key,
                version_id,
                ObjectLinkReadMode::Follow,
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

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, bucket_name, object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut object = match version_id {
            Some(version_id) => {
                let object = metadata_journal::read_object_version(
                    &self.storage,
                    &bucket,
                    &self.signing_key,
                    object_key,
                    version_id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Object version not found"))?;
                if object.deleted_at.is_some() {
                    return Err(Status::not_found("Object version is a delete marker"));
                }
                object
            }
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object not found"))?,
        };
        let mut followed_link = None;
        if version_id.is_none() && object.kind == object_links::ObjectEntryKind::Link {
            if link_mode == ObjectLinkReadMode::Metadata {
                return Err(Status::failed_precondition("ObjectLinkMetadataRead"));
            }
            let (target, link) = self
                .resolve_followed_link(&bucket, object, claims.as_ref())
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
        self.read_object_link_for_tenant(claims, None, bucket_name, object_key, version_id)
            .await
    }

    pub async fn read_object_link_for_tenant(
        &self,
        claims: Option<auth::Claims>,
        route_tenant_id: Option<i64>,
        bucket_name: &str,
        object_key: &str,
        version_id: Option<uuid::Uuid>,
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
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !self
                .object_read_allowed(claims, bucket_name, object_key, None)
                .await?
            {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let object = match version_id {
            Some(version_id) => metadata_journal::read_object_version(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
                version_id,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object link not found"))?,
            None => metadata_journal::read_current_object(
                &self.storage,
                &bucket,
                &self.signing_key,
                object_key,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Object link not found"))?,
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

        // Allow public buckets to bypass auth; otherwise require appropriate scope
        let bucket = self
            .get_authorized_bucket(claims.as_ref(), route_tenant_id, bucket_name)
            .await?;
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        let mut objects = metadata_journal::read_current_directory_objects(
            &self.storage,
            &bucket,
            &self.signing_key,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        objects.retain(|object| {
            object.key.starts_with(prefix)
                && object.key.as_str() > start_after
                && !validation::is_reserved_internal_key(&object.key)
        });
        objects.sort_by(|left, right| left.key.cmp(&right.key));

        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .expect("private bucket listing has claims after authorization");
            objects = self
                .filter_objects_visible_to_reader(claims, bucket_name, objects, None)
                .await?;
        }

        let listing =
            visible_object_listing(objects, prefix, normalized_list_limit(limit), delimiter);
        Ok((listing.objects, listing.common_prefixes))
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
        if !bucket.is_public_read {
            let claims = claims
                .as_ref()
                .ok_or_else(|| Status::permission_denied("Permission denied"))?;
            if !auth::is_authorized(AnvilAction::ObjectList, bucket_name, &claims.scopes) {
                return Err(Status::permission_denied("Permission denied"));
            }
        }

        if bucket.is_public_read {
            return metadata_journal::read_object_versions(
                &self.storage,
                &bucket,
                &self.signing_key,
                prefix,
                key_marker,
                version_id_marker,
                normalized_list_limit(limit),
            )
            .await
            .map_err(|e| Status::internal(e.to_string()));
        }

        let claims = claims
            .as_ref()
            .expect("private bucket version listing has claims after authorization");
        self.list_visible_object_versions(
            claims,
            bucket_name,
            &bucket,
            prefix,
            key_marker,
            version_id_marker,
            normalized_list_limit(limit),
        )
        .await
    }

    pub async fn current_object_for_write_precondition(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<Option<Object>, Status> {
        self.validate_write_request(bucket_name, object_key, scopes)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        metadata_journal::read_current_object(&self.storage, &bucket, &self.signing_key, object_key)
            .await
            .map_err(|e| Status::internal(e.to_string()))
    }

    pub async fn current_object_for_mutation_precondition(
        &self,
        tenant_id: i64,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        action: AnvilAction,
    ) -> Result<Option<Object>, Status> {
        self.validate_object_request(bucket_name, object_key, scopes, action)?;
        let bucket = self.get_tenant_bucket(tenant_id, bucket_name).await?;
        metadata_journal::read_current_object(&self.storage, &bucket, &self.signing_key, object_key)
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
    ) -> Result<Object, Status> {
        self.validate_write_request(
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
        )?;
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

        let copied = self
            .persistence
            .create_object(
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
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        self.publish_object_watch_event(
            claims.tenant_id,
            &destination_bucket,
            &copied,
            "copy",
            false,
        )
        .await?;

        Ok(copied)
    }

    pub async fn compose_object(
        &self,
        claims: auth::Claims,
        sources: Vec<ComposeSource>,
        destination_bucket_name: &str,
        destination_object_key: &str,
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
            claims.tenant_id,
            destination_bucket_name,
            destination_object_key,
            &claims.scopes,
            composed_stream,
            ObjectWriteOptions::default(),
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
            claims.tenant_id,
            bucket_name,
            object_key,
            &claims.scopes,
            tokio_stream::iter(vec![Ok(patched_bytes)]),
            ObjectWriteOptions {
                content_type: Some("application/json".to_string()),
                user_metadata: None,
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

        let bucket = match tenant_id {
            Some(tenant_id) => {
                bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
                    .ok_or_else(|| Status::not_found("Bucket not found for this tenant"))
            }
            None => bucket_journal::read_public_bucket_by_name(&self.storage, bucket_name)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::not_found("Public bucket not found")),
        }?;

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
            if !bucket.is_public_read {
                let claims =
                    claims.ok_or_else(|| Status::permission_denied("Permission denied"))?;
                if !self
                    .object_read_allowed(claims, &bucket.name, &link.target_key, None)
                    .await?
                {
                    return Err(Status::permission_denied("Permission denied"));
                }
            }

            let target = match link.target_version {
                Some(version_id) => {
                    metadata_journal::read_object_version(
                        &self.storage,
                        bucket,
                        &self.signing_key,
                        &link.target_key,
                        version_id,
                    )
                    .await
                }
                None => {
                    metadata_journal::read_current_object(
                        &self.storage,
                        bucket,
                        &self.signing_key,
                        &link.target_key,
                    )
                    .await
                }
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
        let object_resource = format!("{bucket_name}/{object_key}");
        access_control::scope_or_relationship_allows(
            &self.storage,
            claims,
            AnvilAction::ObjectRead,
            &object_resource,
            "object",
            &object_resource,
            "reader",
            authz_revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))
    }

    async fn filter_objects_visible_to_reader(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        objects: Vec<Object>,
        authz_revision: Option<i64>,
    ) -> Result<Vec<Object>, Status> {
        let mut visible = Vec::new();
        for object in objects {
            if self
                .object_read_allowed(claims, bucket_name, &object.key, authz_revision)
                .await?
            {
                visible.push(object);
            }
        }
        Ok(visible)
    }

    async fn list_visible_object_versions(
        &self,
        claims: &auth::Claims,
        bucket_name: &str,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage, Status> {
        let requested_limit = normalized_list_limit(limit).max(1) as usize;
        let visible_target = requested_limit.saturating_add(1);
        let page_limit = i32::try_from(visible_target.max(100)).unwrap_or(i32::MAX);
        let mut visible = Vec::<ObjectVersion>::new();
        let mut current_key_marker = key_marker.to_string();
        let mut current_version_marker = version_id_marker;

        loop {
            let page = metadata_journal::read_object_versions(
                &self.storage,
                bucket,
                &self.signing_key,
                prefix,
                &current_key_marker,
                current_version_marker,
                page_limit,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

            for version in page.versions {
                if self
                    .object_read_allowed(claims, bucket_name, &version.object.key, None)
                    .await?
                {
                    visible.push(version);
                    if visible.len() >= visible_target {
                        break;
                    }
                }
            }

            if visible.len() >= visible_target || !page.is_truncated {
                break;
            }

            let Some(next_key_marker) = page.next_key_marker else {
                break;
            };
            current_key_marker = next_key_marker;
            current_version_marker = page.next_version_id_marker;
        }

        let is_truncated = visible.len() > requested_limit;
        if is_truncated {
            visible.truncate(requested_limit);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            visible
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

        Ok(ObjectVersionsPage {
            versions: visible,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    pub(super) async fn publish_object_watch_event(
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

    pub(super) fn validate_write_request(
        &self,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
    ) -> Result<(), Status> {
        self.validate_object_request(bucket_name, object_key, scopes, AnvilAction::ObjectWrite)
    }

    fn validate_object_request(
        &self,
        bucket_name: &str,
        object_key: &str,
        scopes: &[String],
        action: AnvilAction,
    ) -> Result<(), Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        if validation::is_reserved_internal_key(object_key) {
            return Err(Status::permission_denied("UnauthorizedReservedNamespace"));
        }
        if !validation::is_valid_object_key(object_key) {
            return Err(Status::invalid_argument("Invalid object key"));
        }
        if !auth::is_authorized(action, &format!("{}/{}", bucket_name, object_key), scopes) {
            return Err(Status::permission_denied("Permission denied"));
        }
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
