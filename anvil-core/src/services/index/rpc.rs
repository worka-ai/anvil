use super::*;

fn index_write_transaction_id(options: Option<&WriteOptions>) -> Result<Option<&str>, Status> {
    let Some(transaction_id) = options.and_then(|options| options.transaction_id.as_deref()) else {
        return Ok(None);
    };
    if transaction_id.trim().is_empty() {
        return Err(Status::invalid_argument("transaction_id must not be empty"));
    }
    Ok(Some(transaction_id))
}

#[tonic::async_trait]
impl IndexService for AppState {
    type WatchIndexDefinitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexDefinitionResponse, Status>> + Send>,
    >;
    type WatchIndexPartitionStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<WatchIndexPartitionResponse, Status>> + Send>,
    >;

    async fn create_index(
        &self,
        request: Request<CreateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let kind = concrete_index_kind(req.kind)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexCreate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        validate_index_definition_shape(kind, &build_policy, &extractor, &self.config)?;
        let transaction_id = index_write_transaction_id(req.options.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));

        let index = self
            .persistence
            .create_index_definition(
                claims.tenant_id,
                bucket.id,
                &req.name,
                kind,
                selector,
                extractor,
                &req.authorization_mode,
                build_policy,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        if transaction_id.is_none() {
            access_control::grant_index_defaults(
                &self.persistence,
                &bucket,
                &index.name,
                &claims.sub,
                &claims.sub,
                "grant creator index owner",
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        }
        self.publish_index_definition_event_with_transaction(
            &bucket,
            &index,
            "create",
            transaction_id,
            transaction_principal.as_deref(),
        )
        .await?;
        if transaction_id.is_none() {
            self.persistence
                .enqueue_index_build_for_index(&bucket, &index)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn update_index(
        &self,
        request: Request<UpdateIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexUpdate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let selector = parse_json_field("selector_json", &req.selector_json)?;
        let extractor = parse_json_field("extractor_json", &req.extractor_json)?;
        let build_policy = parse_json_field("build_policy_json", &req.build_policy_json)?;
        validate_authorization_mode(&req.authorization_mode)?;
        let transaction_id = index_write_transaction_id(req.options.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let existing = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        validate_index_definition_shape(&existing.kind, &build_policy, &extractor, &self.config)?;

        let index = self
            .persistence
            .update_index_definition(
                claims.tenant_id,
                bucket.id,
                &req.name,
                selector,
                extractor,
                &req.authorization_mode,
                build_policy,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event_with_transaction(
            &bucket,
            &index,
            "update",
            transaction_id,
            transaction_principal.as_deref(),
        )
        .await?;
        if transaction_id.is_none() {
            self.persistence
                .enqueue_index_build_for_index(&bucket, &index)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        }

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn disable_index(
        &self,
        request: Request<DisableIndexRequest>,
    ) -> Result<Response<IndexDefinitionResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexUpdate,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let transaction_id = index_write_transaction_id(req.options.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let index = self
            .persistence
            .disable_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event_with_transaction(
            &bucket,
            &index,
            "disable",
            transaction_id,
            transaction_principal.as_deref(),
        )
        .await?;

        Ok(Response::new(IndexDefinitionResponse {
            index: Some(index_record(&bucket.name, index)?),
        }))
    }

    async fn drop_index(
        &self,
        request: Request<DropIndexRequest>,
    ) -> Result<Response<DropIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.name)?;
        let resource = index_resource(&req.bucket_name, &req.name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexDelete,
            &resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let transaction_id = index_write_transaction_id(req.options.as_ref())?;
        let transaction_principal = transaction_id
            .map(|_| crate::object_manager::transaction_principal_from_claims(&claims));
        let index = self
            .persistence
            .drop_index_definition(claims.tenant_id, bucket.id, &req.name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        self.publish_index_definition_event_with_transaction(
            &bucket,
            &index,
            "drop",
            transaction_id,
            transaction_principal.as_deref(),
        )
        .await?;
        Ok(Response::new(DropIndexResponse {}))
    }

    async fn list_indexes(
        &self,
        request: Request<ListIndexesRequest>,
    ) -> Result<Response<ListIndexesResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let indexes = index_journal::read_current_index_definition_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            req.include_disabled,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?
        .into_iter()
        .map(|event| index_record_from_event(&event))
        .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexesResponse { indexes }))
    }

    async fn query_index(
        &self,
        request: Request<QueryIndexRequest>,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        let index_resource = format!("{}/{}", req.bucket_name, req.index_name);
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &index_resource,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;

        match index.kind.as_str() {
            "path" | "metadata_filter" => {
                self.query_metadata_backed_index(&claims, &bucket, &index, req)
                    .await
            }
            "typed_json" => {
                self.query_typed_json_index(&claims, &bucket, &index, req)
                    .await
            }
            "full_text" => {
                self.query_full_text_index(&claims, &bucket, &index, req)
                    .await
            }
            "vector" => self.query_vector_index(&claims, &bucket, &index, req).await,
            "hybrid" => self.query_hybrid_index(&claims, &bucket, &index, req).await,
            _ => Err(Status::failed_precondition("IndexDoesNotSupportQuery")),
        }
    }

    async fn query_spec(
        &self,
        request: Request<QuerySpecRequest>,
    ) -> Result<Response<QuerySpecResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        let spec = AnvilQuerySpec::parse(&req.query_spec_json)?;
        let bucket_name = spec.scope.bucket_name.trim();
        if bucket_name.is_empty() {
            return Err(Status::invalid_argument(
                "QuerySpec scope.bucket_name is required",
            ));
        }
        if let Some(storage_tenant) = spec.scope.anvil_storage_tenant_id.as_deref()
            && !storage_tenant.is_empty()
            && storage_tenant != claims.tenant_id.to_string()
        {
            return Err(Status::permission_denied(
                "QuerySpec storage tenant does not match authenticated tenant",
            ));
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            bucket_name,
        )
        .await?;
        let bucket = self.get_index_bucket(claims.tenant_id, bucket_name).await?;
        let plan = self
            .plan_query_spec(&claims, &bucket, &spec, req.accept_degraded)
            .await?;
        let response = {
            if plan.typed_filter_index.is_some() {
                self.query_composite_query_spec(
                    &claims,
                    &bucket,
                    &plan,
                    &req.page_token,
                    req.lag_timeout_ms,
                )
                .await?
            } else {
                let query_req =
                    plan.single_query_request(&bucket.name, &req.page_token, req.lag_timeout_ms)?;
                match plan.index.kind.as_str() {
                    "path" | "metadata_filter" => {
                        self.query_metadata_backed_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "typed_json" => {
                        self.query_typed_json_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "full_text" => {
                        self.query_full_text_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "vector" => {
                        self.query_vector_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    "hybrid" => {
                        self.query_hybrid_index(&claims, &bucket, &plan.index, query_req)
                            .await?
                    }
                    _ => return Err(Status::failed_precondition("IndexDoesNotSupportQuerySpec")),
                }
                .into_inner()
            }
        };
        if spec.consistency.allow_stale_index == Some(false) && !response.is_caught_up {
            return Err(Status::failed_precondition("IndexLagging"));
        }

        Ok(Response::new(QuerySpecResponse {
            result: Some(response),
            canonical_query_hash: plan.canonical_query_hash,
            plan_json: plan.plan_json,
            diagnostics: plan.diagnostics,
        }))
    }

    async fn watch_index_definition(
        &self,
        request: Request<WatchIndexDefinitionRequest>,
    ) -> Result<Response<Self::WatchIndexDefinitionStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexWatch,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let snapshot = index_journal::read_index_definition_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut live = self.index_watch_tx.subscribe();

        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.id);
                if tx
                    .send(index_definition_event_response(&event))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            loop {
                match live.recv().await {
                    Ok(event) => {
                        if event.tenant_id != claims.tenant_id
                            || event.bucket_id != bucket.id
                            || event.id <= last_cursor
                        {
                            continue;
                        }
                        last_cursor = event.id;
                        if tx
                            .send(index_definition_event_response(&event))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = tx
                            .send(Err(Status::data_loss(
                                "Index definition watch fell behind retained live event window",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexDefinitionStream
        ))
    }

    async fn watch_index_partition(
        &self,
        request: Request<WatchIndexPartitionRequest>,
    ) -> Result<Response<Self::WatchIndexPartitionStream>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        validate_index_name(&req.index_name)?;
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexWatch,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let index = self
            .persistence
            .get_index_definition(claims.tenant_id, bucket.id, &req.index_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .filter(|index| index.enabled)
            .ok_or_else(|| Status::not_found("Index definition not found"))?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let partition_id = if req.partition_id.trim().is_empty() {
            hex::encode(crate::formats::hash32(index_storage_id.as_bytes()))
        } else {
            validate_hex32(&req.partition_id, "partition_id")?;
            req.partition_id
        };
        let after_cursor = join_u128(req.after_cursor_low, req.after_cursor_high);
        let snapshot = index_partition_watch::list_index_partition_watch_events(
            &self.storage,
            claims.tenant_id,
            bucket.id,
            &index_storage_id,
            &partition_id,
            after_cursor,
            1000,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;

        let storage = self.storage.clone();
        let bucket_name = bucket.name.clone();
        let index_name = index.name.clone();
        let tenant_id = claims.tenant_id;
        let bucket_id = bucket.id;
        let (tx, rx) = mpsc::channel(32);
        tokio::spawn(async move {
            let mut last_cursor = after_cursor;
            for event in snapshot {
                last_cursor = last_cursor.max(event.cursor);
                if tx
                    .send(index_partition_event_response(
                        &bucket_name,
                        &index_name,
                        &index_storage_id,
                        &partition_id,
                        event,
                    ))
                    .await
                    .is_err()
                {
                    return;
                }
            }

            let mut interval = tokio::time::interval(Duration::from_millis(500));
            loop {
                interval.tick().await;
                let events = match index_partition_watch::list_index_partition_watch_events(
                    &storage,
                    tenant_id,
                    bucket_id,
                    &index_storage_id,
                    &partition_id,
                    last_cursor,
                    1000,
                )
                .await
                {
                    Ok(events) => events,
                    Err(error) => {
                        let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                };
                for event in events {
                    last_cursor = last_cursor.max(event.cursor);
                    if tx
                        .send(index_partition_event_response(
                            &bucket_name,
                            &index_name,
                            &index_storage_id,
                            &partition_id,
                            event,
                        ))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Ok(Response::new(
            Box::pin(ReceiverStream::new(rx)) as Self::WatchIndexPartitionStream
        ))
    }

    async fn list_index_diagnostics(
        &self,
        request: Request<ListIndexDiagnosticsRequest>,
    ) -> Result<Response<ListIndexDiagnosticsResponse>, Status> {
        let claims = request
            .extensions()
            .get::<auth::Claims>()
            .cloned()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?;
        let req = request.into_inner();
        if !req.index_name.is_empty() {
            validate_index_name(&req.index_name)?;
        }
        if !req.severity.is_empty() {
            validate_diagnostic_severity(&req.severity)?;
        }
        access_control::require_action(
            &self.storage,
            &self.persistence,
            &claims,
            AnvilAction::IndexRead,
            &req.bucket_name,
        )
        .await?;
        let bucket = self
            .get_index_bucket(claims.tenant_id, &req.bucket_name)
            .await?;
        let after_cursor = i64::try_from(req.after_cursor)
            .map_err(|_| Status::invalid_argument("after_cursor exceeds supported range"))?;
        let limit = i32::try_from(req.limit)
            .map_err(|_| Status::invalid_argument("limit exceeds supported range"))?;
        let diagnostics = self
            .persistence
            .list_index_diagnostics(
                claims.tenant_id,
                bucket.id,
                &req.index_name,
                &req.severity,
                after_cursor,
                limit,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .into_iter()
            .map(index_diagnostic_record)
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Response::new(ListIndexDiagnosticsResponse { diagnostics }))
    }
}
