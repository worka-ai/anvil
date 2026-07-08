use super::*;

impl AppState {
    pub(super) async fn get_index_bucket(
        &self,
        tenant_id: i64,
        bucket_name: &str,
    ) -> Result<crate::persistence::Bucket, Status> {
        if !validation::is_valid_bucket_name(bucket_name) {
            return Err(Status::invalid_argument("Invalid bucket name"));
        }
        bucket_journal::read_current_bucket(&self.storage, tenant_id, bucket_name)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| Status::not_found("Bucket not found"))
    }

    pub(super) async fn publish_index_definition_event(
        &self,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        event_type: &str,
    ) -> Result<crate::persistence::IndexDefinitionEvent, Status> {
        let event = self
            .persistence
            .create_index_definition_event(
                bucket.tenant_id,
                bucket.id,
                &bucket.name,
                index,
                event_type,
            )
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.index_watch_tx.send(event.clone());
        Ok(event)
    }

    pub(super) async fn plan_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        spec: &AnvilQuerySpec,
        accept_degraded: bool,
    ) -> Result<QuerySpecPlan, Status> {
        let shape = spec.shape()?;
        if shape.can_relation.as_deref().unwrap_or("read") != "read" {
            return Err(Status::failed_precondition(
                "QuerySpec only supports read authorisation relation for index results",
            ));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        if let Some(min_authz_revision) = shape.min_authz_revision
            && latest_authz_revision < min_authz_revision
        {
            return Err(Status::failed_precondition("AuthzRevisionLagging"));
        }
        let indexes = self
            .persistence
            .list_index_definitions(claims.tenant_id, bucket.id, false)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let selected = select_query_spec_indexes(&indexes, &shape, accept_degraded)?;
        if selected.primary.authorization_mode == "inherit_object" && shape.can_relation.is_none() {
            return Err(Status::failed_precondition(
                "QuerySpec requires an explicit can predicate for protected resources",
            ));
        }

        let plan_json = serde_json::json!({
            "schema": "anvil.query.plan.v1",
            "planner": if selected.typed_filter.is_some() {
                "primitive-index-intersection"
            } else {
                "primitive-index-single-stage"
            },
            "selected_index": {
                "name": selected.primary.name,
                "kind": selected.primary.kind,
                "version": selected.primary.version,
                "authorization_mode": selected.primary.authorization_mode,
            },
            "filter_index": selected.typed_filter.as_ref().map(|index| serde_json::json!({
                "name": index.name,
                "kind": index.kind,
                "version": index.version,
                "authorization_mode": index.authorization_mode,
            })),
            "source_kind": shape.source_kind,
            "path_prefix": shape.path_prefix,
            "typed_predicate_count": shape.typed_predicates.len(),
            "typed_order_count": shape.typed_order.len(),
            "uses_full_text": shape.query_text.is_some(),
            "uses_vector": shape.query_vector.is_some(),
            "authz_relation": shape.can_relation,
            "authz_revision": latest_authz_revision,
            "degraded": false,
        })
        .to_string();

        Ok(QuerySpecPlan {
            index: selected.primary,
            typed_filter_index: selected.typed_filter,
            canonical_query_hash: stable_json_hash(&spec.canonical_json()?),
            plan_json,
            diagnostics: Vec::new(),
            query_text: shape.query_text.unwrap_or_default(),
            query_vector: shape.query_vector.unwrap_or_default(),
            phrase: shape.phrase,
            path_prefix: shape.path_prefix.unwrap_or_default(),
            typed_predicates: shape.typed_predicates,
            typed_order: shape.typed_order,
            limit: shape.limit,
            require_caught_up_to_watch_cursor: shape
                .min_source_cursor
                .map(|cursor| cursor.to_string())
                .unwrap_or_default(),
        })
    }

    pub(super) fn index_page_token_signing_key(&self) -> Result<Vec<u8>, Status> {
        hex::decode(&self.config.anvil_secret_encryption_key)
            .map_err(|_| Status::internal("Invalid index page token signing key"))
    }

    pub(super) async fn execute_composite_query_spec(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        plan: &QuerySpecPlan,
        page_token: &str,
        lag_timeout_ms: u64,
    ) -> Result<QueryIndexResponse, Status> {
        let typed_filter_index = plan
            .typed_filter_index
            .as_ref()
            .ok_or_else(|| Status::internal("composite QuerySpec missing typed filter index"))?;
        let overfetch_limit = query_spec_overfetch_limit(plan.limit);
        let primary_req = QueryIndexRequest {
            bucket_name: bucket.name.clone(),
            index_name: plan.index.name.clone(),
            query_text: plan.query_text.clone(),
            query_vector: plan.query_vector.clone(),
            limit: overfetch_limit,
            phrase: plan.phrase,
            path_prefix: plan.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: String::new(),
            typed_order_json: String::new(),
            page_token: String::new(),
            require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        };
        let primary = match plan.index.kind.as_str() {
            "full_text" => {
                self.query_full_text_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "vector" => {
                self.query_vector_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            "hybrid" => {
                self.query_hybrid_index(claims, bucket, &plan.index, primary_req)
                    .await?
            }
            _ => {
                return Err(Status::failed_precondition(
                    "QuerySpec composite primary index must be full_text, vector or hybrid",
                ));
            }
        }
        .into_inner();

        let typed_req = QueryIndexRequest {
            bucket_name: bucket.name.clone(),
            index_name: typed_filter_index.name.clone(),
            query_text: String::new(),
            query_vector: Vec::new(),
            limit: overfetch_limit,
            phrase: false,
            path_prefix: plan.path_prefix.clone(),
            metadata_filters_json: String::new(),
            typed_predicates_json: serde_json::to_string(&plan.typed_predicates)
                .map_err(|e| Status::internal(e.to_string()))?,
            typed_order_json: serde_json::to_string(&plan.typed_order)
                .map_err(|e| Status::internal(e.to_string()))?,
            page_token: String::new(),
            require_caught_up_to_watch_cursor: plan.require_caught_up_to_watch_cursor.clone(),
            lag_timeout_ms,
        };
        let typed = self
            .query_typed_json_index(claims, bucket, typed_filter_index, typed_req)
            .await?
            .into_inner();

        let mut typed_by_version = BTreeMap::new();
        for hit in typed.hits {
            let typed_values = typed_values_from_query_hit(&hit)?;
            typed_by_version.insert(hit.object_version_id.clone(), typed_values);
        }

        let mut hits = Vec::new();
        for mut hit in primary.hits {
            let Some(typed_values) = typed_by_version.get(&hit.object_version_id) else {
                continue;
            };
            hit.metadata_json = merge_composite_metadata(&hit.metadata_json, typed_values)?;
            hits.push(hit);
        }

        if plan.typed_order.is_empty() {
            hits.sort_by(|left, right| {
                compare_score_hits(
                    left.score,
                    &left.object_version_id,
                    right.score,
                    &right.object_version_id,
                )
            });
        } else {
            hits.sort_by(|left, right| compare_query_spec_hits_by_typed_order(left, right, plan));
        }

        let authz_revision = primary.authz_revision.max(typed.authz_revision);
        let index_generation = primary.index_generation.max(typed.index_generation);
        let index_name = composite_query_spec_index_name(&plan.index, typed_filter_index);
        let predicate_hash = composite_query_spec_predicate_hash(
            plan,
            primary.index_generation,
            typed.index_generation,
        );
        let order_hash = composite_query_spec_order_hash(plan);
        let signing_key = self.index_page_token_signing_key()?;
        let index_definition_version =
            composite_index_definition_version(&plan.index, typed_filter_index);
        let binding = IndexPageTokenBinding::with_index_inputs(
            &self.config,
            claims,
            "query_spec_composite",
            &bucket.name,
            &index_name,
            index_generation,
            index_definition_version,
            vec![
                IndexPageTokenInput {
                    index_id: plan.index.name.clone(),
                    definition_hash: stable_string_hash(&format!(
                        "{}:{}",
                        plan.index.name,
                        plan.index.version.max(0)
                    )),
                    generation: primary.index_generation,
                },
                IndexPageTokenInput {
                    index_id: typed_filter_index.name.clone(),
                    definition_hash: stable_string_hash(&format!(
                        "{}:{}",
                        typed_filter_index.name,
                        typed_filter_index.version.max(0)
                    )),
                    generation: typed.index_generation,
                },
            ],
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let token = IndexPageToken::decode(page_token, &signing_key)?;
        if let Some(token) = &token {
            token.validate(&binding)?;
        }
        if let Some(token) = token.as_ref() {
            hits = hits
                .into_iter()
                .filter(|hit| query_spec_hit_after_cursor(hit, token, plan).unwrap_or(false))
                .collect();
        }

        let requested_limit = query_limit(plan.limit);
        let has_more = hits.len() > requested_limit;
        if has_more {
            hits.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            hits.last()
                .map(|hit| {
                    let last_sort_values = query_spec_hit_sort_values(hit, plan)?;
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        last_sort_values,
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(QueryIndexResponse {
            hits,
            index_kind: primary.index_kind,
            index_generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "query_spec_composite",
                "planner": "primitive-index-intersection",
                "primary_index": plan.index.name,
                "typed_filter_index": typed_filter_index.name,
                "overfetch_limit": overfetch_limit,
                "primary_scoring": serde_json::from_str::<JsonValue>(&primary.scoring_recipe_json)
                    .unwrap_or(JsonValue::String(primary.scoring_recipe_json)),
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: primary
                .source_watch_cursor_high
                .max(typed.source_watch_cursor_high),
            index_watch_cursor_applied: primary
                .index_watch_cursor_applied
                .min(typed.index_watch_cursor_applied),
            is_caught_up: primary.is_caught_up && typed.is_caught_up,
            lag_record_count_hint: primary
                .lag_record_count_hint
                .max(typed.lag_record_count_hint),
        })
    }

    pub(super) async fn query_full_text_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument("query_text is required"));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for full_text indexes",
            ));
        }
        let definition = full_text_definition(index)?;
        let filters = QueryFilters::from_request(&req)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let Some(segment) =
            full_text_segment::read_latest_full_text_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        else {
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let authz_revision = authz_revision.max(segment.header.authz_revision);
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let search_hits = search_query::query_full_text_segment(
            &segment,
            search_query::FullTextSegmentQuery {
                query: &req.query_text,
                tokenizer: &definition.tokenizer,
                positions_enabled: definition.positions_enabled,
                phrase: req.phrase,
                bm25: Bm25Config::default(),
                authorized_labels: permission_filter
                    .as_ref()
                    .map(|filter| &filter.authorized_labels),
                limit: segment.postings.len().max(1),
            },
        )
        .map_err(full_text_query_status)?;
        let predicate_hash = score_based_predicate_hash("full_text", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "full_text",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let mut candidates = Vec::with_capacity(search_hits.len());
        for hit in search_hits {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            let object_version_id = object_ref.object_version_id.clone();
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key: object_ref.object_key,
                object_version_id,
                document_id: hit.document_id,
                field_id: u32::from(hit.field_id),
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "matched_terms": hit.matched_terms,
                    "authz_label_hash": hex::encode(hit.authz_label_hash),
                })
                .to_string(),
            });
        }
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({"kind": "bm25", "k1": 1.2, "b": 0.75})
                .to_string(),
            next_page_token,
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
        }))
    }

    pub(super) async fn query_metadata_backed_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for path or metadata_filter indexes",
            ));
        }
        if !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_vector is not valid for path or metadata_filter indexes",
            ));
        }
        if index.kind == "metadata_filter" && req.metadata_filters_json.trim().is_empty() {
            return Err(Status::invalid_argument(
                "metadata_filters_json is required for metadata_filter indexes",
            ));
        }
        let filters = QueryFilters::from_request(&req)?;
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let latest_cursor = self
            .persistence
            .latest_object_watch_cursor(claims.tenant_id, bucket.id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };
        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let segment =
            typed_field_segment::read_latest_typed_field_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
        let Some(segment) = segment else {
            if latest_cursor == 0 {
                return Ok(Response::new(QueryIndexResponse {
                    hits: Vec::new(),
                    index_kind,
                    index_generation: 0,
                    authz_revision: 0,
                    scoring_recipe_json: serde_json::json!({
                        "kind": index.kind.as_str(),
                        "score": "constant",
                        "source": "corestore_typed_field_segment",
                    })
                    .to_string(),
                    next_page_token: String::new(),
                    source_watch_cursor_high: 0,
                    index_watch_cursor_applied: 0,
                    is_caught_up: true,
                    lag_record_count_hint: 0,
                }));
            }
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        if segment.header.source_kind != "object_metadata" {
            return Err(Status::failed_precondition(
                "MetadataBackedIndexSourceKindMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment.header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        let latest_authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        let authz_revision = latest_authz_revision.max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let predicate_hash = metadata_backed_predicate_hash(&index.kind, &req);
        let order_hash = stable_string_hash("object_key:asc,source_identity:asc");
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "metadata_backed",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let mut rows = Vec::new();
        for row in segment.rows {
            if validation::is_reserved_internal_key(&row.object_key) {
                continue;
            }
            let object_ref = QueryObjectRef::from_typed_field_row(&row)?;
            if !filters.matches(&object_ref)? {
                continue;
            }
            if let Some(permission_filter) = permission_filter.as_ref()
                && !permission_filter.allows_object_key(&object_ref.object_key)
            {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            rows.push((row.source_identity.clone(), object_ref));
        }

        rows.sort_by(|(left_source, left), (right_source, right)| {
            left.object_key
                .cmp(&right.object_key)
                .then(left_source.cmp(right_source))
        });
        if let Some(token) = page_token.as_ref() {
            rows.retain(|(source_identity, object_ref)| {
                object_key_after_cursor(
                    &object_ref.object_key,
                    source_identity,
                    &token.last_sort_values,
                    &token.last_source_identity,
                )
            });
        }

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|(source_identity, object_ref)| {
                    let mut sort_values = BTreeMap::new();
                    sort_values.insert(
                        "object_key".to_string(),
                        JsonValue::String(object_ref.object_key.clone()),
                    );
                    IndexPageToken::for_cursor(&binding, source_identity.clone(), sort_values)
                        .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let mut hits = Vec::with_capacity(rows.len());
        for (_, object_ref) in rows {
            let metadata_json = serde_json::json!({
                "bucket_name": bucket.name,
                "user_metadata": object_ref.user_meta.clone(),
                "created_at_nanos": object_ref.created_at_nanos,
                "authz_revision": object_ref.authz_revision,
            })
            .to_string();
            hits.push(IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json,
            });
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision: authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": index.kind.as_str(),
                "score": "constant",
                "source": "corestore_typed_field_segment",
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment.header.source_cursor,
            is_caught_up: segment.header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment.header.source_cursor),
        }))
    }

    pub(super) async fn query_typed_json_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.trim().is_empty() || !req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text and query_vector are not valid for typed_json indexes",
            ));
        }
        let definition = TypedJsonIndexDefinition::from_index(index)?;
        let predicates = TypedPredicate::parse_list(&req.typed_predicates_json)?;
        let order = TypedOrder::parse_list(&req.typed_order_json, &definition.default_order)?;
        let predicate_hash = stable_json_hash(&req.typed_predicates_json);
        let order_hash = stable_json_hash(&serde_json::to_string(&order).unwrap_or_default());
        let requested_limit = query_limit(req.limit);
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;

        let latest_cursor = if definition.source_kind == "append_record" {
            u64::try_from(
                crate::append_journal::append_record_source_cursor(
                    &self.storage,
                    claims.tenant_id,
                    bucket.id,
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?,
            )
            .unwrap_or(u64::MAX)
        } else {
            self.persistence
                .latest_object_watch_cursor(claims.tenant_id, bucket.id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .max(0) as u64
        };
        let required_cursor = if !req.require_caught_up_to_watch_cursor.trim().is_empty() {
            let required_cursor = req
                .require_caught_up_to_watch_cursor
                .parse::<u64>()
                .map_err(|_| {
                    Status::invalid_argument("Invalid require_caught_up_to_watch_cursor")
                })?;
            if latest_cursor < required_cursor {
                return Err(Status::failed_precondition("IndexLagging"));
            }
            Some(required_cursor)
        } else {
            None
        };

        let index_storage_id =
            index_journal::index_storage_id(bucket.tenant_id, bucket.id, index.id);
        let segment =
            typed_field_segment::read_latest_typed_field_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
                .ok_or_else(|| Status::failed_precondition("TypedJsonIndexNotMaterialised"))?;
        if segment.header.source_kind != definition.source_kind {
            return Err(Status::failed_precondition(
                "TypedJsonIndexSourceKindMismatch",
            ));
        }
        let expected_fields = definition
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<Vec<_>>();
        if segment
            .header
            .field_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
            != expected_fields
        {
            return Err(Status::failed_precondition(
                "TypedJsonIndexFieldSetMismatch",
            ));
        }
        if required_cursor.is_some_and(|cursor| segment.header.source_cursor < cursor) {
            return Err(Status::failed_precondition("IndexLagging"));
        }
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "typed_json",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }

        let mut rows = Vec::new();
        for row in segment.rows {
            if !req.path_prefix.trim().is_empty() && !row.object_key.starts_with(&req.path_prefix) {
                continue;
            }
            if let Some(permission_filter) = permission_filter.as_ref()
                && !permission_filter.allows_object_key(&row.object_key)
            {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &row.object_key,
                    row.authz_revision,
                )
                .await?
            {
                continue;
            }
            let row = TypedIndexRow::from_segment_row(row);
            if predicates.iter().all(|predicate| predicate.matches(&row)) {
                rows.push(row);
            }
        }

        rows.sort_by(|left, right| compare_typed_rows(left, right, &order));
        if let Some(token) = page_token.as_ref() {
            rows = rows
                .into_iter()
                .filter(|row| {
                    compare_typed_row_to_cursor(
                        row,
                        &token.last_sort_values,
                        &token.last_source_identity,
                        &order,
                    )
                    .is_gt()
                })
                .collect();
        }

        let has_more = rows.len() > requested_limit;
        if has_more {
            rows.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            rows.last()
                .map(|row| {
                    IndexPageToken::for_cursor(
                        &binding,
                        row.source_identity.clone(),
                        row.values.clone(),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let hits = rows
            .into_iter()
            .map(|row| IndexQueryHit {
                kind: index_kind,
                score: 1.0,
                object_key: row.object_key,
                object_version_id: row.object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: 0,
                chunk_id: 0,
                source_start: 0,
                source_len: 0,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "typed_values": row.values,
                })
                .to_string(),
            })
            .collect();

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "typed_json",
                "score": "constant",
                "source": definition.source_kind,
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: latest_cursor,
            index_watch_cursor_applied: segment.header.source_cursor,
            is_caught_up: segment.header.source_cursor >= latest_cursor,
            lag_record_count_hint: latest_cursor.saturating_sub(segment.header.source_cursor),
        }))
    }

    pub(super) async fn query_hybrid_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if req.query_text.trim().is_empty() && req.query_vector.is_empty() {
            return Err(Status::invalid_argument(
                "query_text or query_vector is required for hybrid indexes",
            ));
        }

        let requested_limit = query_limit(req.limit);
        let internal_limit = internal_candidate_limit_for_request(&req, &index.authorization_mode);
        let filters = QueryFilters::from_request(&req)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let mut combined = BTreeMap::<[u8; 16], HybridAccum>::new();
        let mut generation = 0;
        let mut text_generation = 0;
        let mut vector_generation = 0;
        let mut authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let has_text = !req.query_text.trim().is_empty();
        let has_vector = !req.query_vector.is_empty();

        if has_text {
            let definition = full_text_definition(index)?;
            let Some(segment) =
                full_text_segment::read_latest_full_text_segment(&self.storage, &index_storage_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            else {
                return Err(Status::failed_precondition("IndexUnavailable"));
            };
            text_generation = segment.header.generation;
            generation = generation.max(segment.header.generation);
            authz_revision = authz_revision.max(segment.header.authz_revision);
            let search_hits = search_query::query_full_text_segment(
                &segment,
                search_query::FullTextSegmentQuery {
                    query: &req.query_text,
                    tokenizer: &definition.tokenizer,
                    positions_enabled: definition.positions_enabled,
                    phrase: req.phrase,
                    bm25: Bm25Config::default(),
                    authorized_labels: permission_filter
                        .as_ref()
                        .map(|filter| &filter.authorized_labels),
                    limit: internal_limit,
                },
            )
            .map_err(full_text_query_status)?;
            for hit in search_hits {
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                entry.text_score += hit.score;
                entry.document_id = hit.document_id;
                entry.field_id = u32::from(hit.field_id);
            }
        }

        if has_vector {
            let Some(segment) =
                vector_segment::read_latest_vector_segment(&self.storage, &index_storage_id)
                    .await
                    .map_err(|e| Status::internal(e.to_string()))?
            else {
                return Err(Status::failed_precondition("IndexUnavailable"));
            };
            if req.query_vector.len() != usize::from(segment.header.dimension) {
                return Err(Status::invalid_argument("query_vector dimension mismatch"));
            }
            vector_generation = segment.header.generation;
            generation = generation.max(segment.header.generation);
            authz_revision = authz_revision.max(segment.header.authz_revision);
            let metric = VectorMetric::from_name(&segment.header.metric)
                .map_err(|e| Status::internal(e.to_string()))?;
            let search_hits = search_query::query_vector_segment(
                &segment,
                &req.query_vector,
                metric,
                permission_filter
                    .as_ref()
                    .map(|filter| &filter.authorized_labels),
                internal_limit,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
            for hit in search_hits {
                let entry = combined
                    .entry(hit.object_version_id)
                    .or_insert_with(|| HybridAccum::new(hit.object_version_id));
                entry.vector_score = entry.vector_score.max(hit.score);
                entry.vector_id = hit.vector_id;
                entry.chunk_id = hit.chunk_id;
                entry.source_start = hit.source_start;
                entry.source_len = hit.source_len;
            }
        }

        let (text_weight, vector_weight, freshness_weight) = match (has_text, has_vector) {
            (true, true) => (0.55, 0.35, 0.10),
            (true, false) => (1.0, 0.0, 0.0),
            (false, true) => (0.0, 1.0, 0.0),
            (false, false) => unreachable!("validated above"),
        };

        let mut candidates = Vec::new();
        for item in combined.into_values() {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, item.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            candidates.push(HybridCandidate { item, object_ref });
        }

        score_hybrid_candidates(
            &mut candidates,
            has_text,
            has_vector,
            text_weight,
            vector_weight,
            freshness_weight,
        );
        candidates.sort_by(|left, right| {
            right
                .item
                .score
                .partial_cmp(&left.item.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    left.item
                        .object_version_id
                        .cmp(&right.item.object_version_id)
                })
        });
        let predicate_hash = score_based_predicate_hash("hybrid", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "hybrid",
            &bucket.name,
            &index.name,
            generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for candidate in candidates {
                if score_after_cursor(
                    candidate.item.score,
                    &candidate.object_ref.object_version_id,
                    token,
                )? {
                    filtered.push(candidate);
                }
            }
            candidates = filtered;
        }
        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|candidate| {
                    IndexPageToken::for_cursor(
                        &binding,
                        candidate.object_ref.object_version_id.clone(),
                        score_sort_values(
                            candidate.item.score,
                            &candidate.object_ref.object_version_id,
                        ),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        let index_kind = index_kind_value_from_str(&index.kind)?;
        let mut hits = Vec::with_capacity(candidates.len().min(requested_limit));
        for candidate in candidates {
            let item = candidate.item;
            let object_ref = candidate.object_ref;
            hits.push(IndexQueryHit {
                kind: index_kind,
                score: item.score,
                object_key: object_ref.object_key,
                object_version_id: object_ref.object_version_id,
                document_id: item.document_id,
                field_id: item.field_id,
                vector_id: item.vector_id,
                chunk_id: item.chunk_id,
                source_start: item.source_start,
                source_len: item.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "text_score": item.text_score,
                    "vector_score": item.vector_score,
                    "freshness_score": item.freshness_score,
                    "normalized_text_score": item.normalized_text_score,
                    "normalized_vector_score": item.normalized_vector_score,
                })
                .to_string(),
            });
        }

        Ok(Response::new(QueryIndexResponse {
            hits,
            index_kind,
            index_generation: generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "hybrid",
                "text_weight": text_weight,
                "vector_weight": vector_weight,
                "freshness_weight": freshness_weight,
                "index_generations": {
                    "full_text": text_generation,
                    "vector": vector_generation,
                    "max": generation
                }
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
        }))
    }

    pub(super) async fn query_vector_index(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        req: QueryIndexRequest,
    ) -> Result<Response<QueryIndexResponse>, Status> {
        if !req.query_text.is_empty() {
            return Err(Status::invalid_argument(
                "query_text is not valid for vector indexes",
            ));
        }
        if req.query_vector.is_empty() {
            return Err(Status::invalid_argument("query_vector is required"));
        }
        let filters = QueryFilters::from_request(&req)?;
        let index_storage_id =
            index_journal::index_storage_id(index.tenant_id, index.bucket_id, index.id);
        let Some(segment) =
            vector_segment::read_latest_vector_segment(&self.storage, &index_storage_id)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
        else {
            return Err(Status::failed_precondition("IndexUnavailable"));
        };
        if req.query_vector.len() != usize::from(segment.header.dimension) {
            return Err(Status::invalid_argument("query_vector dimension mismatch"));
        }
        let metric = VectorMetric::from_name(&segment.header.metric)
            .map_err(|e| Status::internal(e.to_string()))?;
        let requested_limit = query_limit(req.limit);
        let index_kind = index_kind_value_from_str(&index.kind)?;
        let authz_revision = self
            .persistence
            .latest_authz_revision(claims.tenant_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .max(0) as u64;
        let authz_revision = authz_revision.max(segment.header.authz_revision);
        let permission_filter = self
            .query_permission_filter(claims, bucket, &index.authorization_mode, authz_revision)
            .await?;
        let predicate_hash = score_based_predicate_hash("vector", &req);
        let order_hash = score_order_hash();
        let signing_key = self.index_page_token_signing_key()?;
        let binding = IndexPageTokenBinding::single_index(
            &self.config,
            claims,
            "vector",
            &bucket.name,
            &index.name,
            segment.header.generation,
            index.version.max(0) as u64,
            authz_revision,
            predicate_hash.clone(),
            order_hash.clone(),
        );
        let page_token = IndexPageToken::decode(req.page_token.as_str(), &signing_key)?;
        if let Some(token) = &page_token {
            token.validate(&binding)?;
        }
        let search_hits = search_query::query_vector_segment(
            &segment,
            &req.query_vector,
            metric,
            permission_filter
                .as_ref()
                .map(|filter| &filter.authorized_labels),
            segment.entries.len(),
        )
        .map_err(|e| Status::internal(e.to_string()))?;
        let mut candidates = Vec::with_capacity(search_hits.len());
        for hit in search_hits {
            let object_ref = match self
                .object_ref_for_query_hit(bucket.id, hit.object_version_id)
                .await?
            {
                Some(object_ref) => object_ref,
                None if index.authorization_mode == "inherit_object" => continue,
                None => QueryObjectRef::default(),
            };
            if !filters.matches(&object_ref)? {
                continue;
            }
            if !self
                .query_hit_visible(
                    claims,
                    &index.authorization_mode,
                    &bucket.name,
                    &object_ref.object_key,
                    authz_revision,
                )
                .await?
            {
                continue;
            }
            let object_version_id = object_ref.object_version_id.clone();
            candidates.push(IndexQueryHit {
                kind: index_kind,
                score: hit.score,
                object_key: object_ref.object_key,
                object_version_id,
                document_id: 0,
                field_id: 0,
                vector_id: hit.vector_id,
                chunk_id: hit.chunk_id,
                source_start: hit.source_start,
                source_len: hit.source_len,
                metadata_json: serde_json::json!({
                    "bucket_name": bucket.name,
                    "metric": segment.header.metric,
                    "modality": segment.header.modality,
                })
                .to_string(),
            });
        }
        candidates.sort_by(|left, right| {
            compare_score_hits(
                left.score,
                &left.object_version_id,
                right.score,
                &right.object_version_id,
            )
        });
        if let Some(token) = page_token.as_ref() {
            let mut filtered = Vec::new();
            for hit in candidates {
                if score_after_cursor(hit.score, &hit.object_version_id, token)? {
                    filtered.push(hit);
                }
            }
            candidates = filtered;
        }

        let has_more = candidates.len() > requested_limit;
        if has_more {
            candidates.truncate(requested_limit);
        }
        let next_page_token = if has_more {
            candidates
                .last()
                .map(|hit| {
                    IndexPageToken::for_cursor(
                        &binding,
                        hit.object_version_id.clone(),
                        score_sort_values(hit.score, &hit.object_version_id),
                    )
                    .encode(&signing_key)
                })
                .transpose()?
                .unwrap_or_default()
        } else {
            String::new()
        };

        Ok(Response::new(QueryIndexResponse {
            hits: candidates,
            index_kind,
            index_generation: segment.header.generation,
            authz_revision,
            scoring_recipe_json: serde_json::json!({
                "kind": "vector",
                "metric": segment.header.metric,
                "max_candidate_multiplier": 20
            })
            .to_string(),
            next_page_token,
            source_watch_cursor_high: 0,
            index_watch_cursor_applied: 0,
            is_caught_up: true,
            lag_record_count_hint: 0,
        }))
    }

    pub(super) async fn object_ref_for_query_hit(
        &self,
        bucket_id: i64,
        version_bytes: [u8; 16],
    ) -> Result<Option<QueryObjectRef>, Status> {
        let version_id = uuid::Uuid::from_bytes(version_bytes);
        let object = self
            .persistence
            .get_object_version_by_id(bucket_id, version_id)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(object.map(|object| QueryObjectRef {
            object_version_id: version_id.to_string(),
            object_key: object.key,
            user_meta: object.user_meta,
            created_at_nanos: object.created_at.timestamp_nanos_opt().unwrap_or(0),
            authz_revision: object.authz_revision,
        }))
    }

    pub(super) async fn query_hit_visible(
        &self,
        claims: &auth::Claims,
        authorization_mode: &str,
        bucket_name: &str,
        object_key: &str,
        authz_revision: u64,
    ) -> Result<bool, Status> {
        match authorization_mode {
            "inherit_object" => {
                if object_key.is_empty() {
                    return Ok(false);
                }
                let object_resource = format!("{bucket_name}/{object_key}");
                if auth::is_authorized(AnvilAction::ObjectRead, &object_resource, &claims.scopes) {
                    return Ok(true);
                }
                let revision = i64::try_from(authz_revision)
                    .map_err(|_| Status::internal("Invalid authz revision"))?;
                access_control::relationship_allows(
                    &self.storage,
                    claims,
                    "object",
                    &object_resource,
                    "reader",
                    Some(revision),
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))
            }
            "index_only" | "public" => Ok(true),
            _ => Ok(false),
        }
    }

    pub(super) async fn query_permission_filter(
        &self,
        claims: &auth::Claims,
        bucket: &crate::persistence::Bucket,
        authorization_mode: &str,
        authz_revision: u64,
    ) -> Result<Option<QueryPermissionFilter>, Status> {
        if authorization_mode != "inherit_object" {
            return Ok(None);
        }
        if auth::is_authorized(
            AnvilAction::ObjectRead,
            &format!("{}/", bucket.name),
            &claims.scopes,
        ) {
            return Ok(None);
        }

        let mut object_keys = BTreeSet::new();
        let mut object_key_prefixes = BTreeSet::new();
        let mut grants_bucket_read = false;
        for scope in &claims.scopes {
            collect_object_scope(
                scope,
                &bucket.name,
                &mut object_keys,
                &mut object_key_prefixes,
                &mut grants_bucket_read,
            );
        }
        if grants_bucket_read {
            return Ok(None);
        }

        let revision = i64::try_from(authz_revision)
            .map_err(|_| Status::internal("Invalid authz revision"))?;
        let direct_objects = authz_journal::list_current_authz_objects_at_revision(
            &self.storage,
            claims.tenant_id,
            &encode_realm_namespace(DEFAULT_AUTHZ_REALM_ID, "object"),
            "reader",
            access_control::APP_SUBJECT_KIND,
            &claims.sub,
            "",
            revision,
        )
        .await
        .map_err(|e| Status::internal(e.to_string()))?;
        let bucket_prefix = format!("{}/", bucket.name);
        for object_id in direct_objects {
            if let Some(key) = object_id.strip_prefix(&bucket_prefix)
                && !key.is_empty()
            {
                object_keys.insert(key.to_string());
            }
        }

        let mut existing_keys = BTreeSet::new();
        let mut authorized_labels = BTreeSet::new();
        for key in &object_keys {
            let Some(object) = self
                .persistence
                .get_object(bucket.id, key)
                .await
                .map_err(|e| Status::internal(e.to_string()))?
            else {
                continue;
            };
            existing_keys.insert(key.clone());
            authorized_labels.insert(query_object_authz_label_hash(bucket, &object));
        }
        for prefix in &object_key_prefixes {
            let (objects, _) = self
                .persistence
                .list_objects(
                    bucket.id,
                    prefix,
                    "",
                    QUERY_PERMISSION_PREFIX_OBJECT_CAP.saturating_add(1),
                    "",
                )
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            if objects.len() > QUERY_PERMISSION_PREFIX_OBJECT_CAP as usize {
                return Err(Status::failed_precondition(
                    "AuthzPermissionSetTooLargeForPrefixScope",
                ));
            }
            for object in objects {
                existing_keys.insert(object.key.clone());
                authorized_labels.insert(query_object_authz_label_hash(bucket, &object));
            }
        }

        Ok(Some(QueryPermissionFilter {
            object_keys: existing_keys,
            authorized_labels,
        }))
    }
}
