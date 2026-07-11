use super::*;
use crate::query_planner::{
    AuthzCandidateReader, AuthzCandidateRequest, AuthzDecision, BoundaryCandidateReader,
    BoundaryCandidateRequest, CandidateSet, CandidateSetKind, CandidateSetScope, CoreDocId,
    IndexCandidateReader, IndexCandidateRequest, ObjectAuthzKey, OrderedDocTuple, QueryPlanRequest,
    RangePlanRequest, ReadRangePlan,
};
use std::collections::BTreeSet;

#[derive(Debug, Clone)]
pub(super) struct PlannerCandidateDoc {
    pub(super) doc_id: CoreDocId,
    pub(super) manifest_ref: String,
    pub(super) logical_start: u64,
    pub(super) logical_end: u64,
    pub(super) order_tuple: Vec<Vec<u8>>,
    pub(super) authz_key: ObjectAuthzKey,
}

#[derive(Debug, Clone)]
pub(super) struct PlannerCandidateSnapshot {
    pub(super) scope: CandidateSetScope,
    pub(super) index_candidates: CandidateSet,
    pub(super) boundary_candidates: CandidateSet,
    pub(super) docs: Vec<PlannerCandidateDoc>,
}

#[derive(Debug, Clone)]
pub(super) struct PlannerBoundaryCandidateAdapter {
    snapshot: PlannerCandidateSnapshot,
}

impl PlannerBoundaryCandidateAdapter {
    pub(super) fn new(snapshot: PlannerCandidateSnapshot) -> Self {
        Self { snapshot }
    }
}

impl BoundaryCandidateReader for PlannerBoundaryCandidateAdapter {
    async fn boundary_candidates(
        &self,
        request: BoundaryCandidateRequest,
    ) -> anyhow::Result<CandidateSet> {
        validate_boundary_request_scope(&self.snapshot.scope, &request)?;
        Ok(self.snapshot.boundary_candidates.clone())
    }
}

#[derive(Debug, Clone)]
pub(super) struct PlannerAuthzCandidateAdapter {
    storage: crate::storage::Storage,
    tenant_id: i64,
    authorization_mode: String,
    bucket: crate::persistence::Bucket,
    snapshot: PlannerCandidateSnapshot,
}

impl PlannerAuthzCandidateAdapter {
    pub(super) fn new(
        storage: crate::storage::Storage,
        tenant_id: i64,
        authorization_mode: impl Into<String>,
        bucket: crate::persistence::Bucket,
        snapshot: PlannerCandidateSnapshot,
    ) -> Self {
        Self {
            storage,
            tenant_id,
            authorization_mode: authorization_mode.into(),
            bucket,
            snapshot,
        }
    }
}

impl AuthzCandidateReader for PlannerAuthzCandidateAdapter {
    async fn candidate_set(&self, request: AuthzCandidateRequest) -> anyhow::Result<CandidateSet> {
        if self.authorization_mode != "inherit_object" {
            return Ok(CandidateSet::all_within_partition(
                request.candidate_scope,
                request.partition_id,
            ));
        }
        let allowance = self.inherited_object_allowance(&request).await?;
        if allowance.bucket_wide {
            return Ok(CandidateSet::all_within_partition(
                request.candidate_scope,
                request.partition_id,
            ));
        }
        let doc_ordinals = self
            .snapshot
            .docs
            .iter()
            .filter(|doc| {
                allowance
                    .object_ids
                    .contains(&doc.authz_key.canonical_object_id)
            })
            .map(|doc| doc.doc_id.ordinal());
        Ok(CandidateSet::bitmap_from_ordinals(
            request.candidate_scope,
            request.partition_id,
            doc_ordinals,
        ))
    }

    async fn verify_page(
        &self,
        request: AuthzCandidateRequest,
        object_keys: Vec<ObjectAuthzKey>,
    ) -> anyhow::Result<Vec<AuthzDecision>> {
        if self.authorization_mode != "inherit_object" {
            return Ok(object_keys
                .into_iter()
                .map(|object_key| AuthzDecision {
                    object_key,
                    allowed: true,
                    revision: request.revision,
                })
                .collect());
        }
        let allowance = self.inherited_object_allowance(&request).await?;
        Ok(object_keys
            .into_iter()
            .map(|object_key| {
                let allowed = allowance.bucket_wide
                    || allowance
                        .object_ids
                        .contains(&object_key.canonical_object_id);
                AuthzDecision {
                    object_key,
                    allowed,
                    revision: request.revision,
                }
            })
            .collect())
    }
}

#[derive(Debug, Clone)]
struct PlannerAuthzAllowance {
    bucket_wide: bool,
    object_ids: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct PlannerCandidateSubject {
    subject_kind: String,
    subject_id: String,
    caveat_hash: String,
}

impl PlannerAuthzCandidateAdapter {
    async fn inherited_object_allowance(
        &self,
        request: &AuthzCandidateRequest,
    ) -> anyhow::Result<PlannerAuthzAllowance> {
        let subject = parse_planner_candidate_subject(&request.subject);
        let mut object_ids = BTreeSet::new();
        let mut bucket_wide = false;

        let tenant_reader = crate::authz_segment::AuthzSegmentCandidateReader::new(
            self.storage.clone(),
            self.tenant_id,
        );
        let tenant_candidates = tenant_reader.candidate_set(request.clone()).await?;
        for doc in &self.snapshot.docs {
            let authz_doc_id = doc.authz_key.doc_id(request.partition_id);
            if tenant_candidates.contains_doc_id(authz_doc_id) {
                object_ids.insert(doc.authz_key.canonical_object_id.clone());
            }
        }

        if let Some(system_segment) = crate::authz_segment::read_latest_authz_tuple_segment(
            &self.storage,
            crate::system_realm::SYSTEM_STORAGE_TENANT_ID,
        )
        .await?
        {
            if system_segment.header.generation != request.system_revision {
                anyhow::bail!("AuthzCandidateSetStale");
            }
            let system_revision = request.system_revision;
            let system_object_namespace = access_control::system_realm_namespace(
                crate::system_realm::SYSTEM_OBJECT_NAMESPACE,
            );
            let system_bucket_namespace = access_control::system_realm_namespace(
                crate::system_realm::SYSTEM_BUCKET_NAMESPACE,
            );
            let bucket_object_id = access_control::bucket_object_id(&self.bucket);

            for row in &system_segment.list_objects {
                if !planner_authz_row_subject_matches(row, &subject)
                    || row.revision > system_revision
                {
                    continue;
                }
                if row.namespace == system_bucket_namespace
                    && row.relation == "get_object"
                    && row.object_id == bucket_object_id
                {
                    bucket_wide = true;
                } else if row.namespace == system_object_namespace && row.relation == "get" {
                    if let Some(default_object_id) =
                        self.default_object_id_for_system_object_id(&row.object_id)
                    {
                        object_ids.insert(default_object_id);
                    }
                }
            }
        } else if request.system_revision > 0 {
            anyhow::bail!("AuthzCandidateSetStale");
        }

        Ok(PlannerAuthzAllowance {
            bucket_wide,
            object_ids,
        })
    }

    fn default_object_id_for_system_object_id(&self, system_object_id: &str) -> Option<String> {
        let prefix = format!("{}/", self.bucket.id);
        let object_key = system_object_id.strip_prefix(&prefix)?;
        Some(format!("{}/{}", self.bucket.name, object_key))
    }
}

fn planner_authz_row_subject_matches(
    row: &crate::authz_segment::AuthzListObjectsRow,
    subject: &PlannerCandidateSubject,
) -> bool {
    row.subject_kind == subject.subject_kind
        && row.subject_id == subject.subject_id
        && row.caveat_hash == subject.caveat_hash
}

fn parse_planner_candidate_subject(subject: &str) -> PlannerCandidateSubject {
    let (subject_kind, rest) = subject
        .split_once(':')
        .map(|(kind, id)| (kind.to_string(), id.to_string()))
        .unwrap_or_else(|| ("user".to_string(), subject.to_string()));
    let (subject_id, caveat_hash) = rest
        .split_once('@')
        .map(|(id, caveat)| (id.to_string(), caveat.to_string()))
        .unwrap_or((rest, String::new()));
    PlannerCandidateSubject {
        subject_kind,
        subject_id,
        caveat_hash,
    }
}

#[derive(Debug, Clone)]
pub(super) struct PlannerIndexCandidateAdapter {
    snapshot: PlannerCandidateSnapshot,
}

impl PlannerIndexCandidateAdapter {
    pub(super) fn new(snapshot: PlannerCandidateSnapshot) -> Self {
        Self { snapshot }
    }
}

impl IndexCandidateReader for PlannerIndexCandidateAdapter {
    async fn predicate_candidates(
        &self,
        request: IndexCandidateRequest,
    ) -> anyhow::Result<CandidateSet> {
        validate_index_request_scope(&self.snapshot.scope, &request)?;
        Ok(self.snapshot.index_candidates.clone())
    }

    async fn range_plan(&self, request: RangePlanRequest) -> anyhow::Result<Vec<ReadRangePlan>> {
        self.snapshot.range_plan(request)
    }
}

pub(super) async fn execute_corestore_query_plan(
    storage: &crate::storage::Storage,
    claims: &auth::Claims,
    bucket: &crate::persistence::Bucket,
    authorization_mode: &str,
    authz_scope: &QueryAuthzScope,
    snapshot: &PlannerCandidateSnapshot,
    limit: usize,
) -> Result<crate::query_planner::QueryPlanResult, Status> {
    let boundary_reader = PlannerBoundaryCandidateAdapter::new(snapshot.clone());
    let authz_reader = PlannerAuthzCandidateAdapter::new(
        storage.clone(),
        claims.tenant_id,
        authorization_mode.to_string(),
        bucket.clone(),
        snapshot.clone(),
    );
    let index_reader = PlannerIndexCandidateAdapter::new(snapshot.clone());
    let planner = crate::query_planner::CoreStoreQueryPlanner {
        boundary_reader: &boundary_reader,
        authz_reader: &authz_reader,
        index_reader: &index_reader,
    };
    let request = snapshot.query_plan_request(
        authz_scope,
        access_control::APP_SUBJECT_KIND,
        &claims.sub,
        u32::try_from(limit).unwrap_or(u32::MAX),
        None,
    )?;
    planner
        .plan(request)
        .await
        .map_err(|e| Status::failed_precondition(e.to_string()))
}

impl PlannerCandidateSnapshot {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_index_query_hits(
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        index_generation: u64,
        root_generation: u64,
        authz_revision: u64,
        authz_scope: &QueryAuthzScope,
        predicate_hash: String,
        order_hash: String,
        boundary_schema_generation_hash: String,
        segment_ref: &str,
        hits: &[IndexQueryHit],
    ) -> Result<Self, Status> {
        let partition_id = planner_partition_id(bucket, index);
        let scope = planner_candidate_scope(
            bucket,
            index,
            index_generation,
            root_generation,
            authz_revision,
            authz_scope,
            predicate_hash,
            order_hash,
            boundary_schema_generation_hash,
        )?;
        let docs = hits
            .iter()
            .map(|hit| {
                let authz_key = ObjectAuthzKey::realm_object(
                    &authz_scope.object_namespace,
                    format!("{}/{}", bucket.name, hit.object_key),
                );
                let doc_id = source_doc_id(
                    partition_id,
                    "score_hit",
                    &authz_scope.object_namespace,
                    format!(
                        "{}/{}/{}/{}/{}/{}",
                        bucket.name,
                        hit.object_key,
                        hit.object_version_id,
                        hit.document_id,
                        hit.field_id,
                        hit.vector_id
                    ),
                );
                PlannerCandidateDoc {
                    doc_id,
                    manifest_ref: segment_ref.to_string(),
                    logical_start: u64::from(hit.field_id).saturating_add(hit.document_id),
                    logical_end: u64::from(hit.field_id)
                        .saturating_add(hit.document_id)
                        .saturating_add(1),
                    order_tuple: descending_score_tuple(
                        hit.score,
                        uuid::Uuid::parse_str(&hit.object_version_id)
                            .map(|id| *id.as_bytes())
                            .unwrap_or([0; 16]),
                    ),
                    authz_key,
                }
            })
            .collect::<Vec<_>>();
        Self::from_docs(scope, partition_id, docs, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_hybrid_candidates(
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        index_generation: u64,
        root_generation: u64,
        authz_revision: u64,
        authz_scope: &QueryAuthzScope,
        predicate_hash: String,
        order_hash: String,
        boundary_schema_generation_hash: String,
        segment_family_ref: &str,
        candidates: &[HybridCandidate],
    ) -> Result<Self, Status> {
        let partition_id = planner_partition_id(bucket, index);
        let scope = planner_candidate_scope(
            bucket,
            index,
            index_generation,
            root_generation,
            authz_revision,
            authz_scope,
            predicate_hash,
            order_hash,
            boundary_schema_generation_hash,
        )?;
        let docs = candidates
            .iter()
            .map(|candidate| {
                let authz_key = ObjectAuthzKey::realm_object(
                    &authz_scope.object_namespace,
                    format!("{}/{}", bucket.name, candidate.object_ref.object_key),
                );
                let doc_id = source_doc_id(
                    partition_id,
                    "hybrid",
                    &authz_scope.object_namespace,
                    format!(
                        "{}/{}/{}/{}/{}/{}",
                        bucket.name,
                        candidate.object_ref.object_key,
                        uuid::Uuid::from_bytes(candidate.item.object_version_id),
                        candidate.item.document_id,
                        candidate.item.field_id,
                        candidate.item.vector_id
                    ),
                );
                PlannerCandidateDoc {
                    doc_id,
                    manifest_ref: segment_family_ref.to_string(),
                    logical_start: candidate.item.document_id,
                    logical_end: candidate.item.document_id.saturating_add(1),
                    order_tuple: descending_score_tuple(
                        candidate.item.score,
                        candidate.item.object_version_id,
                    ),
                    authz_key,
                }
            })
            .collect::<Vec<_>>();
        Self::from_docs(scope, partition_id, docs, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_typed_value_entries(
        bucket: &crate::persistence::Bucket,
        index: &crate::persistence::IndexDefinition,
        index_generation: u64,
        root_generation: u64,
        authz_revision: u64,
        authz_scope: &QueryAuthzScope,
        predicate_hash: String,
        order_hash: String,
        boundary_schema_generation_hash: String,
        segment_ref: &str,
        entries: &[TypedValueCandidateEntry],
    ) -> Result<Self, Status> {
        let partition_id = planner_partition_id(bucket, index);
        let scope = planner_candidate_scope(
            bucket,
            index,
            index_generation,
            root_generation,
            authz_revision,
            authz_scope,
            predicate_hash,
            order_hash,
            boundary_schema_generation_hash,
        )?;
        let docs = entries
            .iter()
            .filter_map(|entry| {
                object_key_from_source_identity(&entry.source_identity).map(|object_key| {
                    let authz_key = ObjectAuthzKey::realm_object(
                        &authz_scope.object_namespace,
                        format!("{}/{}", bucket.name, object_key),
                    );
                    let doc_id = source_doc_id(
                        partition_id,
                        "typed_value",
                        &authz_scope.object_namespace,
                        format!("{}/{}", bucket.name, entry.source_identity),
                    );
                    PlannerCandidateDoc {
                        doc_id,
                        manifest_ref: segment_ref.to_string(),
                        logical_start: entry.row_ordinal as u64,
                        logical_end: entry.row_ordinal.saturating_add(1) as u64,
                        order_tuple: vec![entry.source_identity.as_bytes().to_vec()],
                        authz_key,
                    }
                })
            })
            .collect::<Vec<_>>();
        Self::from_docs(scope, partition_id, docs, None)
    }

    fn from_docs(
        scope: CandidateSetScope,
        partition_id: u64,
        docs: Vec<PlannerCandidateDoc>,
        boundary_candidates: Option<CandidateSet>,
    ) -> Result<Self, Status> {
        let index_candidates = ordered_candidate_set_from_docs(scope.clone(), partition_id, &docs);
        if matches!(
            index_candidates.kind,
            CandidateSetKind::AllWithinPartition { .. }
        ) {
            return Err(Status::failed_precondition("IndexCapabilityMissing"));
        }
        let boundary_candidates = boundary_candidates.unwrap_or(CandidateSet {
            scope: scope.clone(),
            kind: CandidateSetKind::AllWithinPartition { partition_id },
        });
        Ok(Self {
            scope,
            index_candidates,
            boundary_candidates,
            docs,
        })
    }

    pub(super) fn query_plan_request(
        &self,
        authz_scope: &QueryAuthzScope,
        subject_kind: &str,
        subject_id: &str,
        limit: u32,
        page_token: Option<String>,
    ) -> Result<QueryPlanRequest, Status> {
        let partition_id = self
            .index_candidates
            .partition_id()
            .ok_or_else(|| Status::failed_precondition("IndexCapabilityMissing"))?;
        Ok(QueryPlanRequest {
            boundary: BoundaryCandidateRequest {
                root_key_hash: self.scope.root_key_hash.clone(),
                root_generation: self.scope.root_generation,
                bucket_name: String::new(),
                boundary_schema_generation_hash: self.scope.boundary_schema_generation_hash.clone(),
                boundary_predicate_json: String::new(),
            },
            authz: AuthzCandidateRequest {
                authz_scope: authz_scope.scope_hash.clone(),
                candidate_scope: self.scope.clone(),
                partition_id,
                subject: format!("{subject_kind}:{subject_id}"),
                relation: authz_scope.relation.clone(),
                object_namespace: authz_scope.object_namespace.clone(),
                revision: authz_scope.revision,
                system_revision: authz_scope.system_revision,
                root_generation: self.scope.root_generation,
            },
            index: IndexCandidateRequest {
                index_id: self.scope.index_id.clone(),
                predicate_json: self.scope.predicate_hash.clone(),
                order_json: Some(self.scope.order_hash.clone()),
                generation: self.scope.index_generation,
                boundary_predicate_json: None,
            },
            limit,
            page_token,
        })
    }

    pub(super) fn selected_object_ids(
        &self,
        candidates: &CandidateSet,
    ) -> Result<BTreeSet<String>, Status> {
        self.doc_ids_from_candidates(candidates)
            .map(|doc_ids| {
                doc_ids
                    .into_iter()
                    .filter_map(|doc_id| {
                        self.docs
                            .iter()
                            .find(|doc| doc.doc_id == doc_id)
                            .map(|doc| doc.authz_key.canonical_object_id.clone())
                    })
                    .collect()
            })
            .map_err(|e| Status::internal(e.to_string()))
    }

    fn range_plan(&self, request: RangePlanRequest) -> anyhow::Result<Vec<ReadRangePlan>> {
        let selected = self.doc_ids_from_candidates(&request.candidates)?;
        let limit = usize::try_from(request.limit).unwrap_or(usize::MAX);
        let mut ranges = Vec::new();
        for doc_id in selected.into_iter().take(limit) {
            let Some(doc) = self.docs.iter().find(|doc| doc.doc_id == doc_id) else {
                anyhow::bail!("IndexCandidateMissingRange");
            };
            ranges.push(ReadRangePlan {
                manifest_hash: stable_string_hash(&doc.manifest_ref),
                logical_start: doc.logical_start,
                logical_end: doc.logical_end,
                doc_ids: vec![doc.doc_id],
                authz_keys: vec![doc.authz_key.clone()],
            });
        }
        Ok(ranges)
    }

    fn doc_ids_from_candidates(&self, candidates: &CandidateSet) -> anyhow::Result<Vec<CoreDocId>> {
        candidates.scope.validate()?;
        if !self.scope.compatible_with(&candidates.scope) {
            anyhow::bail!("IndexGenerationMismatch");
        }
        let mut selected = Vec::new();
        for doc in &self.docs {
            if candidate_set_contains(candidates, doc.doc_id)? {
                selected.push(doc.doc_id);
            }
        }
        Ok(selected)
    }
}

pub(super) fn ensure_planner_supported_query_shape(
    index_kind: &str,
    req: &QueryIndexRequest,
) -> Result<(), Status> {
    match index_kind {
        "full_text" | "vector" | "hybrid" => {
            let score_only = matches!(index_kind, "full_text" | "vector");
            if (score_only
                && (!req.path_prefix.trim().is_empty()
                    || query_json_field_has_terms(&req.metadata_filters_json)))
                || query_json_field_has_terms(&req.boundary_predicates_json)
                || query_json_field_has_terms(&req.typed_predicates_json)
                || query_json_field_has_terms(&req.typed_order_json)
            {
                return Err(Status::failed_precondition("IndexCapabilityMissing"));
            }
        }
        "path" => {
            if req.path_prefix.trim().is_empty() {
                return Err(Status::failed_precondition("IndexCapabilityMissing"));
            }
            if query_json_field_has_terms(&req.metadata_filters_json)
                || query_json_field_has_terms(&req.boundary_predicates_json)
                || query_json_field_has_terms(&req.typed_predicates_json)
                || query_json_field_has_terms(&req.typed_order_json)
            {
                return Err(Status::failed_precondition("IndexCapabilityMissing"));
            }
        }
        "metadata_filter" => {
            if !query_json_field_has_terms(&req.metadata_filters_json) {
                return Err(Status::failed_precondition("IndexCapabilityMissing"));
            }
        }
        "typed_json" => {
            if !query_json_field_has_terms(&req.typed_predicates_json)
                && !query_json_field_has_terms(&req.boundary_predicates_json)
                && req.path_prefix.trim().is_empty()
            {
                return Err(Status::failed_precondition("IndexCapabilityMissing"));
            }
        }
        _ => return Err(Status::failed_precondition("IndexCapabilityMissing")),
    }
    Ok(())
}

fn query_json_field_has_terms(raw: &str) -> bool {
    let trimmed = raw.trim();
    !matches!(trimmed, "" | "[]" | "{}" | "null")
}

#[allow(clippy::too_many_arguments)]
fn planner_candidate_scope(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
    index_generation: u64,
    root_generation: u64,
    _authz_revision: u64,
    authz_scope: &QueryAuthzScope,
    predicate_hash: String,
    order_hash: String,
    boundary_schema_generation_hash: String,
) -> Result<CandidateSetScope, Status> {
    let scope = CandidateSetScope {
        root_key_hash: authz_aware_query_scope_hash(
            "root",
            authz_scope,
            serde_json::json!({
                "schema": "anvil.query.root_key.v1",
                "tenant_id": bucket.tenant_id,
                "bucket_id": bucket.id,
                "bucket_name": bucket.name,
                "index_id": index.id,
                "index_name": index.name,
            }),
        ),
        root_generation,
        index_id: index.name.clone(),
        index_generation,
        authz_realm_id: authz_scope.realm_id.clone(),
        authz_scope_hash: authz_scope.scope_hash.clone(),
        authz_object_namespace: authz_scope.object_namespace.clone(),
        authz_relation: authz_scope.relation.clone(),
        authz_principal_hash: authz_scope.principal_hash.clone(),
        authz_revision: authz_scope.revision_fence(),
        boundary_schema_generation_hash,
        predicate_hash,
        order_hash,
    };
    scope
        .validate()
        .map_err(|e| Status::internal(format!("Invalid planner candidate scope: {e}")))?;
    Ok(scope)
}

fn planner_partition_id(
    bucket: &crate::persistence::Bucket,
    index: &crate::persistence::IndexDefinition,
) -> u64 {
    let hash = hash32(
        format!(
            "tenant:{}:bucket:{}:index:{}",
            bucket.tenant_id, bucket.id, index.id
        )
        .as_bytes(),
    );
    u64::from_le_bytes(hash[0..8].try_into().expect("hash prefix is eight bytes"))
}

fn ordered_candidate_set_from_docs(
    scope: CandidateSetScope,
    partition_id: u64,
    docs: &[PlannerCandidateDoc],
) -> CandidateSet {
    if docs.is_empty() {
        return CandidateSet::empty(scope);
    }
    CandidateSet {
        scope,
        kind: CandidateSetKind::OrderedTuples {
            partition_id,
            tuples: docs
                .iter()
                .map(|doc| OrderedDocTuple {
                    order_tuple: doc.order_tuple.clone(),
                    doc_id: doc.doc_id,
                })
                .collect(),
        },
    }
}

fn candidate_set_contains(candidates: &CandidateSet, doc_id: CoreDocId) -> anyhow::Result<bool> {
    match &candidates.kind {
        CandidateSetKind::Empty => Ok(false),
        CandidateSetKind::AllWithinPartition { partition_id } => {
            Ok(doc_id.partition_id() == *partition_id)
        }
        CandidateSetKind::Bitmap {
            partition_id,
            ordinal_bitmap_bytes,
        } => {
            if doc_id.partition_id() != *partition_id {
                return Ok(false);
            }
            if ordinal_bitmap_bytes.len() % 8 != 0 {
                anyhow::bail!("IndexCandidateBitmapCorrupt");
            }
            Ok(ordinal_bitmap_bytes
                .chunks_exact(8)
                .any(|chunk| u64::from_le_bytes(chunk.try_into().unwrap()) == doc_id.ordinal()))
        }
        CandidateSetKind::SortedDocIdRanges { ranges, .. } => {
            Ok(ranges.iter().any(|range| range.contains(doc_id)))
        }
        CandidateSetKind::OrderedTuples { tuples, .. } => {
            Ok(tuples.iter().any(|tuple| tuple.doc_id == doc_id))
        }
    }
}

fn object_key_from_source_identity(source_identity: &str) -> Option<&str> {
    if let Some((object_key, _)) = source_identity.split_once('#') {
        return (!object_key.is_empty()).then_some(object_key);
    }
    (!source_identity.is_empty()).then_some(source_identity)
}

fn source_doc_id(
    partition_id: u64,
    source_kind: &str,
    namespace: &str,
    source_identity: impl AsRef<str>,
) -> CoreDocId {
    CoreDocId::new(
        partition_id,
        crate::query_planner::stable_doc_ordinal(&[
            namespace,
            source_kind,
            source_identity.as_ref(),
        ]),
    )
}

fn validate_boundary_request_scope(
    scope: &CandidateSetScope,
    request: &BoundaryCandidateRequest,
) -> anyhow::Result<()> {
    if scope.root_key_hash != request.root_key_hash
        || scope.root_generation != request.root_generation
        || scope.boundary_schema_generation_hash != request.boundary_schema_generation_hash
    {
        anyhow::bail!("IndexGenerationMismatch");
    }
    Ok(())
}

fn validate_index_request_scope(
    scope: &CandidateSetScope,
    request: &IndexCandidateRequest,
) -> anyhow::Result<()> {
    if scope.index_id != request.index_id || scope.index_generation != request.generation {
        anyhow::bail!("IndexGenerationMismatch");
    }
    let predicate_hash = request_or_json_hash(&request.predicate_json, "predicate_json")?;
    if scope.predicate_hash != predicate_hash {
        anyhow::bail!("IndexGenerationMismatch");
    }
    if let Some(order_json) = request.order_json.as_ref() {
        let order_hash = request_or_json_hash(order_json, "order_json")?;
        if scope.order_hash != order_hash {
            anyhow::bail!("IndexGenerationMismatch");
        }
    }
    Ok(())
}

fn request_or_json_hash(raw: &str, field_name: &str) -> anyhow::Result<String> {
    if ensure_algorithm_prefixed_hash(raw, field_name).is_ok() {
        return Ok(raw.to_string());
    }
    stable_json_hash_checked(raw, field_name).map_err(|e| anyhow::anyhow!(e.to_string()))
}

fn descending_score_tuple(score: f32, object_version_id: [u8; 16]) -> Vec<Vec<u8>> {
    let mut score_key = (u32::MAX - score.to_bits()).to_be_bytes().to_vec();
    if score.is_nan() {
        score_key = u32::MAX.to_be_bytes().to_vec();
    }
    vec![score_key, object_version_id.to_vec()]
}
