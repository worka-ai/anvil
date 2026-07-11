use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Stable document identity shared by object, stream, registry, PersonalDB, and
/// control-plane index writers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct CoreDocId(pub u128);

impl CoreDocId {
    pub fn new(partition_id: u64, ordinal: u64) -> Self {
        Self(((partition_id as u128) << 64) | ordinal as u128)
    }

    pub fn partition_id(self) -> u64 {
        (self.0 >> 64) as u64
    }

    pub fn ordinal(self) -> u64 {
        self.0 as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateSetScope {
    pub root_key_hash: String,
    pub root_generation: u64,
    pub index_id: String,
    pub index_generation: u64,
    pub authz_realm_id: String,
    pub authz_scope_hash: String,
    pub authz_object_namespace: String,
    pub authz_relation: String,
    pub authz_principal_hash: String,
    pub authz_revision: u64,
    pub boundary_schema_generation_hash: String,
    pub predicate_hash: String,
    pub order_hash: String,
}

impl CandidateSetScope {
    pub fn compatible_with(&self, other: &Self) -> bool {
        self.root_key_hash == other.root_key_hash
            && self.root_generation == other.root_generation
            && self.index_id == other.index_id
            && self.index_generation == other.index_generation
            && self.authz_realm_id == other.authz_realm_id
            && self.authz_scope_hash == other.authz_scope_hash
            && self.authz_object_namespace == other.authz_object_namespace
            && self.authz_relation == other.authz_relation
            && self.authz_principal_hash == other.authz_principal_hash
            && self.authz_revision == other.authz_revision
            && self.boundary_schema_generation_hash == other.boundary_schema_generation_hash
            && self.predicate_hash == other.predicate_hash
            && self.order_hash == other.order_hash
    }

    pub fn validate(&self) -> Result<()> {
        ensure_algorithm_prefixed_hash(&self.root_key_hash, "root_key_hash")?;
        ensure_algorithm_prefixed_hash(&self.authz_scope_hash, "authz_scope_hash")?;
        ensure_algorithm_prefixed_hash(&self.authz_principal_hash, "authz_principal_hash")?;
        ensure_algorithm_prefixed_hash(
            &self.boundary_schema_generation_hash,
            "boundary_schema_generation_hash",
        )?;
        ensure_algorithm_prefixed_hash(&self.predicate_hash, "predicate_hash")?;
        ensure_algorithm_prefixed_hash(&self.order_hash, "order_hash")?;
        if self.authz_realm_id.trim().is_empty()
            || self.authz_object_namespace.trim().is_empty()
            || self.authz_relation.trim().is_empty()
        {
            bail!("AuthzScopeMissing");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocIdRange {
    pub start_inclusive: CoreDocId,
    pub end_exclusive: CoreDocId,
}

impl DocIdRange {
    pub fn contains(&self, doc_id: CoreDocId) -> bool {
        self.start_inclusive <= doc_id && doc_id < self.end_exclusive
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedDocTuple {
    pub order_tuple: Vec<Vec<u8>>,
    pub doc_id: CoreDocId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateSetKind {
    Empty,
    AllWithinPartition {
        partition_id: u64,
    },
    Bitmap {
        partition_id: u64,
        roaring_bitmap_bytes: Vec<u8>,
    },
    SortedDocIdRanges {
        partition_id: u64,
        ranges: Vec<DocIdRange>,
    },
    OrderedTuples {
        partition_id: u64,
        tuples: Vec<OrderedDocTuple>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CandidateSet {
    pub scope: CandidateSetScope,
    pub kind: CandidateSetKind,
}

impl CandidateSet {
    pub fn empty(scope: CandidateSetScope) -> Self {
        Self {
            scope,
            kind: CandidateSetKind::Empty,
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.kind, CandidateSetKind::Empty)
    }

    pub fn estimated_count(&self) -> u64 {
        match &self.kind {
            CandidateSetKind::Empty => 0,
            CandidateSetKind::AllWithinPartition { .. } => u64::MAX,
            CandidateSetKind::Bitmap {
                roaring_bitmap_bytes,
                ..
            } => decode_bitmap_ordinals(roaring_bitmap_bytes)
                .map(|ordinals| ordinals.len() as u64)
                .unwrap_or(0),
            CandidateSetKind::SortedDocIdRanges { ranges, .. } => ranges
                .iter()
                .map(|range| {
                    range
                        .end_exclusive
                        .0
                        .saturating_sub(range.start_inclusive.0)
                })
                .sum::<u128>()
                .min(u64::MAX as u128)
                as u64,
            CandidateSetKind::OrderedTuples { tuples, .. } => tuples.len() as u64,
        }
    }

    pub fn partition_id(&self) -> Option<u64> {
        match &self.kind {
            CandidateSetKind::Empty => None,
            CandidateSetKind::AllWithinPartition { partition_id }
            | CandidateSetKind::Bitmap { partition_id, .. }
            | CandidateSetKind::SortedDocIdRanges { partition_id, .. }
            | CandidateSetKind::OrderedTuples { partition_id, .. } => Some(*partition_id),
        }
    }

    pub fn intersect(&self, other: &Self) -> Result<Self> {
        self.scope.validate()?;
        other.scope.validate()?;
        if !self.scope.compatible_with(&other.scope) {
            bail!("IndexGenerationMismatch");
        }
        if self.is_empty() || other.is_empty() {
            return Ok(Self::empty(self.scope.clone()));
        }
        if self.partition_id() != other.partition_id() {
            return Ok(Self::empty(self.scope.clone()));
        }
        match (&self.kind, &other.kind) {
            (CandidateSetKind::Empty, CandidateSetKind::Empty) => {
                Ok(Self::empty(self.scope.clone()))
            }
            (CandidateSetKind::AllWithinPartition { .. }, _) => Ok(other.clone()),
            (_, CandidateSetKind::AllWithinPartition { .. }) => Ok(self.clone()),
            (
                CandidateSetKind::SortedDocIdRanges {
                    partition_id: left_partition,
                    ranges: left_ranges,
                },
                CandidateSetKind::SortedDocIdRanges {
                    ranges: right_ranges,
                    ..
                },
            ) => Ok(Self {
                scope: self.scope.clone(),
                kind: CandidateSetKind::SortedDocIdRanges {
                    partition_id: *left_partition,
                    ranges: intersect_ranges(left_ranges, right_ranges),
                },
            }),
            (
                CandidateSetKind::OrderedTuples {
                    partition_id,
                    tuples,
                },
                rhs,
            ) => Ok(Self::ordered_subset(
                self.scope.clone(),
                *partition_id,
                tuples,
                rhs,
            )),
            (
                lhs,
                CandidateSetKind::OrderedTuples {
                    partition_id,
                    tuples,
                },
            ) => Ok(Self::ordered_subset(
                self.scope.clone(),
                *partition_id,
                tuples,
                lhs,
            )),
            (
                CandidateSetKind::Bitmap {
                    partition_id: left_partition,
                    roaring_bitmap_bytes: left,
                },
                CandidateSetKind::Bitmap {
                    roaring_bitmap_bytes: right,
                    ..
                },
            ) => {
                let left = decode_bitmap_ordinals(left)?;
                let right = decode_bitmap_ordinals(right)?;
                let ordinals = left.intersection(&right).copied().collect::<Vec<_>>();
                Ok(Self::bitmap_from_ordinals(
                    self.scope.clone(),
                    *left_partition,
                    ordinals,
                ))
            }
            (CandidateSetKind::Bitmap { partition_id, .. }, rhs)
            | (rhs, CandidateSetKind::Bitmap { partition_id, .. }) => {
                let ordinals = candidate_kind_ordinals(rhs)?;
                let filtered = candidate_kind_ordinals(&self.kind)?
                    .intersection(&ordinals)
                    .copied()
                    .collect::<Vec<_>>();
                Ok(Self::bitmap_from_ordinals(
                    self.scope.clone(),
                    *partition_id,
                    filtered,
                ))
            }
            (CandidateSetKind::SortedDocIdRanges { partition_id, .. }, rhs)
            | (rhs, CandidateSetKind::SortedDocIdRanges { partition_id, .. }) => {
                let lhs_ordinals = candidate_kind_ordinals(&self.kind)?;
                let rhs_ordinals = candidate_kind_ordinals(rhs)?;
                let filtered = lhs_ordinals
                    .intersection(&rhs_ordinals)
                    .copied()
                    .collect::<Vec<_>>();
                Ok(Self::bitmap_from_ordinals(
                    self.scope.clone(),
                    *partition_id,
                    filtered,
                ))
            }
        }
    }

    fn bitmap_from_ordinals(
        scope: CandidateSetScope,
        partition_id: u64,
        ordinals: Vec<u64>,
    ) -> Self {
        if ordinals.is_empty() {
            return Self::empty(scope);
        }
        let mut bytes = Vec::with_capacity(ordinals.len() * 8);
        for ordinal in ordinals {
            bytes.extend_from_slice(&ordinal.to_le_bytes());
        }
        Self {
            scope,
            kind: CandidateSetKind::Bitmap {
                partition_id,
                roaring_bitmap_bytes: bytes,
            },
        }
    }

    fn ordered_subset(
        scope: CandidateSetScope,
        partition_id: u64,
        tuples: &[OrderedDocTuple],
        filter: &CandidateSetKind,
    ) -> Self {
        let tuples = tuples
            .iter()
            .filter(|tuple| candidate_kind_contains(filter, tuple.doc_id))
            .cloned()
            .collect::<Vec<_>>();
        if tuples.is_empty() {
            Self::empty(scope)
        } else {
            Self {
                scope,
                kind: CandidateSetKind::OrderedTuples {
                    partition_id,
                    tuples,
                },
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectAuthzKey {
    pub namespace: String,
    pub canonical_object_id: String,
}

impl ObjectAuthzKey {
    pub fn object(
        bucket_id: impl AsRef<str>,
        object_key: impl AsRef<str>,
        version: impl AsRef<str>,
    ) -> Self {
        Self {
            namespace: "object".to_string(),
            canonical_object_id: format!(
                "{}/{}/{}",
                bucket_id.as_ref(),
                object_key.as_ref(),
                version.as_ref()
            ),
        }
    }

    pub fn index_doc(index_id: impl AsRef<str>, doc_id: CoreDocId) -> Self {
        Self {
            namespace: "index_doc".to_string(),
            canonical_object_id: format!("{}/{}", index_id.as_ref(), doc_id.0),
        }
    }

    pub fn registry(
        registry_kind: impl AsRef<str>,
        namespace: impl AsRef<str>,
        package: impl AsRef<str>,
        version_or_digest: impl AsRef<str>,
    ) -> Self {
        Self {
            namespace: "registry".to_string(),
            canonical_object_id: format!(
                "{}/{}/{}/{}",
                registry_kind.as_ref(),
                namespace.as_ref(),
                package.as_ref(),
                version_or_digest.as_ref()
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzCandidateRequest {
    pub authz_scope: String,
    pub subject: String,
    pub relation: String,
    pub object_namespace: String,
    pub revision: u64,
    pub root_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexCandidateRequest {
    pub index_id: String,
    pub predicate_json: String,
    pub order_json: Option<String>,
    pub generation: u64,
    pub boundary_predicate_json: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryCandidateRequest {
    pub root_key_hash: String,
    pub root_generation: u64,
    pub bucket_name: String,
    pub boundary_schema_generation_hash: String,
    pub boundary_predicate_json: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RangePlanRequest {
    pub candidates: CandidateSet,
    pub limit: u32,
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadRangePlan {
    pub manifest_hash: String,
    pub logical_start: u64,
    pub logical_end: u64,
    pub doc_ids: Vec<CoreDocId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuthzDecision {
    pub object_key: ObjectAuthzKey,
    pub allowed: bool,
    pub revision: u64,
}

pub trait AuthzCandidateReader {
    async fn candidate_set(&self, request: AuthzCandidateRequest) -> Result<CandidateSet>;

    async fn verify_page(
        &self,
        request: AuthzCandidateRequest,
        object_keys: Vec<ObjectAuthzKey>,
    ) -> Result<Vec<AuthzDecision>>;
}

pub trait BoundaryCandidateReader {
    async fn boundary_candidates(&self, request: BoundaryCandidateRequest) -> Result<CandidateSet>;
}

pub trait IndexCandidateReader {
    async fn predicate_candidates(&self, request: IndexCandidateRequest) -> Result<CandidateSet>;

    async fn range_plan(&self, request: RangePlanRequest) -> Result<Vec<ReadRangePlan>>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanRequest {
    pub boundary: BoundaryCandidateRequest,
    pub authz: AuthzCandidateRequest,
    pub index: IndexCandidateRequest,
    pub limit: u32,
    pub page_token: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanResult {
    pub candidates: CandidateSet,
    pub ranges: Vec<ReadRangePlan>,
    pub final_authz: Vec<AuthzDecision>,
    pub metrics: QueryPlanMetrics,
}

pub struct CoreStoreQueryPlanner<'a, B, A, I> {
    pub boundary_reader: &'a B,
    pub authz_reader: &'a A,
    pub index_reader: &'a I,
}

impl<'a, B, A, I> CoreStoreQueryPlanner<'a, B, A, I>
where
    B: BoundaryCandidateReader + Sync,
    A: AuthzCandidateReader + Sync,
    I: IndexCandidateReader + Sync,
{
    pub async fn plan(&self, request: QueryPlanRequest) -> Result<QueryPlanResult> {
        let boundary_candidates = self
            .boundary_reader
            .boundary_candidates(request.boundary.clone())
            .await?;
        let authz_candidates = self
            .authz_reader
            .candidate_set(request.authz.clone())
            .await?;
        let index_candidates = self
            .index_reader
            .predicate_candidates(request.index.clone())
            .await?;
        if matches!(
            index_candidates.kind,
            CandidateSetKind::AllWithinPartition { .. }
        ) {
            bail!("IndexCapabilityMissing");
        }

        let boundary_count = boundary_candidates.estimated_count();
        let authz_count = authz_candidates.estimated_count();
        let index_count = index_candidates.estimated_count();
        let intersection = index_candidates
            .intersect(&boundary_candidates)?
            .intersect(&authz_candidates)?;
        let intersection_count = intersection.estimated_count();
        let ranges = self
            .index_reader
            .range_plan(RangePlanRequest {
                candidates: intersection.clone(),
                limit: request.limit,
                page_token: request.page_token,
            })
            .await?;
        let authz_keys = ranges
            .iter()
            .flat_map(|range| {
                range
                    .doc_ids
                    .iter()
                    .copied()
                    .map(|doc_id| ObjectAuthzKey::index_doc(&request.index.index_id, doc_id))
            })
            .collect::<Vec<_>>();
        let final_authz = self
            .authz_reader
            .verify_page(request.authz, authz_keys)
            .await?;
        if final_authz.iter().any(|decision| !decision.allowed) {
            bail!("AuthzCandidateSetStale");
        }
        let payload_ranges_planned = ranges.len() as u64;
        let payload_bytes_planned = ranges
            .iter()
            .map(|range| range.logical_end.saturating_sub(range.logical_start))
            .sum();
        Ok(QueryPlanResult {
            candidates: intersection,
            ranges,
            final_authz,
            metrics: QueryPlanMetrics {
                input_candidate_count: index_count,
                boundary_candidate_count: boundary_count,
                authz_candidate_count: authz_count,
                index_candidate_count: index_count,
                intersection_candidate_count: intersection_count,
                payload_ranges_planned,
                payload_bytes_planned,
                payload_bytes_read: 0,
                full_scan_forbidden_count: 0,
            },
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryPlanMetrics {
    pub input_candidate_count: u64,
    pub boundary_candidate_count: u64,
    pub authz_candidate_count: u64,
    pub index_candidate_count: u64,
    pub intersection_candidate_count: u64,
    pub payload_ranges_planned: u64,
    pub payload_bytes_planned: u64,
    pub payload_bytes_read: u64,
    pub full_scan_forbidden_count: u64,
}

fn intersect_ranges(left: &[DocIdRange], right: &[DocIdRange]) -> Vec<DocIdRange> {
    let mut out = Vec::new();
    let mut i = 0;
    let mut j = 0;
    while i < left.len() && j < right.len() {
        let start = left[i].start_inclusive.max(right[j].start_inclusive);
        let end = left[i].end_exclusive.min(right[j].end_exclusive);
        if start < end {
            out.push(DocIdRange {
                start_inclusive: start,
                end_exclusive: end,
            });
        }
        if left[i].end_exclusive < right[j].end_exclusive {
            i += 1;
        } else {
            j += 1;
        }
    }
    out
}

fn candidate_kind_ordinals(kind: &CandidateSetKind) -> Result<BTreeSet<u64>> {
    match kind {
        CandidateSetKind::Empty => Ok(BTreeSet::new()),
        CandidateSetKind::AllWithinPartition { .. } => bail!("IndexCapabilityMissing"),
        CandidateSetKind::Bitmap {
            roaring_bitmap_bytes,
            ..
        } => decode_bitmap_ordinals(roaring_bitmap_bytes),
        CandidateSetKind::SortedDocIdRanges { ranges, .. } => {
            let mut out = BTreeSet::new();
            for range in ranges {
                let start_partition = range.start_inclusive.partition_id();
                let end_partition = range.end_exclusive.partition_id();
                if start_partition != end_partition {
                    bail!("IndexCapabilityMissing");
                }
                for ordinal in range.start_inclusive.ordinal()..range.end_exclusive.ordinal() {
                    out.insert(ordinal);
                }
            }
            Ok(out)
        }
        CandidateSetKind::OrderedTuples { tuples, .. } => {
            Ok(tuples.iter().map(|tuple| tuple.doc_id.ordinal()).collect())
        }
    }
}

fn candidate_kind_contains(kind: &CandidateSetKind, doc_id: CoreDocId) -> bool {
    match kind {
        CandidateSetKind::Empty => false,
        CandidateSetKind::AllWithinPartition { partition_id } => {
            doc_id.partition_id() == *partition_id
        }
        CandidateSetKind::SortedDocIdRanges { ranges, .. } => {
            ranges.iter().any(|range| range.contains(doc_id))
        }
        CandidateSetKind::OrderedTuples { tuples, .. } => {
            tuples.iter().any(|tuple| tuple.doc_id == doc_id)
        }
        CandidateSetKind::Bitmap {
            roaring_bitmap_bytes,
            ..
        } => decode_bitmap_ordinals(roaring_bitmap_bytes)
            .map(|docs| docs.contains(&doc_id.ordinal()))
            .unwrap_or(false),
    }
}

fn decode_bitmap_ordinals(bytes: &[u8]) -> Result<BTreeSet<u64>> {
    if bytes.len() % 8 != 0 {
        bail!("IndexCapabilityMissing");
    }
    Ok(bytes
        .chunks_exact(8)
        .map(|chunk| u64::from_le_bytes(chunk.try_into().expect("validated chunk length")))
        .collect())
}

fn ensure_algorithm_prefixed_hash(value: &str, field_name: &str) -> Result<()> {
    let Some((algorithm, digest)) = value.split_once(':') else {
        bail!("{field_name} missing algorithm prefix");
    };
    let expected_len = match algorithm {
        "blake3" | "sha256" => 64,
        _ => bail!("{field_name} unsupported hash algorithm"),
    };
    if digest.len() != expected_len
        || !digest
            .as_bytes()
            .iter()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        bail!("{field_name} invalid hash digest");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> CandidateSetScope {
        CandidateSetScope {
            root_key_hash: format!("sha256:{}", "0".repeat(64)),
            root_generation: 7,
            index_id: "idx".to_string(),
            index_generation: 3,
            authz_realm_id: "realm".to_string(),
            authz_scope_hash: format!("sha256:{}", "1".repeat(64)),
            authz_object_namespace: "realm/object".to_string(),
            authz_relation: "reader".to_string(),
            authz_principal_hash: format!("sha256:{}", "2".repeat(64)),
            authz_revision: 11,
            boundary_schema_generation_hash: format!("sha256:{}", "3".repeat(64)),
            predicate_hash: format!("sha256:{}", "4".repeat(64)),
            order_hash: format!("sha256:{}", "5".repeat(64)),
        }
    }

    fn bitmap(ordinals: &[u64]) -> CandidateSet {
        let mut bytes = Vec::new();
        for ordinal in ordinals {
            bytes.extend_from_slice(&ordinal.to_le_bytes());
        }
        CandidateSet {
            scope: scope(),
            kind: CandidateSetKind::Bitmap {
                partition_id: 4,
                roaring_bitmap_bytes: bytes,
            },
        }
    }

    #[test]
    fn candidate_sets_intersect_bitmap_ranges_and_ordered_tuples() {
        let bitmap = bitmap(&[1, 2, 3, 8, 13]);
        let ranges = CandidateSet {
            scope: scope(),
            kind: CandidateSetKind::SortedDocIdRanges {
                partition_id: 4,
                ranges: vec![DocIdRange {
                    start_inclusive: CoreDocId::new(4, 2),
                    end_exclusive: CoreDocId::new(4, 9),
                }],
            },
        };
        let ordered = CandidateSet {
            scope: scope(),
            kind: CandidateSetKind::OrderedTuples {
                partition_id: 4,
                tuples: vec![
                    OrderedDocTuple {
                        order_tuple: vec![b"a".to_vec()],
                        doc_id: CoreDocId::new(4, 1),
                    },
                    OrderedDocTuple {
                        order_tuple: vec![b"b".to_vec()],
                        doc_id: CoreDocId::new(4, 2),
                    },
                    OrderedDocTuple {
                        order_tuple: vec![b"c".to_vec()],
                        doc_id: CoreDocId::new(4, 8),
                    },
                ],
            },
        };

        let intersected = bitmap
            .intersect(&ranges)
            .unwrap()
            .intersect(&ordered)
            .unwrap();
        let CandidateSetKind::OrderedTuples { tuples, .. } = intersected.kind else {
            panic!("ordered query should retain result ordering");
        };
        assert_eq!(
            tuples
                .iter()
                .map(|tuple| tuple.doc_id.ordinal())
                .collect::<Vec<_>>(),
            vec![2, 8]
        );
    }
}
