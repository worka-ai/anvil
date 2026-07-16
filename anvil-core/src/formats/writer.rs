use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Result, bail};

use crate::core_store::{
    CORE_META_MAX_INLINE_PAYLOAD_BYTES, CoreBoundaryValue, CoreLogicalRangeHint, CoreMetaBatchOp,
    CoreMetaBatchOpKind, CoreMetaRowCommonProto, CoreMetaVisibilityState, CoreObjectRef,
    CorePipelinePolicy, CoreSharedRangeMarker, CoreTraceContext, WriteLogicalFilePathRequest,
    WriteLogicalFileRequest, sha256_hex,
};

use super::{EncodedWriterSegment, FileFamily, Hash32, RangeIndexEntry, encode_writer_segment};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum WriterFamily {
    ObjectBlob,
    Stream,
    FullText,
    Vector,
    TypedMetadata,
    Authz,
    PersonalDb,
    GitSource,
    Registry,
    MeshControl,
    CoreControl,
}

impl WriterFamily {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ObjectBlob => "object_blob",
            Self::Stream => "stream",
            Self::FullText => "full_text",
            Self::Vector => "vector",
            Self::TypedMetadata => "typed_index",
            Self::Authz => "authz",
            Self::PersonalDb => "personaldb",
            Self::GitSource => "git_source",
            Self::Registry => "registry",
            Self::MeshControl => "mesh_control",
            Self::CoreControl => "core_control",
        }
    }

    pub fn all() -> [Self; 11] {
        [
            Self::ObjectBlob,
            Self::Stream,
            Self::FullText,
            Self::Vector,
            Self::TypedMetadata,
            Self::Authz,
            Self::PersonalDb,
            Self::GitSource,
            Self::Registry,
            Self::MeshControl,
            Self::CoreControl,
        ]
    }

    pub fn from_name(value: &str) -> Option<Self> {
        Self::all()
            .into_iter()
            .find(|family| family.as_str() == value)
    }
}

pub fn canonical_logical_file_id(
    writer_family: WriterFamily,
    writer_generation: u64,
    stable_name: &str,
    content_hash: &[u8],
) -> String {
    let mut bytes = Vec::new();
    for part in [
        "anvil.logical_file_id.v1",
        writer_family.as_str(),
        &writer_generation.to_string(),
        stable_name,
    ] {
        bytes.extend_from_slice(&(part.len() as u64).to_le_bytes());
        bytes.extend_from_slice(part.as_bytes());
    }
    bytes.extend_from_slice(&(content_hash.len() as u64).to_le_bytes());
    bytes.extend_from_slice(content_hash);
    format!("lf_{}", sha256_hex(&bytes))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ByteSource {
    InlineBytes(Vec<u8>),
    LandedBytes {
        landing_id: String,
        hash: String,
        length: u64,
        relative_path: String,
    },
    TempFile {
        path: std::path::PathBuf,
        hash: String,
        length: u64,
    },
    ExistingCoreObject {
        object_ref: CoreObjectRef,
        byte_start: u64,
        byte_end: u64,
    },
}

impl ByteSource {
    pub fn inline(bytes: Vec<u8>) -> Self {
        Self::InlineBytes(bytes)
    }

    pub fn logical_len(&self) -> u64 {
        match self {
            Self::InlineBytes(bytes) => bytes.len() as u64,
            Self::LandedBytes { length, .. } | Self::TempFile { length, .. } => *length,
            Self::ExistingCoreObject {
                byte_start,
                byte_end,
                ..
            } => byte_end.saturating_sub(*byte_start),
        }
    }

    pub fn into_inline_bytes(self) -> Result<Vec<u8>> {
        match self {
            Self::InlineBytes(bytes) => Ok(bytes),
            Self::LandedBytes { .. } | Self::TempFile { .. } | Self::ExistingCoreObject { .. } => {
                bail!(
                    "ByteSource must be materialised through CoreStore admission before this call"
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RangeStatistics {
    pub encoded_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundaryStrength {
    Required,
    Preferred,
    DiagnosticOnly,
}

impl BoundaryStrength {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Required => "required",
            Self::Preferred => "preferred",
            Self::DiagnosticOnly => "diagnostic_only",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharedRangeMarker {
    pub record_kind: String,
    pub reason: String,
    pub boundary_dimension_ids: Vec<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalRangeHint {
    pub byte_start: u64,
    pub byte_end: u64,
    pub writer_record_kind: String,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub statistics: RangeStatistics,
    pub preferred_block_boundary: BoundaryStrength,
    pub prefetch_group: Option<String>,
    pub shared_range: Option<SharedRangeMarker>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityClass {
    InlineMetadata,
    ErasureCodedBytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub policy_id: String,
    pub target_segment_bytes: u64,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            policy_id: "corestore-default".to_string(),
            target_segment_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadProfile {
    pub profile_id: String,
    pub prefetch_group: Option<String>,
}

impl Default for ReadProfile {
    fn default() -> Self {
        Self {
            profile_id: "range-read-default".to_string(),
            prefetch_group: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalFileWrite {
    pub logical_file_id: String,
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub bytes: ByteSource,
    pub ranges: Vec<LogicalRangeHint>,
    pub durability_class: DurabilityClass,
    pub compaction_policy: CompactionPolicy,
    pub read_profile: ReadProfile,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
}

impl LogicalFileWrite {
    pub fn into_write_logical_file_request(self) -> Result<WriteLogicalFileRequest> {
        let source = self.bytes.into_inline_bytes()?;
        Ok(WriteLogicalFileRequest {
            writer_family: self.writer_family.as_str().to_string(),
            generation: self.writer_generation,
            logical_file_id: self.logical_file_id,
            source,
            range_hints: writer_ranges_to_core_hints(self.ranges),
            pipeline_policy: self.pipeline_policy,
            trace_context: self.trace_context,
            boundary_values: self.boundary_values,
            mutation_id: self.mutation_id,
            region_id: self.region_id,
        })
    }

    pub fn into_write_logical_file_path_request(
        self,
        source_path: std::path::PathBuf,
        source_hash: String,
        source_len: u64,
    ) -> WriteLogicalFilePathRequest {
        WriteLogicalFilePathRequest {
            writer_family: self.writer_family.as_str().to_string(),
            generation: self.writer_generation,
            logical_file_id: self.logical_file_id,
            source_path,
            source_len,
            source_hash,
            range_hints: writer_ranges_to_core_hints(self.ranges),
            pipeline_policy: self.pipeline_policy,
            trace_context: self.trace_context,
            boundary_values: self.boundary_values,
            mutation_id: self.mutation_id,
            region_id: self.region_id,
        }
    }
}

fn writer_ranges_to_core_hints(ranges: Vec<LogicalRangeHint>) -> Vec<CoreLogicalRangeHint> {
    ranges
        .into_iter()
        .map(|range| CoreLogicalRangeHint {
            range_id: format!("{}-{}", range.byte_start, range.byte_end),
            byte_start: range.byte_start,
            byte_end: range.byte_end,
            writer_record_kind: range.writer_record_kind,
            boundary_values: range.boundary_values,
            writer_statistics: range.statistics.encoded_bytes,
            preferred_block_boundary: range.preferred_block_boundary.as_str().to_string(),
            boundary_dimension_ids: Vec::new(),
            prefetch_next_range_ids: range.prefetch_group.into_iter().collect(),
            shared_range: range.shared_range.map(|marker| CoreSharedRangeMarker {
                record_kind: marker.record_kind,
                reason: marker.reason,
                boundary_dimension_ids: marker.boundary_dimension_ids,
            }),
        })
        .collect()
}

#[derive(Debug, Clone, PartialEq)]
pub enum CoreMetaMutationKind {
    Put(Vec<u8>),
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoreMetaMutation {
    pub column_family: &'static str,
    pub table_id: u16,
    pub tuple_key: Vec<u8>,
    pub kind: CoreMetaMutationKind,
    pub common: Option<CoreMetaRowCommonProto>,
    pub visibility_state: CoreMetaVisibilityState,
    pub root_key_hash: String,
    pub post_root_generation: u64,
    pub transaction_id: String,
}

impl CoreMetaMutation {
    pub fn put(
        column_family: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        payload: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
        transaction_id: String,
    ) -> Self {
        let (root_key_hash, post_root_generation, visibility_state) = common
            .as_ref()
            .map(|common| {
                (
                    common.root_key_hash.clone(),
                    common.root_generation,
                    common.visibility_state_enum(),
                )
            })
            .unwrap_or_else(|| (String::new(), 0, CoreMetaVisibilityState::Committed));
        Self {
            column_family,
            table_id,
            tuple_key,
            kind: CoreMetaMutationKind::Put(payload),
            common,
            visibility_state,
            root_key_hash,
            post_root_generation,
            transaction_id,
        }
    }

    pub fn delete(
        column_family: &'static str,
        table_id: u16,
        tuple_key: Vec<u8>,
        common: Option<CoreMetaRowCommonProto>,
        transaction_id: String,
    ) -> Self {
        let (root_key_hash, post_root_generation, visibility_state) = common
            .as_ref()
            .map(|common| {
                (
                    common.root_key_hash.clone(),
                    common.root_generation,
                    common.visibility_state_enum(),
                )
            })
            .unwrap_or_else(|| (String::new(), 0, CoreMetaVisibilityState::Committed));
        Self {
            column_family,
            table_id,
            tuple_key,
            kind: CoreMetaMutationKind::Delete,
            common,
            visibility_state,
            root_key_hash,
            post_root_generation,
            transaction_id,
        }
    }

    pub fn as_batch_op(&self) -> CoreMetaBatchOp<'_> {
        let kind = match &self.kind {
            CoreMetaMutationKind::Put(payload) => CoreMetaBatchOpKind::Put(payload),
            CoreMetaMutationKind::Delete => CoreMetaBatchOpKind::Delete,
        };
        CoreMetaBatchOp {
            cf: self.column_family,
            table_id: self.table_id,
            tuple_key: &self.tuple_key,
            common: self.common.clone(),
            kind,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterPlanInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub estimated_bytes: u64,
    pub estimated_records: u64,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterBuildInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub source_bytes: Vec<u8>,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriterRecoveryInput {
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub observed_coremeta_rows: Vec<Vec<u8>>,
    pub observed_logical_files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterPlan {
    pub logical_files: Vec<LogicalFileWrite>,
    pub core_meta_mutations: Vec<CoreMetaMutation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterBuildOutput {
    pub logical_files: Vec<LogicalFileWrite>,
    pub core_meta_mutations: Vec<CoreMetaMutation>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WriterRecoveryPlan {
    pub mutations_to_commit: Vec<CoreMetaMutation>,
    pub logical_files_to_rewrite: Vec<LogicalFileWrite>,
    pub orphaned_logical_files: Vec<String>,
}

pub trait CoreFormatWriter {
    fn family(&self) -> WriterFamily;

    fn plan(&self, input: WriterPlanInput) -> Result<WriterPlan>;

    fn build(&self, input: WriterBuildInput) -> Result<WriterBuildOutput>;

    fn recover(&self, input: WriterRecoveryInput) -> Result<WriterRecoveryPlan>;
}

#[derive(Default)]
pub struct CoreFormatWriterRegistry {
    writers: BTreeMap<WriterFamily, Box<dyn CoreFormatWriter + Send + Sync>>,
}

impl CoreFormatWriterRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_default_opaque_writers() -> Self {
        let mut registry = Self::new();
        for family in WriterFamily::all() {
            registry.register(Box::new(OpaqueLogicalFileWriter::new(family)));
        }
        registry
    }

    pub fn register(&mut self, writer: Box<dyn CoreFormatWriter + Send + Sync>) {
        self.writers.insert(writer.family(), writer);
    }

    pub fn get(&self, family: WriterFamily) -> Option<&(dyn CoreFormatWriter + Send + Sync)> {
        self.writers.get(&family).map(|writer| writer.as_ref())
    }

    pub fn require(&self, family: WriterFamily) -> Result<&(dyn CoreFormatWriter + Send + Sync)> {
        self.get(family).ok_or_else(|| {
            anyhow::anyhow!("CoreFormatWriter is not registered for {}", family.as_str())
        })
    }

    pub fn registered_families(&self) -> BTreeSet<WriterFamily> {
        self.writers.keys().copied().collect()
    }
}

#[derive(Debug, Clone)]
pub struct OpaqueLogicalFileWriter {
    family: WriterFamily,
}

impl OpaqueLogicalFileWriter {
    pub fn new(family: WriterFamily) -> Self {
        Self { family }
    }
}

impl CoreFormatWriter for OpaqueLogicalFileWriter {
    fn family(&self) -> WriterFamily {
        self.family
    }

    fn plan(&self, input: WriterPlanInput) -> Result<WriterPlan> {
        let bytes = Vec::with_capacity(input.estimated_bytes.min(usize::MAX as u64) as usize);
        let plan_hash = sha256_hex(
            format!(
                "{}\0{}\0{}\0{}",
                input.writer_family.as_str(),
                input.writer_generation,
                input.estimated_bytes,
                input.estimated_records
            )
            .as_bytes(),
        );
        Ok(WriterPlan {
            logical_files: vec![LogicalFileWrite {
                logical_file_id: canonical_logical_file_id(
                    input.writer_family,
                    input.writer_generation,
                    &input.logical_file_id,
                    plan_hash.as_bytes(),
                ),
                writer_family: input.writer_family,
                writer_generation: input.writer_generation,
                bytes: ByteSource::InlineBytes(bytes),
                ranges: Vec::new(),
                durability_class: DurabilityClass::ErasureCodedBytes,
                compaction_policy: CompactionPolicy::default(),
                read_profile: ReadProfile::default(),
                pipeline_policy: CorePipelinePolicy::default(),
                trace_context: CoreTraceContext::default(),
                boundary_values: input.boundary_values,
                mutation_id: format!(
                    "writer-plan-{}",
                    sha256_hex(
                        format!(
                            "{}\0{}\0{}",
                            input.writer_family.as_str(),
                            input.writer_generation,
                            plan_hash
                        )
                        .as_bytes()
                    )
                ),
                region_id: input
                    .options
                    .get("region_id")
                    .cloned()
                    .unwrap_or_else(|| "local".to_string()),
            }],
            core_meta_mutations: Vec::new(),
        })
    }

    fn build(&self, input: WriterBuildInput) -> Result<WriterBuildOutput> {
        let source_hash = sha256_hex(&input.source_bytes);
        Ok(WriterBuildOutput {
            logical_files: vec![LogicalFileWrite {
                logical_file_id: canonical_logical_file_id(
                    input.writer_family,
                    input.writer_generation,
                    &input.logical_file_id,
                    source_hash.as_bytes(),
                ),
                writer_family: input.writer_family,
                writer_generation: input.writer_generation,
                bytes: ByteSource::InlineBytes(input.source_bytes),
                ranges: Vec::new(),
                durability_class: DurabilityClass::ErasureCodedBytes,
                compaction_policy: CompactionPolicy::default(),
                read_profile: ReadProfile::default(),
                pipeline_policy: input.pipeline_policy,
                trace_context: input.trace_context,
                boundary_values: input.boundary_values,
                mutation_id: input.mutation_id,
                region_id: input.region_id,
            }],
            core_meta_mutations: Vec::new(),
        })
    }

    fn recover(&self, _input: WriterRecoveryInput) -> Result<WriterRecoveryPlan> {
        Ok(WriterRecoveryPlan {
            mutations_to_commit: Vec::new(),
            logical_files_to_rewrite: Vec::new(),
            orphaned_logical_files: Vec::new(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriterSegmentBuildInput {
    pub file_family: FileFamily,
    pub writer_family: WriterFamily,
    pub writer_generation: u64,
    pub logical_file_id: String,
    pub header_proto: Vec<u8>,
    pub body: Vec<u8>,
    pub range_index: Vec<u8>,
    pub record_count: u64,
    pub first_record_hash: Hash32,
    pub last_record_hash: Hash32,
    pub boundary_values: Vec<CoreBoundaryValue>,
    pub mutation_id: String,
    pub region_id: String,
    pub pipeline_policy: CorePipelinePolicy,
    pub trace_context: CoreTraceContext,
}

#[derive(Debug, Clone)]
pub struct WriterSegmentBuildOutput {
    pub encoded: EncodedWriterSegment,
    pub logical_file: LogicalFileWrite,
    pub range_index_entries: Vec<RangeIndexEntry>,
}

pub fn build_writer_segment_logical_file(
    input: WriterSegmentBuildInput,
) -> Result<WriterSegmentBuildOutput> {
    let range_index_entries = super::decode_range_index(&input.range_index)?;
    let encoded = encode_writer_segment(
        input.file_family,
        0,
        input.header_proto,
        &input.body,
        &input.range_index,
        input.record_count,
        input.first_record_hash,
        input.last_record_hash,
    )?;
    let logical_file = LogicalFileWrite {
        logical_file_id: input.logical_file_id,
        writer_family: input.writer_family,
        writer_generation: input.writer_generation,
        bytes: ByteSource::InlineBytes(encoded.bytes.clone()),
        ranges: range_index_entries
            .iter()
            .map(|entry| LogicalRangeHint {
                byte_start: entry.logical_start,
                byte_end: entry.logical_end,
                writer_record_kind: input.file_family.writer_family_name().to_string(),
                boundary_values: input.boundary_values.clone(),
                statistics: RangeStatistics {
                    encoded_bytes: entry.stats_ref.clone(),
                },
                preferred_block_boundary: BoundaryStrength::Preferred,
                prefetch_group: None,
                shared_range: None,
            })
            .collect(),
        durability_class: if encoded.bytes.len() <= CORE_META_MAX_INLINE_PAYLOAD_BYTES {
            DurabilityClass::InlineMetadata
        } else {
            DurabilityClass::ErasureCodedBytes
        },
        compaction_policy: CompactionPolicy::default(),
        read_profile: ReadProfile::default(),
        pipeline_policy: input.pipeline_policy,
        trace_context: input.trace_context,
        boundary_values: input.boundary_values,
        mutation_id: input.mutation_id,
        region_id: input.region_id,
    };
    Ok(WriterSegmentBuildOutput {
        encoded,
        logical_file,
        range_index_entries,
    })
}
