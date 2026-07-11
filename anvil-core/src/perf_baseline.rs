use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::path::Path;

pub const BASELINE_SCHEMA: &str = "anvil.perf.baseline_manifest.v1";
pub const BASELINE_DATASET_ID: &str = "anvil-corestore-baseline-v1";
pub const BASELINE_SEED: &str = "anvil-corestore-baseline-v1-seed-42";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BaselineManifest {
    pub schema: String,
    pub dataset_id: String,
    pub seed: String,
    pub hardware_profile: HardwareProfile,
    pub warmup: RunWindow,
    pub run: RunWindow,
    pub object_distribution: ObjectDistribution,
    pub query_corpus: QueryCorpus,
    pub vector_corpus: VectorCorpus,
    pub authz_graph: AuthzGraph,
    pub personaldb: PersonalDbWorkload,
    pub references: ReferenceStores,
    pub pass_fail: PassFailPolicy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct HardwareProfile {
    pub name: String,
    pub nodes: u32,
    pub cells: u32,
    pub regions: u32,
    pub disk_class: String,
    pub network_gbps: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RunWindow {
    #[serde(default)]
    pub duration_seconds: u64,
    #[serde(default)]
    pub discard_metrics: bool,
    #[serde(default)]
    pub sample_traces_per_scenario: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectDistribution {
    pub tenant_count: u64,
    pub project_count: u64,
    pub small_weights: BTreeMap<String, u32>,
    pub medium_weights: BTreeMap<String, u32>,
    pub large_weights: BTreeMap<String, u32>,
    pub key_algorithm: String,
    pub metadata_algorithm: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryCorpus {
    pub text_generator: String,
    pub query_selector: String,
    pub term_queries: u32,
    pub boolean_queries: u32,
    pub phrase_queries: u32,
    pub field_queries: u32,
    pub selectivity_buckets: Vec<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VectorCorpus {
    pub generator: String,
    pub dimensions: Vec<u16>,
    pub oracle_sample_size: u64,
    pub recall_k: u32,
    pub distance: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AuthzGraph {
    pub generator: String,
    pub tenants: u64,
    pub subjects: u64,
    pub objects: u64,
    pub direct_relation_ratio: f64,
    pub computed_userset_ratio: f64,
    pub tuple_to_userset_ratio: f64,
    pub max_traversal_depth: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PersonalDbWorkload {
    pub generator: String,
    pub groups: u64,
    pub actors_per_group: u32,
    pub changeset_size_weights: BTreeMap<String, u32>,
    pub conflict_ratio_bps: u32,
    pub snapshot_every_changesets: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReferenceStores {
    pub minio: MinioReference,
    pub external_comparisons: Vec<ExternalComparison>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MinioReference {
    pub image: String,
    pub config: String,
    pub versioning: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExternalComparison {
    pub name: String,
    pub required: bool,
    pub skip_reason_required_when_absent: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PassFailPolicy {
    pub object_thresholds: String,
    pub non_object_thresholds: String,
    pub regression_threshold_percent: u32,
    pub minimum_valid_samples_per_metric: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratedObjectKey {
    pub tenant_no: u64,
    pub project_no: u64,
    pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GeneratedObjectMetadata {
    pub customer_tenant: String,
    pub project: String,
    pub content_type: String,
    pub created_day: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BaselineRunSummary {
    pub schema: String,
    pub command: String,
    pub dataset_id: String,
    pub manifest_hash: String,
    pub git_commit: String,
    pub machine_class: String,
    pub total_elapsed_ms: u64,
    pub rocksdb_pending_bytes: u64,
    pub final_corestore_bytes: u64,
    pub dedupe_ratio: f64,
    pub compression_ratio: f64,
    pub fsync_count: u64,
    pub pass: bool,
    pub scenarios: Vec<BaselineScenarioSummary>,
    pub slowest_traced_spans: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BaselineScenarioSummary {
    pub name: String,
    pub valid: bool,
    pub samples: u64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub throughput_per_sec: f64,
    pub bytes_written: u64,
    pub bytes_read: u64,
    pub fsync_count: u64,
    pub slowest_trace_ids: Vec<String>,
    pub failure_reason: Option<String>,
}

impl BaselineRunSummary {
    pub fn smoke(
        command: impl Into<String>,
        manifest: &BaselineManifest,
        git_commit: impl Into<String>,
        machine_class: impl Into<String>,
        total_elapsed: std::time::Duration,
        scenarios: Vec<BaselineScenarioSummary>,
    ) -> Result<Self> {
        let pass = scenarios.iter().all(|scenario| scenario.valid);
        let fsync_count = scenarios
            .iter()
            .map(|scenario| scenario.fsync_count)
            .sum::<u64>();
        let slowest_traced_spans = scenarios
            .iter()
            .flat_map(|scenario| scenario.slowest_trace_ids.iter().cloned())
            .take(50)
            .collect();
        Ok(Self {
            schema: "anvil.perf.performance_summary.v1".to_string(),
            command: command.into(),
            dataset_id: manifest.dataset_id.clone(),
            manifest_hash: manifest.manifest_hash()?,
            git_commit: git_commit.into(),
            machine_class: machine_class.into(),
            total_elapsed_ms: total_elapsed.as_millis().min(u128::from(u64::MAX)) as u64,
            rocksdb_pending_bytes: 0,
            final_corestore_bytes: 0,
            dedupe_ratio: 1.0,
            compression_ratio: 1.0,
            fsync_count,
            pass,
            scenarios,
            slowest_traced_spans,
        })
    }
}

impl BaselineScenarioSummary {
    pub fn single_sample(name: impl Into<String>, duration: std::time::Duration) -> Self {
        let duration_ms = duration.as_secs_f64() * 1000.0;
        Self {
            name: name.into(),
            valid: true,
            samples: 1,
            p50_ms: duration_ms,
            p90_ms: duration_ms,
            p95_ms: duration_ms,
            p99_ms: duration_ms,
            throughput_per_sec: if duration_ms > 0.0 {
                1000.0 / duration_ms
            } else {
                0.0
            },
            bytes_written: 0,
            bytes_read: 0,
            fsync_count: 0,
            slowest_trace_ids: Vec::new(),
            failure_reason: None,
        }
    }
}

impl BaselineManifest {
    pub fn release_default() -> Self {
        Self {
            schema: BASELINE_SCHEMA.to_string(),
            dataset_id: BASELINE_DATASET_ID.to_string(),
            seed: BASELINE_SEED.to_string(),
            hardware_profile: HardwareProfile {
                name: "release-10-node-nvme".to_string(),
                nodes: 10,
                cells: 5,
                regions: 1,
                disk_class: "nvme".to_string(),
                network_gbps: 10,
            },
            warmup: RunWindow {
                duration_seconds: 300,
                discard_metrics: true,
                sample_traces_per_scenario: 0,
            },
            run: RunWindow {
                duration_seconds: 1800,
                discard_metrics: false,
                sample_traces_per_scenario: 1000,
            },
            object_distribution: ObjectDistribution {
                tenant_count: 100_000,
                project_count: 1_000,
                small_weights: weights([("1KiB", 40), ("4KiB", 35), ("16KiB", 20), ("64KiB", 5)]),
                medium_weights: weights([("256KiB", 50), ("1MiB", 35), ("4MiB", 15)]),
                large_weights: weights([("128MiB", 80), ("1GiB", 19), ("5GiB", 1)]),
                key_algorithm: "corestore-key-fixed-v1 mapped to /tenant/{tenant}/project/{project}/object/{ordinal}".to_string(),
                metadata_algorithm: "deterministic tenant/project/content_type/day from seed and ordinal".to_string(),
            },
            query_corpus: QueryCorpus {
                text_generator: "markov-fixed-v1".to_string(),
                query_selector: "query-selector-fixed-v1".to_string(),
                term_queries: 1000,
                boolean_queries: 1000,
                phrase_queries: 500,
                field_queries: 500,
                selectivity_buckets: vec![0.0001, 0.001, 0.01, 0.1],
            },
            vector_corpus: VectorCorpus {
                generator: "deterministic-normalized-random-v1".to_string(),
                dimensions: vec![384, 768],
                oracle_sample_size: 100_000,
                recall_k: 20,
                distance: "cosine".to_string(),
            },
            authz_graph: AuthzGraph {
                generator: "authz-graph-fixed-v1".to_string(),
                tenants: 100_000,
                subjects: 1_000_000,
                objects: 10_000_000,
                direct_relation_ratio: 0.55,
                computed_userset_ratio: 0.30,
                tuple_to_userset_ratio: 0.15,
                max_traversal_depth: 8,
            },
            personaldb: PersonalDbWorkload {
                generator: "personaldb-changeset-fixed-v1".to_string(),
                groups: 10_000,
                actors_per_group: 8,
                changeset_size_weights: weights([("1KiB", 40), ("4KiB", 40), ("16KiB", 15), ("64KiB", 5)]),
                conflict_ratio_bps: 300,
                snapshot_every_changesets: 1000,
            },
            references: ReferenceStores {
                minio: MinioReference {
                    image: "minio/minio:RELEASE.2026-06-13T11-33-47Z".to_string(),
                    config: "single-node-nvme-or-distributed-matching-anvil".to_string(),
                    versioning: true,
                },
                external_comparisons: vec![ExternalComparison {
                    name: "s3-compatible-cloud".to_string(),
                    required: false,
                    skip_reason_required_when_absent: true,
                }],
            },
            pass_fail: PassFailPolicy {
                object_thresholds: "section-26.2".to_string(),
                non_object_thresholds: "section-26.3".to_string(),
                regression_threshold_percent: 10,
                minimum_valid_samples_per_metric: 1000,
            },
        }
    }

    pub fn validate_release_contract(&self) -> Result<()> {
        if self.schema != BASELINE_SCHEMA {
            bail!("baseline manifest schema mismatch");
        }
        if self.dataset_id != BASELINE_DATASET_ID {
            bail!("baseline dataset id mismatch");
        }
        if self.seed.trim().is_empty() {
            bail!("baseline seed must not be empty");
        }
        if self.hardware_profile.nodes == 0 || self.hardware_profile.cells == 0 {
            bail!("baseline hardware profile must define nodes and cells");
        }
        if self.object_distribution.tenant_count == 0 || self.object_distribution.project_count == 0
        {
            bail!("baseline object distribution must define tenants and projects");
        }
        validate_weights("small_weights", &self.object_distribution.small_weights)?;
        validate_weights("medium_weights", &self.object_distribution.medium_weights)?;
        validate_weights("large_weights", &self.object_distribution.large_weights)?;
        validate_weights(
            "changeset_size_weights",
            &self.personaldb.changeset_size_weights,
        )?;
        if self.query_corpus.selectivity_buckets.is_empty() {
            bail!("baseline query corpus must define selectivity buckets");
        }
        if self.references.minio.image.trim().is_empty() {
            bail!("baseline requires a pinned MinIO image");
        }
        Ok(())
    }

    pub fn canonical_json(&self) -> Result<String> {
        let value = serde_json::to_value(self).context("serialize baseline manifest")?;
        Ok(canonical_json(&value).to_string())
    }

    pub fn manifest_hash(&self) -> Result<String> {
        Ok(format!(
            "blake3:{}",
            blake3::hash(self.canonical_json()?.as_bytes()).to_hex()
        ))
    }

    pub fn write_json_file(&self, path: impl AsRef<Path>) -> Result<()> {
        self.validate_release_contract()?;
        let bytes = serde_json::to_vec_pretty(self).context("encode baseline manifest")?;
        crate::perf::write_non_authoritative_file(path.as_ref(), &bytes)
            .with_context(|| format!("write {}", path.as_ref().display()))
    }
}

pub fn load_baseline_manifest(path: impl AsRef<Path>) -> Result<BaselineManifest> {
    let bytes = std::fs::read(path.as_ref())
        .with_context(|| format!("read {}", path.as_ref().display()))?;
    let manifest: BaselineManifest = serde_json::from_slice(&bytes)
        .with_context(|| format!("parse {}", path.as_ref().display()))?;
    manifest.validate_release_contract()?;
    Ok(manifest)
}

pub fn generated_object_key(manifest: &BaselineManifest, ordinal: u64) -> GeneratedObjectKey {
    let tenant_no = ordinal % manifest.object_distribution.tenant_count;
    let project_no = (ordinal / manifest.object_distribution.tenant_count)
        % manifest.object_distribution.project_count;
    let suffix = hash_hex_parts(&[&manifest.seed, "object-key", &ordinal.to_string()]);
    GeneratedObjectKey {
        tenant_no,
        project_no,
        key: format!(
            "/tenant/{tenant_no}/project/{project_no}/object/{}",
            &suffix[..24]
        ),
    }
}

pub fn generated_object_metadata(
    manifest: &BaselineManifest,
    ordinal: u64,
) -> GeneratedObjectMetadata {
    let key = generated_object_key(manifest, ordinal);
    let customer_tenant = deterministic_uuid_v5_like(&manifest.dataset_id, "tenant", key.tenant_no);
    let content_type = weighted_choice(
        &weights([
            ("application/json", 40),
            ("text/plain", 30),
            ("application/octet-stream", 30),
        ]),
        deterministic_u64(&[&manifest.seed, "content-type", &ordinal.to_string()]),
    )
    .unwrap_or_else(|| "application/octet-stream".to_string());
    let day_offset = ordinal % 365;
    GeneratedObjectMetadata {
        customer_tenant,
        project: format!("project-{project_no:06}", project_no = key.project_no),
        content_type,
        created_day: format_day_2026(day_offset),
    }
}

pub fn deterministic_vector(manifest: &BaselineManifest, ordinal: u64, dimension: u16) -> Vec<f32> {
    let mut values = Vec::with_capacity(dimension as usize);
    let mut norm = 0.0_f32;
    for component in 0..dimension {
        let raw = deterministic_u64(&[
            &manifest.seed,
            "vector",
            &ordinal.to_string(),
            &component.to_string(),
        ]);
        let unit = (raw as f64 / u64::MAX as f64) as f32;
        let value = unit.mul_add(2.0, -1.0);
        norm += value * value;
        values.push(value);
    }
    if norm == 0.0 {
        if let Some(first) = values.first_mut() {
            *first = 1.0;
        }
        return values;
    }
    let norm = norm.sqrt();
    for value in &mut values {
        *value /= norm;
    }
    values
}

pub fn weighted_choice(weights: &BTreeMap<String, u32>, draw: u64) -> Option<String> {
    let total = weights.values().map(|value| u64::from(*value)).sum::<u64>();
    if total == 0 {
        return None;
    }
    let mut remaining = draw % total;
    for (key, weight) in weights {
        let weight = u64::from(*weight);
        if remaining < weight {
            return Some(key.clone());
        }
        remaining -= weight;
    }
    weights.keys().next_back().cloned()
}

pub fn baseline_trace_context(
    manifest: &BaselineManifest,
    scenario: &str,
    ordinal: u64,
) -> BTreeMap<String, String> {
    BTreeMap::from([
        ("schema".to_string(), "anvil.trace_event.v1".to_string()),
        ("dataset_id".to_string(), manifest.dataset_id.clone()),
        (
            "manifest_hash".to_string(),
            manifest.manifest_hash().unwrap_or_default(),
        ),
        ("scenario".to_string(), scenario.to_string()),
        ("ordinal".to_string(), ordinal.to_string()),
        (
            "trace_id".to_string(),
            hash_hex_parts(&[&manifest.seed, scenario, &ordinal.to_string()])[..32].to_string(),
        ),
    ])
}

fn validate_weights(label: &str, weights: &BTreeMap<String, u32>) -> Result<()> {
    if weights.is_empty() {
        bail!("{label} must not be empty");
    }
    if weights.values().all(|value| *value == 0) {
        bail!("{label} must contain at least one non-zero weight");
    }
    Ok(())
}

fn weights<const N: usize>(items: [(&str, u32); N]) -> BTreeMap<String, u32> {
    items
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn deterministic_u64(parts: &[&str]) -> u64 {
    let hash = hash_bytes_parts(parts);
    u64::from_le_bytes(hash[..8].try_into().expect("hash prefix is 8 bytes"))
}

fn hash_hex_parts(parts: &[&str]) -> String {
    hex::encode(hash_bytes_parts(parts))
}

fn hash_bytes_parts(parts: &[&str]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"anvil.query.plan.v1");
    for part in parts {
        hasher.update(&(part.len() as u64).to_le_bytes());
        hasher.update(part.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn deterministic_uuid_v5_like(dataset_id: &str, label: &str, ordinal: u64) -> String {
    let mut bytes = hash_bytes_parts(&[dataset_id, label, &ordinal.to_string()]);
    bytes[6] = (bytes[6] & 0x0f) | 0x50;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format!(
        "{}-{}-{}-{}-{}",
        hex::encode(&bytes[0..4]),
        hex::encode(&bytes[4..6]),
        hex::encode(&bytes[6..8]),
        hex::encode(&bytes[8..10]),
        hex::encode(&bytes[10..16])
    )
}

fn format_day_2026(offset: u64) -> String {
    const DAYS: [u16; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
    let mut remaining = offset % 365;
    for (month_index, days) in DAYS.iter().enumerate() {
        if remaining < u64::from(*days) {
            return format!(
                "2026-{month:02}-{day:02}",
                month = month_index + 1,
                day = remaining + 1
            );
        }
        remaining -= u64::from(*days);
    }
    "2026-12-31".to_string()
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Object(map) => JsonValue::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), canonical_json(value)))
                .collect(),
        ),
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn baseline_manifest_defaults_validate_and_hash_stably() {
        let manifest = BaselineManifest::release_default();
        manifest.validate_release_contract().unwrap();
        assert_eq!(
            manifest.manifest_hash().unwrap(),
            manifest.manifest_hash().unwrap()
        );
        assert_eq!(
            generated_object_key(&manifest, 42),
            generated_object_key(&manifest, 42)
        );
        assert_eq!(
            generated_object_metadata(&manifest, 42),
            generated_object_metadata(&manifest, 42)
        );
        assert_eq!(
            deterministic_vector(&manifest, 42, 8),
            deterministic_vector(&manifest, 42, 8)
        );
    }
}
