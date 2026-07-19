use anvil_core::core_store::{
    CF_INLINE_PAYLOADS, CoreMetaBatchOp, CoreMetaBatchOpKind, CoreMetaStore, CoreMetaTuplePart,
    TABLE_INLINE_PAYLOAD_ROW, core_meta_committed_row_common, core_meta_root_key_hash,
    core_meta_tuple_key, encode_core_meta_inline_payload_row,
};
use anyhow::{Context, Result};

use crate::config::{GateManifest, ProfileSpec};

pub const NOISE_PREFIX: &str = "00-unrelated";
pub const SMALL_PREFIX: &str = "50-small";
pub const LARGE_PREFIX: &str = "90-large";
pub const MUTATION_PREFIX: &str = "95-mutations";

pub struct Dataset {
    _directory: tempfile::TempDir,
    pub store: CoreMetaStore,
}

impl Dataset {
    pub fn create(manifest: &GateManifest, profile: &ProfileSpec) -> Result<Self> {
        let directory = tempfile::Builder::new()
            .prefix("anvil-coremeta-release-gate-")
            .tempdir()
            .context("create CoreMeta benchmark directory")?;
        let store = CoreMetaStore::open(directory.path())?;

        load_prefix(
            &store,
            &manifest.seed,
            NOISE_PREFIX,
            profile.unrelated_rows,
            profile,
        )?;
        load_prefix(
            &store,
            &manifest.seed,
            SMALL_PREFIX,
            profile.small_rows,
            profile,
        )?;
        load_prefix(
            &store,
            &manifest.seed,
            LARGE_PREFIX,
            profile.large_rows,
            profile,
        )?;

        Ok(Self {
            _directory: directory,
            store,
        })
    }
}

pub fn prefix_key(prefix: &str) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("coremeta-release-gate"),
        CoreMetaTuplePart::Utf8(prefix),
    ])
}

pub fn row_key(prefix: &str, ordinal: u64) -> Result<Vec<u8>> {
    core_meta_tuple_key(&[
        CoreMetaTuplePart::Utf8("coremeta-release-gate"),
        CoreMetaTuplePart::Utf8(prefix),
        CoreMetaTuplePart::U64(ordinal),
    ])
}

pub fn encoded_payload(
    seed: &str,
    prefix: &str,
    ordinal: u64,
    payload_bytes: usize,
    generation: u64,
) -> Result<Vec<u8>> {
    let raw_payload = deterministic_payload(seed, prefix, ordinal, payload_bytes);
    encode_core_meta_inline_payload_row(
        &raw_payload,
        core_meta_committed_row_common(
            "coremeta-release-gate",
            core_meta_root_key_hash(&format!("perf/{prefix}")),
            generation,
            format!("coremeta-release-gate-{prefix}-{generation}"),
            ordinal,
        ),
    )
}

fn load_prefix(
    store: &CoreMetaStore,
    seed: &str,
    prefix: &str,
    row_count: u64,
    profile: &ProfileSpec,
) -> Result<()> {
    let mut first = 0_u64;
    while first < row_count {
        let end = row_count.min(first.saturating_add(profile.batch_rows as u64));
        let keys = (first..end)
            .map(|ordinal| row_key(prefix, ordinal))
            .collect::<Result<Vec<_>>>()?;
        let payloads = (first..end)
            .map(|ordinal| encoded_payload(seed, prefix, ordinal, profile.payload_bytes, 1))
            .collect::<Result<Vec<_>>>()?;
        let operations = keys
            .iter()
            .zip(&payloads)
            .map(|(key, payload)| CoreMetaBatchOp {
                cf: CF_INLINE_PAYLOADS,
                table_id: TABLE_INLINE_PAYLOAD_ROW,
                tuple_key: key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(payload),
            })
            .collect::<Vec<_>>();
        store.write_batch(&operations)?;
        first = end;
    }
    Ok(())
}

fn deterministic_payload(seed: &str, prefix: &str, ordinal: u64, bytes: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes);
    let mut block = 0_u64;
    while output.len() < bytes {
        let mut hasher = blake3::Hasher::new();
        for value in [seed.as_bytes(), prefix.as_bytes()] {
            hasher.update(&(value.len() as u64).to_le_bytes());
            hasher.update(value);
        }
        hasher.update(&ordinal.to_le_bytes());
        hasher.update(&block.to_le_bytes());
        output.extend_from_slice(hasher.finalize().as_bytes());
        block = block.saturating_add(1);
    }
    output.truncate(bytes);
    output
}
