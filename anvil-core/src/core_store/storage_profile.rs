use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use super::meta::CORE_META_MAX_INLINE_PAYLOAD_BYTES;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreStorageClass {
    pub class_id: String,
    pub description: String,
    pub metadata_profile: CoreMetadataProfile,
    pub byte_profile: CoreByteStorageProfile,
    pub inline_payload_policy: CoreInlinePayloadPolicy,
    pub min_cell_spread: u16,
    pub tenant_selectable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreMetadataProfile {
    pub profile_id: String,
    pub replica_count: u16,
    pub prepare_quorum: u16,
    pub certificate_persist_quorum: u16,
    pub fsync_mode: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreByteStorageProfile {
    pub profile_id: String,
    pub codec_id: String,
    pub data_shards: u16,
    pub parity_shards: u16,
    pub read_quorum: u16,
    pub write_publish_threshold: u16,
    pub target_block_bytes: u64,
    pub max_shard_bytes: u64,
    pub compression: String,
    pub encryption: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreInlinePayloadPolicy {
    pub enabled: bool,
    pub max_raw_payload_bytes: u32,
    pub absolute_encoded_record_max_bytes: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CoreStorageClassCatalog {
    pub default_class_id: String,
    pub classes: BTreeMap<String, CoreStorageClass>,
}

impl CoreStorageClassCatalog {
    pub fn release_defaults() -> Self {
        let mut classes = BTreeMap::new();
        let standard = CoreStorageClass {
            class_id: "standard-r3-ec4-2".to_string(),
            description: "three metadata replicas with erasure-coded byte shards".to_string(),
            metadata_profile: CoreMetadataProfile::metadata_r3_q2(),
            byte_profile: CoreByteStorageProfile::ec_4_2(),
            inline_payload_policy: CoreInlinePayloadPolicy::default_tiny_object_fast_path(),
            min_cell_spread: 3,
            tenant_selectable: true,
        };
        classes.insert(standard.class_id.clone(), standard);
        let replicated = CoreStorageClass {
            class_id: "low-latency-replicated".to_string(),
            description: "replicated byte profile for tiny hot operational data".to_string(),
            metadata_profile: CoreMetadataProfile::metadata_r3_q2(),
            byte_profile: CoreByteStorageProfile::replicated_3(),
            inline_payload_policy: CoreInlinePayloadPolicy::default_tiny_object_fast_path(),
            min_cell_spread: 3,
            tenant_selectable: false,
        };
        classes.insert(replicated.class_id.clone(), replicated);
        Self {
            default_class_id: "standard-r3-ec4-2".to_string(),
            classes,
        }
    }

    pub fn select(&self, requested: Option<&str>) -> Result<&CoreStorageClass> {
        let id = requested.unwrap_or(&self.default_class_id);
        let class = self
            .classes
            .get(id)
            .ok_or_else(|| anyhow::anyhow!("CoreStore storage class {id} is not defined"))?;
        class.validate()?;
        Ok(class)
    }
}

impl CoreMetadataProfile {
    pub fn metadata_r3_q2() -> Self {
        Self {
            profile_id: "metadata-r3-q2".to_string(),
            replica_count: 3,
            prepare_quorum: 2,
            certificate_persist_quorum: 2,
            fsync_mode: "wal-sync".to_string(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.replica_count == 0
            || self.prepare_quorum == 0
            || self.certificate_persist_quorum == 0
            || self.prepare_quorum > self.replica_count
            || self.certificate_persist_quorum > self.replica_count
        {
            bail!("CoreStore metadata profile quorum values are invalid");
        }
        if self.fsync_mode != "wal-sync" && self.fsync_mode != "all-sync" {
            bail!("CoreStore metadata profile fsync_mode is unsupported");
        }
        Ok(())
    }
}

impl CoreByteStorageProfile {
    pub fn ec_4_2() -> Self {
        Self {
            profile_id: "ec-4-2".to_string(),
            codec_id: "rs-gf256-vandermonde-0x11d-v1/ec-4-2".to_string(),
            data_shards: 4,
            parity_shards: 2,
            read_quorum: 4,
            write_publish_threshold: 6,
            target_block_bytes: 64 * 1024 * 1024,
            max_shard_bytes: 16 * 1024 * 1024,
            compression: "zstd".to_string(),
            encryption: "none".to_string(),
        }
    }

    pub fn replicated_3() -> Self {
        Self {
            profile_id: "replicated-3".to_string(),
            codec_id: "rs-gf256-vandermonde-0x11d-v1/replicated-3".to_string(),
            data_shards: 1,
            parity_shards: 2,
            read_quorum: 1,
            write_publish_threshold: 3,
            target_block_bytes: 16 * 1024 * 1024,
            max_shard_bytes: 16 * 1024 * 1024,
            compression: "zstd".to_string(),
            encryption: "none".to_string(),
        }
    }

    pub fn validate(&self) -> Result<()> {
        let total = self.data_shards.saturating_add(self.parity_shards);
        if self.data_shards == 0
            || total == 0
            || self.read_quorum < self.data_shards
            || self.write_publish_threshold < self.read_quorum
            || self.write_publish_threshold > total
        {
            bail!("CoreStore byte profile quorum values are invalid");
        }
        if self.target_block_bytes == 0 || self.max_shard_bytes == 0 {
            bail!("CoreStore byte profile block sizing is invalid");
        }
        Ok(())
    }
}

impl CoreInlinePayloadPolicy {
    pub fn default_tiny_object_fast_path() -> Self {
        Self {
            enabled: true,
            max_raw_payload_bytes: 32 * 1024,
            absolute_encoded_record_max_bytes: 64 * 1024,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.max_raw_payload_bytes > 64 * 1024
            || self.absolute_encoded_record_max_bytes > 64 * 1024
            || self.max_raw_payload_bytes > self.absolute_encoded_record_max_bytes
        {
            bail!("CoreStore inline payload policy exceeds the metadata value cap");
        }
        Ok(())
    }

    pub fn effective_raw_payload_cap_bytes(&self) -> u64 {
        u64::from(self.max_raw_payload_bytes).min(CORE_META_MAX_INLINE_PAYLOAD_BYTES as u64)
    }
}

impl CoreStorageClass {
    pub fn validate(&self) -> Result<()> {
        self.metadata_profile.validate()?;
        self.byte_profile.validate()?;
        self.inline_payload_policy.validate()?;
        if self.min_cell_spread == 0 {
            bail!("CoreStore storage class must require at least one cell");
        }
        Ok(())
    }
}
