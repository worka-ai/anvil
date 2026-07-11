use super::*;
use crate::object_links;
use crate::persistence::{Bucket, Object, ObjectVersion, ObjectVersionsPage};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use prost::Message;
use serde_json::Value as JsonValue;

const CORE_OBJECT_METADATA_SCHEMA: &str = "anvil.core.object_metadata.v1";

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataRowProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    id: i64,
    #[prost(int64, tag = "4")]
    tenant_id: i64,
    #[prost(int64, tag = "5")]
    bucket_id: i64,
    #[prost(string, tag = "6")]
    key: String,
    #[prost(string, tag = "7")]
    kind: String,
    #[prost(string, tag = "8")]
    content_hash: String,
    #[prost(int64, tag = "9")]
    size: i64,
    #[prost(string, tag = "10")]
    etag: String,
    #[prost(string, tag = "11")]
    content_type: String,
    #[prost(bool, tag = "12")]
    has_content_type: bool,
    #[prost(string, tag = "13")]
    version_id: String,
    #[prost(string, tag = "14")]
    mutation_id: String,
    #[prost(string, tag = "15")]
    index_policy_snapshot: String,
    #[prost(string, tag = "16")]
    user_metadata_hash: String,
    #[prost(int64, tag = "17")]
    authz_revision: i64,
    #[prost(string, tag = "18")]
    record_hash: String,
    #[prost(string, tag = "19")]
    created_at: String,
    #[prost(string, tag = "20")]
    deleted_at: String,
    #[prost(bool, tag = "21")]
    has_deleted_at: bool,
    #[prost(string, tag = "22")]
    storage_class: String,
    #[prost(bool, tag = "23")]
    has_storage_class: bool,
    #[prost(bytes = "vec", tag = "24")]
    user_meta_json: Vec<u8>,
    #[prost(bool, tag = "25")]
    has_user_meta: bool,
    #[prost(bytes = "vec", tag = "26")]
    shard_map_target: Vec<u8>,
    #[prost(bool, tag = "27")]
    has_shard_map: bool,
    #[prost(bytes = "vec", tag = "28")]
    checksum: Vec<u8>,
    #[prost(bool, tag = "29")]
    has_checksum: bool,
    #[prost(message, optional, tag = "30")]
    link: Option<ObjectLinkTargetProto>,
    #[prost(string, tag = "31")]
    shard_map_kind: String,
    #[prost(bool, tag = "32")]
    delete_marker: bool,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectMetadataCounterProto {
    #[prost(message, optional, tag = "1")]
    common: Option<CoreMetaRowCommonProto>,
    #[prost(string, tag = "2")]
    schema: String,
    #[prost(int64, tag = "3")]
    max_id: i64,
}

#[derive(Clone, PartialEq, Message)]
struct ObjectLinkTargetProto {
    #[prost(string, tag = "1")]
    target_key: String,
    #[prost(string, tag = "2")]
    target_version: String,
    #[prost(bool, tag = "3")]
    has_target_version: bool,
    #[prost(string, tag = "4")]
    resolution: String,
    #[prost(uint64, tag = "5")]
    generation: u64,
    #[prost(string, tag = "6")]
    created_at: String,
    #[prost(string, tag = "7")]
    created_by: String,
}

impl CoreStore {
    pub async fn put_object_metadata(&self, bucket: &Bucket, object: &Object) -> Result<()> {
        validate_object_scope(bucket, object)?;
        let _guard = self.write_lock.lock().await;
        let current_key = object_current_key(bucket, &object.key);
        let version_key = object_version_key(bucket, &object.key, object.version_id);
        let current_list_key = object_current_list_key(bucket, &object.key);
        let version_list_key = object_version_list_key(bucket, &object.key, object.version_id);
        let root_generation = object.id.max(0) as u64;
        let current_history_key =
            object_current_history_key(bucket, &object.key, root_generation, object.version_id);
        let version_history_key =
            object_version_history_key(bucket, &object.key, object.version_id, root_generation);
        let counter_key = object_id_counter_key(bucket);
        let payload = encode_object_metadata_row_at_generation(object, root_generation)?;
        let counter_payload =
            self.object_id_counter_payload_at_generation(bucket, object.id, root_generation)?;
        self.commit_coremeta_batch_by_embedded_roots(
            &object.mutation_id.to_string(),
            &[
                CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_VERSIONS,
                    table_id: TABLE_OBJECT_VERSION_META_ROW,
                    tuple_key: &version_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_list_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_VERSIONS,
                    table_id: TABLE_OBJECT_VERSION_META_ROW,
                    tuple_key: &version_list_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_history_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_VERSIONS,
                    table_id: TABLE_OBJECT_VERSION_META_ROW,
                    tuple_key: &version_history_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&payload),
                },
                CoreMetaBatchOp {
                    cf: CF_OBJECT_VERSIONS,
                    table_id: TABLE_OBJECT_VERSION_META_ROW,
                    tuple_key: &counter_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&counter_payload),
                },
            ],
        )
        .await?;
        drop(_guard);
        if let Some(data_target) = object_data_target_from_shard_map(object.shard_map.as_ref())? {
            let boundary_values = match &data_target {
                ObjectDataTarget::LogicalFile { locator, .. } => {
                    let manifest = self.read_logical_file_manifest(locator).await?;
                    manifest_boundary_values(&manifest)
                }
                ObjectDataTarget::ObjectRef { object_ref, .. } => {
                    self.read_object_manifest(object_ref).await?.boundary_values
                }
            };
            if !boundary_values.is_empty() {
                let bucket_key = boundary_schema_bucket_key(bucket.tenant_id, &bucket.name);
                self.put_boundary_values_for_object(
                    &bucket_key,
                    data_target.target_string(),
                    &boundary_values,
                )
                .await?;
            }
        }
        Ok(())
    }

    pub async fn record_object_metadata_mutation_id(
        &self,
        bucket: &Bucket,
        object_id: i64,
    ) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let counter_key = object_id_counter_key(bucket);
        let counter_payload = self.object_id_counter_payload_at_generation(
            bucket,
            object_id,
            object_id.max(0) as u64,
        )?;
        self.commit_coremeta_batch_by_embedded_roots(
            &format!(
                "object-metadata-counter:{}:{}:{object_id}",
                bucket.tenant_id, bucket.id
            ),
            &[CoreMetaBatchOp {
                cf: CF_OBJECT_VERSIONS,
                table_id: TABLE_OBJECT_VERSION_META_ROW,
                tuple_key: &counter_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&counter_payload),
            }],
        )
        .await?;
        Ok(())
    }

    pub async fn next_object_metadata_id(&self, bucket: &Bucket) -> Result<i64> {
        let max_id = match self.meta.get(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_id_counter_key(bucket),
        )? {
            Some(bytes) => decode_object_metadata_counter(&bytes)?.max_id,
            None => self.max_object_metadata_id_from_rows(bucket)?,
        };
        max_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("object id overflow"))
    }

    pub async fn read_current_object_metadata(
        &self,
        bucket: &Bucket,
        object_key: &str,
    ) -> Result<Option<Object>> {
        self.read_current_object_metadata_with_generation(bucket, object_key, None)
            .await
    }

    pub async fn read_current_object_metadata_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        root_generation: u64,
    ) -> Result<Option<Object>> {
        self.read_current_object_metadata_with_generation(bucket, object_key, Some(root_generation))
            .await
    }

    async fn read_current_object_metadata_with_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        root_generation: Option<u64>,
    ) -> Result<Option<Object>> {
        if let Some(root_generation) = root_generation {
            let mut candidates = Vec::new();
            for row in self.meta.scan_prefix(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                &object_current_history_prefix(bucket),
            )? {
                let decoded = decode_object_metadata_row_with_common(&row.payload)?;
                validate_object_scope(bucket, &decoded.object)?;
                if decoded.object.key == object_key && decoded.root_generation <= root_generation {
                    candidates.push(decoded);
                }
            }
            candidates.sort_by(|left, right| {
                right
                    .root_generation
                    .cmp(&left.root_generation)
                    .then_with(|| right.object.created_at.cmp(&left.object.created_at))
                    .then_with(|| right.object.id.cmp(&left.object.id))
                    .then_with(|| right.object.version_id.cmp(&left.object.version_id))
            });
            let Some(decoded) = candidates.into_iter().next() else {
                return Ok(None);
            };
            if decoded.object.deleted_at.is_some() {
                return Ok(None);
            }
            return Ok(Some(decoded.object));
        }

        let Some(bytes) = self.meta.get(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            &object_current_key(bucket, object_key),
        )?
        else {
            return Ok(None);
        };
        let object = decode_object_metadata_row(&bytes)?;
        validate_object_scope(bucket, &object)?;
        if object.key != object_key {
            bail!("CoreStore object metadata current row key mismatch");
        }
        if object.deleted_at.is_some() {
            return Ok(None);
        }
        Ok(Some(object))
    }

    pub async fn read_object_version_metadata(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        self.read_object_version_metadata_with_generation(bucket, object_key, version_id, None)
            .await
    }

    pub async fn read_object_version_metadata_at_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
        root_generation: u64,
    ) -> Result<Option<Object>> {
        self.read_object_version_metadata_with_generation(
            bucket,
            object_key,
            version_id,
            Some(root_generation),
        )
        .await
    }

    async fn read_object_version_metadata_with_generation(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
        root_generation: Option<u64>,
    ) -> Result<Option<Object>> {
        if let Some(root_generation) = root_generation {
            let mut candidates = Vec::new();
            for row in self.meta.scan_prefix(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &object_version_history_prefix(bucket),
            )? {
                let decoded = decode_object_metadata_row_with_common(&row.payload)?;
                validate_object_scope(bucket, &decoded.object)?;
                if decoded.object.key == object_key
                    && decoded.object.version_id == version_id
                    && decoded.root_generation <= root_generation
                {
                    candidates.push(decoded);
                }
            }
            candidates.sort_by(|left, right| right.root_generation.cmp(&left.root_generation));
            let Some(decoded) = candidates.into_iter().next() else {
                return Ok(None);
            };
            if decoded.object.deleted_at.is_some() {
                return Ok(None);
            }
            return Ok(Some(decoded.object));
        }
        let Some(bytes) = self.meta.get(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_key(bucket, object_key, version_id),
        )?
        else {
            return Ok(None);
        };
        let decoded = decode_object_metadata_row_with_common(&bytes)?;
        validate_object_scope(bucket, &decoded.object)?;
        if decoded.object.key != object_key || decoded.object.version_id != version_id {
            bail!("CoreStore object metadata version row key mismatch");
        }
        Ok(Some(decoded.object))
    }

    pub async fn read_object_version_metadata_by_id(
        &self,
        bucket: &Bucket,
        version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        for row in self.meta.scan_prefix(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_list_prefix(bucket),
        )? {
            let object = decode_object_metadata_row(&row.payload)?;
            validate_object_scope(bucket, &object)?;
            if object.version_id == version_id {
                return Ok(Some(object));
            }
        }
        Ok(None)
    }

    pub async fn list_current_object_metadata(&self, bucket: &Bucket) -> Result<Vec<Object>> {
        self.list_current_object_metadata_with_generation(bucket, None)
            .await
    }

    pub async fn list_current_object_metadata_at_generation(
        &self,
        bucket: &Bucket,
        root_generation: u64,
    ) -> Result<Vec<Object>> {
        self.list_current_object_metadata_with_generation(bucket, Some(root_generation))
            .await
    }

    async fn list_current_object_metadata_with_generation(
        &self,
        bucket: &Bucket,
        root_generation: Option<u64>,
    ) -> Result<Vec<Object>> {
        if let Some(root_generation) = root_generation {
            let mut best_by_key =
                std::collections::BTreeMap::<String, DecodedObjectMetadataRow>::new();
            for row in self.meta.scan_prefix(
                CF_OBJECT_HEADS,
                TABLE_OBJECT_HEAD_ROW,
                &object_current_history_prefix(bucket),
            )? {
                let decoded = decode_object_metadata_row_with_common(&row.payload)?;
                validate_object_scope(bucket, &decoded.object)?;
                if decoded.root_generation > root_generation {
                    continue;
                }
                let replace = best_by_key.get(&decoded.object.key).is_none_or(|existing| {
                    decoded.root_generation > existing.root_generation
                        || (decoded.root_generation == existing.root_generation
                            && decoded.object.created_at > existing.object.created_at)
                        || (decoded.root_generation == existing.root_generation
                            && decoded.object.created_at == existing.object.created_at
                            && decoded.object.id > existing.object.id)
                });
                if replace {
                    best_by_key.insert(decoded.object.key.clone(), decoded);
                }
            }
            let mut objects = best_by_key
                .into_values()
                .filter_map(|decoded| {
                    decoded
                        .object
                        .deleted_at
                        .is_none()
                        .then_some(decoded.object)
                })
                .collect::<Vec<_>>();
            objects.sort_by(|left, right| left.key.cmp(&right.key));
            return Ok(objects);
        }

        let mut objects = Vec::new();
        for row in self.meta.scan_prefix(
            CF_OBJECT_HEADS,
            TABLE_OBJECT_HEAD_ROW,
            &object_current_list_prefix(bucket),
        )? {
            let object = decode_object_metadata_row(&row.payload)?;
            validate_object_scope(bucket, &object)?;
            if object.deleted_at.is_none() {
                objects.push(object);
            }
        }
        objects.sort_by(|left, right| left.key.cmp(&right.key));
        Ok(objects)
    }

    pub async fn list_object_versions_metadata(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
    ) -> Result<ObjectVersionsPage> {
        self.list_object_versions_metadata_with_generation(
            bucket,
            prefix,
            key_marker,
            version_id_marker,
            limit,
            None,
        )
        .await
    }

    pub async fn list_object_versions_metadata_at_generation(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
        root_generation: u64,
    ) -> Result<ObjectVersionsPage> {
        self.list_object_versions_metadata_with_generation(
            bucket,
            prefix,
            key_marker,
            version_id_marker,
            limit,
            Some(root_generation),
        )
        .await
    }

    async fn list_object_versions_metadata_with_generation(
        &self,
        bucket: &Bucket,
        prefix: &str,
        key_marker: &str,
        version_id_marker: Option<uuid::Uuid>,
        limit: i32,
        root_generation: Option<u64>,
    ) -> Result<ObjectVersionsPage> {
        let mut versions_by_key = match root_generation {
            Some(root_generation) => {
                self.object_versions_by_key_at_generation(bucket, root_generation)?
            }
            None => self.object_versions_by_key(bucket)?,
        };
        let marker = match version_id_marker {
            Some(version_id_marker) => {
                let marker = versions_by_key.get(key_marker).and_then(|versions| {
                    versions
                        .iter()
                        .find(|object| object.version_id == version_id_marker)
                });
                let Some(marker) = marker else {
                    return Ok(ObjectVersionsPage {
                        versions: Vec::new(),
                        is_truncated: false,
                        next_key_marker: None,
                        next_version_id_marker: None,
                    });
                };
                Some(marker.clone())
            }
            None => None,
        };

        for versions in versions_by_key.values_mut() {
            sort_object_versions_descending(versions);
        }

        let mut selected = Vec::new();
        for versions in versions_by_key.into_values() {
            for (index, object) in versions.into_iter().enumerate() {
                if !object.key.starts_with(prefix)
                    || crate::validation::is_reserved_internal_key(&object.key)
                {
                    continue;
                }
                if let Some(marker) = marker.as_ref() {
                    if object.key.as_str() < key_marker {
                        continue;
                    }
                    if object.key == key_marker && !version_sorts_after_marker(&object, marker) {
                        continue;
                    }
                } else if object.key.as_str() <= key_marker {
                    continue;
                }

                selected.push(ObjectVersion {
                    is_delete_marker: object.deleted_at.is_some(),
                    is_latest: index == 0,
                    object,
                });
            }
        }

        let limit = limit.max(1) as usize;
        let is_truncated = selected.len() > limit;
        if is_truncated {
            selected.truncate(limit);
        }
        let (next_key_marker, next_version_id_marker) = if is_truncated {
            selected
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
            versions: selected,
            is_truncated,
            next_key_marker,
            next_version_id_marker,
        })
    }

    pub async fn delete_object_version_metadata(
        &self,
        bucket: &Bucket,
        object_key: &str,
        version_id: uuid::Uuid,
    ) -> Result<()> {
        let _guard = self.write_lock.lock().await;
        let current_key = object_current_key(bucket, object_key);
        let current_list_key = object_current_list_key(bucket, object_key);
        let version_key = object_version_key(bucket, object_key, version_id);
        let version_list_key = object_version_list_key(bucket, object_key, version_id);

        let current = self
            .meta
            .get(CF_OBJECT_HEADS, TABLE_OBJECT_HEAD_ROW, &current_key)?
            .map(|bytes| decode_object_metadata_row(&bytes))
            .transpose()?;
        let original = self
            .meta
            .get(
                CF_OBJECT_VERSIONS,
                TABLE_OBJECT_VERSION_META_ROW,
                &version_key,
            )?
            .map(|bytes| decode_object_metadata_row(&bytes))
            .transpose()?
            .ok_or_else(|| anyhow!("CoreStore object version metadata row missing"))?;
        let deleted_is_current = current
            .as_ref()
            .is_some_and(|object| object.key == object_key && object.version_id == version_id);
        let replacement = if deleted_is_current {
            self.latest_object_version_for_key_after_delete(bucket, object_key, version_id)?
        } else {
            None
        };
        let root_generation = self
            .max_object_metadata_id_from_rows(bucket)?
            .saturating_add(1)
            .max(1) as u64;
        let mut tombstone = original.clone();
        tombstone.id = i64::try_from(root_generation).unwrap_or(i64::MAX);
        tombstone.mutation_id = uuid::Uuid::new_v4();
        tombstone.deleted_at = Some(chrono::Utc::now());
        tombstone.record_hash = format!("sha256:{}", sha256_hex(tombstone.mutation_id.as_bytes()));
        let tombstone_payload = encode_object_metadata_row_at_generation_with_delete_marker(
            &tombstone,
            root_generation,
            false,
        )?;
        let replacement_payload = replacement
            .as_ref()
            .map(|object| encode_object_metadata_row_at_generation(object, root_generation))
            .transpose()?;
        let version_history_key =
            object_version_history_key(bucket, object_key, version_id, root_generation);

        let mut ops = vec![
            CoreMetaBatchOp {
                cf: CF_OBJECT_VERSIONS,
                table_id: TABLE_OBJECT_VERSION_META_ROW,
                tuple_key: &version_key,
                common: None,
                kind: CoreMetaBatchOpKind::Delete,
            },
            CoreMetaBatchOp {
                cf: CF_OBJECT_VERSIONS,
                table_id: TABLE_OBJECT_VERSION_META_ROW,
                tuple_key: &version_list_key,
                common: None,
                kind: CoreMetaBatchOpKind::Delete,
            },
            CoreMetaBatchOp {
                cf: CF_OBJECT_VERSIONS,
                table_id: TABLE_OBJECT_VERSION_META_ROW,
                tuple_key: &version_history_key,
                common: None,
                kind: CoreMetaBatchOpKind::Put(&tombstone_payload),
            },
        ];
        let mut current_history_key_holder = None;
        if deleted_is_current {
            if let Some(replacement_payload) = replacement_payload.as_ref() {
                let replacement_version = replacement
                    .as_ref()
                    .map(|object| object.version_id)
                    .unwrap_or(version_id);
                current_history_key_holder = Some(object_current_history_key(
                    bucket,
                    object_key,
                    root_generation,
                    replacement_version,
                ));
                let current_history_key = current_history_key_holder.as_ref().unwrap();
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(replacement_payload),
                });
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_list_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(replacement_payload),
                });
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_history_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(replacement_payload),
                });
            } else {
                current_history_key_holder = Some(object_current_history_key(
                    bucket,
                    object_key,
                    root_generation,
                    version_id,
                ));
                let current_history_key = current_history_key_holder.as_ref().unwrap();
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Delete,
                });
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_list_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Delete,
                });
                ops.push(CoreMetaBatchOp {
                    cf: CF_OBJECT_HEADS,
                    table_id: TABLE_OBJECT_HEAD_ROW,
                    tuple_key: &current_history_key,
                    common: None,
                    kind: CoreMetaBatchOpKind::Put(&tombstone_payload),
                });
            }
        }

        self.commit_coremeta_batch_by_embedded_roots(
            &format!("delete-object-version:{version_id}"),
            &ops,
        )
        .await?;
        Ok(())
    }

    fn object_versions_by_key(
        &self,
        bucket: &Bucket,
    ) -> Result<std::collections::BTreeMap<String, Vec<Object>>> {
        let mut versions_by_key = std::collections::BTreeMap::<String, Vec<Object>>::new();
        for row in self.meta.scan_prefix(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_list_prefix(bucket),
        )? {
            let decoded = decode_object_metadata_row_with_common(&row.payload)?;
            validate_object_scope(bucket, &decoded.object)?;
            if decoded.object.deleted_at.is_some() && !decoded.delete_marker {
                continue;
            }
            versions_by_key
                .entry(decoded.object.key.clone())
                .or_default()
                .push(decoded.object);
        }
        Ok(versions_by_key)
    }

    fn object_versions_by_key_at_generation(
        &self,
        bucket: &Bucket,
        root_generation: u64,
    ) -> Result<std::collections::BTreeMap<String, Vec<Object>>> {
        let mut latest_by_version =
            std::collections::BTreeMap::<(String, uuid::Uuid), DecodedObjectMetadataRow>::new();
        for row in self.meta.scan_prefix(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_history_prefix(bucket),
        )? {
            let decoded = decode_object_metadata_row_with_common(&row.payload)?;
            validate_object_scope(bucket, &decoded.object)?;
            if decoded.root_generation <= root_generation {
                let key = (decoded.object.key.clone(), decoded.object.version_id);
                let replace = latest_by_version
                    .get(&key)
                    .is_none_or(|existing| decoded.root_generation > existing.root_generation);
                if replace {
                    latest_by_version.insert(key, decoded);
                }
            }
        }
        let mut versions_by_key = std::collections::BTreeMap::<String, Vec<Object>>::new();
        for decoded in latest_by_version.into_values() {
            if decoded.object.deleted_at.is_some() && !decoded.delete_marker {
                continue;
            }
            versions_by_key
                .entry(decoded.object.key.clone())
                .or_default()
                .push(decoded.object);
        }
        Ok(versions_by_key)
    }

    fn latest_object_version_for_key_after_delete(
        &self,
        bucket: &Bucket,
        object_key: &str,
        deleted_version_id: uuid::Uuid,
    ) -> Result<Option<Object>> {
        let mut versions = self
            .object_versions_by_key(bucket)?
            .remove(object_key)
            .unwrap_or_default()
            .into_iter()
            .filter(|object| object.version_id != deleted_version_id)
            .collect::<Vec<_>>();
        sort_object_versions_descending(&mut versions);
        Ok(versions.into_iter().next())
    }

    fn object_id_counter_payload(&self, bucket: &Bucket, candidate_id: i64) -> Result<Vec<u8>> {
        self.object_id_counter_payload_at_generation(
            bucket,
            candidate_id,
            candidate_id.max(0) as u64,
        )
    }

    fn object_id_counter_payload_at_generation(
        &self,
        bucket: &Bucket,
        candidate_id: i64,
        root_generation: u64,
    ) -> Result<Vec<u8>> {
        let current_max = match self.meta.get(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_id_counter_key(bucket),
        )? {
            Some(bytes) => decode_object_metadata_counter(&bytes)?.max_id,
            None => self.max_object_metadata_id_from_rows(bucket)?,
        };
        let effective_max = candidate_id.max(current_max);
        encode_object_metadata_counter_at_generation(
            bucket,
            effective_max,
            root_generation.max(effective_max.max(0) as u64),
        )
    }

    fn max_object_metadata_id_from_rows(&self, bucket: &Bucket) -> Result<i64> {
        let mut max_id = 0;
        for row in self.meta.scan_prefix(
            CF_OBJECT_VERSIONS,
            TABLE_OBJECT_VERSION_META_ROW,
            &object_version_list_prefix(bucket),
        )? {
            let object = decode_object_metadata_row(&row.payload)?;
            validate_object_scope(bucket, &object)?;
            max_id = max_id.max(object.id);
        }
        Ok(max_id)
    }
}

fn object_current_key(bucket: &Bucket, object_key: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
    ])
}

fn object_version_key(bucket: &Bucket, object_key: &str, version_id: uuid::Uuid) -> Vec<u8> {
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
    ])
}

fn object_current_list_prefix(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-list-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

fn object_current_list_key(bucket: &Bucket, object_key: &str) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-list-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
    ])
}

fn object_current_history_prefix(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-history-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

fn object_current_history_key(
    bucket: &Bucket,
    object_key: &str,
    root_generation: u64,
    version_id: uuid::Uuid,
) -> Vec<u8> {
    let root_generation = root_generation.to_be_bytes();
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-history-current",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        &root_generation,
        version_id.as_bytes(),
    ])
}

fn object_version_history_prefix(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-history-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

fn object_version_history_key(
    bucket: &Bucket,
    object_key: &str,
    version_id: uuid::Uuid,
    root_generation: u64,
) -> Vec<u8> {
    let version_id = version_id.to_string();
    let root_generation = root_generation.to_be_bytes();
    meta_tuple_key(&[
        b"object-history-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
        &root_generation,
    ])
}

fn object_version_list_prefix(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-list-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

fn object_version_list_key(bucket: &Bucket, object_key: &str, version_id: uuid::Uuid) -> Vec<u8> {
    let version_id = version_id.to_string();
    meta_tuple_key(&[
        b"object-list-version",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
        object_key.as_bytes(),
        version_id.as_bytes(),
    ])
}

fn object_id_counter_key(bucket: &Bucket) -> Vec<u8> {
    meta_tuple_key(&[
        b"object-id-counter",
        &bucket.tenant_id.to_be_bytes(),
        &bucket.id.to_be_bytes(),
    ])
}

fn sort_object_versions_descending(objects: &mut [Object]) {
    objects.sort_by(|left, right| {
        right
            .created_at
            .cmp(&left.created_at)
            .then_with(|| right.id.cmp(&left.id))
            .then_with(|| right.version_id.cmp(&left.version_id))
    });
}

fn version_sorts_after_marker(object: &Object, marker: &Object) -> bool {
    object.created_at < marker.created_at
        || (object.created_at == marker.created_at
            && (object.id < marker.id
                || (object.id == marker.id && object.version_id < marker.version_id)))
}

fn object_metadata_common(object: &Object) -> CoreMetaRowCommonProto {
    object_metadata_common_at_generation(
        object,
        object.id.max(0) as u64,
        object.mutation_id.to_string(),
    )
}

fn object_metadata_common_at_generation(
    object: &Object,
    root_generation: u64,
    transaction_id: impl Into<String>,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        object_metadata_realm_id(object.tenant_id),
        object_metadata_root_key_hash(object.tenant_id, object.bucket_id),
        root_generation,
        transaction_id,
        object.created_at.timestamp_nanos_opt().unwrap_or_default() as u64,
    )
}

fn object_metadata_common_for_bucket(
    bucket: &Bucket,
    root_generation: u64,
    transaction_id: impl Into<String>,
) -> CoreMetaRowCommonProto {
    core_meta_committed_row_common(
        object_metadata_realm_id(bucket.tenant_id),
        object_metadata_root_key_hash(bucket.tenant_id, bucket.id),
        root_generation,
        transaction_id,
        unix_timestamp_nanos(),
    )
}

fn validate_object_metadata_common(
    common: &CoreMetaRowCommonProto,
    tenant_id: i64,
    bucket_id: i64,
    mutation_id: &str,
) -> Result<()> {
    if common.realm_id != object_metadata_realm_id(tenant_id) {
        bail!("CoreStore object metadata row realm mismatch");
    }
    if common.root_key_hash != object_metadata_root_key_hash(tenant_id, bucket_id) {
        bail!("CoreStore object metadata row root hash mismatch");
    }
    if !mutation_id.is_empty() && common.transaction_id != mutation_id {
        bail!("CoreStore object metadata row transaction id mismatch");
    }
    if common.visibility_state_enum() != CoreMetaVisibilityState::Committed {
        bail!("CoreStore object metadata row is not committed");
    }
    Ok(())
}

fn object_metadata_realm_id(tenant_id: i64) -> String {
    format!("tenant/{tenant_id}")
}

fn object_metadata_root_key_hash(tenant_id: i64, bucket_id: i64) -> String {
    core_meta_root_key_hash(&format!("object-metadata/{tenant_id}/{bucket_id}"))
}

fn validate_object_scope(bucket: &Bucket, object: &Object) -> Result<()> {
    if object.tenant_id != bucket.tenant_id || object.bucket_id != bucket.id {
        bail!("CoreStore object metadata row scope mismatch");
    }
    Ok(())
}

fn encode_object_metadata_row(object: &Object) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation(object, object.id.max(0) as u64)
}

fn encode_object_metadata_row_at_generation(
    object: &Object,
    root_generation: u64,
) -> Result<Vec<u8>> {
    encode_object_metadata_row_at_generation_with_delete_marker(
        object,
        root_generation,
        object.deleted_at.is_some(),
    )
}

fn encode_object_metadata_row_at_generation_with_delete_marker(
    object: &Object,
    root_generation: u64,
    delete_marker: bool,
) -> Result<Vec<u8>> {
    let proto = ObjectMetadataRowProto {
        common: Some(object_metadata_common_at_generation(
            object,
            root_generation,
            object.mutation_id.to_string(),
        )),
        schema: CORE_OBJECT_METADATA_SCHEMA.to_string(),
        id: object.id,
        tenant_id: object.tenant_id,
        bucket_id: object.bucket_id,
        key: object.key.clone(),
        kind: match object.kind {
            object_links::ObjectEntryKind::Blob => "blob".to_string(),
            object_links::ObjectEntryKind::Link => "link".to_string(),
        },
        content_hash: object.content_hash.clone(),
        size: object.size,
        etag: object.etag.clone(),
        content_type: object.content_type.clone().unwrap_or_default(),
        has_content_type: object.content_type.is_some(),
        version_id: object.version_id.to_string(),
        mutation_id: object.mutation_id.to_string(),
        index_policy_snapshot: object.index_policy_snapshot.clone(),
        user_metadata_hash: object.user_metadata_hash.clone(),
        authz_revision: object.authz_revision,
        record_hash: object.record_hash.clone(),
        created_at: object.created_at.to_rfc3339(),
        deleted_at: object
            .deleted_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        has_deleted_at: object.deleted_at.is_some(),
        storage_class: object.storage_class.clone().unwrap_or_default(),
        has_storage_class: object.storage_class.is_some(),
        user_meta_json: optional_json_bytes(object.user_meta.as_ref())?.unwrap_or_default(),
        has_user_meta: object.user_meta.is_some(),
        shard_map_target: optional_object_data_target_bytes(object.shard_map.as_ref())?
            .map(|target| target.1)
            .unwrap_or_default(),
        has_shard_map: object.shard_map.is_some(),
        checksum: object.checksum.clone().unwrap_or_default(),
        has_checksum: object.checksum.is_some(),
        link: object.link.as_ref().map(link_to_proto),
        shard_map_kind: optional_object_data_target_bytes(object.shard_map.as_ref())?
            .map(|target| target.0)
            .unwrap_or_default(),
        delete_marker,
    };
    encode_deterministic(&proto)
}

#[derive(Debug, Clone)]
struct DecodedObjectMetadataRow {
    object: Object,
    root_generation: u64,
    delete_marker: bool,
}

fn decode_object_metadata_row(bytes: &[u8]) -> Result<Object> {
    Ok(decode_object_metadata_row_with_common(bytes)?.object)
}

fn decode_object_metadata_row_with_common(bytes: &[u8]) -> Result<DecodedObjectMetadataRow> {
    let proto = ObjectMetadataRowProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "object metadata row")?;
    if proto.schema != CORE_OBJECT_METADATA_SCHEMA {
        bail!("CoreStore object metadata row has invalid schema");
    }
    let common = proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore object metadata row missing CoreMeta common"))?;
    validate_object_metadata_common(common, proto.tenant_id, proto.bucket_id, &proto.mutation_id)?;
    let root_generation = common.root_generation;
    let object = Object {
        id: proto.id,
        tenant_id: proto.tenant_id,
        bucket_id: proto.bucket_id,
        key: proto.key,
        kind: match proto.kind.as_str() {
            "blob" => object_links::ObjectEntryKind::Blob,
            "link" => object_links::ObjectEntryKind::Link,
            _ => bail!("CoreStore object metadata row has invalid object kind"),
        },
        content_hash: proto.content_hash,
        size: proto.size,
        etag: proto.etag,
        content_type: proto.has_content_type.then_some(proto.content_type),
        version_id: uuid::Uuid::parse_str(&proto.version_id)
            .context("CoreStore object metadata row version_id is invalid")?,
        mutation_id: uuid::Uuid::parse_str(&proto.mutation_id)
            .context("CoreStore object metadata row mutation_id is invalid")?,
        index_policy_snapshot: proto.index_policy_snapshot,
        user_metadata_hash: proto.user_metadata_hash,
        authz_revision: proto.authz_revision,
        record_hash: proto.record_hash,
        created_at: parse_datetime(&proto.created_at, "created_at")?,
        deleted_at: if proto.has_deleted_at {
            Some(parse_datetime(&proto.deleted_at, "deleted_at")?)
        } else {
            None
        },
        storage_class: if proto.has_storage_class {
            Some(proto.storage_class)
        } else {
            None
        },
        user_meta: if proto.has_user_meta {
            Some(decode_canonical_json_bytes(
                &proto.user_meta_json,
                "CoreStore object metadata user_meta",
            )?)
        } else {
            None
        },
        shard_map: if proto.has_shard_map {
            Some(shard_map_from_object_data_target(
                &proto.shard_map_kind,
                &proto.shard_map_target,
            )?)
        } else {
            None
        },
        checksum: proto.has_checksum.then_some(proto.checksum),
        link: proto.link.map(link_from_proto).transpose()?,
    };
    Ok(DecodedObjectMetadataRow {
        object,
        root_generation,
        delete_marker: proto.delete_marker,
    })
}

fn encode_object_metadata_counter(bucket: &Bucket, max_id: i64) -> Result<Vec<u8>> {
    encode_object_metadata_counter_at_generation(bucket, max_id, max_id.max(0) as u64)
}

fn encode_object_metadata_counter_at_generation(
    bucket: &Bucket,
    max_id: i64,
    root_generation: u64,
) -> Result<Vec<u8>> {
    encode_deterministic(&ObjectMetadataCounterProto {
        common: Some(core_meta_committed_row_common(
            object_metadata_realm_id(bucket.tenant_id),
            object_metadata_root_key_hash(bucket.tenant_id, bucket.id),
            root_generation,
            String::new(),
            unix_timestamp_nanos(),
        )),
        schema: "anvil.core.object_metadata_counter.v1".to_string(),
        max_id,
    })
}

fn decode_object_metadata_counter(bytes: &[u8]) -> Result<ObjectMetadataCounterProto> {
    let proto = ObjectMetadataCounterProto::decode(bytes)?;
    ensure_round_trips(&proto, bytes, "object metadata counter")?;
    if proto.schema != "anvil.core.object_metadata_counter.v1" {
        bail!("CoreStore object metadata counter row has invalid schema");
    }
    proto
        .common
        .as_ref()
        .ok_or_else(|| anyhow!("CoreStore object metadata counter row missing CoreMeta common"))?;
    Ok(proto)
}

fn link_to_proto(link: &object_links::ObjectLinkTarget) -> ObjectLinkTargetProto {
    ObjectLinkTargetProto {
        target_key: link.target_key.clone(),
        target_version: link
            .target_version
            .map(|value| value.to_string())
            .unwrap_or_default(),
        has_target_version: link.target_version.is_some(),
        resolution: match link.resolution {
            object_links::ObjectLinkResolution::Follow => "follow".to_string(),
            object_links::ObjectLinkResolution::Redirect => "redirect".to_string(),
        },
        generation: link.generation,
        created_at: link.created_at.to_rfc3339(),
        created_by: link.created_by.clone(),
    }
}

fn link_from_proto(proto: ObjectLinkTargetProto) -> Result<object_links::ObjectLinkTarget> {
    Ok(object_links::ObjectLinkTarget {
        target_key: proto.target_key,
        target_version: if proto.has_target_version {
            Some(
                uuid::Uuid::parse_str(&proto.target_version)
                    .context("CoreStore object link target version is invalid")?,
            )
        } else {
            None
        },
        resolution: match proto.resolution.as_str() {
            "follow" => object_links::ObjectLinkResolution::Follow,
            "redirect" => object_links::ObjectLinkResolution::Redirect,
            _ => bail!("CoreStore object link resolution is invalid"),
        },
        generation: proto.generation,
        created_at: parse_datetime(&proto.created_at, "link.created_at")?,
        created_by: proto.created_by,
    })
}

fn parse_datetime(value: &str, field: &str) -> Result<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(value)
        .with_context(|| format!("CoreStore object metadata row {field} is invalid"))?
        .with_timezone(&Utc))
}

fn optional_json_bytes(value: Option<&JsonValue>) -> Result<Option<Vec<u8>>> {
    value
        .map(|value| serde_json::to_vec(&canonical_json(value)))
        .transpose()
        .map_err(Into::into)
}

fn decode_canonical_json_bytes(bytes: &[u8], label: &str) -> Result<JsonValue> {
    let value: JsonValue = serde_json::from_slice(bytes)?;
    if serde_json::to_vec(&canonical_json(&value))? != bytes {
        bail!("{label} is not canonical JSON");
    }
    Ok(value)
}

enum ObjectDataTarget {
    LogicalFile {
        locator: CoreManifestLocator,
        target: String,
        bytes: Vec<u8>,
    },
    ObjectRef {
        object_ref: CoreObjectRef,
        target: String,
        bytes: Vec<u8>,
    },
}

impl ObjectDataTarget {
    fn kind(&self) -> &'static str {
        match self {
            Self::LogicalFile { .. } => "logical_file",
            Self::ObjectRef { .. } => "object_ref",
        }
    }

    fn bytes(&self) -> &[u8] {
        match self {
            Self::LogicalFile { bytes, .. } => bytes,
            Self::ObjectRef { bytes, .. } => bytes,
        }
    }

    fn target_string(&self) -> &str {
        match self {
            Self::LogicalFile { target, .. } => target,
            Self::ObjectRef { target, .. } => target,
        }
    }
}

fn optional_object_data_target_bytes(
    value: Option<&JsonValue>,
) -> Result<Option<(String, Vec<u8>)>> {
    value
        .map(|value| {
            object_data_target_from_json(value)
                .map(|target| (target.kind().to_string(), target.bytes().to_vec()))
        })
        .transpose()
}

fn object_data_target_from_shard_map(
    value: Option<&JsonValue>,
) -> Result<Option<ObjectDataTarget>> {
    value.map(object_data_target_from_json).transpose()
}

fn object_data_target_from_json(value: &JsonValue) -> Result<ObjectDataTarget> {
    if value.get("schema").and_then(JsonValue::as_str) != Some("anvil.core.object_data_target.v1") {
        bail!("CoreStore object metadata shard map is not canonical object data target");
    }
    let kind = value
        .get("kind")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map kind is missing"))?;
    let target = value
        .get("target")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| anyhow!("CoreStore object metadata shard map target is missing"))?;
    match kind {
        "logical_file" => {
            let bytes = URL_SAFE_NO_PAD
                .decode(target)
                .context("CoreStore object metadata logical-file target is not base64url")?;
            let locator = decode_manifest_locator_proto(&bytes)?;
            Ok(ObjectDataTarget::LogicalFile {
                locator,
                target: target.to_string(),
                bytes,
            })
        }
        "object_ref" => {
            let object_ref = decode_core_object_ref_target(target)?;
            Ok(ObjectDataTarget::ObjectRef {
                object_ref,
                target: target.to_string(),
                bytes: target.as_bytes().to_vec(),
            })
        }
        other => {
            bail!("CoreStore object metadata logical-file shard map kind {other:?} is unsupported")
        }
    }
}

fn shard_map_from_object_data_target(kind: &str, target: &[u8]) -> Result<JsonValue> {
    match kind {
        "logical_file" => {
            decode_manifest_locator_proto(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "logical_file",
                "target": URL_SAFE_NO_PAD.encode(target),
            }))
        }
        "object_ref" => {
            let target = std::str::from_utf8(target)
                .context("CoreStore object metadata object-ref target is not UTF-8")?;
            decode_core_object_ref_target(target)?;
            Ok(serde_json::json!({
                "schema": "anvil.core.object_data_target.v1",
                "kind": "object_ref",
                "target": target,
            }))
        }
        other => {
            bail!("CoreStore object metadata logical-file shard map kind {other:?} is unsupported")
        }
    }
}

fn canonical_json(value: &JsonValue) -> JsonValue {
    match value {
        JsonValue::Array(values) => JsonValue::Array(values.iter().map(canonical_json).collect()),
        JsonValue::Object(values) => {
            let mut sorted = serde_json::Map::new();
            let mut keys = values.keys().collect::<Vec<_>>();
            keys.sort();
            for key in keys {
                sorted.insert(key.clone(), canonical_json(&values[key]));
            }
            JsonValue::Object(sorted)
        }
        scalar => scalar.clone(),
    }
}

fn encode_deterministic<M: Message>(message: &M) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    message.encode(&mut bytes)?;
    Ok(bytes)
}

fn ensure_round_trips<M: Message>(message: &M, bytes: &[u8], label: &str) -> Result<()> {
    let mut canonical = Vec::new();
    message.encode(&mut canonical)?;
    if canonical != bytes {
        bail!("CoreStore {label} protobuf is not deterministic canonical encoding");
    }
    Ok(())
}
