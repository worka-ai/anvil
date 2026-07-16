use super::local::write_file_atomic;
use super::*;
use crate::formats::writer::{
    ByteSource, CoreMetaMutation, DurabilityClass, LogicalFileWrite, WriterBuildOutput,
};
use anyhow::{Context, bail};
use std::time::Instant;
use tokio::fs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CoreFormatWriteReceipt {
    pub logical_file_ids: Vec<String>,
    pub manifest_hashes: Vec<String>,
    pub written_object_refs: Vec<CoreObjectRef>,
    pub coremeta_certificate_hashes: Vec<String>,
}

impl CoreStore {
    pub async fn write_format_build_output(
        &self,
        output: WriterBuildOutput,
    ) -> Result<CoreFormatWriteReceipt> {
        let _guard = self.acquire_corestore_write_lock().await;
        let mut logical_file_ids = Vec::new();
        let mut manifest_hashes = Vec::new();
        let mut written_object_refs = Vec::new();
        for logical_file in output.logical_files {
            let logical_file_id = logical_file.logical_file_id.clone();
            let writer_family = logical_file.writer_family.as_str().to_string();
            let materialise_started_at = Instant::now();
            crate::perf::record_duration(
                "anvil_materialisation_lag_ms",
                &[
                    ("writer_family", writer_family.as_str()),
                    ("bucket", "local"),
                    ("boundary_schema_generation", "0"),
                ],
                materialise_started_at.elapsed(),
            );
            record_corestore_trace_event("materialiser.plan", "ok");
            let writer_started_at = Instant::now();
            let inline_metadata = logical_file.durability_class == DurabilityClass::InlineMetadata;
            let (object_ref, manifest_hash) = if inline_metadata {
                let ByteSource::InlineBytes(bytes) = logical_file.bytes else {
                    bail!("CoreFormat inline metadata requires an inline byte source");
                };
                let object_ref = self
                    .put_inline_blob(
                        PutBlob {
                            logical_name: logical_file.logical_file_id,
                            bytes,
                            boundary_values: logical_file.boundary_values,
                            region_id: logical_file.region_id,
                            mutation_id: logical_file.mutation_id,
                        },
                        logical_file.writer_family.as_str(),
                        self.storage_class_catalog()
                            .select(None)?
                            .inline_payload_policy
                            .clone(),
                    )
                    .await
                    .with_context(|| format!("write inline CoreFormat file {logical_file_id}"))?;
                let manifest_hash = object_ref.hash.clone();
                (object_ref, manifest_hash)
            } else {
                let written = self
                    .write_format_logical_file(logical_file)
                    .await
                    .with_context(|| format!("write CoreFormat logical file {logical_file_id}"))?;
                let manifest_hash = written.locator.manifest_hash.clone();
                (
                    core_object_ref_from_logical_file_write(&written),
                    manifest_hash,
                )
            };
            crate::perf::record_duration(
                "anvil_writer_build_duration_ms",
                &[
                    ("writer_family", writer_family.as_str()),
                    ("output_kind", "logical_file"),
                    ("status", "ok"),
                ],
                writer_started_at.elapsed(),
            );
            record_corestore_trace_event("writer.build", "ok");
            manifest_hashes.push(manifest_hash);
            logical_file_ids.push(logical_file_id);
            written_object_refs.push(object_ref);
        }

        let coremeta_certificate_hashes = self
            .commit_format_coremeta_mutations(output.core_meta_mutations)
            .await?;
        Ok(CoreFormatWriteReceipt {
            logical_file_ids,
            manifest_hashes,
            written_object_refs,
            coremeta_certificate_hashes,
        })
    }

    async fn write_format_logical_file(
        &self,
        logical_file: LogicalFileWrite,
    ) -> Result<CoreLogicalFileWrite> {
        match logical_file.bytes.clone() {
            ByteSource::InlineBytes(_) => {
                self.write_logical_file_with_locator(
                    logical_file.into_write_logical_file_request()?,
                )
                .await
            }
            ByteSource::TempFile { path, hash, length } => {
                self.write_logical_file_path_with_locator(
                    logical_file.into_write_logical_file_path_request(
                        path,
                        normalise_sha256_hash(&hash)?,
                        length,
                    ),
                )
                .await
            }
            ByteSource::LandedBytes {
                hash,
                length,
                relative_path,
                ..
            } => {
                let source_path = self
                    .storage()
                    .resolve_relative_storage_path(&relative_path)?;
                self.write_logical_file_path_with_locator(
                    logical_file.into_write_logical_file_path_request(
                        source_path,
                        normalise_sha256_hash(&hash)?,
                        length,
                    ),
                )
                .await
            }
            ByteSource::ExistingCoreObject {
                object_ref,
                byte_start,
                byte_end,
            } => {
                let (path, hash, length) = self
                    .materialise_existing_core_object_range_to_staged_file(
                        object_ref, byte_start, byte_end,
                    )
                    .await?;
                let result = self
                    .write_logical_file_path_with_locator(
                        logical_file.into_write_logical_file_path_request(
                            path.clone(),
                            hash,
                            length,
                        ),
                    )
                    .await;
                let _ = fs::remove_file(&path).await;
                result
            }
        }
    }

    async fn materialise_existing_core_object_range_to_staged_file(
        &self,
        object_ref: CoreObjectRef,
        byte_start: u64,
        byte_end: u64,
    ) -> Result<(std::path::PathBuf, String, u64)> {
        if byte_end < byte_start || byte_end > object_ref.logical_size {
            bail!("CoreFormatWriter existing object byte range is invalid");
        }
        let bytes = self
            .get_blob_range(GetBlobRange {
                object_ref,
                range: CoreByteRange {
                    start: byte_start,
                    end_exclusive: byte_end,
                },
            })
            .await?;
        let hash = format!("sha256:{}", sha256_hex(&bytes));
        let length = bytes.len() as u64;
        let path = self
            .storage()
            .temp_dir_path()
            .join(format!("format-source-{}.tmp", uuid::Uuid::new_v4()));
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let started_at = Instant::now();
        write_file_atomic(&path, &bytes).await?;
        crate::perf::record_io_duration(
            "core_store",
            "format_writer_stage_existing_core_object_range",
            &path,
            length,
            started_at.elapsed(),
        );
        Ok((path, hash, length))
    }

    async fn commit_format_coremeta_mutations(
        &self,
        mutations: Vec<CoreMetaMutation>,
    ) -> Result<Vec<String>> {
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        let transaction_id = common_format_transaction_id(&mutations)?;
        let ops = mutations
            .iter()
            .map(CoreMetaMutation::as_batch_op)
            .collect::<Vec<_>>();
        let outcomes = self
            .commit_coremeta_batch_by_embedded_roots(&transaction_id, &ops)
            .await?;
        Ok(outcomes
            .into_iter()
            .map(|outcome| outcome.certificate_hash)
            .collect())
    }
}

fn common_format_transaction_id(mutations: &[CoreMetaMutation]) -> Result<String> {
    let Some(first) = mutations.first() else {
        bail!("CoreFormatWriter mutation list is empty");
    };
    for mutation in mutations {
        if mutation.transaction_id != first.transaction_id {
            bail!("CoreFormatWriter output spans multiple transaction ids");
        }
    }
    Ok(first.transaction_id.clone())
}

fn normalise_sha256_hash(hash: &str) -> Result<String> {
    if hash.starts_with("sha256:") {
        Ok(hash.to_string())
    } else {
        Ok(format!("sha256:{hash}"))
    }
}
