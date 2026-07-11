use std::fs;
use std::path::Path;

fn repo_file(path: &str) -> String {
    fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path))
        .unwrap_or_else(|error| panic!("failed to read {path}: {error}"))
}

#[test]
fn byte_pipeline_uses_content_defined_chunks_and_unified_shards() {
    let logical = repo_file("anvil-core/src/core_store/local_logical_files.rs");
    assert!(logical.contains("content_defined_chunk_ranges"));
    assert!(logical.contains("gear_hash_byte"));
    assert!(logical.contains("preferred_block_boundary"));

    let init = repo_file("anvil-core/src/core_store/local_init_blob.rs");
    let erasure = repo_file("anvil-core/src/core_store/local_erasure.rs");
    let distribution = repo_file("anvil-core/src/core_store/local_block_distribution.rs");
    let reads = repo_file("anvil-core/src/core_store/local_blob_read.rs");
    for expected in [
        "encode_erasure_shards",
        "byte_pipeline.compress",
        "byte_pipeline.encrypt",
    ] {
        assert!(init.contains(expected), "missing {expected}");
    }
    assert!(reads.contains("read_logical_range"));
    for expected in [
        "local_block_id_for_stored_block",
        "anvil.block.id.v2",
        "stored_hash",
    ] {
        assert!(
            erasure.contains(expected) || init.contains(expected),
            "missing {expected}"
        );
    }
    for expected in [
        "dedupe_existing_block_shard",
        "byte_pipeline.dedupe",
        "block.shard_write",
        "block.shard_fsync",
        "record_dedupe_hit_ratio",
        "boundary_summary_hash: None",
    ] {
        assert!(distribution.contains(expected), "missing {expected}");
    }
}
