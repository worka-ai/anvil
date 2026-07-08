//! CoreStore is the single durable storage boundary for Anvil.
//!
//! The current implementation is deliberately introduced as a breaking internal
//! boundary: feature code should move to these primitives instead of writing its
//! own durable journal files. The local backend below is the first backend used
//! by tests and single-node development; distributed placement/repair will live
//! behind the same API.

mod encoding;
mod local;
mod types;

use std::future::Future;

use anyhow::Result;

pub use encoding::*;
pub use local::{CorePipelineKeyring, CoreStore, CoreStoreCommitError, is_stream_head_mismatch};
pub use types::*;

pub trait CoreStoreBlockApi {
    fn write_logical_file(
        &self,
        request: WriteLogicalFileRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send;

    fn read_logical_range(
        &self,
        request: ReadLogicalRangeRequest,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send;

    fn read_logical_file_manifest(
        &self,
        locator: CoreManifestLocator,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send;

    fn verify_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> impl Future<Output = Result<CoreLogicalFileVerificationReport>> + Send;
}

impl CoreStoreBlockApi for CoreStore {
    fn write_logical_file(
        &self,
        request: WriteLogicalFileRequest,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send {
        CoreStore::write_logical_file(self, request)
    }

    fn read_logical_range(
        &self,
        request: ReadLogicalRangeRequest,
    ) -> impl Future<Output = Result<Vec<u8>>> + Send {
        CoreStore::read_logical_range(self, request)
    }

    fn read_logical_file_manifest(
        &self,
        locator: CoreManifestLocator,
    ) -> impl Future<Output = Result<CoreLogicalFileManifest>> + Send {
        async move { CoreStore::read_logical_file_manifest(self, &locator).await }
    }

    fn verify_manifest(
        &self,
        manifest: &CoreLogicalFileManifest,
    ) -> impl Future<Output = Result<CoreLogicalFileVerificationReport>> + Send {
        CoreStore::verify_logical_file_manifest(self, manifest)
    }
}
