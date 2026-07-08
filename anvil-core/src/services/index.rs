use crate::anvil_api::index_service_server::IndexService;
use crate::anvil_api::*;
use crate::{
    AppState, access_control, auth, authz_journal,
    authz_scope::{DEFAULT_AUTHZ_REALM_ID, encode_realm_namespace},
    bucket_journal,
    config::Config,
    error_codes::AnvilErrorCode,
    formats::{
        full_text::{Bm25Config, FullTextIndexDefinition, FullTextQueryError},
        hash32,
        vector::VectorMetric,
    },
    full_text_segment, index_journal, index_partition_watch,
    permissions::AnvilAction,
    search_query,
    services::watch_envelope::{self, WatchEnvelopeParts},
    typed_field_segment, validation, vector_segment,
};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sha2::Sha256;
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

type HmacSha256 = Hmac<Sha256>;

const INDEX_PAGE_TOKEN_VERSION: u8 = 1;
const INDEX_PAGE_TOKEN_DOMAIN: &[u8] = b"anvil-index-page-token-v1";
const INDEX_PAGE_TOKEN_TTL_SECONDS: i64 = 15 * 60;
const QUERY_PERMISSION_PREFIX_OBJECT_CAP: i32 = 10_000;

mod operations;
mod query;
mod rpc;
mod validation_helpers;

use query::*;
pub(crate) use validation_helpers::index_kind_value_from_str;
use validation_helpers::*;

#[cfg(test)]
mod tests;
