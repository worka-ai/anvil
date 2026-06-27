use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::auth;
use anvil_core::permissions::AnvilAction;
use anvil_core::validation;
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_core::Stream;
use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::pin::Pin;

#[derive(Deserialize)]
struct CreateBucketConfiguration {
    #[serde(rename = "LocationConstraint")]
    location_constraint: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CompleteMultipartUploadXml {
    #[serde(rename = "Part", default)]
    parts: Vec<CompleteMultipartUploadXmlPart>,
}

#[derive(Debug, Deserialize)]
struct CompleteMultipartUploadXmlPart {
    #[serde(rename = "PartNumber")]
    part_number: i32,
    #[serde(rename = "ETag")]
    etag: String,
}

fn s3_error(code: &str, message: &str, status: axum::http::StatusCode) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n</Error>\n",
        code,
        xml_escape(message)
    );
    Response::builder()
        .status(status)
        .header("Content-Type", "application/xml")
        .body(Body::from(body))
        .unwrap()
}
pub fn app(state: AppState) -> Router {
    let public = Router::new()
        .route("/ready", get(readiness_check))
        .with_state(state.clone());

    let s3_routes = Router::new()
        .route("/", get(list_buckets)) // ListBuckets
        .route(
            "/{bucket}",
            put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
                .get(list_objects),
        )
        .route(
            "/{bucket}/",
            get(list_objects)
                .put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket),
        )
        .route(
            "/{bucket}/{*path}",
            get(get_object)
                .put(put_object)
                .post(post_object)
                .delete(delete_object)
                .head(head_object),
        )
        .with_state(state.clone())
        .route_layer(middleware::from_fn(aws_chunked_decoder))
        .route_layer(middleware::from_fn_with_state(state.clone(), sigv4_auth))
        .route_layer(middleware::from_fn(reserved_namespace_guard));

    public.merge(s3_routes)
}

async fn reserved_namespace_guard(req: Request, next: Next) -> Response {
    if request_targets_reserved_namespace(&req) {
        return s3_error(
            "AccessDenied",
            "UnauthorizedReservedNamespace",
            axum::http::StatusCode::FORBIDDEN,
        );
    }
    next.run(req).await
}

fn request_targets_reserved_namespace(req: &Request) -> bool {
    let path = req.uri().path().trim_start_matches('/');
    let mut parts = path.splitn(2, '/');
    let _bucket = parts.next();

    if let Some(object_key) = parts.next() {
        if validation::is_reserved_internal_key(object_key) {
            return true;
        }
    }

    req.uri().query().is_some_and(|query| {
        query.split('&').any(|pair| {
            let mut fields = pair.splitn(2, '=');
            matches!(fields.next(), Some("prefix"))
                && fields
                    .next()
                    .map(percent_decode_query_component)
                    .is_some_and(|prefix| validation::is_reserved_internal_key(&prefix))
        })
    })
}

fn percent_decode_query_component(value: &str) -> String {
    let value = value.replace('+', " ");
    percent_decode(value.as_bytes())
}

fn percent_decode(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_value(bytes[i + 1]), hex_value(bytes[i + 2])) {
                out.push((hi << 4) | lo);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

async fn list_buckets(State(state): State<AppState>, req: Request) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    match state
        .bucket_manager
        .list_buckets(claims.tenant_id, claims.scopes.as_slice())
        .await
    {
        Ok(buckets) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListAllMyBucketsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str("  <Owner>\n");
            xml.push_str(&format!("    <ID>{}</ID>\n", claims.tenant_id));
            // DisplayName is not stored, so we'll use tenant_id for now.
            xml.push_str(&format!(
                "    <DisplayName>{}</DisplayName>\n",
                claims.tenant_id
            ));
            xml.push_str("  </Owner>\n");
            xml.push_str("  <Buckets>\n");
            for b in buckets {
                xml.push_str("    <Bucket>\n");
                xml.push_str(&format!("      <Name>{}</Name>\n", xml_escape(&b.name)));
                xml.push_str(&format!(
                    "      <CreationDate>{}</CreationDate>\n",
                    b.created_at.to_rfc3339()
                ));
                xml.push_str("    </Bucket>\n");
            }
            xml.push_str("  </Buckets>\n");
            xml.push_str("</ListAllMyBucketsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    req: Request,
) -> Response {
    // The S3 `CreateBucket` operation can contain an XML body with the location
    // constraint. We must consume the body for the handler to be matched correctly,
    // even if we don't use the content for now.

    // Claims may be absent for anonymous; handler will enforce bucket public access
    let claims = req.extensions().get::<Claims>().cloned();
    let claims = match claims {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    let region = if !bytes.is_empty() {
        if let Ok(config) = quick_xml::de::from_reader::<_, CreateBucketConfiguration>(&bytes[..]) {
            config.location_constraint.unwrap_or(state.region.clone())
        } else {
            state.region.clone()
        }
    } else {
        state.region.clone()
    };

    match state
        .bucket_manager
        .create_bucket(claims.tenant_id, &bucket, &region, &claims.scopes)
        .await
    {
        Ok(_) => (axum::http::StatusCode::OK, "").into_response(),
        Err(status) => match status.code() {
            tonic::Code::AlreadyExists => s3_error(
                "BucketAlreadyExists",
                status.message(),
                axum::http::StatusCode::CONFLICT,
            ),
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn delete_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    match state
        .bucket_manager
        .delete_bucket(&bucket, claims.scopes.as_slice())
        .await
    {
        Ok(_) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::NotFound => s3_error(
                "NoSuchBucket",
                status.message(),
                axum::http::StatusCode::NOT_FOUND,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

fn s3_redirect(region: &str) -> Response {
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>PermanentRedirect</Code>\n  <Message>The bucket is in this region: {}. Please use this region to retry the request.</Message>\n  <BucketRegion>{}</BucketRegion>\n</Error>\n",
        region, region
    );
    Response::builder()
        .status(axum::http::StatusCode::MOVED_PERMANENTLY)
        .header("Content-Type", "application/xml")
        .header("x-amz-bucket-region", region)
        .body(Body::from(body))
        .unwrap()
}

async fn head_bucket(
    State(state): State<AppState>,
    Path(bucket_name): Path<String>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials for HEAD request",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    match state
        .db
        .get_bucket_by_name(claims.tenant_id, &bucket_name)
        .await
    {
        Ok(Some(bucket)) => {
            if bucket.region != state.region {
                return s3_redirect(&bucket.region);
            }
            (axum::http::StatusCode::OK, "").into_response()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn list_objects(
    State(state): State<AppState>,
    bucket: Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    if q.contains_key("versions") {
        let request_is_authenticated = req.extensions().get::<Claims>().is_some();
        return list_object_versions_response(state, claims, &bucket, &q, request_is_authenticated)
            .await;
    }

    if q.contains_key("uploads") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        return list_multipart_uploads_response(state, claims, &bucket, &q).await;
    }

    if q.contains_key("location") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        return get_bucket_location_response(state, claims, &bucket).await;
    }

    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let continuation_token = q
        .get("continuation-token")
        .or_else(|| q.get("continuationToken"))
        .cloned();
    let start_after = continuation_token.clone().unwrap_or_else(|| {
        q.get("start-after")
            .or_else(|| q.get("startAfter"))
            .cloned()
            .unwrap_or_default()
    });
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);
    let fetch_limit = max_keys.saturating_add(1);

    match state
        .object_manager
        .list_objects(
            claims,
            &bucket,
            &prefix,
            &start_after,
            fetch_limit,
            &delimiter,
        )
        .await
    {
        Ok((mut objects, mut common_prefixes)) => {
            // Basic ListObjectsV2 XML
            let requested_max_keys = if max_keys <= 0 {
                1000
            } else {
                max_keys as usize
            };
            let total_count = objects.len() + common_prefixes.len();
            let is_truncated = total_count > requested_max_keys;
            let mut next_continuation_token = None;
            if is_truncated {
                while objects.len() + common_prefixes.len() > requested_max_keys {
                    if let Some(prefix) = common_prefixes.pop() {
                        next_continuation_token = Some(prefix);
                    } else if let Some(object) = objects.pop() {
                        next_continuation_token = Some(object.key);
                    }
                }
                if next_continuation_token.is_none() {
                    next_continuation_token = objects
                        .last()
                        .map(|object| object.key.clone())
                        .or_else(|| common_prefixes.last().cloned());
                }
            }
            let key_count = (objects.len() + common_prefixes.len()) as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            if let Some(token) = continuation_token {
                xml.push_str(&format!(
                    "  <ContinuationToken>{}</ContinuationToken>\n",
                    xml_escape(&token)
                ));
            }
            xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
            if let Some(token) = next_continuation_token {
                xml.push_str(&format!(
                    "  <NextContinuationToken>{}</NextContinuationToken>\n",
                    xml_escape(&token)
                ));
            }
            for o in objects {
                xml.push_str("  <Contents>\n");
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&o.key)));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    o.created_at.to_rfc3339()
                ));
                xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", o.etag));
                xml.push_str(&format!("    <Size>{}</Size>\n", o.size));
                xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                xml.push_str("  </Contents>\n");
            }
            for p in common_prefixes {
                xml.push_str("  <CommonPrefixes>\n");
                xml.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&p)));
                xml.push_str("  </CommonPrefixes>\n");
            }
            xml.push_str("</ListBucketResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchBucket",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn get_bucket_location_response(state: AppState, claims: Claims, bucket: &str) -> Response {
    if !auth::is_authorized(AnvilAction::BucketRead, bucket, &claims.scopes) {
        return s3_error(
            "AccessDenied",
            "Permission denied",
            axum::http::StatusCode::FORBIDDEN,
        );
    }

    match state.db.get_bucket_by_name(claims.tenant_id, bucket).await {
        Ok(Some(bucket)) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<LocationConstraint xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">{}</LocationConstraint>\n",
                xml_escape(&bucket.region)
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("x-amz-bucket-region", bucket.region)
                .body(Body::from(xml))
                .unwrap()
        }
        Ok(None) => s3_error(
            "NoSuchBucket",
            "The specified bucket does not exist",
            axum::http::StatusCode::NOT_FOUND,
        ),
        Err(e) => s3_error(
            "InternalError",
            &e.to_string(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn list_multipart_uploads_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    q: &HashMap<String, String>,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let max_uploads: i32 = q
        .get("max-uploads")
        .or_else(|| q.get("maxUploads"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_multipart_uploads(
            claims.tenant_id,
            bucket,
            &prefix,
            &key_marker,
            max_uploads,
            &claims.scopes,
        )
        .await
    {
        Ok(uploads) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <KeyMarker>{}</KeyMarker>\n  <UploadIdMarker></UploadIdMarker>\n  <NextKeyMarker></NextKeyMarker>\n  <NextUploadIdMarker></NextUploadIdMarker>\n  <Delimiter></Delimiter>\n  <Prefix>{}</Prefix>\n  <MaxUploads>{}</MaxUploads>\n  <IsTruncated>false</IsTruncated>\n",
                xml_escape(bucket),
                xml_escape(&key_marker),
                xml_escape(&prefix),
                max_uploads
            );
            for upload in uploads {
                xml.push_str("  <Upload>\n");
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&upload.key)));
                xml.push_str(&format!("    <UploadId>{}</UploadId>\n", upload.upload_id));
                xml.push_str(&format!(
                    "    <Initiated>{}</Initiated>\n",
                    upload.created_at.to_rfc3339()
                ));
                xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                xml.push_str("  </Upload>\n");
            }
            xml.push_str("</ListMultipartUploadsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchBucket"),
    }
}

async fn list_object_versions_response(
    state: AppState,
    claims: Option<Claims>,
    bucket: &str,
    q: &HashMap<String, String>,
    request_is_authenticated: bool,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let key_marker = q
        .get("key-marker")
        .or_else(|| q.get("keyMarker"))
        .cloned()
        .unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_object_versions(claims, bucket, &prefix, &key_marker, max_keys)
        .await
    {
        Ok(versions) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!(
                "  <KeyMarker>{}</KeyMarker>\n",
                xml_escape(&key_marker)
            ));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str("  <IsTruncated>false</IsTruncated>\n");
            for version in versions {
                let object = version.object;
                let tag = if version.is_delete_marker {
                    "DeleteMarker"
                } else {
                    "Version"
                };
                xml.push_str(&format!("  <{}>\n", tag));
                xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
                xml.push_str(&format!(
                    "    <VersionId>{}</VersionId>\n",
                    object.version_id
                ));
                xml.push_str(&format!(
                    "    <IsLatest>{}</IsLatest>\n",
                    if version.is_latest { "true" } else { "false" }
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    object.created_at.to_rfc3339()
                ));
                if !version.is_delete_marker {
                    xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
                    xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
                    xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
                }
                xml.push_str(&format!("  </{}>\n", tag));
            }
            xml.push_str("</ListVersionsResult>\n");

            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => {
            s3_status_to_response_for_auth(status, request_is_authenticated, "NoSuchBucket")
        }
    }
}

fn s3_status_to_response_for_auth(
    status: tonic::Status,
    request_is_authenticated: bool,
    not_found_code: &str,
) -> Response {
    match status.code() {
        tonic::Code::FailedPrecondition => {
            if status.message().starts_with("Bucket is in region ") {
                let region = status.message().trim_start_matches("Bucket is in region ");
                return s3_redirect(region);
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => {
            if !request_is_authenticated {
                s3_error(
                    "AccessDenied",
                    status.message(),
                    axum::http::StatusCode::FORBIDDEN,
                )
            } else {
                s3_error(
                    not_found_code,
                    status.message(),
                    axum::http::StatusCode::NOT_FOUND,
                )
            }
        }
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn readiness_check(State(state): State<AppState>) -> Response {
    // DB readiness: attempt a lightweight operation. If Persistence exposes no ping, rely on pool creation success earlier.
    // Cluster readiness: at least 1 peer known (self included)
    let peers = state.cluster.read().await.len();
    if peers >= 1 {
        (axum::http::StatusCode::OK, "READY").into_response()
    } else {
        let body = serde_json::json!({"status":"not_ready","peers":peers});
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            axum::response::Json(body),
        )
            .into_response()
    }
}

async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();
    if let Some(upload_id) = q.get("uploadId") {
        let claims = match claims {
            Some(claims) => claims,
            None => {
                return s3_error(
                    "AccessDenied",
                    "Missing credentials",
                    axum::http::StatusCode::FORBIDDEN,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return list_multipart_parts_response(state, claims, bucket, key, upload_id).await;
    }

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };
    let requested_range = match parse_http_range(req.headers(), None) {
        Ok(range) => range,
        Err(response) => return response,
    };

    match state
        .object_manager
        .get_object(claims, bucket, key, version_id)
        .await
    {
        Ok((object, stream)) => {
            let range = match requested_range {
                Some(range_header) => match range_header.resolve(object.size as u64) {
                    Ok(range) => Some(range),
                    Err(response) => return response,
                },
                None => None,
            };
            let (status, content_length, body_stream) = match range {
                Some(range) => (
                    axum::http::StatusCode::PARTIAL_CONTENT,
                    range.len() as i64,
                    slice_stream_by_range(stream, range),
                ),
                None => (axum::http::StatusCode::OK, object.size, stream),
            };
            let mut builder = Response::builder()
                .status(status)
                .header("Content-Type", object.content_type.unwrap_or_default())
                .header("Content-Length", content_length)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            if let Some(range) = range {
                builder = builder.header(
                    "Content-Range",
                    format!("bytes {}-{}/{}", range.start, range.end, object.size),
                );
            }
            builder
                .body(Body::from_stream(
                    body_stream.map(|r| r.map_err(|e| axum::Error::new(e))),
                ))
                .unwrap()
        }
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn list_multipart_parts_response(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
) -> Response {
    match state
        .object_manager
        .list_multipart_parts(claims.tenant_id, &bucket, &key, upload_id, &claims.scopes)
        .await
    {
        Ok(parts) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n  <PartNumberMarker>0</PartNumberMarker>\n  <NextPartNumberMarker>0</NextPartNumberMarker>\n  <MaxParts>1000</MaxParts>\n  <IsTruncated>false</IsTruncated>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                upload_id
            );
            for part in parts {
                xml.push_str("  <Part>\n");
                xml.push_str(&format!(
                    "    <PartNumber>{}</PartNumber>\n",
                    part.part_number
                ));
                xml.push_str(&format!(
                    "    <LastModified>{}</LastModified>\n",
                    part.created_at.to_rfc3339()
                ));
                xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", part.etag));
                xml.push_str(&format!("    <Size>{}</Size>\n", part.size));
                xml.push_str("  </Part>\n");
            }
            xml.push_str("</ListPartsResult>\n");
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchUpload"),
    }
}

async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };
    let copy_source = match req.headers().get("x-amz-copy-source") {
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.to_owned()),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid x-amz-copy-source",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };

    if let Some(copy_source) = copy_source {
        return copy_object(state, claims, bucket, key, copy_source).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let part_number = match q
            .get("partNumber")
            .and_then(|value| value.parse::<i32>().ok())
        {
            Some(part_number) => part_number,
            None => {
                return s3_error(
                    "InvalidArgument",
                    "Missing or invalid partNumber",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let body_stream = req.into_body().into_data_stream().map(|r| {
            r.map(|chunk| chunk.to_vec())
                .map_err(|e| tonic::Status::internal(e.to_string()))
        });
        return upload_part(
            state,
            claims,
            bucket,
            key,
            upload_id,
            part_number,
            body_stream,
        )
        .await;
    }

    let body_stream = req.into_body().into_data_stream().map(|r| {
        r.map(|chunk| chunk.to_vec())
            .map_err(|e| tonic::Status::internal(e.to_string()))
    });

    match state
        .object_manager
        .put_object(claims.tenant_id, &bucket, &key, &claims.scopes, body_stream)
        .await
    {
        Ok(object) => Response::builder()
            .status(200)
            .header("ETag", object.etag)
            .header("x-amz-version-id", object.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => s3_error(
                "NoSuchBucket",
                status.message(),
                axum::http::StatusCode::NOT_FOUND,
            ),
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn post_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    if q.contains_key("uploads") {
        return initiate_multipart_upload(state, claims, bucket, key).await;
    }

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap_or_default();
        return complete_multipart_upload(state, claims, bucket, key, upload_id, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported POST object operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

async fn initiate_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
) -> Response {
    match state
        .object_manager
        .initiate_multipart_upload(claims.tenant_id, &bucket, &key, &claims.scopes)
        .await
    {
        Ok(upload_id) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n</InitiateMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                upload_id
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchBucket"),
    }
}

async fn upload_part(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    part_number: i32,
    body_stream: impl Stream<Item = Result<Vec<u8>, tonic::Status>> + Unpin,
) -> Response {
    match state
        .object_manager
        .upload_part(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            part_number,
            &claims.scopes,
            body_stream,
        )
        .await
    {
        Ok(etag) => Response::builder()
            .status(200)
            .header("ETag", format!("\"{}\"", etag))
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchUpload"),
    }
}

async fn complete_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
    body: axum::body::Bytes,
) -> Response {
    let completed = match quick_xml::de::from_reader::<_, CompleteMultipartUploadXml>(&body[..]) {
        Ok(completed) => completed,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid CompleteMultipartUpload body: {}", error),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };
    let parts = completed
        .parts
        .into_iter()
        .map(|part| anvil_core::object_manager::CompleteMultipartPart {
            part_number: part.part_number,
            etag: part.etag,
        })
        .collect();

    match state
        .object_manager
        .complete_multipart_upload(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            parts,
            &claims.scopes,
        )
        .await
    {
        Ok(object) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Location>/{}/{}</Location>\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <ETag>\"{}\"</ETag>\n</CompleteMultipartUploadResult>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                xml_escape(&bucket),
                xml_escape(&key),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchUpload"),
    }
}

async fn copy_object(
    state: AppState,
    claims: Claims,
    destination_bucket: String,
    destination_key: String,
    copy_source: String,
) -> Response {
    let (source_bucket, source_key, source_version_id) = match parse_copy_source(&copy_source) {
        Ok(source) => source,
        Err(response) => return response,
    };

    let (_source_object, source_stream) = match state
        .object_manager
        .get_object(
            Some(claims.clone()),
            source_bucket,
            source_key,
            source_version_id,
        )
        .await
    {
        Ok(source) => source,
        Err(status) => return copy_status_to_response(status, "NoSuchKey"),
    };

    match state
        .object_manager
        .put_object(
            claims.tenant_id,
            &destination_bucket,
            &destination_key,
            &claims.scopes,
            source_stream,
        )
        .await
    {
        Ok(object) => {
            let xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CopyObjectResult>\n  <LastModified>{}</LastModified>\n  <ETag>\"{}\"</ETag>\n</CopyObjectResult>\n",
                object.created_at.to_rfc3339(),
                object.etag
            );
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
                .header("ETag", object.etag)
                .header("x-amz-version-id", object.version_id.to_string())
                .body(Body::from(xml))
                .unwrap()
        }
        Err(status) => copy_status_to_response(status, "NoSuchBucket"),
    }
}

fn parse_copy_source(value: &str) -> Result<(String, String, Option<uuid::Uuid>), Response> {
    let value = value.trim_start_matches('/');
    let (path, query) = value.split_once('?').unwrap_or((value, ""));
    let Some((bucket, key)) = path.split_once('/') else {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    };
    let bucket = percent_decode_path_component(bucket);
    let key = percent_decode_path_component(key);
    if bucket.is_empty() || key.is_empty() {
        return Err(s3_error(
            "InvalidArgument",
            "Invalid x-amz-copy-source",
            axum::http::StatusCode::BAD_REQUEST,
        ));
    }
    let version_id = query
        .split('&')
        .filter_map(|pair| pair.split_once('='))
        .find_map(|(name, value)| {
            (name == "versionId" || name == "version-id")
                .then(|| percent_decode_query_component(value))
        })
        .filter(|value| !value.is_empty())
        .map(|value| {
            uuid::Uuid::parse_str(&value).map_err(|_| {
                s3_error(
                    "InvalidArgument",
                    "Invalid source versionId",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()?;

    Ok((bucket, key, version_id))
}

fn percent_decode_path_component(value: &str) -> String {
    percent_decode(value.as_bytes())
}

fn copy_status_to_response(status: tonic::Status, not_found_code: &str) -> Response {
    match status.code() {
        tonic::Code::FailedPrecondition => {
            if status.message().starts_with("Bucket is in region ") {
                let region = status.message().trim_start_matches("Bucket is in region ");
                return s3_redirect(region);
            }
            s3_error(
                "PreconditionFailed",
                status.message(),
                axum::http::StatusCode::PRECONDITION_FAILED,
            )
        }
        tonic::Code::NotFound => s3_error(
            not_found_code,
            status.message(),
            axum::http::StatusCode::NOT_FOUND,
        ),
        tonic::Code::PermissionDenied => s3_error(
            "AccessDenied",
            status.message(),
            axum::http::StatusCode::FORBIDDEN,
        ),
        tonic::Code::InvalidArgument => s3_error(
            "InvalidArgument",
            status.message(),
            axum::http::StatusCode::BAD_REQUEST,
        ),
        _ => s3_error(
            "InternalError",
            status.message(),
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
        ),
    }
}

async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(c) => c,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    if let Some(upload_id) = q.get("uploadId") {
        let upload_id = match uuid::Uuid::parse_str(upload_id) {
            Ok(upload_id) => upload_id,
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid uploadId",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return abort_multipart_upload(state, claims, bucket, key, upload_id).await;
    }

    match state
        .object_manager
        .delete_object(claims.tenant_id, &bucket, &key, &claims.scopes)
        .await
    {
        Ok(delete_marker) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .header("x-amz-delete-marker", "true")
            .header("x-amz-version-id", delete_marker.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => Response::builder()
                .status(axum::http::StatusCode::NO_CONTENT)
                .body(Body::empty())
                .unwrap(),
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            tonic::Code::InvalidArgument => s3_error(
                "InvalidArgument",
                status.message(),
                axum::http::StatusCode::BAD_REQUEST,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

async fn abort_multipart_upload(
    state: AppState,
    claims: Claims,
    bucket: String,
    key: String,
    upload_id: uuid::Uuid,
) -> Response {
    match state
        .object_manager
        .abort_multipart_upload(claims.tenant_id, &bucket, &key, upload_id, &claims.scopes)
        .await
    {
        Ok(()) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
            .body(Body::empty())
            .unwrap(),
        Err(status) => s3_status_to_response_for_auth(status, true, "NoSuchUpload"),
    }
}

async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();
    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };

    match state
        .object_manager
        .head_object(claims, &bucket, &key, version_id)
        .await
    {
        Ok(object) => Response::builder()
            .status(200)
            .header("Content-Type", object.content_type.unwrap_or_default())
            .header("Content-Length", object.size)
            .header("ETag", object.etag)
            .header("Accept-Ranges", "bytes")
            .header("x-amz-version-id", object.version_id.to_string())
            .body(Body::empty())
            .unwrap(),
        Err(status) => match status.code() {
            tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                s3_error(
                    "PreconditionFailed",
                    status.message(),
                    axum::http::StatusCode::PRECONDITION_FAILED,
                )
            }
            tonic::Code::NotFound => {
                if req.extensions().get::<Claims>().is_none() {
                    s3_error(
                        "AccessDenied",
                        status.message(),
                        axum::http::StatusCode::FORBIDDEN,
                    )
                } else {
                    s3_error(
                        "NoSuchKey",
                        status.message(),
                        axum::http::StatusCode::NOT_FOUND,
                    )
                }
            }
            tonic::Code::PermissionDenied => s3_error(
                "AccessDenied",
                status.message(),
                axum::http::StatusCode::FORBIDDEN,
            ),
            _ => s3_error(
                "InternalError",
                status.message(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            ),
        },
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ByteRange {
    start: u64,
    end: u64,
}

impl ByteRange {
    fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestedByteRange {
    FromStart { start: u64, end: Option<u64> },
    Suffix { len: u64 },
}

impl RequestedByteRange {
    fn resolve(self, object_size: u64) -> Result<ByteRange, Response> {
        if object_size == 0 {
            return Err(invalid_range_response(object_size));
        }
        match self {
            Self::FromStart { start, end } => {
                if start >= object_size {
                    return Err(invalid_range_response(object_size));
                }
                let end = end.unwrap_or(object_size - 1).min(object_size - 1);
                if end < start {
                    return Err(invalid_range_response(object_size));
                }
                Ok(ByteRange { start, end })
            }
            Self::Suffix { len } => {
                if len == 0 {
                    return Err(invalid_range_response(object_size));
                }
                let len = len.min(object_size);
                Ok(ByteRange {
                    start: object_size - len,
                    end: object_size - 1,
                })
            }
        }
    }
}

fn parse_http_range(
    headers: &axum::http::HeaderMap,
    object_size: Option<u64>,
) -> Result<Option<RequestedByteRange>, Response> {
    let Some(value) = headers.get(axum::http::header::RANGE) else {
        return Ok(None);
    };
    let value = value.to_str().map_err(|_| {
        s3_error(
            "InvalidRange",
            "Invalid Range header",
            axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
        )
    })?;
    if value.contains(',') {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    }
    let Some(spec) = value.strip_prefix("bytes=") else {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    };
    let Some((start, end)) = spec.split_once('-') else {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    };
    if start.is_empty() && end.is_empty() {
        return Err(invalid_range_response(object_size.unwrap_or(0)));
    }
    let requested = if start.is_empty() {
        RequestedByteRange::Suffix {
            len: end
                .parse()
                .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
        }
    } else {
        RequestedByteRange::FromStart {
            start: start
                .parse()
                .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
            end: if end.is_empty() {
                None
            } else {
                Some(
                    end.parse()
                        .map_err(|_| invalid_range_response(object_size.unwrap_or(0)))?,
                )
            },
        }
    };
    Ok(Some(requested))
}

fn invalid_range_response(object_size: u64) -> Response {
    Response::builder()
        .status(axum::http::StatusCode::RANGE_NOT_SATISFIABLE)
        .header("Content-Range", format!("bytes */{}", object_size))
        .header("Content-Type", "application/xml")
        .body(Body::from(format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>InvalidRange</Code>\n  <Message>Invalid Range header</Message>\n</Error>\n"
        )))
        .unwrap()
}

fn slice_stream_by_range(
    mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>>,
    range: ByteRange,
) -> Pin<Box<dyn Stream<Item = Result<Vec<u8>, tonic::Status>> + Send + 'static>> {
    Box::pin(async_stream::try_stream! {
        let mut offset = 0u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            let chunk_len = chunk.len() as u64;
            if chunk_len == 0 {
                continue;
            }
            let chunk_start = offset;
            let chunk_end = offset + chunk_len - 1;
            offset += chunk_len;

            if chunk_end < range.start {
                continue;
            }
            if chunk_start > range.end {
                break;
            }

            let from = range.start.saturating_sub(chunk_start) as usize;
            let to_exclusive = (range.end.min(chunk_end) - chunk_start + 1) as usize;
            yield chunk[from..to_exclusive].to_vec();
        }
    })
}

fn parse_s3_version_id(q: &HashMap<String, String>) -> Result<Option<uuid::Uuid>, Response> {
    q.get("versionId")
        .or_else(|| q.get("version-id"))
        .filter(|value| !value.is_empty())
        .map(|value| {
            uuid::Uuid::parse_str(value).map_err(|_| {
                s3_error(
                    "InvalidArgument",
                    "Invalid versionId",
                    axum::http::StatusCode::BAD_REQUEST,
                )
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::TryStreamExt;

    fn request(uri: &str) -> Request {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    fn range_headers(value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::RANGE, value.parse().unwrap());
        headers
    }

    #[test]
    fn reserved_namespace_guard_detects_object_keys() {
        assert!(request_targets_reserved_namespace(&request(
            "/bucket/_anvil/authz/tuples"
        )));
        assert!(request_targets_reserved_namespace(&request(
            "/bucket/_anvil/personaldb/group"
        )));
        assert!(!request_targets_reserved_namespace(&request(
            "/bucket/customer/_anvil/authz/visible"
        )));
    }

    #[test]
    fn reserved_namespace_guard_detects_list_prefixes() {
        assert!(request_targets_reserved_namespace(&request(
            "/bucket?list-type=2&prefix=_anvil%2Fauthz%2F"
        )));
        assert!(request_targets_reserved_namespace(&request(
            "/bucket?prefix=_anvil/personaldb/"
        )));
        assert!(!request_targets_reserved_namespace(&request(
            "/bucket?prefix=customer%2F_anvil%2Fauthz%2F"
        )));
    }

    #[test]
    fn range_parser_resolves_standard_and_suffix_ranges() {
        let standard = parse_http_range(&range_headers("bytes=2-5"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(standard, ByteRange { start: 2, end: 5 });

        let open_ended = parse_http_range(&range_headers("bytes=7-"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(open_ended, ByteRange { start: 7, end: 9 });

        let suffix = parse_http_range(&range_headers("bytes=-4"), Some(10))
            .unwrap()
            .unwrap()
            .resolve(10)
            .unwrap();
        assert_eq!(suffix, ByteRange { start: 6, end: 9 });
    }

    #[test]
    fn range_parser_rejects_multi_ranges_and_unsatisfied_ranges() {
        assert!(parse_http_range(&range_headers("bytes=0-1,4-5"), Some(10)).is_err());
        assert!(
            parse_http_range(&range_headers("bytes=20-30"), Some(10))
                .unwrap()
                .unwrap()
                .resolve(10)
                .is_err()
        );
    }

    #[tokio::test]
    async fn range_stream_slices_across_chunk_boundaries() {
        let stream = Box::pin(futures_util::stream::iter(vec![
            Ok(b"abc".to_vec()),
            Ok(b"defg".to_vec()),
            Ok(b"hij".to_vec()),
        ]));
        let body = slice_stream_by_range(stream, ByteRange { start: 2, end: 7 })
            .try_concat()
            .await
            .unwrap();
        assert_eq!(body, b"cdefgh");
    }

    #[test]
    fn copy_source_parser_accepts_encoded_bucket_key_and_version() {
        let (bucket, key, version_id) = parse_copy_source(
            "/source-bucket/path%20with%20space/file.txt?versionId=550e8400-e29b-41d4-a716-446655440000",
        )
        .unwrap();
        assert_eq!(bucket, "source-bucket");
        assert_eq!(key, "path with space/file.txt");
        assert_eq!(
            version_id.unwrap(),
            uuid::Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()
        );
    }

    #[test]
    fn copy_source_parser_rejects_missing_key() {
        assert!(parse_copy_source("/source-bucket").is_err());
    }
}
