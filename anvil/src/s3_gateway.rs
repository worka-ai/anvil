use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::auth;
use anvil_core::object_manager::ObjectWriteOptions;
use anvil_core::permissions::AnvilAction;
use anvil_core::persistence::Object;
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
struct BucketVersioningConfigurationXml {
    #[serde(rename = "Status")]
    status: Option<String>,
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

#[derive(Debug, Deserialize)]
struct DeleteObjectsXml {
    #[serde(rename = "Object", default)]
    objects: Vec<DeleteObjectsXmlObject>,
    #[serde(rename = "Quiet")]
    quiet: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct DeleteObjectsXmlObject {
    #[serde(rename = "Key")]
    key: String,
    #[serde(rename = "VersionId")]
    version_id: Option<String>,
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
                .post(post_bucket)
                .get(list_objects),
        )
        .route(
            "/{bucket}/",
            get(list_objects)
                .put(create_bucket)
                .delete(delete_bucket)
                .post(post_bucket)
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
            "UnauthorizedReservedNamespace",
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

    if request_copy_source_targets_reserved_namespace(req.headers()) {
        return true;
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

fn request_copy_source_targets_reserved_namespace(headers: &axum::http::HeaderMap) -> bool {
    let Some(copy_source) = headers
        .get("x-amz-copy-source")
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };

    let copy_source = copy_source.trim_start_matches('/');
    let (path, _) = copy_source.split_once('?').unwrap_or((copy_source, ""));
    let Some((_, key)) = path.split_once('/') else {
        return false;
    };
    let key = percent_decode_path_component(key);
    !key.is_empty() && validation::is_reserved_internal_key(&key)
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
    Query(q): Query<HashMap<String, String>>,
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

    if q.contains_key("versioning") {
        return put_bucket_versioning_response(state, claims, &bucket, req).await;
    }

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

async fn get_bucket_versioning_response(state: AppState, claims: Claims, bucket: &str) -> Response {
    if !auth::is_authorized(AnvilAction::BucketRead, bucket, &claims.scopes) {
        return s3_error(
            "AccessDenied",
            "Permission denied",
            axum::http::StatusCode::FORBIDDEN,
        );
    }

    match state.db.get_bucket_by_name(claims.tenant_id, bucket).await {
        Ok(Some(_)) => {
            let xml = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Status>Enabled</Status>\n</VersioningConfiguration>\n";
            Response::builder()
                .status(200)
                .header("Content-Type", "application/xml")
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

async fn put_bucket_versioning_response(
    state: AppState,
    claims: Claims,
    bucket: &str,
    req: Request,
) -> Response {
    if !auth::is_authorized(AnvilAction::BucketWrite, bucket, &claims.scopes) {
        return s3_error(
            "AccessDenied",
            "Permission denied",
            axum::http::StatusCode::FORBIDDEN,
        );
    }

    match state.db.get_bucket_by_name(claims.tenant_id, bucket).await {
        Ok(Some(_)) => {}
        Ok(None) => {
            return s3_error(
                "NoSuchBucket",
                "The specified bucket does not exist",
                axum::http::StatusCode::NOT_FOUND,
            );
        }
        Err(e) => {
            return s3_error(
                "InternalError",
                &e.to_string(),
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            );
        }
    }

    let bytes = axum::body::to_bytes(req.into_body(), 1024 * 1024)
        .await
        .unwrap_or_default();
    if !bytes.is_empty() {
        match quick_xml::de::from_reader::<_, BucketVersioningConfigurationXml>(&bytes[..]) {
            Ok(config) => {
                if config
                    .status
                    .as_deref()
                    .is_some_and(|status| status != "Enabled")
                {
                    return s3_error(
                        "NotImplemented",
                        "Bucket versioning can only be enabled",
                        axum::http::StatusCode::NOT_IMPLEMENTED,
                    );
                }
            }
            Err(e) => {
                return s3_error(
                    "MalformedXML",
                    &format!("Invalid versioning configuration: {e}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        }
    }

    (axum::http::StatusCode::OK, "").into_response()
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
        .delete_bucket(claims.tenant_id, &bucket, claims.scopes.as_slice())
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
            tonic::Code::FailedPrecondition => s3_error(
                "BucketNotEmpty",
                status.message(),
                axum::http::StatusCode::CONFLICT,
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

    if q.contains_key("versioning") {
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
        return get_bucket_versioning_response(state, claims, &bucket).await;
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

    let is_list_v2 = q
        .get("list-type")
        .or_else(|| q.get("listType"))
        .is_some_and(|value| value == "2");
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let continuation_token = q
        .get("continuation-token")
        .or_else(|| q.get("continuationToken"))
        .cloned();
    let marker = q.get("marker").cloned().unwrap_or_default();
    let start_after = if is_list_v2 {
        continuation_token.clone().unwrap_or_else(|| {
            q.get("start-after")
                .or_else(|| q.get("startAfter"))
                .cloned()
                .unwrap_or_default()
        })
    } else {
        marker.clone()
    };
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
        Ok((objects, common_prefixes)) => {
            let requested_max_keys = if max_keys <= 0 {
                1000
            } else {
                max_keys as usize
            };
            let (entries, is_truncated, next_marker) =
                paginate_list_bucket_entries(objects, common_prefixes, requested_max_keys);
            let key_count = entries.len() as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            if is_list_v2 {
                if let Some(token) = continuation_token {
                    xml.push_str(&format!(
                        "  <ContinuationToken>{}</ContinuationToken>\n",
                        xml_escape(&token)
                    ));
                }
                xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            } else {
                xml.push_str(&format!("  <Marker>{}</Marker>\n", xml_escape(&marker)));
            }
            if !delimiter.is_empty() {
                xml.push_str(&format!(
                    "  <Delimiter>{}</Delimiter>\n",
                    xml_escape(&delimiter)
                ));
            }
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
            if let Some(token) = next_marker {
                if is_list_v2 {
                    xml.push_str(&format!(
                        "  <NextContinuationToken>{}</NextContinuationToken>\n",
                        xml_escape(&token)
                    ));
                } else {
                    xml.push_str(&format!(
                        "  <NextMarker>{}</NextMarker>\n",
                        xml_escape(&token)
                    ));
                }
            }
            for entry in entries {
                append_list_bucket_entry_xml(&mut xml, entry);
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

async fn post_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(q): Query<HashMap<String, String>>,
    req: Request,
) -> Response {
    let claims = match req.extensions().get::<Claims>().cloned() {
        Some(claims) => claims,
        None => {
            return s3_error(
                "AccessDenied",
                "Missing credentials",
                axum::http::StatusCode::FORBIDDEN,
            );
        }
    };

    if q.contains_key("delete") {
        let bytes = match axum::body::to_bytes(req.into_body(), 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(error) => {
                return s3_error(
                    "InvalidRequest",
                    &format!("Failed to read DeleteObjects body: {error}"),
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        };
        return delete_objects(state, claims, bucket, bytes).await;
    }

    s3_error(
        "InvalidArgument",
        "Unsupported bucket POST operation",
        axum::http::StatusCode::BAD_REQUEST,
    )
}

async fn delete_objects(
    state: AppState,
    claims: Claims,
    bucket: String,
    body: axum::body::Bytes,
) -> Response {
    let request = match quick_xml::de::from_reader::<_, DeleteObjectsXml>(&body[..]) {
        Ok(request) => request,
        Err(error) => {
            return s3_error(
                "MalformedXML",
                &format!("Invalid DeleteObjects body: {error}"),
                axum::http::StatusCode::BAD_REQUEST,
            );
        }
    };

    let quiet = request.quiet.unwrap_or(false);
    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    for object in request.objects {
        let key = object.key;
        let requested_version_id = object.version_id;

        if validation::is_reserved_internal_key(&key) {
            errors.push(DeleteObjectError {
                key,
                version_id: requested_version_id,
                code: "UnauthorizedReservedNamespace".to_string(),
                message: "UnauthorizedReservedNamespace".to_string(),
            });
            continue;
        }

        let version_id = match requested_version_id.as_deref() {
            Some("") | None => None,
            Some(version_id) => match uuid::Uuid::parse_str(version_id) {
                Ok(version_id) => Some(version_id),
                Err(_) => {
                    errors.push(DeleteObjectError {
                        key,
                        version_id: requested_version_id,
                        code: "InvalidArgument".to_string(),
                        message: "Invalid versionId".to_string(),
                    });
                    continue;
                }
            },
        };

        let delete_result = if let Some(version_id) = version_id {
            state
                .object_manager
                .delete_object_version(claims.tenant_id, &bucket, &key, version_id, &claims.scopes)
                .await
        } else {
            state
                .object_manager
                .delete_object(claims.tenant_id, &bucket, &key, &claims.scopes)
                .await
        };

        match delete_result {
            Ok(delete_marker) => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: requested_version_id,
                        delete_marker: Some(delete_marker.deleted_at.is_some()),
                        delete_marker_version_id: delete_marker
                            .deleted_at
                            .is_some()
                            .then(|| delete_marker.version_id.to_string()),
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                if !quiet {
                    deleted.push(DeletedObject {
                        key,
                        version_id: None,
                        delete_marker: None,
                        delete_marker_version_id: None,
                    });
                }
            }
            Err(status) if status.code() == tonic::Code::FailedPrecondition => {
                if status.message().starts_with("Bucket is in region ") {
                    let region = status.message().trim_start_matches("Bucket is in region ");
                    return s3_redirect(region);
                }
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
            Err(status) => {
                errors.push(DeleteObjectError::from_status(
                    key,
                    requested_version_id,
                    status,
                ));
            }
        }
    }

    delete_objects_result_response(deleted, errors)
}

#[derive(Debug)]
struct DeletedObject {
    key: String,
    version_id: Option<String>,
    delete_marker: Option<bool>,
    delete_marker_version_id: Option<String>,
}

#[derive(Debug)]
struct DeleteObjectError {
    key: String,
    version_id: Option<String>,
    code: String,
    message: String,
}

impl DeleteObjectError {
    fn from_status(key: String, version_id: Option<String>, status: tonic::Status) -> Self {
        let code = match status.code() {
            tonic::Code::PermissionDenied => "AccessDenied",
            tonic::Code::InvalidArgument => "InvalidArgument",
            tonic::Code::NotFound => "NoSuchKey",
            tonic::Code::Unimplemented => "NotImplemented",
            _ => "InternalError",
        };
        Self {
            key,
            version_id,
            code: code.to_string(),
            message: status.message().to_string(),
        }
    }
}

fn delete_objects_result_response(
    deleted: Vec<DeletedObject>,
    errors: Vec<DeleteObjectError>,
) -> Response {
    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
    );

    for object in deleted {
        xml.push_str("  <Deleted>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
        if let Some(version_id) = object.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        if let Some(delete_marker) = object.delete_marker {
            xml.push_str(&format!(
                "    <DeleteMarker>{}</DeleteMarker>\n",
                if delete_marker { "true" } else { "false" }
            ));
        }
        if let Some(version_id) = object.delete_marker_version_id {
            xml.push_str(&format!(
                "    <DeleteMarkerVersionId>{}</DeleteMarkerVersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str("  </Deleted>\n");
    }

    for error in errors {
        xml.push_str("  <Error>\n");
        xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&error.key)));
        if let Some(version_id) = error.version_id {
            xml.push_str(&format!(
                "    <VersionId>{}</VersionId>\n",
                xml_escape(&version_id)
            ));
        }
        xml.push_str(&format!("    <Code>{}</Code>\n", xml_escape(&error.code)));
        xml.push_str(&format!(
            "    <Message>{}</Message>\n",
            xml_escape(&error.message)
        ));
        xml.push_str("  </Error>\n");
    }

    xml.push_str("</DeleteResult>\n");
    Response::builder()
        .status(200)
        .header("Content-Type", "application/xml")
        .body(Body::from(xml))
        .unwrap()
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
    let upload_id_marker = match q
        .get("upload-id-marker")
        .or_else(|| q.get("uploadIdMarker"))
    {
        Some(value) => match uuid::Uuid::parse_str(value) {
            Ok(upload_id) => Some(upload_id),
            Err(_) => {
                return s3_error(
                    "InvalidArgument",
                    "Invalid upload-id-marker",
                    axum::http::StatusCode::BAD_REQUEST,
                );
            }
        },
        None => None,
    };
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
            upload_id_marker,
            max_uploads,
            &claims.scopes,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListMultipartUploadsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <KeyMarker>{}</KeyMarker>\n  <UploadIdMarker>{}</UploadIdMarker>\n",
                xml_escape(bucket),
                xml_escape(&key_marker),
                upload_id_marker
                    .map(|marker| marker.to_string())
                    .as_deref()
                    .map(xml_escape)
                    .unwrap_or_default()
            );
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_upload_id_marker) = page.next_upload_id_marker {
                xml.push_str(&format!(
                    "  <NextUploadIdMarker>{next_upload_id_marker}</NextUploadIdMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <Delimiter></Delimiter>\n  <Prefix>{}</Prefix>\n  <MaxUploads>{}</MaxUploads>\n  <IsTruncated>{}</IsTruncated>\n",
                xml_escape(&prefix),
                max_uploads,
                if page.is_truncated { "true" } else { "false" }
            ));
            for upload in page.uploads {
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
    let version_id_marker = q
        .get("version-id-marker")
        .or_else(|| q.get("versionIdMarker"))
        .cloned()
        .unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_object_versions(
            claims,
            bucket,
            &prefix,
            &key_marker,
            &version_id_marker,
            max_keys,
        )
        .await
    {
        Ok(page) => {
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListVersionsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", xml_escape(bucket)));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!(
                "  <KeyMarker>{}</KeyMarker>\n",
                xml_escape(&key_marker)
            ));
            xml.push_str(&format!(
                "  <VersionIdMarker>{}</VersionIdMarker>\n",
                xml_escape(&version_id_marker)
            ));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            if let Some(next_key_marker) = page.next_key_marker.as_deref() {
                xml.push_str(&format!(
                    "  <NextKeyMarker>{}</NextKeyMarker>\n",
                    xml_escape(next_key_marker)
                ));
            }
            if let Some(next_version_id_marker) = page.next_version_id_marker {
                xml.push_str(&format!(
                    "  <NextVersionIdMarker>{}</NextVersionIdMarker>\n",
                    next_version_id_marker
                ));
            }
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if page.is_truncated { "true" } else { "false" }
            ));
            for version in page.versions {
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

#[derive(Debug)]
enum ListBucketEntry {
    Object(Object),
    Prefix(String),
}

fn paginate_list_bucket_entries(
    objects: Vec<Object>,
    common_prefixes: Vec<String>,
    requested_max_keys: usize,
) -> (Vec<ListBucketEntry>, bool, Option<String>) {
    let mut entries = objects
        .into_iter()
        .map(ListBucketEntry::Object)
        .chain(common_prefixes.into_iter().map(ListBucketEntry::Prefix))
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| {
        left.marker()
            .cmp(right.marker())
            .then_with(|| left.kind_order().cmp(&right.kind_order()))
    });

    let is_truncated = entries.len() > requested_max_keys;
    if is_truncated {
        entries.truncate(requested_max_keys);
    }
    let next_continuation_token = is_truncated
        .then(|| entries.last().map(|entry| entry.marker().to_string()))
        .flatten();

    (entries, is_truncated, next_continuation_token)
}

impl ListBucketEntry {
    fn marker(&self) -> &str {
        match self {
            Self::Object(object) => &object.key,
            Self::Prefix(prefix) => prefix,
        }
    }

    fn kind_order(&self) -> u8 {
        match self {
            Self::Object(_) => 0,
            Self::Prefix(_) => 1,
        }
    }
}

fn append_list_bucket_entry_xml(xml: &mut String, entry: ListBucketEntry) {
    match entry {
        ListBucketEntry::Object(object) => {
            xml.push_str("  <Contents>\n");
            xml.push_str(&format!("    <Key>{}</Key>\n", xml_escape(&object.key)));
            xml.push_str(&format!(
                "    <LastModified>{}</LastModified>\n",
                object.created_at.to_rfc3339()
            ));
            xml.push_str(&format!("    <ETag>\"{}\"</ETag>\n", object.etag));
            xml.push_str(&format!("    <Size>{}</Size>\n", object.size));
            xml.push_str("    <StorageClass>STANDARD</StorageClass>\n");
            xml.push_str("  </Contents>\n");
        }
        ListBucketEntry::Prefix(prefix) => {
            xml.push_str("  <CommonPrefixes>\n");
            xml.push_str(&format!("    <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str("  </CommonPrefixes>\n");
        }
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

fn s3_user_metadata(headers: &axum::http::HeaderMap) -> Option<serde_json::Value> {
    let mut values = serde_json::Map::new();
    for (name, value) in headers {
        let Some(metadata_key) = name.as_str().strip_prefix("x-amz-meta-") else {
            continue;
        };
        let Ok(metadata_value) = value.to_str() else {
            continue;
        };
        values.insert(
            metadata_key.to_string(),
            serde_json::Value::String(metadata_value.to_string()),
        );
    }
    if values.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(values))
    }
}

fn add_s3_user_metadata_headers(
    mut builder: axum::http::response::Builder,
    user_meta: Option<&serde_json::Value>,
) -> axum::http::response::Builder {
    let Some(serde_json::Value::Object(values)) = user_meta else {
        return builder;
    };
    for (key, value) in values {
        if let Some(value) = value.as_str() {
            builder = builder.header(format!("x-amz-meta-{key}"), value);
        }
    }
    builder
}

#[cfg(test)]
mod list_bucket_pagination_tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn list_bucket_pagination_uses_last_returned_entry_as_continuation_token() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("page/a.txt"), object("page/b.txt")],
            Vec::new(),
            1,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("page/a.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["page/a.txt"]
        );
    }

    #[test]
    fn list_bucket_pagination_merges_objects_and_common_prefixes_before_truncating() {
        let (entries, is_truncated, token) = paginate_list_bucket_entries(
            vec![object("root/b.txt")],
            vec!["root/a/".to_string(), "root/c/".to_string()],
            2,
        );

        assert!(is_truncated);
        assert_eq!(token.as_deref(), Some("root/b.txt"));
        assert_eq!(
            entries
                .iter()
                .map(ListBucketEntry::marker)
                .collect::<Vec<_>>(),
            vec!["root/a/", "root/b.txt"]
        );
    }

    fn object(key: &str) -> Object {
        Object {
            id: 0,
            tenant_id: 0,
            bucket_id: 0,
            key: key.to_string(),
            content_hash: String::new(),
            size: 0,
            etag: String::new(),
            content_type: None,
            version_id: uuid::Uuid::nil(),
            mutation_id: uuid::Uuid::nil(),
            index_policy_snapshot: String::new(),
            user_metadata_hash: String::new(),
            authz_revision: 0,
            record_hash: String::new(),
            created_at: Utc::now(),
            deleted_at: None,
            storage_class: None,
            user_meta: None,
            shard_map: None,
            inline_payload: None,
            checksum: None,
        }
    }
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
        return list_multipart_parts_response(state, claims, bucket, key, upload_id, &q).await;
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
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
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
            builder = add_s3_user_metadata_headers(builder, object.user_meta.as_ref());
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
    q: &HashMap<String, String>,
) -> Response {
    let part_number_marker: i32 = q
        .get("part-number-marker")
        .or_else(|| q.get("partNumberMarker"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(0);
    let max_parts: i32 = q
        .get("max-parts")
        .or_else(|| q.get("maxParts"))
        .and_then(|value| value.parse().ok())
        .unwrap_or(1000);
    match state
        .object_manager
        .list_multipart_parts(
            claims.tenant_id,
            &bucket,
            &key,
            upload_id,
            part_number_marker,
            max_parts,
            &claims.scopes,
        )
        .await
    {
        Ok(page) => {
            let mut xml = format!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\n  <Bucket>{}</Bucket>\n  <Key>{}</Key>\n  <UploadId>{}</UploadId>\n  <PartNumberMarker>{}</PartNumberMarker>\n",
                xml_escape(&bucket),
                xml_escape(&key),
                upload_id,
                part_number_marker
            );
            if let Some(next_part_number_marker) = page.next_part_number_marker {
                xml.push_str(&format!(
                    "  <NextPartNumberMarker>{next_part_number_marker}</NextPartNumberMarker>\n"
                ));
            }
            xml.push_str(&format!(
                "  <MaxParts>{}</MaxParts>\n  <IsTruncated>{}</IsTruncated>\n",
                max_parts,
                if page.is_truncated { "true" } else { "false" }
            ));
            for part in page.parts {
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
        return copy_object(state, claims, bucket, key, copy_source, req.headers()).await;
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

    if request_has_write_etag_preconditions(req.headers()) {
        let current = match state
            .object_manager
            .current_object_for_write_precondition(claims.tenant_id, &bucket, &key, &claims.scopes)
            .await
        {
            Ok(current) => current,
            Err(status) => return s3_status_to_response_for_auth(status, true, "NoSuchBucket"),
        };
        if let Some(response) = evaluate_write_etag_preconditions(
            req.headers(),
            current.as_ref().map(|object| object.etag.as_str()),
        ) {
            return response;
        }
    }

    let options = ObjectWriteOptions {
        content_type: req
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .map(ToString::to_string),
        user_metadata: s3_user_metadata(req.headers()),
    };
    let body_stream = req.into_body().into_data_stream().map(|r| {
        r.map(|chunk| chunk.to_vec())
            .map_err(|e| tonic::Status::internal(e.to_string()))
    });

    match state
        .object_manager
        .put_object(
            claims.tenant_id,
            &bucket,
            &key,
            &claims.scopes,
            body_stream,
            options,
        )
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
    headers: &axum::http::HeaderMap,
) -> Response {
    let (source_bucket, source_key, source_version_id) = match parse_copy_source(&copy_source) {
        Ok(source) => source,
        Err(response) => return response,
    };

    let (source_object, source_stream) = match state
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

    if let Some(response) =
        evaluate_copy_source_preconditions(headers, &source_object.etag, source_object.created_at)
    {
        return response;
    }

    match state
        .object_manager
        .put_object(
            claims.tenant_id,
            &destination_bucket,
            &destination_key,
            &claims.scopes,
            source_stream,
            ObjectWriteOptions {
                content_type: source_object.content_type,
                user_metadata: source_object.user_meta,
            },
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

    let version_id = match parse_s3_version_id(&q) {
        Ok(version_id) => version_id,
        Err(response) => return response,
    };

    if let Some(version_id) = version_id {
        return match state
            .object_manager
            .delete_object_version(claims.tenant_id, &bucket, &key, version_id, &claims.scopes)
            .await
        {
            Ok(deleted) => {
                let mut builder = Response::builder()
                    .status(axum::http::StatusCode::NO_CONTENT)
                    .header("x-amz-version-id", deleted.version_id.to_string());
                if deleted.deleted_at.is_some() {
                    builder = builder.header("x-amz-delete-marker", "true");
                }
                builder.body(Body::empty()).unwrap()
            }
            Err(status) => s3_delete_status_to_response(status),
        };
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
        Err(status) => s3_delete_status_to_response(status),
    }
}

fn s3_delete_status_to_response(status: tonic::Status) -> Response {
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
        Ok(object) => {
            if let Some(response) =
                evaluate_object_preconditions(req.headers(), &object.etag, object.created_at)
            {
                return response;
            }
            let builder = Response::builder()
                .status(200)
                .header(
                    "Content-Type",
                    object.content_type.clone().unwrap_or_default(),
                )
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .header("Accept-Ranges", "bytes")
                .header("x-amz-version-id", object.version_id.to_string());
            add_s3_user_metadata_headers(builder, object.user_meta.as_ref())
                .body(Body::empty())
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

fn evaluate_object_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: &str,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> Option<Response> {
    if let Some(value) = headers.get(axum::http::header::IF_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if !etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_UNMODIFIED_SINCE) {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) > condition_time {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_NONE_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if etag_condition_matches(value, current_etag) {
            return Some(not_modified_response(current_etag));
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_MODIFIED_SINCE) {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) <= condition_time {
            return Some(not_modified_response(current_etag));
        }
    }
    None
}

fn request_has_write_etag_preconditions(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key(axum::http::header::IF_MATCH)
        || headers.contains_key(axum::http::header::IF_NONE_MATCH)
}

fn evaluate_write_etag_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: Option<&str>,
) -> Option<Response> {
    if let Some(value) = headers.get(axum::http::header::IF_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if !current_etag.is_some_and(|etag| etag_condition_matches(value, etag)) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get(axum::http::header::IF_NONE_MATCH) {
        let value = value.to_str().unwrap_or_default();
        if current_etag.is_some_and(|etag| etag_condition_matches(value, etag)) {
            return Some(precondition_failed_response());
        }
    }
    None
}

fn evaluate_copy_source_preconditions(
    headers: &axum::http::HeaderMap,
    current_etag: &str,
    last_modified: chrono::DateTime<chrono::Utc>,
) -> Option<Response> {
    if let Some(value) = headers.get("x-amz-copy-source-if-match") {
        let value = value.to_str().unwrap_or_default();
        if !etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-unmodified-since") {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) > condition_time {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-none-match") {
        let value = value.to_str().unwrap_or_default();
        if etag_condition_matches(value, current_etag) {
            return Some(precondition_failed_response());
        }
    }
    if let Some(value) = headers.get("x-amz-copy-source-if-modified-since") {
        let Ok(value) = value.to_str() else {
            return Some(precondition_failed_response());
        };
        let Ok(condition_time) = httpdate::parse_http_date(value) else {
            return Some(precondition_failed_response());
        };
        if object_last_modified_time(last_modified) <= condition_time {
            return Some(precondition_failed_response());
        }
    }
    None
}

fn etag_condition_matches(header_value: &str, current_etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || normalize_etag(candidate) == current_etag)
}

fn normalize_etag(value: &str) -> &str {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
}

fn precondition_failed_response() -> Response {
    s3_error(
        "PreconditionFailed",
        "At least one precondition did not hold",
        axum::http::StatusCode::PRECONDITION_FAILED,
    )
}

fn not_modified_response(current_etag: &str) -> Response {
    Response::builder()
        .status(axum::http::StatusCode::NOT_MODIFIED)
        .header("ETag", current_etag)
        .body(Body::empty())
        .unwrap()
}

fn object_last_modified_time(value: chrono::DateTime<chrono::Utc>) -> std::time::SystemTime {
    let seconds = value.timestamp();
    if seconds <= 0 {
        std::time::UNIX_EPOCH
    } else {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds as u64)
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

    fn request_with_copy_source(uri: &str, copy_source: &str) -> Request {
        Request::builder()
            .uri(uri)
            .header("x-amz-copy-source", copy_source)
            .body(Body::empty())
            .unwrap()
    }

    fn range_headers(value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(axum::http::header::RANGE, value.parse().unwrap());
        headers
    }

    fn etag_headers(name: axum::http::header::HeaderName, value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(name, value.parse().unwrap());
        headers
    }

    fn x_amz_headers(name: &'static str, value: &str) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            axum::http::HeaderName::from_static(name),
            value.parse().unwrap(),
        );
        headers
    }

    fn http_date_headers(
        name: axum::http::header::HeaderName,
        value: std::time::SystemTime,
    ) -> axum::http::HeaderMap {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(name, httpdate::fmt_http_date(value).parse().unwrap());
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
    fn reserved_namespace_guard_detects_copy_source_keys() {
        assert!(request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "source/_anvil/authz/tuples")
        ));
        assert!(request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "/source/_anvil%2Fauthz%2Ftuples")
        ));
        assert!(!request_targets_reserved_namespace(
            &request_with_copy_source(
                "/bucket/destination",
                "source/customer/_anvil/authz/visible"
            )
        ));
        assert!(!request_targets_reserved_namespace(
            &request_with_copy_source("/bucket/destination", "malformed-copy-source")
        ));
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

    #[test]
    fn etag_preconditions_match_strong_weak_and_list_values() {
        assert!(etag_condition_matches("\"abc\"", "abc"));
        assert!(etag_condition_matches("W/\"abc\"", "abc"));
        assert!(etag_condition_matches("\"nope\", \"abc\"", "abc"));
        assert!(etag_condition_matches("*", "abc"));
        assert!(!etag_condition_matches("\"nope\"", "abc"));
    }

    #[test]
    fn etag_preconditions_return_s3_status_responses() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 123_000_000).unwrap();
        let failed = evaluate_object_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
            "abc",
            last_modified,
        )
        .expect("if-match mismatch should fail");
        assert_eq!(failed.status(), axum::http::StatusCode::PRECONDITION_FAILED);

        let not_modified = evaluate_object_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
            "abc",
            last_modified,
        )
        .expect("if-none-match match should return not modified");
        assert_eq!(not_modified.status(), axum::http::StatusCode::NOT_MODIFIED);

        assert!(
            evaluate_object_preconditions(
                &etag_headers(axum::http::header::IF_NONE_MATCH, "\"other\""),
                "abc",
                last_modified,
            )
            .is_none()
        );
    }

    #[test]
    fn write_etag_preconditions_require_existing_match() {
        let failed_missing = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
            None,
        )
        .expect("If-Match without current object should fail");
        assert_eq!(
            failed_missing.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        assert!(
            evaluate_write_etag_preconditions(
                &etag_headers(axum::http::header::IF_MATCH, "\"abc\""),
                Some("abc"),
            )
            .is_none()
        );

        let failed_mismatch = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_MATCH, "\"other\""),
            Some("abc"),
        )
        .expect("If-Match mismatch should fail");
        assert_eq!(
            failed_mismatch.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn write_etag_preconditions_enforce_if_none_match() {
        assert!(
            evaluate_write_etag_preconditions(
                &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
                None,
            )
            .is_none()
        );

        let failed_existing = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "\"abc\""),
            Some("abc"),
        )
        .expect("matching If-None-Match should fail writes");
        assert_eq!(
            failed_existing.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        let failed_star = evaluate_write_etag_preconditions(
            &etag_headers(axum::http::header::IF_NONE_MATCH, "*"),
            Some("abc"),
        )
        .expect("If-None-Match wildcard should fail existing object writes");
        assert_eq!(
            failed_star.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn copy_source_preconditions_return_precondition_failed() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let before = exact_second - std::time::Duration::from_secs(1);
        let after = exact_second + std::time::Duration::from_secs(1);

        assert!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-match", "\"abc\""),
                "abc",
                last_modified,
            )
            .is_none()
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-match", "\"other\""),
                "abc",
                last_modified,
            )
            .expect("source If-Match mismatch should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers("x-amz-copy-source-if-none-match", "\"abc\""),
                "abc",
                last_modified,
            )
            .expect("source If-None-Match hit should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers(
                    "x-amz-copy-source-if-unmodified-since",
                    &httpdate::fmt_http_date(before),
                ),
                "abc",
                last_modified,
            )
            .expect("source If-Unmodified-Since before modification should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
        assert_eq!(
            evaluate_copy_source_preconditions(
                &x_amz_headers(
                    "x-amz-copy-source-if-modified-since",
                    &httpdate::fmt_http_date(after),
                ),
                "abc",
                last_modified,
            )
            .expect("source If-Modified-Since after modification should fail")
            .status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );
    }

    #[test]
    fn date_preconditions_compare_against_second_precision_last_modified() {
        let last_modified = chrono::DateTime::from_timestamp(1_700_000_000, 999_000_000).unwrap();
        let exact_second = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let before = exact_second - std::time::Duration::from_secs(1);
        let after = exact_second + std::time::Duration::from_secs(1);

        let unmodified_since_before = evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, before),
            "abc",
            last_modified,
        )
        .expect("older if-unmodified-since should fail");
        assert_eq!(
            unmodified_since_before.status(),
            axum::http::StatusCode::PRECONDITION_FAILED
        );

        assert!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_UNMODIFIED_SINCE, exact_second),
                "abc",
                last_modified,
            )
            .is_none()
        );

        let modified_since_exact = evaluate_object_preconditions(
            &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, exact_second),
            "abc",
            last_modified,
        )
        .expect("equal if-modified-since should be not modified");
        assert_eq!(
            modified_since_exact.status(),
            axum::http::StatusCode::NOT_MODIFIED
        );

        assert!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, before),
                "abc",
                last_modified,
            )
            .is_none()
        );
        assert_eq!(
            evaluate_object_preconditions(
                &http_date_headers(axum::http::header::IF_MODIFIED_SINCE, after),
                "abc",
                last_modified,
            )
            .expect("future if-modified-since should be not modified")
            .status(),
            axum::http::StatusCode::NOT_MODIFIED
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
