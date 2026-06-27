use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use anvil_core::validation;
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, Request, State},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_util::stream::StreamExt;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct CreateBucketConfiguration {
    #[serde(rename = "LocationConstraint")]
    location_constraint: Option<String>,
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
            put(create_bucket).head(head_bucket).get(list_objects),
        )
        .route(
            "/{bucket}/",
            get(list_objects).put(create_bucket).head(head_bucket),
        )
        .route(
            "/{bucket}/{*path}",
            get(get_object)
                .put(put_object)
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
    let bytes = value.as_bytes();
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
            // return s3_error(
            //     "AccessDenied",
            //     "Missing credentials",
            //     axum::http::StatusCode::FORBIDDEN,
            // );
            return (axum::http::StatusCode::OK, "OK").into_response();
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

    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let start_after = q
        .get("start-after")
        .or_else(|| q.get("startAfter"))
        .cloned()
        .unwrap_or_default();
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys: i32 = q
        .get("max-keys")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    match state
        .object_manager
        .list_objects(claims, &bucket, &prefix, &start_after, max_keys, &delimiter)
        .await
    {
        Ok((objects, common_prefixes)) => {
            // Basic ListObjectsV2 XML
            let is_truncated = false; // TODO: support continuation tokens
            let key_count = objects.len() as i32;
            let mut xml = String::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">
",
            );
            xml.push_str(&format!("  <Name>{}</Name>\n", &*bucket));
            xml.push_str(&format!("  <Prefix>{}</Prefix>\n", xml_escape(&prefix)));
            xml.push_str(&format!("  <KeyCount>{}</KeyCount>\n", key_count));
            xml.push_str(&format!("  <MaxKeys>{}</MaxKeys>\n", max_keys));
            xml.push_str(&format!(
                "  <IsTruncated>{}</IsTruncated>\n",
                if is_truncated { "true" } else { "false" }
            ));
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
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    match state.object_manager.get_object(claims, bucket, key).await {
        Ok((object, stream)) => {
            let body = Body::from_stream(stream.map(|r| r.map_err(|e| axum::Error::new(e))));
            Response::builder()
                .status(200)
                .header("Content-Type", object.content_type.unwrap_or_default())
                .header("Content-Length", object.size)
                .header("ETag", object.etag)
                .body(body)
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

async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
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

async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
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
        .object_manager
        .delete_object(claims.tenant_id, &bucket, &key, &claims.scopes)
        .await
    {
        Ok(()) => Response::builder()
            .status(axum::http::StatusCode::NO_CONTENT)
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

async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    req: Request,
) -> Response {
    let claims = req.extensions().get::<Claims>().cloned();

    match state
        .object_manager
        .head_object(claims, &bucket, &key)
        .await
    {
        Ok(object) => Response::builder()
            .status(200)
            .header("Content-Type", object.content_type.unwrap_or_default())
            .header("Content-Length", object.size)
            .header("ETag", object.etag)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn request(uri: &str) -> Request {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
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
}
