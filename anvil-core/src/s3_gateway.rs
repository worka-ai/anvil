use crate::AppState;
use crate::auth::Claims;
use crate::s3_auth::{aws_chunked_decoder, sigv4_auth};
use axum::{
    Router,
    body::Body,
    extract::{Path, Query, Request, State},
    middleware,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use futures_util::stream::StreamExt;
use std::collections::HashMap;

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
            get(get_object).put(put_object).head(head_object),
        )
        .with_state(state.clone())
        .route_layer(middleware::from_fn(aws_chunked_decoder))
        .route_layer(middleware::from_fn_with_state(state.clone(), sigv4_auth));

    public.merge(s3_routes)
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
    //let _ = body.collect().await;
    // let body_stream = req.into_body().into_data_stream().map(|r| {
    //     r.map(|chunk| chunk.to_vec())
    //         .map_err(|e| tonic::Status::internal(e.to_string()))
    // }).collect::<Vec<_>>();
    // println!("{:?}", body_stream);
    match state
        .bucket_manager
        .create_bucket(claims.tenant_id, &bucket, &state.region, &claims.scopes)
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
        .get_bucket_by_name(claims.tenant_id, &bucket_name, &state.region)
        .await
    {
        Ok(Some(_)) => (axum::http::StatusCode::OK, "").into_response(),
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
