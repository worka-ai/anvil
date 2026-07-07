use crate::anvil_api::internal_proxy_service_server::InternalProxyService;
use crate::anvil_api::*;
use crate::object_manager::{ObjectLinkReadMode, ObjectWriteOptions};
use crate::{AppState, auth, permissions::AnvilAction};
use futures_util::StreamExt;
use http::HeaderValue;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

#[tonic::async_trait]
impl InternalProxyService for AppState {
    type ProxyObjectStream = std::pin::Pin<
        Box<dyn futures_core::Stream<Item = Result<ProxyResponseChunk, Status>> + Send>,
    >;

    async fn proxy_object(
        &self,
        request: Request<tonic::Streaming<ProxyRequestChunk>>,
    ) -> Result<Response<Self::ProxyObjectStream>, Status> {
        let internal_claims = request
            .extensions()
            .get::<auth::Claims>()
            .ok_or_else(|| Status::unauthenticated("Missing claims"))?
            .clone();

        let mut stream = request.into_inner();
        let header = match stream.next().await {
            Some(Ok(chunk)) => match chunk.part {
                Some(proxy_request_chunk::Part::Header(header)) => header,
                Some(proxy_request_chunk::Part::Body(_)) => {
                    return Err(Status::invalid_argument(
                        "first proxy request chunk must be a header",
                    ));
                }
                None => return Err(Status::invalid_argument("empty proxy request chunk")),
            },
            Some(Err(status)) => return Err(status),
            None => return Err(Status::invalid_argument("empty proxy request stream")),
        };

        validate_internal_proxy_claims(&internal_claims, &header)?;
        let original_claims = decode_proxy_authz_context(&header)?;
        validate_proxy_principal_matches_header(&original_claims, &header)?;

        match header.method.to_ascii_uppercase().as_str() {
            "GET" => proxy_get_or_head(self, header, original_claims, false).await,
            "HEAD" => proxy_get_or_head(self, header, original_claims, true).await,
            "PUT" => proxy_put(self, header, original_claims, stream).await,
            "DELETE" => proxy_delete(self, header, original_claims).await,
            method => Err(Status::invalid_argument(format!(
                "unsupported proxy method {method}"
            ))),
        }
    }
}

async fn proxy_get_or_head(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
    head_only: bool,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    let version_id = parse_proxy_version_id(&header)?;
    let result = if head_only {
        let object = state
            .object_manager
            .head_object_with_link_mode_for_tenant(
                Some(original_claims),
                Some(tenant_id),
                &header.bucket_name,
                &header.object_key,
                version_id,
                ObjectLinkReadMode::Follow,
            )
            .await?;
        ProxyReadEither::Head(object.object)
    } else {
        ProxyReadEither::Get(
            state
                .object_manager
                .get_object_with_link_mode_for_tenant(
                    Some(original_claims),
                    Some(tenant_id),
                    header.bucket_name.clone(),
                    header.object_key.clone(),
                    version_id,
                    None,
                    ObjectLinkReadMode::Follow,
                )
                .await?,
        )
    };

    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        match result {
            ProxyReadEither::Head(object) => {
                let _ = tx
                    .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                        request_id: header.request_id,
                        status: 200,
                        headers: object_response_headers(&object),
                        trailers: Vec::new(),
                        committed: false,
                    })))
                    .await;
            }
            ProxyReadEither::Get(result) => {
                let object = result.object;
                if tx
                    .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                        request_id: header.request_id,
                        status: 200,
                        headers: object_response_headers(&object),
                        trailers: Vec::new(),
                        committed: false,
                    })))
                    .await
                    .is_err()
                {
                    return;
                }
                let mut data_stream = result.stream;
                while let Some(chunk) = data_stream.next().await {
                    match chunk {
                        Ok(bytes) => {
                            if tx
                                .send(Ok(ProxyResponseChunk {
                                    part: Some(proxy_response_chunk::Part::Body(bytes)),
                                }))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                        Err(status) => {
                            let _ = tx.send(Err(status)).await;
                            break;
                        }
                    }
                }
            }
        }
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

async fn proxy_put(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
    stream: tonic::Streaming<ProxyRequestChunk>,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    if tenant_id != original_claims.tenant_id {
        return Err(Status::permission_denied(
            "proxy tenant does not match original principal",
        ));
    }
    if proxy_request_has_write_etag_preconditions(&header.headers) {
        let current = state
            .object_manager
            .current_object_for_write_precondition(
                tenant_id,
                &header.bucket_name,
                &header.object_key,
                &original_claims.scopes,
            )
            .await?;
        evaluate_proxy_write_etag_preconditions(
            &header.headers,
            current.as_ref().map(|object| object.etag.as_str()),
        )?;
    }
    let content_type = proxy_header_string(&header.headers, "content-type");
    let data_stream = stream.map(|chunk_result| match chunk_result {
        Ok(chunk) => match chunk.part {
            Some(proxy_request_chunk::Part::Body(bytes)) => Ok(bytes),
            Some(proxy_request_chunk::Part::Header(_)) => Err(Status::invalid_argument(
                "proxy request may contain only one header chunk",
            )),
            None => Ok(Vec::new()),
        },
        Err(status) => Err(status),
    });

    let object = state
        .object_manager
        .put_object(
            tenant_id,
            &header.bucket_name,
            &header.object_key,
            &original_claims.scopes,
            data_stream,
            ObjectWriteOptions {
                content_type,
                user_metadata: proxy_user_metadata(&header.headers),
            },
        )
        .await?;

    unary_proxy_response(
        header.request_id,
        200,
        true,
        object_response_headers(&object),
    )
}

async fn proxy_delete(
    state: &AppState,
    header: ProxyRequestHeader,
    original_claims: auth::Claims,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let tenant_id = parse_proxy_tenant_id(&header)?;
    if tenant_id != original_claims.tenant_id {
        return Err(Status::permission_denied(
            "proxy tenant does not match original principal",
        ));
    }
    let deleted = if let Some(version_id) = parse_proxy_version_id(&header)? {
        state
            .object_manager
            .delete_object_version(
                tenant_id,
                &header.bucket_name,
                &header.object_key,
                version_id,
                &original_claims.scopes,
            )
            .await?
    } else {
        state
            .object_manager
            .delete_object(
                tenant_id,
                &header.bucket_name,
                &header.object_key,
                &original_claims.scopes,
            )
            .await?
    };

    unary_proxy_response(
        header.request_id,
        204,
        true,
        object_response_headers(&deleted),
    )
}

fn unary_proxy_response(
    request_id: String,
    status: u32,
    committed: bool,
    headers: Vec<ProxyHeader>,
) -> Result<Response<<AppState as InternalProxyService>::ProxyObjectStream>, Status> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _ = tx
            .send(Ok(proxy_header_chunk(ProxyResponseHeader {
                request_id,
                status,
                headers,
                trailers: Vec::new(),
                committed,
            })))
            .await;
    });
    Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
}

fn proxy_header_chunk(header: ProxyResponseHeader) -> ProxyResponseChunk {
    ProxyResponseChunk {
        part: Some(proxy_response_chunk::Part::Header(header)),
    }
}

fn object_response_headers(object: &crate::persistence::Object) -> Vec<ProxyHeader> {
    let mut headers = vec![
        proxy_header("etag", object.etag.as_bytes()),
        proxy_header(
            "x-anvil-version-id",
            object.version_id.to_string().as_bytes(),
        ),
        proxy_header("x-anvil-record-hash", object.record_hash.as_bytes()),
        proxy_header(
            "x-anvil-mutation-id",
            object.mutation_id.to_string().as_bytes(),
        ),
        proxy_header("content-length", object.size.to_string().as_bytes()),
        proxy_header(
            "x-anvil-created-at",
            object.created_at.to_rfc3339().as_bytes(),
        ),
    ];
    if let Some(content_type) = object.content_type.as_deref() {
        headers.push(proxy_header("content-type", content_type.as_bytes()));
    }
    if let Some(serde_json::Value::Object(values)) = object.user_meta.as_ref() {
        for (key, value) in values {
            if let Some(value) = value.as_str() {
                headers.push(proxy_header(&format!("x-amz-meta-{key}"), value.as_bytes()));
            }
        }
    }
    headers
}

fn proxy_header(name: &str, value: &[u8]) -> ProxyHeader {
    ProxyHeader {
        name: name.to_ascii_lowercase(),
        value: value.to_vec(),
    }
}

fn validate_internal_proxy_claims(
    claims: &auth::Claims,
    header: &ProxyRequestHeader,
) -> Result<(), Status> {
    if claims.tenant_id != 0 || !(claims.sub == "internal" || claims.sub == "internal-worker") {
        return Err(Status::permission_denied(
            "Internal proxy requires a node-issued token",
        ));
    }
    let resource = format!(
        "tenant/{}/bucket/{}/{}",
        header.tenant_id, header.bucket_name, header.object_key
    );
    if !auth::is_authorized(AnvilAction::InternalProxyObject, &resource, &claims.scopes) {
        return Err(Status::permission_denied("Permission denied"));
    }
    Ok(())
}

fn decode_proxy_authz_context(header: &ProxyRequestHeader) -> Result<auth::Claims, Status> {
    if header.authz_context.is_empty() {
        return Err(Status::invalid_argument("proxy authz_context is required"));
    }
    serde_json::from_slice::<auth::Claims>(&header.authz_context)
        .map_err(|error| Status::invalid_argument(format!("invalid proxy authz_context: {error}")))
}

fn validate_proxy_principal_matches_header(
    claims: &auth::Claims,
    header: &ProxyRequestHeader,
) -> Result<(), Status> {
    if claims.sub != header.principal_id {
        return Err(Status::permission_denied(
            "proxy principal does not match authz context",
        ));
    }
    if claims.tenant_id != parse_proxy_tenant_id(header)? {
        return Err(Status::permission_denied(
            "proxy tenant does not match authz context",
        ));
    }
    Ok(())
}

fn parse_proxy_tenant_id(header: &ProxyRequestHeader) -> Result<i64, Status> {
    header
        .tenant_id
        .parse::<i64>()
        .map_err(|_| Status::invalid_argument("proxy tenant_id must be an integer"))
}

fn parse_proxy_version_id(header: &ProxyRequestHeader) -> Result<Option<uuid::Uuid>, Status> {
    let Some(value) = proxy_header_string(&header.headers, "x-anvil-version-id") else {
        return Ok(None);
    };
    if value.is_empty() {
        return Ok(None);
    }
    uuid::Uuid::parse_str(&value)
        .map(Some)
        .map_err(|_| Status::invalid_argument("invalid x-anvil-version-id"))
}

fn proxy_header_string(headers: &[ProxyHeader], name: &str) -> Option<String> {
    headers
        .iter()
        .find(|header| header.name.eq_ignore_ascii_case(name))
        .and_then(|header| std::str::from_utf8(&header.value).ok())
        .map(ToOwned::to_owned)
}

fn proxy_user_metadata(headers: &[ProxyHeader]) -> Option<serde_json::Value> {
    let mut values = serde_json::Map::new();
    for header in headers {
        let Some(metadata_key) = header.name.strip_prefix("x-amz-meta-") else {
            continue;
        };
        let Ok(metadata_value) = std::str::from_utf8(&header.value) else {
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

fn proxy_request_has_write_etag_preconditions(headers: &[ProxyHeader]) -> bool {
    proxy_header_string(headers, "if-match").is_some()
        || proxy_header_string(headers, "if-none-match").is_some()
}

fn evaluate_proxy_write_etag_preconditions(
    headers: &[ProxyHeader],
    current_etag: Option<&str>,
) -> Result<(), Status> {
    if let Some(value) = proxy_header_string(headers, "if-match")
        && !current_etag.is_some_and(|etag| proxy_etag_condition_matches(&value, etag))
    {
        return Err(proxy_precondition_failed_status());
    }
    if let Some(value) = proxy_header_string(headers, "if-none-match")
        && current_etag.is_some_and(|etag| proxy_etag_condition_matches(&value, etag))
    {
        return Err(proxy_precondition_failed_status());
    }
    Ok(())
}

fn proxy_etag_condition_matches(header_value: &str, current_etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || normalize_proxy_etag(candidate) == current_etag)
}

fn normalize_proxy_etag(value: &str) -> &str {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
}

fn proxy_precondition_failed_status() -> Status {
    Status::failed_precondition("At least one precondition did not hold")
}

#[allow(dead_code)]
fn metadata_value_to_proxy_header(name: &str, value: &HeaderValue) -> Option<ProxyHeader> {
    Some(proxy_header(name, value.as_bytes()))
}

enum ProxyReadEither {
    Head(crate::persistence::Object),
    Get(crate::object_manager::ObjectReadResult),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    fn claims(sub: &str, tenant_id: i64, scopes: Vec<String>) -> auth::Claims {
        auth::Claims {
            sub: sub.to_string(),
            exp: usize::MAX,
            scopes,
            tenant_id,
            jti: None,
        }
    }

    fn header() -> ProxyRequestHeader {
        ProxyRequestHeader {
            request_id: "req-1".to_string(),
            idempotency_key: "idem-1".to_string(),
            principal_id: "app-a".to_string(),
            tenant_id: "42".to_string(),
            bucket_name: "bucket-a".to_string(),
            object_key: "path/file.txt".to_string(),
            method: "GET".to_string(),
            canonical_host: "bucket-a.tenant.eu-west-1.anvil-storage.test".to_string(),
            canonical_path: "/path/file.txt".to_string(),
            bucket_locator_generation: 7,
            headers: Vec::new(),
            authz_context: serde_json::to_vec(&claims(
                "app-a",
                42,
                vec!["object:*|bucket-a/*".to_string()],
            ))
            .unwrap(),
        }
    }

    #[test]
    fn proxy_auth_requires_internal_token_with_proxy_scope() {
        let header = header();
        validate_internal_proxy_claims(
            &claims("internal", 0, vec!["internal:proxy_object|*".to_string()]),
            &header,
        )
        .unwrap();

        let tenant = validate_internal_proxy_claims(
            &claims("app-a", 42, vec!["internal:proxy_object|*".to_string()]),
            &header,
        )
        .unwrap_err();
        assert_eq!(tenant.code(), Code::PermissionDenied);

        let missing_scope =
            validate_internal_proxy_claims(&claims("internal", 0, vec![]), &header).unwrap_err();
        assert_eq!(missing_scope.code(), Code::PermissionDenied);
    }

    #[test]
    fn proxy_authz_context_must_match_header_principal_and_tenant() {
        let header = header();
        let decoded = decode_proxy_authz_context(&header).unwrap();
        validate_proxy_principal_matches_header(&decoded, &header).unwrap();

        let mut bad_principal = header.clone();
        bad_principal.principal_id = "other".to_string();
        assert_eq!(
            validate_proxy_principal_matches_header(&decoded, &bad_principal)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );

        let mut bad_tenant = header;
        bad_tenant.tenant_id = "43".to_string();
        assert_eq!(
            validate_proxy_principal_matches_header(&decoded, &bad_tenant)
                .unwrap_err()
                .code(),
            Code::PermissionDenied
        );
    }
}
