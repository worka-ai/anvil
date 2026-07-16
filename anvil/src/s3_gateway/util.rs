use super::*;

pub(super) fn s3_error(code: &str, message: &str, status: axum::http::StatusCode) -> Response {
    let request_id = new_s3_request_id();
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error>\n  <Code>{}</Code>\n  <Message>{}</Message>\n  <RequestId>{}</RequestId>\n</Error>\n",
        code,
        xml_escape(message),
        request_id
    );
    Response::builder()
        .status(status)
        .header("Content-Type", "application/xml")
        .header("x-amz-request-id", request_id)
        .body(Body::from(body))
        .unwrap()
}

pub(super) fn new_s3_request_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

pub(super) fn s3_query_map(uri: &Uri) -> HashMap<String, String> {
    uri.query()
        .map(|query| {
            query
                .split('&')
                .filter(|pair| !pair.is_empty())
                .map(|pair| {
                    let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
                    (
                        percent_decode_query_component(name),
                        percent_decode_query_component(value),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(super) fn percent_decode_query_component(value: &str) -> String {
    let value = value.replace('+', " ");
    percent_decode(value.as_bytes())
}

pub(super) fn percent_decode(bytes: &[u8]) -> String {
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

pub(super) fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub(super) fn s3_status_to_response_for_auth(
    status: tonic::Status,
    request_is_authenticated: bool,
    not_found_code: &str,
    cross_region_policy: CrossRegionRoutingPolicy,
) -> Response {
    if let Some(response) = s3_remote_bucket_response_from_status(&status, cross_region_policy) {
        return response;
    }

    match status.code() {
        tonic::Code::FailedPrecondition => {
            if let Some(response) =
                s3_remote_bucket_response_from_status(&status, cross_region_policy)
            {
                return response;
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

pub(super) fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

pub(super) fn percent_decode_path_component(value: &str) -> String {
    percent_decode(value.as_bytes())
}

pub(super) fn parse_s3_version_id(
    q: &HashMap<String, String>,
) -> Result<Option<uuid::Uuid>, Response> {
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
