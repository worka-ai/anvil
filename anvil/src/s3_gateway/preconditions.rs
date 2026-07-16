use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ByteRange {
    pub(super) start: u64,
    pub(super) end: u64,
}

impl ByteRange {
    pub(super) fn len(self) -> u64 {
        self.end - self.start + 1
    }
}

pub(super) fn evaluate_object_preconditions(
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

pub(super) fn request_has_write_etag_preconditions(headers: &axum::http::HeaderMap) -> bool {
    headers.contains_key(axum::http::header::IF_MATCH)
        || headers.contains_key(axum::http::header::IF_NONE_MATCH)
}

pub(super) fn evaluate_write_etag_preconditions(
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

pub(super) fn evaluate_copy_source_preconditions(
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

pub(super) fn etag_condition_matches(header_value: &str, current_etag: &str) -> bool {
    header_value
        .split(',')
        .map(str::trim)
        .any(|candidate| candidate == "*" || normalize_etag(candidate) == current_etag)
}

pub(super) fn normalize_etag(value: &str) -> &str {
    value
        .strip_prefix("W/")
        .unwrap_or(value)
        .trim()
        .trim_matches('"')
}

pub(super) fn precondition_failed_response() -> Response {
    s3_error(
        "PreconditionFailed",
        "At least one precondition did not hold",
        axum::http::StatusCode::PRECONDITION_FAILED,
    )
}

pub(super) fn not_modified_response(current_etag: &str) -> Response {
    Response::builder()
        .status(axum::http::StatusCode::NOT_MODIFIED)
        .header("ETag", current_etag)
        .body(Body::empty())
        .unwrap()
}

pub(super) fn object_last_modified_time(
    value: chrono::DateTime<chrono::Utc>,
) -> std::time::SystemTime {
    let seconds = value.timestamp();
    if seconds <= 0 {
        std::time::UNIX_EPOCH
    } else {
        std::time::UNIX_EPOCH + std::time::Duration::from_secs(seconds as u64)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RequestedByteRange {
    FromStart { start: u64, end: Option<u64> },
    Suffix { len: u64 },
}

impl RequestedByteRange {
    pub(super) fn resolve(self, object_size: u64) -> Result<ByteRange, Response> {
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

pub(super) fn parse_http_range(
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

pub(super) fn invalid_range_response(object_size: u64) -> Response {
    let mut response = s3_error(
        "InvalidRange",
        "Invalid Range header",
        axum::http::StatusCode::RANGE_NOT_SATISFIABLE,
    );
    response.headers_mut().insert(
        axum::http::header::CONTENT_RANGE,
        format!("bytes */{}", object_size).parse().unwrap(),
    );
    response
}

pub(super) fn slice_stream_by_range(
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
