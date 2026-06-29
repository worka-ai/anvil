use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::{AppState, auth::Claims, crypto};
use aws_credential_types::Credentials;
use aws_sigv4::http_request::{
    PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation, SigningParams,
    SigningSettings, UriPathNormalizationMode, sign,
};
use aws_sigv4::sign::v4;
use aws_smithy_runtime_api::client::identity::Identity;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{self, HeaderMap},
    middleware::Next,
    response::Response,
};

use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Date, Month, PrimitiveDateTime, Time as Tm};
use tracing::{debug, info, warn};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug)]
struct AwsChunkedVerification {
    signing_key: Vec<u8>,
    timestamp: String,
    credential_scope: String,
    previous_signature: String,
}

/// Middleware (Stage 2) to decode an `aws-chunked` request body.
/// This runs AFTER `sigv4_auth`.
pub async fn aws_chunked_decoder(req: Request, next: Next) -> Response {
    let (mut parts, body) = req.into_parts();

    let is_streaming = if let Some(encoding) = parts.headers.get("content-encoding") {
        encoding.to_str().unwrap_or("") == "aws-chunked"
    } else {
        false
    };

    if is_streaming {
        let verification = parts.extensions.get::<AwsChunkedVerification>().cloned();
        match decode_aws_chunked_body(body, verification.as_ref()).await {
            Ok(decoded_bytes) => {
                // Remove the chunked encoding header as it's no longer accurate
                parts.headers.remove("content-encoding");
                // Create a new request with the clean body
                let new_req = Request::from_parts(parts, Body::from(decoded_bytes));
                next.run(new_req).await
            }
            Err(e) => {
                warn!(error = %e, "Failed to decode aws-chunked body");
                Response::builder()
                    .status(400)
                    .body(Body::from(format!(
                        "Failed to decode aws-chunked body: {e}"
                    )))
                    .unwrap()
            }
        }
    } else {
        // Not a streaming request, pass it through unmodified.
        let req = Request::from_parts(parts, body);
        next.run(req).await
    }
}

/// Middleware (Stage 1) to perform SigV4 authentication.
/// This must run BEFORE the `aws_chunked_decoder`.
pub async fn sigv4_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();

    // Skip SigV4 for gRPC requests to avoid interfering with tonic
    if let Some(ct) = parts
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|h| h.to_str().ok())
    {
        if ct.starts_with("application/grpc") {
            let req = Request::from_parts(parts, body);
            return next.run(req).await;
        }
    }

    // Your correct detection logic.
    let is_streaming = if let Some(encoding) = parts.headers.get("content-encoding") {
        encoding.to_str().unwrap_or("") == "aws-chunked"
    } else {
        false
    };

    // We need to buffer the body for hashing ONLY if it's NOT a streaming request.
    // For streaming requests, the body is passed through untouched for later decoding.
    let (body_bytes, reconstituted_body) = if !is_streaming {
        let bytes = match body.collect().await {
            Ok(b) => b.to_bytes(),
            Err(e) => {
                warn!(error = %e, "Failed to read body in SigV4 middleware");
                return Response::builder()
                    .status(400)
                    .body(Body::from(format!("Failed to read body: {e}")))
                    .unwrap();
            }
        };
        (Some(bytes.clone()), Body::from(bytes))
    } else {
        (None, body)
    };

    let mut req = Request::from_parts(parts.clone(), reconstituted_body);

    let auth_header = match parts
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        Some(h) if h.starts_with("AWS4-HMAC-SHA256 ") => h,
        _ => {
            let method = parts.method.clone();
            if method == http::Method::GET || method == http::Method::HEAD {
                debug!("No SigV4 for GET/HEAD, deferring auth to handler");
                return next.run(req).await;
            }
            return Response::builder()
                .status(401)
                .body(Body::from("Missing Authorization"))
                .unwrap();
        }
    };

    let parsed = match parse_auth_header(auth_header) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to parse SigV4 Authorization header");
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Invalid Authorization header: {e}")))
                .unwrap();
        }
    };

    let app_details = match state
        .persistence
        .get_app_by_client_id(&parsed.access_key_id)
        .await
    {
        Ok(Some(d)) => d,
        _ => {
            warn!(access_key_id = %parsed.access_key_id, "SigV4 auth failed: Invalid access key");
            return Response::builder()
                .status(403)
                .body(Body::from("Invalid access key"))
                .unwrap();
        }
    };

    let encryption_key = hex::decode(&state.config.anvil_secret_encryption_key)
        .expect("ANVIL_SECRET_ENCRYPTION_KEY must be a valid hex string");
    let secret_bytes = match crypto::decrypt(&app_details.client_secret_encrypted, &encryption_key)
    {
        Ok(s) => s,
        Err(_) => {
            warn!(access_key_id = %parsed.access_key_id, "Failed to decrypt secret for SigV4 auth");
            return Response::builder()
                .status(500)
                .body(Body::from("Failed to decrypt secret"))
                .unwrap();
        }
    };
    let secret = match String::from_utf8(secret_bytes) {
        Ok(s) => s,
        Err(_) => {
            warn!(access_key_id = %parsed.access_key_id, "Decrypted secret is not valid UTF-8");
            return Response::builder()
                .status(500)
                .body(Body::from("Decrypted secret is not valid UTF-8"))
                .unwrap();
        }
    };

    let identity: Identity =
        Credentials::new(&parsed.access_key_id, &secret, None, None, "sigv4-verify").into();

    let signing_time = match parts
        .headers
        .get("x-amz-date")
        .and_then(|h| h.to_str().ok())
        .and_then(parse_x_amz_date)
    {
        Some(t) => t,
        None => match parse_scope_yyyymmdd(&parsed.date) {
            Some(t) => t,
            None => {
                warn!(access_key_id = %parsed.access_key_id, "Missing or invalid X-Amz-Date for SigV4");
                return Response::builder()
                    .status(400)
                    .body(Body::from("Missing or invalid X-Amz-Date"))
                    .unwrap();
            }
        },
    };

    let host = effective_host(&parts);
    let scheme = detect_scheme(&parts.headers, &parts);
    let path_q = parts
        .uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or("/");
    let absolute_url = format!("{scheme}://{host}{path_q}");

    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::Headers;
    settings.percent_encoding_mode = PercentEncodingMode::Single;
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled;
    settings.payload_checksum_kind = aws_sigv4::http_request::PayloadChecksumKind::XAmzSha256;
    settings.expires_in = None;
    settings.excluded_headers = Some(vec![Cow::Borrowed("authorization")]);

    let signing_params: SigningParams = v4::SigningParams::builder()
        .identity(&identity)
        .region(&parsed.region)
        .name(&parsed.service)
        .time(signing_time)
        .settings(settings)
        .build()
        .expect("valid signing params")
        .into();

    // IMPORTANT: use exactly what the client signed, if provided.
    let payload_hash = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            if is_streaming {
                // extremely rare path: streaming but no header present
                "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string()
            } else {
                sha256_hex(
                    body_bytes
                        .as_ref()
                        .expect("non-streaming body bytes present"),
                )
            }
        });

    let mut hdrs: HashMap<String, String> = HashMap::new();
    for (k, v) in parts.headers.iter() {
        if let Ok(val) = v.to_str() {
            hdrs.insert(k.as_str().to_ascii_lowercase(), val.to_string());
        }
    }

    let signed_set: HashSet<&str> = parsed.signed_headers.iter().map(|s| s.as_str()).collect();

    if signed_set.contains("host") && !hdrs.contains_key("host") {
        hdrs.insert("host".to_string(), host.clone());
    }

    let headers_iter = hdrs
        .iter()
        .filter(|(name, _)| signed_set.contains(name.as_str()))
        .map(|(name, val)| (name.as_str(), val.as_str()));

    let signable_req = match SignableRequest::new(
        parts.method.as_str(),
        &absolute_url,
        headers_iter,
        SignableBody::Precomputed(payload_hash.clone()),
    ) {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, access_key_id = %parsed.access_key_id, "Bad request for signing");
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Bad request for signing: {e}")))
                .unwrap();
        }
    };

    // Compute signature for THIS request exactly as the client would have
    let out = match sign(signable_req, &signing_params) {
        Ok(o) => o,
        Err(_) => {
            warn!(access_key_id = %parsed.access_key_id, "SigV4 signature computation failed");
            return Response::builder()
                .status(403)
                .body(Body::from("Signature verification failed"))
                .unwrap();
        }
    };
    let (_instr, computed_sig) = out.into_parts();

    if !constant_time_eq_str(computed_sig.as_str(), &parsed.signature) {
        warn!(access_key_id = %parsed.access_key_id, "SigV4 signature mismatch");
        return Response::builder()
            .status(403)
            .body(Body::from("Signature verification failed"))
            .unwrap();
    }

    info!(access_key_id = %parsed.access_key_id, "SigV4 authentication successful");

    // Attach claims and continue
    let scopes = match state.persistence.get_policies_for_app(app_details.id).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, access_key_id = %parsed.access_key_id, "Failed to fetch policies for app");
            return Response::builder()
                .status(500)
                .body(Body::from("Failed to fetch policies"))
                .unwrap();
        }
    };

    if is_streaming && payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
        let timestamp = parts
            .headers
            .get("x-amz-date")
            .and_then(|h| h.to_str().ok())
            .map(str::to_string)
            .unwrap_or_else(|| format!("{}T000000Z", parsed.date));
        req.extensions_mut().insert(AwsChunkedVerification {
            signing_key: derive_sigv4_signing_key(
                &secret,
                &parsed.date,
                &parsed.region,
                &parsed.service,
            ),
            timestamp,
            credential_scope: format!(
                "{}/{}/{}/aws4_request",
                parsed.date, parsed.region, parsed.service
            ),
            previous_signature: parsed.signature.clone(),
        });
    }

    let claims = Claims {
        sub: app_details.id.to_string(),
        tenant_id: app_details.tenant_id,
        scopes,
        exp: 0, // SigV4 has its own expiry mechanism
    };
    req.extensions_mut().insert(claims);

    next.run(req).await
}

// ----------------- helpers -----------------

/// Decode an `aws-chunked` content-encoded body and, when SigV4 streaming
/// verification metadata is present, verify every chunk signature in the chain.
async fn decode_aws_chunked_body(
    body: Body,
    verification: Option<&AwsChunkedVerification>,
) -> anyhow::Result<bytes::Bytes> {
    use bytes::{Buf, BytesMut};

    let mut buffer = BytesMut::from(body.collect().await?.to_bytes());
    let mut decoded = BytesMut::new();
    let mut previous_signature = verification.map(|v| v.previous_signature.clone());

    loop {
        let header_end = buffer
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| anyhow::anyhow!("Malformed chunk: no header ending found"))?;

        let header_line = std::str::from_utf8(&buffer[..header_end])?.to_string();
        let (chunk_size, chunk_signature) = parse_aws_chunk_header(&header_line)?;
        buffer.advance(header_end + 2);

        if chunk_size == 0 {
            verify_aws_chunk_signature(
                verification,
                &mut previous_signature,
                chunk_signature,
                b"",
            )?;
            consume_aws_chunked_trailers(&mut buffer)?;
            break;
        }

        if buffer.len() < chunk_size + 2 {
            return Err(anyhow::anyhow!(
                "Incomplete chunk data: needed {}, have {}",
                chunk_size + 2,
                buffer.len()
            ));
        }

        let chunk = buffer[..chunk_size].to_vec();
        if &buffer[chunk_size..chunk_size + 2] != b"\r\n" {
            return Err(anyhow::anyhow!("Malformed chunk: missing trailing CRLF"));
        }
        buffer.advance(chunk_size + 2);

        verify_aws_chunk_signature(
            verification,
            &mut previous_signature,
            chunk_signature,
            &chunk,
        )?;
        decoded.extend_from_slice(&chunk);
    }

    Ok(decoded.freeze())
}

fn verify_aws_chunk_signature(
    verification: Option<&AwsChunkedVerification>,
    previous_signature: &mut Option<String>,
    chunk_signature: Option<String>,
    chunk: &[u8],
) -> anyhow::Result<()> {
    let Some(v) = verification else {
        return Ok(());
    };
    let supplied = chunk_signature
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Malformed chunk: missing chunk-signature"))?;
    let previous = previous_signature
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("Missing previous streaming signature"))?;
    let expected = aws_chunk_signature(v, previous, chunk);
    if !constant_time_eq_str(&expected, supplied) {
        return Err(anyhow::anyhow!("Malformed chunk: chunk signature mismatch"));
    }
    *previous_signature = Some(supplied.to_string());
    Ok(())
}

fn consume_aws_chunked_trailers(buffer: &mut bytes::BytesMut) -> anyhow::Result<()> {
    use bytes::Buf;

    loop {
        let line_end = buffer
            .windows(2)
            .position(|w| w == b"\r\n")
            .ok_or_else(|| anyhow::anyhow!("Malformed chunk: unterminated trailing headers"))?;
        if line_end == 0 {
            buffer.advance(2);
            if !buffer.is_empty() {
                return Err(anyhow::anyhow!(
                    "Malformed chunk: trailing bytes after final chunk"
                ));
            }
            return Ok(());
        }
        buffer.advance(line_end + 2);
    }
}

fn parse_aws_chunk_header(header_line: &str) -> anyhow::Result<(usize, Option<String>)> {
    let mut fields = header_line.split(';');
    let hex_size = fields
        .next()
        .ok_or_else(|| anyhow::anyhow!("Malformed chunk header"))?;
    let chunk_size = usize::from_str_radix(hex_size, 16)?;
    let mut chunk_signature = None;
    for field in fields {
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        if name.eq_ignore_ascii_case("chunk-signature") {
            chunk_signature = Some(value.to_string());
        }
    }
    Ok((chunk_size, chunk_signature))
}

fn aws_chunk_signature(
    verification: &AwsChunkedVerification,
    previous_signature: &str,
    chunk: &[u8],
) -> String {
    let empty_hash = sha256_hex(b"");
    let chunk_hash = sha256_hex(chunk);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
        verification.timestamp,
        verification.credential_scope,
        previous_signature,
        empty_hash,
        chunk_hash
    );
    hmac_sha256_hex(&verification.signing_key, string_to_sign.as_bytes())
}

fn derive_sigv4_signing_key(secret: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes());
    let k_region = hmac_sha256(&k_date, region.as_bytes());
    let k_service = hmac_sha256(&k_region, service.as_bytes());
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts keys of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    hex::encode(hmac_sha256(key, data))
}

struct ParsedAuth {
    access_key_id: String,
    date: String, // YYYYMMDD
    region: String,
    service: String,
    signed_headers: Vec<String>, // lowercase, in order
    signature: String,
}

fn effective_host(parts: &http::request::Parts) -> String {
    // 1) HTTP/2 authority from URI, if present
    if let Some(auth) = parts.uri.authority() {
        return auth.as_str().to_string();
    }
    // 2) Host header
    if let Some(h) = parts
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
    {
        return h.to_string();
    }
    // 3) Forwarded host from proxy
    if let Some(h) = parts
        .headers
        .get("x-forwarded-host")
        .and_then(|h| h.to_str().ok())
    {
        return h.to_string();
    }
    "localhost".to_string()
}

// prefer XFP, then URI scheme, then https (since client talked TLS to Caddy)
fn detect_scheme(headers: &HeaderMap, parts: &http::request::Parts) -> &'static str {
    if let Some(v) = headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
    {
        if v.eq_ignore_ascii_case("https") {
            return "https";
        }
        if v.eq_ignore_ascii_case("http") {
            return "http";
        }
    }
    if let Some(s) = parts.uri.scheme_str() {
        if s.eq_ignore_ascii_case("https") {
            return "https";
        }
        if s.eq_ignore_ascii_case("http") {
            return "http";
        }
    }
    "https"
}

// Parse: AWS4-HMAC-SHA256 Credential=AKID/DATE/REGION/SERVICE/aws4_request, SignedHeaders=..., Signature=...
fn parse_auth_header(h: &str) -> Result<ParsedAuth, &'static str> {
    let after = h
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or("missing prefix")?;
    let mut credential = None;
    let mut signature = None;
    let mut signed_headers = None;

    for part in after.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed_headers = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }

    let cred = credential.ok_or("missing Credential")?;
    let sig = signature.ok_or("missing Signature")?.to_string();
    let sh = signed_headers.ok_or("missing SignedHeaders")?;

    let mut pieces = cred.split('/');
    let access_key_id = pieces.next().ok_or("bad Credential")?.to_string();
    let date = pieces.next().ok_or("bad date")?.to_string();
    let region = pieces.next().ok_or("bad region")?.to_string();
    let service = pieces.next().ok_or("bad service")?.to_string();
    // trailing aws4_request ignored

    let signed_headers = sh
        .split(';')
        .map(|s| s.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();

    Ok(ParsedAuth {
        access_key_id,
        date,
        region,
        service,
        signed_headers,
        signature: sig,
    })
}

// Parse "YYYYMMDDTHHMMSSZ" into SystemTime
fn parse_x_amz_date(s: &str) -> Option<SystemTime> {
    if s.len() != 16 || !s.ends_with('Z') || !s.contains('T') {
        return None;
    }
    let (d8, t7) = s.split_at(8); // YYYYMMDD + "THHMMSSZ"
    let t6 = &t7[1..7]; // HHMMSS

    let y = i32::from_str(&d8[0..4]).ok()?;
    let m = u8::from_str(&d8[4..6]).ok()?;
    let d = u8::from_str(&d8[6..8]).ok()?;
    let hh = u8::from_str(&t6[0..2]).ok()?;
    let mm = u8::from_str(&t6[2..4]).ok()?;
    let ss = u8::from_str(&t6[4..6]).ok()?;

    let date = Date::from_calendar_date(y, Month::try_from(m).ok()?, d).ok()?;
    let time = Tm::from_hms(hh.into(), mm.into(), ss.into()).ok()?;
    let odt = PrimitiveDateTime::new(date, time).assume_utc();
    Some(UNIX_EPOCH + Duration::from_secs(odt.unix_timestamp() as u64))
}

// Fallback: YYYYMMDD → midnight UTC
fn parse_scope_yyyymmdd(s: &str) -> Option<SystemTime> {
    if s.len() != 8 {
        return None;
    }
    let y = i32::from_str(&s[0..4]).ok()?;
    let m = u8::from_str(&s[4..6]).ok()?;
    let d = u8::from_str(&s[6..8]).ok()?;
    let date = Date::from_calendar_date(y, Month::try_from(m).ok()?, d).ok()?;
    let time = Tm::from_hms(0, 0, 0).ok()?;
    let odt = PrimitiveDateTime::new(date, time).assume_utc();
    Some(UNIX_EPOCH + Duration::from_secs(odt.unix_timestamp() as u64))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let out = h.finalize();
    out.iter().map(|b| format!("{:02x}", b)).collect()
}

fn constant_time_eq_str(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_verification() -> AwsChunkedVerification {
        let date = "20260629";
        let region = "test-region-1";
        let service = "s3";
        AwsChunkedVerification {
            signing_key: derive_sigv4_signing_key("test-secret", date, region, service),
            timestamp: "20260629T120000Z".to_string(),
            credential_scope: format!("{date}/{region}/{service}/aws4_request"),
            previous_signature: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
        }
    }

    fn signed_chunked_body(chunks: &[&[u8]], verification: &AwsChunkedVerification) -> Vec<u8> {
        let mut previous = verification.previous_signature.clone();
        let mut body = Vec::new();
        for chunk in chunks {
            let signature = aws_chunk_signature(verification, &previous, chunk);
            body.extend_from_slice(
                format!("{:x};chunk-signature={signature}\r\n", chunk.len()).as_bytes(),
            );
            body.extend_from_slice(chunk);
            body.extend_from_slice(b"\r\n");
            previous = signature;
        }
        let final_signature = aws_chunk_signature(verification, &previous, b"");
        body.extend_from_slice(format!("0;chunk-signature={final_signature}\r\n\r\n").as_bytes());
        body
    }

    #[tokio::test]
    async fn aws_chunked_decoder_verifies_signed_chunk_chain() {
        let verification = test_verification();
        let body = signed_chunked_body(&[b"hello", b" world"], &verification);

        let decoded = decode_aws_chunked_body(Body::from(body), Some(&verification))
            .await
            .expect("signed chunk chain should verify");

        assert_eq!(decoded.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn aws_chunked_decoder_rejects_tampered_signed_chunk() {
        let verification = test_verification();
        let mut body = signed_chunked_body(&[b"hello"], &verification);
        let payload_offset = body
            .windows(b"\r\nhello\r\n".len())
            .position(|window| window == b"\r\nhello\r\n")
            .expect("payload marker")
            + 2;
        body[payload_offset] = b'j';

        let error = decode_aws_chunked_body(Body::from(body), Some(&verification))
            .await
            .expect_err("tampered chunk must not verify");

        assert!(
            error.to_string().contains("chunk signature mismatch"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn aws_chunked_decoder_requires_chunk_signatures_when_verifying() {
        let verification = test_verification();
        let body = b"5\r\nhello\r\n0\r\n\r\n".to_vec();

        let error = decode_aws_chunked_body(Body::from(body), Some(&verification))
            .await
            .expect_err("missing chunk-signature must fail");

        assert!(
            error.to_string().contains("missing chunk-signature"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn aws_chunked_decoder_still_decodes_unsigned_legacy_streams() {
        let body = b"5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n".to_vec();

        let decoded = decode_aws_chunked_body(Body::from(body), None)
            .await
            .expect("unsigned legacy stream should decode");

        assert_eq!(decoded.as_ref(), b"hello world");
    }
}
