use std::borrow::Cow;
use std::collections::HashSet;
use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    body::Body,
    extract::{Request, State},
    http::{self, HeaderMap},
    middleware::Next,
    response::Response,
};
use http_body_util::BodyExt;

use aws_credential_types::Credentials;
use aws_smithy_runtime_api::client::identity::Identity;
use aws_sigv4::http_request::{
    sign, PayloadChecksumKind, PercentEncodingMode, SignableBody, SignableRequest, SignatureLocation,
    SigningParams, SigningSettings, UriPathNormalizationMode,
};
use aws_sigv4::sign::v4;

use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use time::{Date, Month, PrimitiveDateTime, Time as Tm};

use crate::{auth::Claims, crypto, AppState};
use tracing::{debug, info, warn};

pub async fn sigv4_auth(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();

    let content_sha256 = parts
        .headers
        .get("x-amz-content-sha256")
        .and_then(|h| h.to_str().ok());

    let is_streaming = content_sha256 == Some("STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    // If it's not a streaming upload, we must buffer the body to verify its hash.
    // If it IS a streaming upload, we leave the body as-is to be handled by the downstream service.
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

    // If no AWS SigV4 auth, decide if we can allow anonymous (GET/HEAD) and defer auth to handlers
    let auth_header = match parts
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        Some(h) if h.starts_with("AWS4-HMAC-SHA256 ") => h,
        _ => {
            let method = parts.method.clone();
            if method == http::Method::GET || method == http::Method::HEAD {
                // Allow anonymous GET/HEAD; handlers will enforce bucket-level public access
                debug!("No SigV4 for GET/HEAD, deferring auth to handler");
                return next.run(req).await;
            }
            return Response::builder()
                .status(401)
                .body(Body::from("Missing Authorization"))
                .unwrap();
        }
    };

    // Parse Authorization header (credential scope, signed headers, signature)
    let parsed = match parse_auth_header(auth_header) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to parse SigV4 Authorization header");
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Invalid Authorization header: {e}")))
                .unwrap()
        }
    };

    // Look up app / secret by access key id
    let app_details = match state.db.get_app_by_client_id(&parsed.access_key_id).await {
        Ok(Some(d)) => d,
        _ => {
            warn!(access_key_id = %parsed.access_key_id, "SigV4 auth failed: Invalid access key");
            return Response::builder()
                .status(403)
                .body(Body::from("Invalid access key"))
                .unwrap()
        }
    };

    let encryption_key = hex::decode(&state.config.worka_secret_encryption_key)
        .expect("WORKA_SECRET_ENCRYPTION_KEY must be a valid hex string");
    let secret_bytes = match crypto::decrypt(&app_details.client_secret_encrypted, &encryption_key) {
        Ok(s) => s,
        Err(_) => {
            warn!(access_key_id = %parsed.access_key_id, "Failed to decrypt secret for SigV4 auth");
            return Response::builder()
                .status(500)
                .body(Body::from("Failed to decrypt secret"))
                .unwrap()
        }
    };
    let secret = match String::from_utf8(secret_bytes) {
        Ok(s) => s,
        Err(_) => {
            warn!(access_key_id = %parsed.access_key_id, "Decrypted secret is not valid UTF-8");
            return Response::builder()
                .status(500)
                .body(Body::from("Decrypted secret is not valid UTF-8"))
                .unwrap()
        }
    };

    // Identity used by the signer
    let identity: Identity =
        Credentials::new(&parsed.access_key_id, &secret, None, None, "sigv4-verify").into();

    // Use EXACT X-Amz-Date (required for matching signature)
    let signing_time = match parts
        .headers
        .get("x-amz-date")
        .and_then(|h| h.to_str().ok())
        .and_then(parse_x_amz_date)
    {
        Some(t) => t,
        None => {
            // Fallback to date in credential scope (midnight UTC) if header missing
            match parse_scope_yyyymmdd(&parsed.date) {
                Some(t) => t,
                None => {
                    warn!(access_key_id = %parsed.access_key_id, "Missing or invalid X-Amz-Date for SigV4");
                    return Response::builder()
                        .status(400)
                        .body(Body::from("Missing or invalid X-Amz-Date"))
                        .unwrap()
                }
            }
        }
    };

    // Build absolute URL (scheme://host/path?query)
    let host = parts
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    let scheme = detect_scheme(&parts.headers);
    let path_q = parts.uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let absolute_url = format!("{scheme}://{host}{path_q}");

    // S3 canonicalization settings
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::Headers;
    settings.percent_encoding_mode = PercentEncodingMode::Single; // no double-encode
    settings.uri_path_normalization_mode = UriPathNormalizationMode::Disabled; // no path normalization
    settings.payload_checksum_kind = PayloadChecksumKind::XAmzSha256; // use x-amz-content-sha256
    settings.expires_in = None;

    // Exclude Authorization (never signed). We will also filter headers to exactly SignedHeaders.
    settings.excluded_headers = Some(vec![Cow::Borrowed("authorization")]);

    // Build signing params (region & service from credential scope)
    let signing_params: SigningParams = v4::SigningParams::builder()
        .identity(&identity)
        .region(&parsed.region)
        .name(&parsed.service) // "s3"
        .time(signing_time)
        .settings(settings)
        .build()
        .expect("valid signing params")
        .into();

    let payload_hash = if is_streaming {
        "STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_string()
    } else {
        content_sha256
            .map(|s| s.to_string())
            .unwrap_or_else(|| sha256_hex(&body_bytes.unwrap()))
    };

    // Only include headers that the client actually signed (from SignedHeaders=...)
    let signed_set: HashSet<&str> = parsed
        .signed_headers
        .iter()
        .map(|s| s.as_str())
        .collect();

    let headers_iter = parts.headers.iter().filter_map(|(k, v)| {
        let name = k.as_str();
        if name.eq_ignore_ascii_case("authorization") {
            return None;
        }
        // Header names in SignedHeaders are lowercase; compare case-insensitively.
        if !signed_set.contains(&name.to_ascii_lowercase().as_str()) {
            return None;
        }
        v.to_str().ok().map(|vs| (name, vs))
    });

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
                .unwrap()
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
                .unwrap()
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
    let scopes = match state.db.get_policies_for_app(app_details.id).await {
        Ok(s) => s,
        Err(e) => {
            warn!(error = %e, access_key_id = %parsed.access_key_id, "Failed to fetch policies for app");
            return Response::builder()
                .status(500)
                .body(Body::from("Failed to fetch policies"))
                .unwrap()
        }
    };

    let claims = Claims {
        sub: parsed.access_key_id,
        tenant_id: app_details.tenant_id,
        scopes,
        exp: 0, // SigV4 has its own expiry mechanism
    };
    req.extensions_mut().insert(claims);

    next.run(req).await
}

// ----------------- helpers -----------------

struct ParsedAuth {
    access_key_id: String,
    date: String, // YYYYMMDD
    region: String,
    service: String,
    signed_headers: Vec<String>, // lowercase, in order
    signature: String,
}

// Parse: AWS4-HMAC-SHA256 Credential=AKID/DATE/REGION/SERVICE/aws4_request, SignedHeaders=..., Signature=...
fn parse_auth_header(h: &str) -> Result<ParsedAuth, &'static str> {
    let after = h.strip_prefix("AWS4-HMAC-SHA256 ").ok_or("missing prefix")?;
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

fn detect_scheme(headers: &HeaderMap) -> &'static str {
    if let Some(v) = headers
        .get("x-forwarded-proto")
        .and_then(|h| h.to_str().ok())
    {
        if v.eq_ignore_ascii_case("https") {
            return "https";
        }
    }
    "http"
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
