use crate::{auth::Claims, crypto, AppState};
use aws_credential_types::Credentials;
use aws_smithy_runtime_api::client::identity::Identity;
use aws_sigv4::http_request::{
    sign, SignableBody, SignableRequest, SigningParams, SigningSettings, SignatureLocation,
};
use aws_sigv4::sign::v4;
use axum::{
    body::Body,
    extract::{Request, State},
    http::{self, HeaderMap},
    middleware::Next,
    response::Response,
};
use http_body_util::BodyExt;
use std::time::SystemTime;

pub async fn sigv4_auth(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let (parts, body) = req.into_parts();
    let body_bytes = match body.collect().await {
        Ok(b) => b.to_bytes(),
        Err(e) => {
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Failed to read body: {}", e)))
                .unwrap();
        }
    };

    // Recreate the request to pass downstream whether verified or not
    let mut req = Request::from_parts(parts.clone(), Body::from(body_bytes.clone()));

    // If there’s no Authorization header, continue anonymously (your original behavior)
    let auth = match parts
        .headers
        .get(http::header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
    {
        Some(a) if a.starts_with("AWS4-HMAC-SHA256 ") => a,
        _ => return next.run(req).await,
    };

    // Parse Authorization header
    let parsed = match parse_auth_header(auth) {
        Ok(p) => p,
        Err(e) => {
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Invalid Authorization header: {}", e)))
                .unwrap()
        }
    };

    // Look up app by access key id
    let app_details = match state.db.get_app_by_client_id(&parsed.access_key_id).await {
        Ok(Some(details)) => details,
        _ => {
            return Response::builder()
                .status(403)
                .body(Body::from("Invalid access key"))
                .unwrap()
        }
    };

    // Decrypt secret
    let secret = match crypto::decrypt(&app_details.client_secret_encrypted) {
        Ok(s) => s,
        Err(_) => {
            return Response::builder()
                .status(500)
                .body(Body::from("Failed to decrypt secret"))
                .unwrap()
        }
    };
    let secret_str = match String::from_utf8(secret) {
        Ok(s) => s,
        Err(_) => {
            return Response::builder()
                .status(500)
                .body(Body::from("Decrypted secret is not valid UTF-8"))
                .unwrap()
        }
    };

    // Build the Identity (access key + secret). Session token is not required for verification.
    let identity: Identity = Credentials::new(
        &parsed.access_key_id,
        &secret_str,
        None,         // session token if you issue them
        None,         // expiry
        "sigv4-verify"
    ).into();

    // Determine the signing timestamp:
    // Use X-Amz-Date when present; otherwise fall back to the date in the Credential scope (midnight).
    let signing_time = match amz_date_to_system_time(parts.headers.get("x-amz-date").and_then(|h| h.to_str().ok()))
        .or_else(|| yyyymmdd_to_system_time(&parsed.date))
    {
        Some(t) => t,
        None => SystemTime::now(), // last resort; will likely fail if client’s clock differs
    };

    // Build a full absolute URL for canonicalization: scheme + host + path?query
    // If you’re behind a proxy/terminator, you may want to honor X-Forwarded-Proto/Host.
    let host = parts
        .headers
        .get(http::header::HOST)
        .and_then(|h| h.to_str().ok())
        .unwrap_or("localhost");
    // If you always run HTTPS in front, hardcode https. Otherwise inspect headers/env.
    let scheme = if is_https(&parts.headers) { "https" } else { "http" };
    let path_q = parts.uri.path_and_query().map(|pq| pq.as_str()).unwrap_or("/");
    let absolute_url = format!("{scheme}://{host}{path_q}");

    // Convert incoming request into a SignableRequest with the SAME headers and body the client signed
    let headers_iter = parts.headers.iter().filter_map(|(k, v)| {
        v.to_str().ok().map(|vs| (k.as_str(), vs))
    });

    let signable_req = match SignableRequest::new(
        parts.method.as_str(),
        &absolute_url,
        headers_iter,
        SignableBody::Bytes(&body_bytes),
    ) {
        Ok(s) => s,
        Err(e) => {
            return Response::builder()
                .status(400)
                .body(Body::from(format!("Bad request for signing: {e}")))
                .unwrap()
        }
    };

    // Settings: put signature in headers (that’s what we’re verifying) and don’t add/alter headers.
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::Headers;

    // Build SigningParams using the values parsed out of the header scope
    let signing_params: SigningParams = v4::SigningParams::builder()
        .identity(&identity)
        .region(&parsed.region)
        .name(&parsed.service)
        .time(signing_time)
        .settings(settings)
        .build()
        .expect("valid signing params")
        .into();

    // Compute signature for the incoming request
    let output = match sign(signable_req, &signing_params) {
        Ok(o) => o,
        Err(_) => {
            return Response::builder()
                .status(403)
                .body(Body::from("Signature verification failed"))
                .unwrap()
        }
    };

    let (_instr, computed_sig) = output.into_parts();

    if !constant_time_eq(&computed_sig.as_str(), &parsed.signature) {
        return Response::builder()
            .status(403)
            .body(Body::from("Signature verification failed"))
            .unwrap();
    }

    // Authorized — fetch scopes, attach claims, and continue
    let scopes = match state.db.get_policies_for_app(app_details.id).await {
        Ok(s) => s,
        Err(_) => {
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
        exp: 0, // Not used here
    };
    req.extensions_mut().insert(claims);

    next.run(req).await
}

// --- helpers ---

struct ParsedAuth {
    access_key_id: String,
    date: String,    // yyyymmdd from Credential scope
    region: String,
    service: String,
    signature: String,
}

// Minimal parser for: `AWS4-HMAC-SHA256 Credential=AKID/DATE/REGION/SERVICE/aws4_request, SignedHeaders=..., Signature=...`
fn parse_auth_header(h: &str) -> Result<ParsedAuth, &'static str> {
    let after = h.strip_prefix("AWS4-HMAC-SHA256 ").ok_or("bad prefix")?;
    let mut credential = None;
    let mut signature = None;
    for part in after.split(',') {
        let part = part.trim();
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = Some(v);
        } else if let Some(v) = part.strip_prefix("Signature=") {
            signature = Some(v);
        }
    }
    let cred = credential.ok_or("missing Credential")?;
    let sig = signature.ok_or("missing Signature")?.to_string();
    let mut pieces = cred.split('/');
    let access_key_id = pieces.next().ok_or("bad Credential")?.to_string();
    let date = pieces.next().ok_or("bad Credential date")?.to_string();
    let region = pieces.next().ok_or("bad Credential region")?.to_string();
    let service = pieces.next().ok_or("bad Credential service")?.to_string();
    // ignore trailing aws4_request
    Ok(ParsedAuth { access_key_id, date, region, service, signature: sig })
}

// Parse `20250101T120304Z` to SystemTime
fn amz_date_to_system_time(s: Option<&str>) -> Option<SystemTime> {
    let s = s?;
    chrono::DateTime::parse_from_str(s, "%Y%m%dT%H%M%SZ")
        .ok()
        .map(|dt| dt.into())
}

// Fallback: turn YYYYMMDD into midnight UTC `SystemTime`
fn yyyymmdd_to_system_time(s: &str) -> Option<SystemTime> {
    chrono::NaiveDate::parse_from_str(s, "%Y%m%d")
        .ok()
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .map(|dt| dt.and_utc().into())
}

// Observe X-Forwarded-Proto if present
fn is_https(headers: &HeaderMap) -> bool {
    if let Some(v) = headers.get("x-forwarded-proto").and_then(|h| h.to_str().ok()) {
        if v.eq_ignore_ascii_case("https") { return true; }
    }
    false
}

// Constant-time comparison for signatures
fn constant_time_eq(a: &str, b: &str) -> bool {
    use subtle::ConstantTimeEq;
    if a.len() != b.len() { return false; }
    a.as_bytes().ct_eq(b.as_bytes()).into()
}