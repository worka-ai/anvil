use crate::validation;
use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, Ipv6Addr},
    str::FromStr,
};

pub const HOST_ALIAS_DESCRIPTOR_SCHEMA: &str = "anvil.mesh.host_alias.v1";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CrossRegionRoutingPolicy {
    RedirectPreferred,
    ProxyPreferred,
    ProxyRequired,
    LocalOnly,
}

impl CrossRegionRoutingPolicy {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RedirectPreferred => "redirect_preferred",
            Self::ProxyPreferred => "proxy_preferred",
            Self::ProxyRequired => "proxy_required",
            Self::LocalOnly => "local_only",
        }
    }
}

impl Default for CrossRegionRoutingPolicy {
    fn default() -> Self {
        Self::RedirectPreferred
    }
}

impl fmt::Display for CrossRegionRoutingPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CrossRegionRoutingPolicy {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim() {
            "redirect_preferred" => Ok(Self::RedirectPreferred),
            "proxy_preferred" => Ok(Self::ProxyPreferred),
            "proxy_required" => Ok(Self::ProxyRequired),
            "local_only" => Ok(Self::LocalOnly),
            other => Err(format!(
                "invalid cross-region routing policy {other:?}; expected redirect_preferred, proxy_preferred, proxy_required, or local_only"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemoteBucketRoutingAction {
    Redirect,
    Proxy,
    RejectLocalOnly,
    ProxyUnavailable,
}

pub fn remote_bucket_routing_action(
    policy: CrossRegionRoutingPolicy,
    proxy_available: bool,
) -> RemoteBucketRoutingAction {
    match policy {
        CrossRegionRoutingPolicy::RedirectPreferred => RemoteBucketRoutingAction::Redirect,
        CrossRegionRoutingPolicy::ProxyPreferred if proxy_available => {
            RemoteBucketRoutingAction::Proxy
        }
        CrossRegionRoutingPolicy::ProxyPreferred => RemoteBucketRoutingAction::Redirect,
        CrossRegionRoutingPolicy::ProxyRequired if proxy_available => {
            RemoteBucketRoutingAction::Proxy
        }
        CrossRegionRoutingPolicy::ProxyRequired => RemoteBucketRoutingAction::ProxyUnavailable,
        CrossRegionRoutingPolicy::LocalOnly => RemoteBucketRoutingAction::RejectLocalOnly,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingConfig {
    pub base_domain: String,
}

impl RoutingConfig {
    pub fn new(base_domain: impl Into<String>) -> Result<Self, RoutingError> {
        let base_domain = normalize_host(&base_domain.into())?;
        if base_domain.is_empty() {
            return Err(RoutingError::InvalidHost);
        }
        Ok(Self { base_domain })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HostAliasDescriptor {
    pub schema: String,
    pub hostname: String,
    pub tenant_id: String,
    pub bucket_name: String,
    pub region: String,
    pub prefix: String,
    pub state: HostAliasState,
    pub created_at: String,
    pub updated_at: String,
    pub generation: u64,
}

impl HostAliasDescriptor {
    pub fn active(
        hostname: impl Into<String>,
        tenant_id: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
        prefix: impl Into<String>,
        config: &RoutingConfig,
    ) -> Result<Self, RoutingError> {
        let hostname = normalize_alias_hostname(&hostname.into())?;
        if native_host_kind(&hostname, &config.base_domain).is_some() {
            return Err(RoutingError::NativeHostAliasOverlap);
        }
        let now = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);
        Ok(Self {
            schema: HOST_ALIAS_DESCRIPTOR_SCHEMA.to_string(),
            hostname,
            tenant_id: tenant_id.into(),
            bucket_name: bucket_name.into(),
            region: region.into(),
            prefix: prefix.into(),
            state: HostAliasState::Active,
            created_at: now.clone(),
            updated_at: now,
            generation: 1,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HostAliasState {
    PendingVerification,
    Active,
    Suspended,
    Deleted,
}

pub fn normalize_alias_hostname(hostname: &str) -> Result<String, RoutingError> {
    normalize_host(hostname)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteRequest<'a> {
    pub host: &'a str,
    pub path: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectRoute {
    pub tenant: String,
    pub bucket: String,
    pub region: String,
    pub key: String,
    pub source: RouteSource,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteSource {
    PathStyle,
    VirtualHost,
    HostAlias { hostname: String, prefix: String },
}

#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
pub enum RoutingError {
    #[error("invalid host")]
    InvalidHost,
    #[error("invalid native Anvil host")]
    InvalidNativeHost,
    #[error("host alias overlaps native Anvil hostname")]
    NativeHostAliasOverlap,
    #[error("unknown host")]
    UnknownHost,
    #[error("invalid path")]
    InvalidPath,
    #[error("invalid percent encoding")]
    InvalidPercentEncoding,
    #[error("path traversal is not allowed")]
    PathTraversal,
    #[error("ambiguous forwarded host chain")]
    AmbiguousForwardedHost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NativeHostKind {
    PathStyle,
    VirtualHost,
}

pub fn parse_object_route(
    request: RouteRequest<'_>,
    config: &RoutingConfig,
    aliases: &[HostAliasDescriptor],
) -> Result<ObjectRoute, RoutingError> {
    let host = normalize_host(request.host)?;

    if let Some(route) = parse_native_route(&host, request.path, config)? {
        return Ok(route);
    }

    let alias = aliases
        .iter()
        .find(|alias| alias.state == HostAliasState::Active && alias.hostname == host)
        .ok_or(RoutingError::UnknownHost)?;
    let key = join_alias_prefix(&alias.prefix, request.path)?;
    Ok(ObjectRoute {
        tenant: alias.tenant_id.clone(),
        bucket: alias.bucket_name.clone(),
        region: alias.region.clone(),
        key,
        source: RouteSource::HostAlias {
            hostname: alias.hostname.clone(),
            prefix: alias.prefix.clone(),
        },
    })
}

pub fn join_alias_prefix(prefix: &str, request_path: &str) -> Result<String, RoutingError> {
    let prefix = prefix.trim_matches('/');
    let request_path = request_path.trim_start_matches('/');
    let raw_joined = match (prefix.is_empty(), request_path.is_empty()) {
        (true, true) => String::new(),
        (true, false) => request_path.to_string(),
        (false, true) => prefix.to_string(),
        (false, false) => format!("{prefix}/{request_path}"),
    };
    decode_and_validate_route_key(&raw_joined)
}

fn parse_native_route(
    host: &str,
    path: &str,
    config: &RoutingConfig,
) -> Result<Option<ObjectRoute>, RoutingError> {
    let Some(stripped) = host.strip_suffix(&format!(".{}", config.base_domain)) else {
        return Ok(None);
    };
    if stripped.is_empty() {
        return Err(RoutingError::InvalidNativeHost);
    }

    let labels = stripped.split('.').collect::<Vec<_>>();
    match labels.as_slice() {
        [region] => {
            validate_label(region)?;
            let (tenant, bucket, key) = parse_path_style_path(path)?;
            Ok(Some(ObjectRoute {
                tenant,
                bucket,
                region: (*region).to_string(),
                key,
                source: RouteSource::PathStyle,
            }))
        }
        [bucket, tenant, region] => {
            validate_label(bucket)?;
            validate_label(tenant)?;
            validate_label(region)?;
            let key = decode_path_key(path)?;
            Ok(Some(ObjectRoute {
                tenant: (*tenant).to_string(),
                bucket: (*bucket).to_string(),
                region: (*region).to_string(),
                key,
                source: RouteSource::VirtualHost,
            }))
        }
        _ => Err(RoutingError::InvalidNativeHost),
    }
}

fn native_host_kind(host: &str, base_domain: &str) -> Option<NativeHostKind> {
    let stripped = host.strip_suffix(&format!(".{base_domain}"))?;
    if stripped.is_empty() {
        return None;
    }
    match stripped.split('.').count() {
        1 => Some(NativeHostKind::PathStyle),
        3 => Some(NativeHostKind::VirtualHost),
        _ => None,
    }
}

fn parse_path_style_path(path: &str) -> Result<(String, String, String), RoutingError> {
    let path = path.trim_start_matches('/');
    let mut parts = path.splitn(3, '/');
    let tenant = parts.next().filter(|value| !value.is_empty());
    let bucket = parts.next().filter(|value| !value.is_empty());
    let key = parts.next().unwrap_or_default();
    let (Some(tenant), Some(bucket)) = (tenant, bucket) else {
        return Err(RoutingError::InvalidPath);
    };
    let tenant = percent_decode_utf8(tenant)?;
    let bucket = percent_decode_utf8(bucket)?;
    if tenant.contains('/') || bucket.contains('/') || tenant.contains('.') || bucket.contains('.')
    {
        return Err(RoutingError::InvalidPath);
    }
    let key = decode_and_validate_route_key(key)?;
    Ok((tenant, bucket, key))
}

fn decode_path_key(path: &str) -> Result<String, RoutingError> {
    decode_and_validate_route_key(path.trim_start_matches('/'))
}

fn decode_and_validate_route_key(raw: &str) -> Result<String, RoutingError> {
    let decoded = percent_decode_utf8(raw)?;
    if decoded.is_empty() {
        return Ok(decoded);
    }
    if !validation::is_valid_object_key(&decoded) {
        if decoded.starts_with('/') || decoded.split('/').any(|seg| seg == "." || seg == "..") {
            return Err(RoutingError::PathTraversal);
        }
        return Err(RoutingError::InvalidPath);
    }
    Ok(decoded)
}

fn percent_decode_utf8(input: &str) -> Result<String, RoutingError> {
    let bytes = input.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(RoutingError::InvalidPercentEncoding);
            }
            let hi = hex_value(bytes[index + 1]).ok_or(RoutingError::InvalidPercentEncoding)?;
            let lo = hex_value(bytes[index + 2]).ok_or(RoutingError::InvalidPercentEncoding)?;
            output.push((hi << 4) | lo);
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| RoutingError::InvalidPercentEncoding)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

pub fn normalize_host(host: &str) -> Result<String, RoutingError> {
    let host = strip_host_port(host.trim()).trim_end_matches('.');
    if host.is_empty() || host.len() > 253 {
        return Err(RoutingError::InvalidHost);
    }
    let ascii = idna::domain_to_ascii(host).map_err(|_| RoutingError::InvalidHost)?;
    let ascii = ascii.to_ascii_lowercase();
    if ascii.len() > 253 || ascii.split('.').any(|label| validate_label(label).is_err()) {
        return Err(RoutingError::InvalidHost);
    }
    Ok(ascii)
}

fn strip_host_port(host: &str) -> &str {
    if let Some(bracketed) = host.strip_prefix('[') {
        if let Some(end) = bracketed.find(']') {
            return &bracketed[..end];
        }
    }
    if host.matches(':').count() == 1 {
        return host.split_once(':').map(|(host, _)| host).unwrap_or(host);
    }
    host
}

fn validate_label(label: &str) -> Result<(), RoutingError> {
    if label.is_empty() || label.len() > 63 {
        return Err(RoutingError::InvalidHost);
    }
    let bytes = label.as_bytes();
    if !bytes[0].is_ascii_lowercase() && !bytes[0].is_ascii_digit() {
        return Err(RoutingError::InvalidHost);
    }
    if !bytes[bytes.len() - 1].is_ascii_lowercase() && !bytes[bytes.len() - 1].is_ascii_digit() {
        return Err(RoutingError::InvalidHost);
    }
    if !bytes
        .iter()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
    {
        return Err(RoutingError::InvalidHost);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForwardedHeaders {
    pub forwarded: Vec<String>,
    pub x_forwarded_host: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TrustedProxy {
    Exact(IpAddr),
    Cidr { network: IpAddr, prefix: u8 },
}

impl TrustedProxy {
    pub fn parse(value: &str) -> Result<Self, RoutingError> {
        let value = value.trim();
        if value.is_empty() {
            return Err(RoutingError::InvalidHost);
        }
        if let Some((addr, prefix)) = value.split_once('/') {
            let network = addr.parse().map_err(|_| RoutingError::InvalidHost)?;
            let prefix: u8 = prefix.parse().map_err(|_| RoutingError::InvalidHost)?;
            match network {
                IpAddr::V4(_) if prefix > 32 => return Err(RoutingError::InvalidHost),
                IpAddr::V6(_) if prefix > 128 => return Err(RoutingError::InvalidHost),
                _ => {}
            }
            return Ok(Self::Cidr { network, prefix });
        }
        Ok(Self::Exact(
            value.parse().map_err(|_| RoutingError::InvalidHost)?,
        ))
    }

    pub fn contains(&self, peer: IpAddr) -> bool {
        match self {
            Self::Exact(addr) => *addr == peer,
            Self::Cidr { network, prefix } => cidr_contains(*network, *prefix, peer),
        }
    }
}

pub fn parse_trusted_proxies(values: &[String]) -> Result<Vec<TrustedProxy>, RoutingError> {
    values
        .iter()
        .filter(|value| !value.trim().is_empty())
        .map(|value| TrustedProxy::parse(value))
        .collect()
}

pub fn effective_host(
    raw_authority: &str,
    remote_peer: IpAddr,
    trusted_proxies: &[TrustedProxy],
    headers: &ForwardedHeaders,
) -> Result<String, RoutingError> {
    let raw = normalize_host(raw_authority)?;
    if !trusted_proxies
        .iter()
        .any(|proxy| proxy.contains(remote_peer))
    {
        return Ok(raw);
    }

    let candidates = forwarded_host_candidates(headers)?;
    match candidates.as_slice() {
        [] => Ok(raw),
        [host] => normalize_host(host),
        _ => Err(RoutingError::AmbiguousForwardedHost),
    }
}

pub fn effective_host_authority(
    raw_authority: &str,
    remote_peer: IpAddr,
    trusted_proxies: &[TrustedProxy],
    headers: &ForwardedHeaders,
) -> Result<String, RoutingError> {
    if !trusted_proxies
        .iter()
        .any(|proxy| proxy.contains(remote_peer))
    {
        return Ok(raw_authority.to_string());
    }

    let candidates = forwarded_host_candidates(headers)?;
    match candidates.as_slice() {
        [] => Ok(raw_authority.to_string()),
        [host] => {
            normalize_host(host)?;
            Ok(host.clone())
        }
        _ => Err(RoutingError::AmbiguousForwardedHost),
    }
}

fn forwarded_host_candidates(headers: &ForwardedHeaders) -> Result<Vec<String>, RoutingError> {
    let mut candidates = Vec::new();
    for header in &headers.forwarded {
        for element in header.split(',') {
            for pair in element.split(';') {
                let Some((name, value)) = pair.trim().split_once('=') else {
                    continue;
                };
                if name.eq_ignore_ascii_case("host") {
                    candidates.push(value.trim_matches('"').to_string());
                }
            }
        }
    }
    for header in &headers.x_forwarded_host {
        candidates.extend(header.split(',').map(|host| host.trim().to_string()));
    }
    candidates.retain(|host| !host.is_empty());
    if candidates.len() > 1 {
        let first = normalize_host(&candidates[0])?;
        let all_same = candidates
            .iter()
            .map(|candidate| normalize_host(candidate))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .all(|candidate| candidate == first);
        if all_same {
            return Ok(vec![candidates[0].clone()]);
        }
    }
    Ok(candidates)
}

fn cidr_contains(network: IpAddr, prefix: u8, peer: IpAddr) -> bool {
    match (network, peer) {
        (IpAddr::V4(network), IpAddr::V4(peer)) if prefix <= 32 => {
            let mask = if prefix == 0 {
                0
            } else {
                u32::MAX << (32 - prefix)
            };
            (u32::from(network) & mask) == (u32::from(peer) & mask)
        }
        (IpAddr::V6(network), IpAddr::V6(peer)) if prefix <= 128 => {
            let mask = if prefix == 0 {
                0
            } else {
                u128::MAX << (128 - prefix)
            };
            (u128::from(network) & mask) == (u128::from(peer) & mask)
        }
        _ => false,
    }
}

#[allow(dead_code)]
fn _assert_ip_conversions(_: Ipv4Addr, _: Ipv6Addr) {}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> RoutingConfig {
        RoutingConfig::new("anvil-storage.com").unwrap()
    }

    #[test]
    fn parses_cross_region_routing_policies_deterministically() {
        assert_eq!(
            "redirect_preferred"
                .parse::<CrossRegionRoutingPolicy>()
                .unwrap(),
            CrossRegionRoutingPolicy::RedirectPreferred
        );
        assert_eq!(
            "proxy_preferred"
                .parse::<CrossRegionRoutingPolicy>()
                .unwrap(),
            CrossRegionRoutingPolicy::ProxyPreferred
        );
        assert_eq!(
            "proxy_required"
                .parse::<CrossRegionRoutingPolicy>()
                .unwrap(),
            CrossRegionRoutingPolicy::ProxyRequired
        );
        assert_eq!(
            "local_only".parse::<CrossRegionRoutingPolicy>().unwrap(),
            CrossRegionRoutingPolicy::LocalOnly
        );
        assert!("redirect-only".parse::<CrossRegionRoutingPolicy>().is_err());
    }

    #[test]
    fn chooses_remote_bucket_action_from_policy_and_proxy_availability() {
        assert_eq!(
            remote_bucket_routing_action(CrossRegionRoutingPolicy::LocalOnly, false),
            RemoteBucketRoutingAction::RejectLocalOnly
        );
        assert_eq!(
            remote_bucket_routing_action(CrossRegionRoutingPolicy::RedirectPreferred, false),
            RemoteBucketRoutingAction::Redirect
        );
        assert_eq!(
            remote_bucket_routing_action(CrossRegionRoutingPolicy::ProxyPreferred, false),
            RemoteBucketRoutingAction::Redirect
        );
        assert_eq!(
            remote_bucket_routing_action(CrossRegionRoutingPolicy::ProxyPreferred, true),
            RemoteBucketRoutingAction::Proxy
        );
        assert_eq!(
            remote_bucket_routing_action(CrossRegionRoutingPolicy::ProxyRequired, false),
            RemoteBucketRoutingAction::ProxyUnavailable
        );
    }

    #[test]
    fn parses_path_style_regional_url() {
        let route = parse_object_route(
            RouteRequest {
                host: "eu-west-1.anvil-storage.com",
                path: "/acme/releases/my-app-v3.exe",
            },
            &config(),
            &[],
        )
        .unwrap();

        assert_eq!(route.tenant, "acme");
        assert_eq!(route.bucket, "releases");
        assert_eq!(route.region, "eu-west-1");
        assert_eq!(route.key, "my-app-v3.exe");
        assert_eq!(route.source, RouteSource::PathStyle);
    }

    #[test]
    fn parses_virtual_host_regional_url() {
        let route = parse_object_route(
            RouteRequest {
                host: "Releases.Acme.EU-West-1.Anvil-Storage.Com.",
                path: "/latest.exe",
            },
            &config(),
            &[],
        )
        .unwrap();

        assert_eq!(route.tenant, "acme");
        assert_eq!(route.bucket, "releases");
        assert_eq!(route.region, "eu-west-1");
        assert_eq!(route.key, "latest.exe");
        assert_eq!(route.source, RouteSource::VirtualHost);
    }

    #[test]
    fn parses_bucket_level_regional_routes_with_empty_key() {
        let path_style = parse_object_route(
            RouteRequest {
                host: "eu-west-1.anvil-storage.com",
                path: "/acme/releases",
            },
            &config(),
            &[],
        )
        .unwrap();
        assert_eq!(path_style.bucket, "releases");
        assert_eq!(path_style.key, "");
        assert_eq!(path_style.source, RouteSource::PathStyle);

        let virtual_host = parse_object_route(
            RouteRequest {
                host: "releases.acme.eu-west-1.anvil-storage.com",
                path: "/",
            },
            &config(),
            &[],
        )
        .unwrap();
        assert_eq!(virtual_host.bucket, "releases");
        assert_eq!(virtual_host.key, "");
        assert_eq!(virtual_host.source, RouteSource::VirtualHost);
    }

    #[test]
    fn rejects_dotted_tenant_or_bucket_virtual_host_form() {
        let err = parse_object_route(
            RouteRequest {
                host: "release.assets.acme.eu-west-1.anvil-storage.com",
                path: "/latest.exe",
            },
            &config(),
            &[],
        )
        .unwrap_err();

        assert_eq!(err, RoutingError::InvalidNativeHost);
    }

    #[test]
    fn custom_alias_joins_prefix_with_one_slash() {
        let config = config();
        let alias = HostAliasDescriptor::active(
            "cdn.customer-domain.com",
            "tenant_acme",
            "releases",
            "eu-west-1",
            "/public/",
            &config,
        )
        .unwrap();

        let route = parse_object_route(
            RouteRequest {
                host: "cdn.customer-domain.com:443",
                path: "/latest.exe",
            },
            &config,
            &[alias],
        )
        .unwrap();

        assert_eq!(route.tenant, "tenant_acme");
        assert_eq!(route.bucket, "releases");
        assert_eq!(route.key, "public/latest.exe");
        assert_eq!(
            route.source,
            RouteSource::HostAlias {
                hostname: "cdn.customer-domain.com".to_string(),
                prefix: "/public/".to_string(),
            }
        );
    }

    #[test]
    fn custom_alias_prefix_joining_rejects_decoded_traversal() {
        let err = join_alias_prefix("public/", "/safe/%2e%2e/secret.txt").unwrap_err();
        assert_eq!(err, RoutingError::PathTraversal);
    }

    #[test]
    fn native_anvil_hosts_cannot_be_aliases() {
        let err = HostAliasDescriptor::active(
            "releases.acme.eu-west-1.anvil-storage.com",
            "tenant_acme",
            "releases",
            "eu-west-1",
            "public/",
            &config(),
        )
        .unwrap_err();

        assert_eq!(err, RoutingError::NativeHostAliasOverlap);
    }

    #[test]
    fn trusted_forwarded_host_wins_only_for_trusted_peer() {
        let trusted = [TrustedProxy::parse("10.0.0.0/24").unwrap()];
        let headers = ForwardedHeaders {
            forwarded: Vec::new(),
            x_forwarded_host: vec!["cdn.customer-domain.com".to_string()],
        };

        let trusted_host = effective_host(
            "internal.anvil-storage.com",
            "10.0.0.9".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap();
        assert_eq!(trusted_host, "cdn.customer-domain.com");

        let untrusted_host = effective_host(
            "raw.example.com",
            "203.0.113.10".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap();
        assert_eq!(untrusted_host, "raw.example.com");
    }

    #[test]
    fn trusted_forwarded_header_host_is_supported() {
        let trusted = [TrustedProxy::parse("10.0.0.0/24").unwrap()];
        let headers = ForwardedHeaders {
            forwarded: vec![
                r#"for=198.51.100.10;proto=https;host="CDN.Customer-Domain.Com:443""#.to_string(),
            ],
            x_forwarded_host: Vec::new(),
        };

        let trusted_host = effective_host(
            "internal.anvil-storage.com",
            "10.0.0.9".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap();
        assert_eq!(trusted_host, "cdn.customer-domain.com");
    }

    #[test]
    fn effective_host_authority_preserves_selected_authority_for_signature_checks() {
        let trusted = [TrustedProxy::parse("10.0.0.0/24").unwrap()];
        let headers = ForwardedHeaders {
            forwarded: Vec::new(),
            x_forwarded_host: vec!["cdn.customer-domain.com:443".to_string()],
        };

        let trusted_host = effective_host_authority(
            "internal.anvil-storage.com:50051",
            "10.0.0.9".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap();
        assert_eq!(trusted_host, "cdn.customer-domain.com:443");

        let untrusted_host = effective_host_authority(
            "raw.example.com:50051",
            "203.0.113.10".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap();
        assert_eq!(untrusted_host, "raw.example.com:50051");
    }

    #[test]
    fn ambiguous_forwarded_hosts_are_rejected() {
        let trusted = [TrustedProxy::parse("10.0.0.0/24").unwrap()];
        let headers = ForwardedHeaders {
            forwarded: Vec::new(),
            x_forwarded_host: vec!["one.example.com, two.example.com".to_string()],
        };

        let err = effective_host(
            "raw.example.com",
            "10.0.0.9".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap_err();
        assert_eq!(err, RoutingError::AmbiguousForwardedHost);
    }

    #[test]
    fn ambiguous_forwarded_header_chain_is_rejected() {
        let trusted = [TrustedProxy::parse("10.0.0.0/24").unwrap()];
        let headers = ForwardedHeaders {
            forwarded: vec![
                "for=198.51.100.10;host=one.example.com, for=10.0.0.9;host=two.example.com"
                    .to_string(),
            ],
            x_forwarded_host: Vec::new(),
        };

        let err = effective_host(
            "raw.example.com",
            "10.0.0.9".parse().unwrap(),
            &trusted,
            &headers,
        )
        .unwrap_err();
        assert_eq!(err, RoutingError::AmbiguousForwardedHost);
    }

    #[test]
    fn trusted_proxy_ranges_parse_exact_and_cidr_values() {
        let proxies = parse_trusted_proxies(&[
            "127.0.0.1".to_string(),
            " 10.0.0.0/24 ".to_string(),
            String::new(),
        ])
        .unwrap();

        assert_eq!(proxies.len(), 2);
        assert!(
            proxies
                .iter()
                .any(|proxy| proxy.contains("127.0.0.1".parse().unwrap()))
        );
        assert!(
            proxies
                .iter()
                .any(|proxy| proxy.contains("10.0.0.7".parse().unwrap()))
        );
        assert!(
            !proxies
                .iter()
                .any(|proxy| proxy.contains("10.0.1.7".parse().unwrap()))
        );
    }
}
