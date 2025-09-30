use anyhow::Result;
use std::net::IpAddr;

/// A trait for discovering the public IP address of the node.
#[async_trait::async_trait]
pub trait IpDiscovery: Send + Sync {
    async fn discover_ip(&self) -> Result<IpAddr>;
}

// --- Static Discovery --- //

pub struct StaticDiscovery {
    addr: IpAddr,
}

impl StaticDiscovery {
    pub fn new(addr: IpAddr) -> Self {
        Self { addr }
    }
}

#[async_trait::async_trait]
impl IpDiscovery for StaticDiscovery {
    async fn discover_ip(&self) -> Result<IpAddr> {
        Ok(self.addr)
    }
}

// --- Local Discovery --- //

pub struct LocalDiscovery;

#[async_trait::async_trait]
impl IpDiscovery for LocalDiscovery {
    async fn discover_ip(&self) -> Result<IpAddr> {
        local_ip_address::local_ip().map_err(Into::into)
    }
}

// --- Cloudflare Discovery --- //

pub struct CloudflareDiscovery;

impl CloudflareDiscovery {
    async fn discover_http(&self) -> Result<IpAddr> {
        let response = reqwest::get("https://www.cloudflare.com/cdn-cgi/trace").await?;
        let trace = response.text().await?;
        for line in trace.lines() {
            if let Some((key, value)) = line.split_once('=') {
                if key == "ip" {
                    return value.parse().map_err(Into::into);
                }
            }
        }
        Err(anyhow::anyhow!("Could not find IP in cloudflare trace"))
    }

    async fn discover_dns(&self) -> Result<IpAddr> {
        use trust_dns_resolver::TokioAsyncResolver;
        use trust_dns_resolver::config::*;

        let resolver =
            TokioAsyncResolver::tokio(ResolverConfig::cloudflare(), ResolverOpts::default());

        let response = resolver.txt_lookup("whoami.cloudflare.").await?;
        let ip_str = response.iter().next().unwrap().to_string();
        ip_str.parse().map_err(Into::into)
    }
}

#[async_trait::async_trait]
impl IpDiscovery for CloudflareDiscovery {
    async fn discover_ip(&self) -> Result<IpAddr> {
        // Prefer DNS, fall back to HTTP
        match self.discover_dns().await {
            Ok(ip) => Ok(ip),
            Err(_) => self.discover_http().await,
        }
    }
}

// --- Google Discovery --- //

pub struct GoogleDiscovery;

#[async_trait::async_trait]
impl IpDiscovery for GoogleDiscovery {
    async fn discover_ip(&self) -> Result<IpAddr> {
        use trust_dns_resolver::TokioAsyncResolver;
        use trust_dns_resolver::config::*;

        let resolver = TokioAsyncResolver::tokio(ResolverConfig::google(), ResolverOpts::default());

        let response = resolver.txt_lookup("o-o.myaddr.l.google.com.").await?;
        let ip_str = response.iter().next().unwrap().to_string();
        ip_str.parse().map_err(Into::into)
    }
}
